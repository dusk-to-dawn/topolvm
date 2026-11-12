package lvmd

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/topolvm/topolvm/internal/lvmd/testutils/fakelvm"
	"github.com/topolvm/topolvm/pkg/lvmd/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func fakeLVNames(t *testing.T, f *fakeLVMD) map[string]*fakelvm.LV {
	t.Helper()
	names := map[string]*fakelvm.LV{}
	for _, vg := range f.lvm.State(t).VGs {
		for _, lv := range vg.LVs {
			names[lv.Name] = lv
		}
	}
	return names
}

// TestCreateLVSnapshotCleansUpAfterFailure checks that a snapshot or clone
// that fails after lvcreate -s leaves no LV behind, so that a retry starts from
// scratch and succeeds.
func TestCreateLVSnapshotCleansUpAfterFailure(t *testing.T) {
	for _, tc := range []struct {
		name       string
		failing    string
		accessType string
		size       uint64
	}{
		// A clone larger than its source is extended with lvresize.
		{"clone whose resize fails", "lvresize", "rw", 20 * fakeGiB},
		// A snapshot is made read-only with lvchange.
		{"snapshot whose lvchange fails", "lvchange", "ro", 10 * fakeGiB},
		// A clone is activated with lvchange.
		{"clone whose lvchange fails", "lvchange", "rw", 10 * fakeGiB},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testContext(t)
			f := startFakeLVMD(t, fakelvm.Options{})
			req := &proto.CreateLVSnapshotRequest{
				Name: "snap", DeviceClass: fakeThinDC, SourceVolume: "existing-thin",
				SizeBytes: int64(tc.size), AccessType: tc.accessType,
			}

			f.lvm.Fail(t, tc.failing)
			if _, err := f.lv.CreateLVSnapshot(ctx, req); status.Code(err) != codes.Internal {
				t.Fatalf("got %v, want an Internal error", err)
			}
			if _, ok := fakeLVNames(t, f)["snap"]; ok {
				t.Fatal("the incomplete snapshot LV was left behind")
			}

			f.lvm.Fail(t, "")
			res, err := f.lv.CreateLVSnapshot(ctx, req)
			if err != nil {
				t.Fatalf("retry failed: %v", err)
			}
			if res.GetSnapshot().GetSizeBytes() != int64(tc.size) {
				t.Errorf("retry made a snapshot of %d bytes, want %d", res.GetSnapshot().GetSizeBytes(), tc.size)
			}
		})
	}
}

// TestCreateLVSnapshotKeepsAnExistingVolume checks that a failed lvcreate -s
// for a name that exists does not remove that volume.
func TestCreateLVSnapshotKeepsAnExistingVolume(t *testing.T) {
	ctx := testContext(t)
	f := startFakeLVMD(t, fakelvm.Options{})
	_, err := f.lv.CreateLVSnapshot(ctx, &proto.CreateLVSnapshotRequest{
		Name: "existing-thick", DeviceClass: fakeThinDC, SourceVolume: "existing-thin", AccessType: "ro",
	})
	if err == nil {
		t.Fatal("a snapshot with the name of an existing volume was created")
	}
	if _, ok := fakeLVNames(t, f)["existing-thick"]; !ok {
		t.Error("the existing volume was removed")
	}
}

// TestConcurrentSnapshotsAndClones runs snapshots and clones of one source at
// once with 8 workers, more than the pool can hold: one must be refused, the
// others must be complete, and the pool must stay within its ratio.
func TestConcurrentSnapshotsAndClones(t *testing.T) {
	ctx := testContext(t)
	f := startFakeLVMD(t, fakelvm.Options{Delay: 50 * time.Millisecond, WriteDelay: 120 * time.Millisecond, LockedDelay: 70 * time.Millisecond})

	// 70 GiB are left in the pool; these ask for 80 GiB.
	reqs := []*proto.CreateLVSnapshotRequest{
		{Name: "snap1", AccessType: "ro", SizeBytes: int64(10 * fakeGiB)},
		{Name: "snap2", AccessType: "ro", SizeBytes: int64(10 * fakeGiB)},
		{Name: "clone1", AccessType: "rw", SizeBytes: int64(10 * fakeGiB)},
		{Name: "clone2", AccessType: "rw", SizeBytes: int64(10 * fakeGiB)},
		{Name: "clone3", AccessType: "rw", SizeBytes: int64(10 * fakeGiB)},
		{Name: "clone4", AccessType: "rw", SizeBytes: int64(10 * fakeGiB)},
		{Name: "bigclone", AccessType: "rw", SizeBytes: int64(20 * fakeGiB)},
	}
	var wg sync.WaitGroup
	results := make([]error, len(reqs))
	sem := make(chan struct{}, 8)
	for i, req := range reqs {
		req.DeviceClass = fakeThinDC
		req.SourceVolume = "existing-thin"
		wg.Go(func() {
			sem <- struct{}{}
			defer func() { <-sem }()
			_, results[i] = f.lv.CreateLVSnapshot(ctx, req)
		})
		time.Sleep(30 * time.Millisecond)
	}
	wg.Wait()

	names := fakeLVNames(t, f)
	refused := 0
	for i, err := range results {
		lv, exists := names[reqs[i].Name]
		switch status.Code(err) {
		case codes.OK:
			if !exists || lv.Size != uint64(reqs[i].SizeBytes) || lv.Origin != "existing-thin" {
				t.Errorf("%s: reported created, but the LV is %+v", reqs[i].Name, lv)
			}
		case codes.ResourceExhausted:
			refused++
			if exists {
				t.Errorf("%s: refused, but its LV exists", reqs[i].Name)
			}
		default:
			t.Errorf("%s: %v", reqs[i].Name, err)
		}
	}
	if refused != 1 {
		t.Errorf("%d refused, want 1", refused)
	}
	var virtual uint64
	for _, lv := range names {
		if lv.Pool == fakePoolName {
			virtual += lv.Size
		}
	}
	if virtual > 80*fakeGiB {
		t.Errorf("thin volumes total %d GiB, more than the 80 GiB the overprovision ratio allows", virtual/fakeGiB)
	}
}

// TestCreateLVSnapshotCleansUpAfterCancellation cancels a clone while each of
// its LVM steps runs. As with lvmd's nsenter prefix, cancelling stops only
// the wrapper: the lvm command itself completes. No LV may be left behind,
// and a retry must succeed.
func TestCreateLVSnapshotCleansUpAfterCancellation(t *testing.T) {
	for _, step := range []string{"lvcreate", "lvs", "lvresize", "lvchange"} {
		t.Run("cancelled during "+step, func(t *testing.T) {
			f := startFakeLVMD(t, fakelvm.Options{LikeNsenter: true})
			// A clone larger than its source runs all four steps.
			req := &proto.CreateLVSnapshotRequest{
				Name: "clone", DeviceClass: fakeThinDC, SourceVolume: "existing-thin",
				SizeBytes: int64(20 * fakeGiB), AccessType: "rw",
			}

			f.lvm.Pause(t, step)
			ctx, cancel := context.WithCancel(testContext(t))
			done := make(chan error, 1)
			go func() {
				_, err := f.lv.CreateLVSnapshot(ctx, req)
				done <- err
			}()
			f.lvm.WaitPaused(t)
			cancel()
			f.lvm.Resume(t)
			if err := <-done; err == nil {
				t.Fatal("the cancelled call succeeded")
			}
			if lv, ok := fakeLVNames(t, f)["clone"]; ok {
				t.Fatalf("the incomplete clone was left behind: %+v", lv)
			}

			f.lvm.Pause(t, "")
			res, err := f.lv.CreateLVSnapshot(testContext(t), req)
			if err != nil {
				t.Fatalf("retry failed: %v", err)
			}
			if res.GetSnapshot().GetSizeBytes() != int64(20*fakeGiB) {
				t.Errorf("retry made a clone of %d bytes", res.GetSnapshot().GetSizeBytes())
			}
		})
	}
}

// TestCreateLVSnapshotOfGrowingSource grows the source between the report and
// lvcreate -s. The snapshot then comes out larger than requested and cannot
// be shrunk: it must be removed, and a retry with the new size must succeed.
func TestCreateLVSnapshotOfGrowingSource(t *testing.T) {
	ctx := testContext(t)
	f := startFakeLVMD(t, fakelvm.Options{})
	req := &proto.CreateLVSnapshotRequest{
		Name: "snap", DeviceClass: fakeThinDC, SourceVolume: "existing-thin",
		SizeBytes: int64(10 * fakeGiB), AccessType: "ro",
	}

	f.lvm.Pause(t, "lvcreate")
	done := make(chan error, 1)
	go func() {
		_, err := f.lv.CreateLVSnapshot(ctx, req)
		done <- err
	}()
	f.lvm.WaitPaused(t)
	if _, err := f.lv.ResizeLV(ctx, &proto.ResizeLVRequest{
		Name: "existing-thin", DeviceClass: fakeThinDC, SizeBytes: int64(15 * fakeGiB),
	}); err != nil {
		t.Fatal(err)
	}
	f.lvm.Resume(t)
	if err := <-done; status.Code(err) != codes.Internal {
		t.Fatalf("got %v, want an Internal error for a snapshot larger than requested", err)
	}
	if lv, ok := fakeLVNames(t, f)["snap"]; ok {
		t.Fatalf("the oversized snapshot was left behind: %+v", lv)
	}

	f.lvm.Pause(t, "")
	req.SizeBytes = int64(15 * fakeGiB)
	res, err := f.lv.CreateLVSnapshot(ctx, req)
	if err != nil {
		t.Fatalf("retry failed: %v", err)
	}
	if res.GetSnapshot().GetSizeBytes() != int64(15*fakeGiB) {
		t.Errorf("retry made a snapshot of %d bytes, want 15 GiB", res.GetSnapshot().GetSizeBytes())
	}
}
