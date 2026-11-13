package lvmd

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/topolvm/topolvm/internal/lvmd/command"
	"github.com/topolvm/topolvm/internal/lvmd/testutils/fakelvm"
	"github.com/topolvm/topolvm/pkg/lvmd/proto"
	lvmdTypes "github.com/topolvm/topolvm/pkg/lvmd/types"
	"google.golang.org/grpc"
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

// expectNoIncompleteSnapshots fails if an LV is left under a build name.
func expectNoIncompleteSnapshots(t *testing.T, f *fakeLVMD) {
	t.Helper()
	for name := range fakeLVNames(t, f) {
		if incompleteSnapshotNamePattern.MatchString(name) {
			t.Errorf("an incomplete snapshot was left behind: %s", name)
		}
	}
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
			expectNoIncompleteSnapshots(t, f)

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

func cleanupFailures(t *testing.T) float64 {
	t.Helper()
	var m dto.Metric
	if err := incompleteSnapshotCleanupFailures.Write(&m); err != nil {
		t.Fatal(err)
	}
	return m.GetCounter().GetValue()
}

// TestCreateLVSnapshotReportsFailedCleanup checks that when the incomplete
// snapshot cannot be removed after a failure either, the error says so and
// the failure is counted, and that the LV left behind keeps its build name:
// a retry does not adopt it, and the sweep at lvmd's next start removes it.
func TestCreateLVSnapshotReportsFailedCleanup(t *testing.T) {
	ctx := testContext(t)
	f := startFakeLVMD(t, fakelvm.Options{})
	req := &proto.CreateLVSnapshotRequest{
		Name: "snap", DeviceClass: fakeThinDC, SourceVolume: "existing-thin", AccessType: "ro",
	}
	before := cleanupFailures(t)

	f.lvm.Fail(t, "lvchange", "lvremove")
	_, err := f.lv.CreateLVSnapshot(ctx, req)
	if status.Code(err) != codes.Internal || !strings.Contains(status.Convert(err).Message(), "removing the incomplete snapshot volume") {
		t.Errorf("got %v, want an Internal error that reports the failed removal", err)
	}
	if got := cleanupFailures(t) - before; got != 1 {
		t.Errorf("counted %v failed removals, want 1", got)
	}
	var leftover []string
	for name := range fakeLVNames(t, f) {
		if incompleteSnapshotNamePattern.MatchString(name) {
			leftover = append(leftover, name)
		}
	}
	if len(leftover) != 1 {
		t.Fatalf("left %v under a build name, want one LV", leftover)
	}

	f.lvm.Fail(t)
	res, err := f.lv.CreateLVSnapshot(ctx, req)
	if err != nil {
		t.Fatalf("retry failed: %v", err)
	}
	if res.GetSnapshot().GetName() != "snap" {
		t.Errorf("retry returned %q, want snap", res.GetSnapshot().GetName())
	}

	vgs, err := command.ListVolumeGroups(ctx)
	if err != nil {
		t.Fatal(err)
	}
	RemoveIncompleteSnapshots(ctx, vgs, []*lvmdTypes.DeviceClass{{
		Name: fakeThinDC, VolumeGroup: fakeVGName, Type: lvmdTypes.TypeThin,
		ThinPoolConfig: &lvmdTypes.ThinPoolConfig{Name: fakePoolName, OverprovisionRatio: 2},
	}})
	expectNoIncompleteSnapshots(t, f)
	if _, ok := fakeLVNames(t, f)["snap"]; !ok {
		t.Error("the sweep removed the complete snapshot")
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
	expectNoIncompleteSnapshots(t, f)
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
			expectNoIncompleteSnapshots(t, f)

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
	expectNoIncompleteSnapshots(t, f)

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

// TestCreateLVSnapshotTwoCallsForOneName runs two calls for one clone, as after
// a topolvm-node restart: the first waits in lvcreate -s while the second
// creates the clone. The first must fail without removing the second's
// clone, whether it was cancelled or not.
func TestCreateLVSnapshotTwoCallsForOneName(t *testing.T) {
	for _, cancelFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("first call cancelled: %v", cancelFirst), func(t *testing.T) {
			f := startFakeLVMD(t, fakelvm.Options{LikeNsenter: true})
			req := &proto.CreateLVSnapshotRequest{
				Name: "clone", DeviceClass: fakeThinDC, SourceVolume: "existing-thin",
				SizeBytes: int64(20 * fakeGiB), AccessType: "rw",
			}

			f.lvm.Pause(t, "lvcreate")
			ctx1, cancel1 := context.WithCancel(testContext(t))
			defer cancel1()
			first := make(chan error, 1)
			go func() {
				_, err := f.lv.CreateLVSnapshot(ctx1, req)
				first <- err
			}()
			f.lvm.WaitPaused(t)

			res, err := f.lv.CreateLVSnapshot(testContext(t), req)
			if err != nil {
				t.Fatalf("the second call failed: %v", err)
			}
			if res.GetSnapshot().GetName() != "clone" {
				t.Errorf("the second call returned %q", res.GetSnapshot().GetName())
			}

			if cancelFirst {
				cancel1()
			}
			f.lvm.Resume(t)
			if err := <-first; err == nil {
				t.Error("the first call succeeded although the clone exists")
			}
			lv, ok := fakeLVNames(t, f)["clone"]
			if !ok {
				t.Fatal("the first call removed the clone that the second call created")
			}
			if lv.Size != 20*fakeGiB {
				t.Errorf("the clone is %d bytes, want 20 GiB", lv.Size)
			}
			expectNoIncompleteSnapshots(t, f)
		})
	}
}

// TestCreateLVSnapshotNameAppearsOnlyWhenComplete checks that a look-up never
// sees the requested name on an unfinished clone, and that a clone whose
// last step completed although the call was cancelled is kept.
func TestCreateLVSnapshotNameAppearsOnlyWhenComplete(t *testing.T) {
	f := startFakeLVMD(t, fakelvm.Options{LikeNsenter: true})
	req := &proto.CreateLVSnapshotRequest{
		Name: "clone", DeviceClass: fakeThinDC, SourceVolume: "existing-thin",
		SizeBytes: int64(20 * fakeGiB), AccessType: "rw",
	}

	f.lvm.Pause(t, "lvrename")
	ctx, cancel := context.WithCancel(testContext(t))
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := f.lv.CreateLVSnapshot(ctx, req)
		done <- err
	}()
	f.lvm.WaitPaused(t)

	// Everything but the rename is done; a look-up must not find the clone.
	list, err := f.vg.GetLVList(testContext(t), &proto.GetLVListRequest{DeviceClass: fakeThinDC})
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range list.GetVolumes() {
		if v.GetName() == "clone" {
			t.Fatal("a look-up found the clone before it was complete")
		}
	}

	// Cancelled during the rename, which lvm still completes.
	cancel()
	f.lvm.Resume(t)
	if err := <-done; err == nil {
		t.Fatal("the cancelled call succeeded")
	}
	lv, ok := fakeLVNames(t, f)["clone"]
	if !ok {
		t.Fatal("the complete clone was removed")
	}
	if lv.Size != 20*fakeGiB {
		t.Errorf("the clone is %d bytes, want 20 GiB", lv.Size)
	}
	expectNoIncompleteSnapshots(t, f)
}

// TestRemoveIncompleteSnapshots checks the sweep lvmd runs when it starts:
// only LVs under a build name are removed.
func TestRemoveIncompleteSnapshots(t *testing.T) {
	ctx := testContext(t)
	f := fakelvm.Start(t, fakelvm.State{
		VGs: []*fakelvm.VG{{
			Name: fakeVGName,
			Size: 100 * fakeGiB,
			Free: 60 * fakeGiB,
			LVs: []*fakelvm.LV{
				{Name: fakePoolName, Size: 40 * fakeGiB, IsPool: true},
				{Name: "existing-thin", Size: 10 * fakeGiB, Pool: fakePoolName, Minor: 100},
				{Name: "clone" + incompleteSnapshotSuffix + "0123456789abcdef", Size: 10 * fakeGiB, Pool: fakePoolName, Origin: "existing-thin", Minor: 101},
				{Name: "other" + incompleteSnapshotSuffix + "not-a-build-name", Size: 1 * fakeGiB, Pool: fakePoolName, Minor: 102},
			},
		}},
		NextMinor: 200,
	}, fakelvm.Options{}, command.LVMCommandPrefix, command.SetLVMCommandPrefix)

	vgs, err := command.ListVolumeGroups(ctx)
	if err != nil {
		t.Fatal(err)
	}
	RemoveIncompleteSnapshots(ctx, vgs, []*lvmdTypes.DeviceClass{{
		Name: fakeThinDC, VolumeGroup: fakeVGName, Type: lvmdTypes.TypeThin,
		ThinPoolConfig: &lvmdTypes.ThinPoolConfig{Name: fakePoolName, OverprovisionRatio: 2},
	}})

	left := map[string]bool{}
	for _, vg := range f.State(t).VGs {
		for _, lv := range vg.LVs {
			left[lv.Name] = true
		}
	}
	for _, name := range []string{fakePoolName, "existing-thin", "other" + incompleteSnapshotSuffix + "not-a-build-name"} {
		if !left[name] {
			t.Errorf("%s was removed", name)
		}
	}
	if left["clone"+incompleteSnapshotSuffix+"0123456789abcdef"] {
		t.Error("the incomplete snapshot was not removed")
	}
}

// watchStub is the server side of a Watch stream that only records reports.
type watchStub struct {
	grpc.ServerStream
	ctx  context.Context
	sent int
}

func (w *watchStub) Context() context.Context { return w.ctx }

func (w *watchStub) Send(*proto.WatchResponse) error {
	w.sent++
	return nil
}

// TestLeftoverBuildsAreSweptWhileLVMDRuns checks the periodic sweep: it
// removes a build whose clean-up failed, using the report taken for a
// watcher, and never a build that a call is still making.
func TestLeftoverBuildsAreSweptWhileLVMDRuns(t *testing.T) {
	interval := leftoverSweepInterval
	leftoverSweepInterval = 0
	t.Cleanup(func() { leftoverSweepInterval = interval })
	ctx := testContext(t)
	f := startFakeLVMD(t, fakelvm.Options{LikeNsenter: true})
	vg := f.vg.(*vgService)
	report := func() {
		t.Helper()
		if err := vg.send(&watchStub{ctx: ctx}); err != nil {
			t.Fatal(err)
		}
	}
	leftovers := func() []string {
		var names []string
		for name := range fakeLVNames(t, f) {
			if incompleteSnapshotNamePattern.MatchString(name) {
				names = append(names, name)
			}
		}
		return names
	}

	t.Log("a build whose clean-up failed is left, then swept")
	f.lvm.Fail(t, "lvchange", "lvremove")
	if _, err := f.lv.CreateLVSnapshot(ctx, &proto.CreateLVSnapshotRequest{
		Name: "snap", DeviceClass: fakeThinDC, SourceVolume: "existing-thin", AccessType: "ro",
	}); err == nil {
		t.Fatal("the snapshot succeeded although lvchange fails")
	}
	f.lvm.Fail(t)
	if left := leftovers(); len(left) != 1 {
		t.Fatalf("left %v, want one build", left)
	}
	f.lvm.Reset(t)
	report()
	if left := leftovers(); len(left) != 0 {
		t.Errorf("the sweep left %v", left)
	}
	// The sweep used the watcher's report: lvremove is its only command.
	expectCommands(t, f, []string{"fullreport", "lvremove"})

	t.Log("a build in progress is not swept")
	f.lvm.Pause(t, "lvrename")
	done := make(chan error, 1)
	go func() {
		_, err := f.lv.CreateLVSnapshot(ctx, &proto.CreateLVSnapshotRequest{
			Name: "clone", DeviceClass: fakeThinDC, SourceVolume: "existing-thin",
			SizeBytes: int64(20 * fakeGiB), AccessType: "rw",
		})
		done <- err
	}()
	f.lvm.WaitPaused(t)
	if left := leftovers(); len(left) != 1 {
		t.Fatalf("building %v, want one build", left)
	}
	report()
	if left := leftovers(); len(left) != 1 {
		t.Error("the sweep removed a build in progress")
	}
	f.lvm.Resume(t)
	if err := <-done; err != nil {
		t.Fatalf("the clone failed: %v", err)
	}
	if lv, ok := fakeLVNames(t, f)["clone"]; !ok || lv.Size != 20*fakeGiB {
		t.Errorf("the clone is missing or incomplete: %v", lv)
	}
	expectNoIncompleteSnapshots(t, f)
}
