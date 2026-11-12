package lvmd

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/topolvm/topolvm/internal/lvmd/testutils/fakelvm"
	"github.com/topolvm/topolvm/pkg/lvmd/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestConcurrentCreateLVKeepsCapacity runs more concurrent CreateLV calls than
// the pool can hold and checks that exactly as many succeed as fit, while
// their reports and lvcreate commands still overlap.
func TestConcurrentCreateLVKeepsCapacity(t *testing.T) {
	ctx := testContext(t)
	// Process start-up outside the lock dominates, as with real LVM.
	f := startFakeLVMD(t, fakelvm.Options{Delay: 50 * time.Millisecond, LockedDelay: 5 * time.Millisecond})

	// The thin pool holds 2 x 40 GiB virtually, 10 GiB of it used: 7 more
	// volumes of 10 GiB fit, the other 5 must be refused.
	const calls = 12
	var wg sync.WaitGroup
	codesSeen := make([]codes.Code, calls)
	for i := range calls {
		wg.Go(func() {
			_, err := f.lv.CreateLV(ctx, &proto.CreateLVRequest{
				Name:        fmt.Sprintf("c%d", i),
				DeviceClass: fakeThinDC,
				SizeBytes:   int64(10 * fakeGiB),
			})
			codesSeen[i] = status.Code(err)
		})
	}
	wg.Wait()

	created, refused := 0, 0
	for i, c := range codesSeen {
		switch c {
		case codes.OK:
			created++
		case codes.ResourceExhausted:
			refused++
		default:
			t.Errorf("call %d: unexpected code %v", i, c)
		}
	}
	if created != 7 || refused != 5 {
		t.Errorf("created %d and refused %d, want 7 and 5", created, refused)
	}

	var virtual uint64
	for _, vg := range f.lvm.State(t).VGs {
		for _, lv := range vg.LVs {
			if lv.Pool == fakePoolName {
				virtual += lv.Size
			}
		}
	}
	if virtual > 80*fakeGiB {
		t.Errorf("thin volumes total %d GiB, more than the 80 GiB the overprovision ratio allows", virtual/fakeGiB)
	}

	if peak := f.lvm.MaxConcurrent(t, "fullreport"); peak < 2 {
		t.Errorf("reports never overlapped (peak %d): the guard serialises them", peak)
	}
	if peak := f.lvm.MaxConcurrent(t, "lvcreate"); peak < 2 {
		t.Errorf("lvcreate processes never overlapped (peak %d): the guard serialises them", peak)
	}
}

// TestConcurrentResizeAndCreateKeepCapacity checks that a resize and
// creations racing for the last free capacity cannot together exceed it.
func TestConcurrentResizeAndCreateKeepCapacity(t *testing.T) {
	ctx := testContext(t)
	f := startFakeLVMD(t, fakelvm.Options{Delay: 50 * time.Millisecond, LockedDelay: 5 * time.Millisecond})

	// 70 GiB free in the thin pool. Growing existing-thin from 10 to 50 GiB
	// takes 40 GiB; four creations of 10 GiB want another 40 GiB.
	var wg sync.WaitGroup
	wg.Go(func() {
		_, _ = f.lv.ResizeLV(ctx, &proto.ResizeLVRequest{Name: "existing-thin", DeviceClass: fakeThinDC, SizeBytes: int64(50 * fakeGiB)})
	})
	for i := range 4 {
		wg.Go(func() {
			_, _ = f.lv.CreateLV(ctx, &proto.CreateLVRequest{Name: fmt.Sprintf("r%d", i), DeviceClass: fakeThinDC, SizeBytes: int64(10 * fakeGiB)})
		})
	}
	wg.Wait()

	var virtual uint64
	for _, vg := range f.lvm.State(t).VGs {
		for _, lv := range vg.LVs {
			if lv.Pool == fakePoolName {
				virtual += lv.Size
			}
		}
	}
	if virtual > 80*fakeGiB {
		t.Errorf("thin volumes total %d GiB, more than the 80 GiB the overprovision ratio allows", virtual/fakeGiB)
	}
}
