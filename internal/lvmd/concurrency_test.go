package lvmd

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/topolvm/topolvm/internal/lvmd/command"
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

// TestStaggeredCreateLVKeepsCapacity feeds creations to 8 workers one at a
// time, each arriving while the previous one's lvcreate holds the lock, which
// is how claims reach a node in a real burst. At most one capacity check is
// then open at any moment, and every one must still count the creations in
// flight.
func TestStaggeredCreateLVKeepsCapacity(t *testing.T) {
	ctx := testContext(t)
	// About as measured on real hardware: a report takes about 100 ms in all,
	// while lvcreate spends about 120 ms starting and scanning before it takes
	// the volume group's lock, then holds it for about 70 ms. A report that
	// starts while the previous creation's lvcreate is still starting up does
	// not show that volume, and the previous creation's check may already be
	// the only one that was open.
	f := startFakeLVMD(t, fakelvm.Options{
		Delay:       50 * time.Millisecond,
		WriteDelay:  120 * time.Millisecond,
		LockedDelay: 70 * time.Millisecond,
	})

	// The thin pool has 70 GiB left under its ratio: 2 volumes of 35 GiB fit.
	// The limit is reached early in the burst, where a report is most likely
	// to miss the creation just before it.
	const calls, workers = 12, 8
	queue := make(chan int)
	results := make(chan codes.Code, calls)
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for i := range queue {
				_, err := f.lv.CreateLV(ctx, &proto.CreateLVRequest{
					Name:        fmt.Sprintf("s%d", i),
					DeviceClass: fakeThinDC,
					SizeBytes:   int64(35 * fakeGiB),
				})
				results <- status.Code(err)
			}
		})
	}
	for i := range calls {
		queue <- i
		// The next creation arrives while this one's lvcreate is starting.
		time.Sleep(80 * time.Millisecond)
	}
	close(queue)
	wg.Wait()
	close(results)

	created, refused := 0, 0
	for c := range results {
		switch c {
		case codes.OK:
			created++
		case codes.ResourceExhausted:
			refused++
		default:
			t.Errorf("unexpected code %v", c)
		}
	}
	if created != 2 || refused != 10 {
		t.Errorf("created %d and refused %d, want 2 and 10", created, refused)
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
}

// TestMaxConcurrentLVMCommands checks that lvmd never runs more LVM commands
// at once than allowed, and that a call waiting for a slot honours its
// context.
func TestMaxConcurrentLVMCommands(t *testing.T) {
	ctx := testContext(t)
	f := startFakeLVMD(t, fakelvm.Options{Delay: 100 * time.Millisecond})
	command.SetMaxConcurrentCommands(2)
	t.Cleanup(func() { command.SetMaxConcurrentCommands(command.DefaultMaxConcurrentCommands) })

	var wg sync.WaitGroup
	for i := range 6 {
		wg.Go(func() {
			_, err := f.lv.CreateLV(ctx, &proto.CreateLVRequest{
				Name: fmt.Sprintf("m%d", i), DeviceClass: fakeThinDC, SizeBytes: int64(fakeGiB),
			})
			if err != nil {
				t.Errorf("CreateLV %d: %v", i, err)
			}
		})
	}
	// While the slots are taken, a call with a short deadline gives up in time.
	time.Sleep(20 * time.Millisecond)
	shortCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := f.vg.GetLVList(shortCtx, &proto.GetLVListRequest{DeviceClass: fakeThinDC})
	if err == nil {
		t.Error("GetLVList succeeded although every command slot was taken")
	}
	// The six creations queued before it take about 900 ms in all.
	if elapsed := time.Since(start); elapsed > 400*time.Millisecond {
		t.Errorf("GetLVList took %v to honour a 50ms deadline", elapsed)
	}
	wg.Wait()

	if peak := f.lvm.MaxConcurrent(t); peak > 2 {
		t.Errorf("%d lvm commands ran at once, want at most 2", peak)
	}
}
