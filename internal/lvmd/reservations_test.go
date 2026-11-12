package lvmd

import "testing"

func notReported(string) (uint64, bool) { return 0, false }

func reportedAs(sizes map[string]uint64) reportedSize {
	return func(name string) (uint64, bool) {
		size, ok := sizes[name]
		return size, ok
	}
}

// A check that is the only one open when it reserves must still leave its
// reservation for the next check: that check's report, taken while the first
// lvcreate runs, does not show the first volume.
func TestReservationSurvivesLoneCheck(t *testing.T) {
	r := newReservations()
	const key = "thin:vg/pool"
	const free = uint64(10)

	a := r.begin(key)
	resA, _ := a.reserve(free, "a", 8, notReported)
	if resA == nil {
		t.Fatal("first reservation refused")
	}
	b := r.begin(key)
	resB, avail := b.reserve(free, "b", 8, notReported)
	if resB != nil {
		t.Errorf("second reservation of 8 granted while 8 of 10 are in flight (available seen: %d)", avail)
	}
	if avail != 2 {
		t.Errorf("available seen by the second check: got %d, want 2", avail)
	}
	r.release(resA)
	r.release(resB)
	if len(r.pools) != 0 {
		t.Errorf("%d pool entries left after every check closed and every reservation was released", len(r.pools))
	}
}

// The same with another check open while the first reserves.
func TestReservationWithOverlappingCheck(t *testing.T) {
	r := newReservations()
	const key = "thin:vg/pool"
	c := r.begin(key)
	a := r.begin(key)
	resA, _ := a.reserve(10, "a", 8, notReported)
	b := r.begin(key)
	if resB, _ := b.reserve(10, "b", 8, notReported); resB != nil {
		t.Error("second reservation granted with a third check open")
	}
	c.close()
	r.release(resA)
	if len(r.pools) != 0 {
		t.Errorf("%d pool entries left", len(r.pools))
	}
}

// Closing a check twice, or after its pool's entry was pruned, leaves no entry behind.
func TestCheckCloseIsIdempotent(t *testing.T) {
	r := newReservations()
	const key = "vg:vg"
	c := r.begin(key)
	res, _ := c.reserve(10, "c", 1, notReported)
	c.close()
	r.release(res)
	c.close()
	if len(r.pools) != 0 {
		t.Errorf("%d pool entries left", len(r.pools))
	}
}

// A change that the check's report already shows is not counted again, so
// a pool can be filled exactly while changes overlap.
func TestReservationNotCountedTwice(t *testing.T) {
	r := newReservations()
	const key = "thin:vg/pool"

	a := r.begin(key)
	resA, _ := a.reserve(10, "a", 6, notReported)
	if resA == nil {
		t.Fatal("first reservation refused")
	}
	// B's report was taken after A's lvcreate: it shows a, and 4 left.
	b := r.begin(key)
	resB, avail := b.reserve(4, "b", 4, reportedAs(map[string]uint64{"a": 6}))
	if resB == nil {
		t.Errorf("second reservation of the last 4 refused (available seen: %d)", avail)
	}
	// C's report shows a, but not b yet: nothing is left.
	c := r.begin(key)
	if resC, avail := c.reserve(4, "c", 1, reportedAs(map[string]uint64{"a": 6})); resC != nil {
		t.Errorf("third reservation granted although b is in flight (available seen: %d)", avail)
	}
	r.release(resA)
	r.release(resB)
}

// A resize counts as the growth its report does not show.
func TestReservationForResize(t *testing.T) {
	r := newReservations()
	const key = "vg:vg"
	a := r.begin(key)
	// x is 2 in the report and is to reach 5: 3 of the free 4 are needed.
	resA, _ := a.reserve(4, "x", 5, reportedAs(map[string]uint64{"x": 2}))
	if resA == nil {
		t.Fatal("resize refused")
	}
	// A report that still shows x at 2: only 1 is left.
	b := r.begin(key)
	if resB, avail := b.reserve(4, "y", 2, reportedAs(map[string]uint64{"x": 2})); resB != nil || avail != 1 {
		t.Errorf("got %v with %d available, want a refusal with 1 available", resB, avail)
	}
	// A report that shows x at 5, with 1 free: x is not counted again.
	c := r.begin(key)
	if resC, avail := c.reserve(1, "y", 1, reportedAs(map[string]uint64{"x": 5})); resC == nil {
		t.Errorf("refused with %d available", avail)
	} else {
		r.release(resC)
	}
	r.release(resA)
}
