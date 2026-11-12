package lvmd

import (
	"sync"

	lvmdTypes "github.com/topolvm/topolvm/pkg/lvmd/types"
)

// reservations accounts for the capacity that calls in progress are about to
// take, so that concurrent CreateLV, ResizeLV and CreateLVSnapshot calls
// cannot all pass a free-space check against similar reports and together
// overcommit a thin pool or volume group.
//
// A call opens a check before it takes the report it checks against, reserves
// capacity for its volume when the check passes, and releases the reservation
// once its lvcreate or lvresize has returned. Only this bookkeeping is done
// under the mutex: the reports and LVM commands of concurrent calls still run
// in parallel.
//
// A reservation names its volume and the size the volume is to reach. A check
// counts, for each change that may be in progress, the part its own report does
// not show yet: the target size minus the volume's size in the report, or the
// whole target when the volume is not in the report. A report taken while a
// change was in progress may or may not include that change, so a released
// reservation keeps counting for every check whose report was started before
// the release. A check therefore never misses a change, and does not count a
// change that its report already shows.
type reservations struct {
	mu sync.Mutex
	// seq numbers checks and releases, so that a release can be ordered
	// before or after the start of a check.
	seq   uint64
	pools map[string]*poolReservations
}

type poolReservations struct {
	inflight map[*reservation]struct{}
	// released holds reservations released while some check of this pool
	// that started earlier was still open.
	released []*reservation
	// open counts the open checks of this pool by the seq they started at.
	open map[uint64]int
}

type reservation struct {
	key string
	// name is the volume the reservation is for, and target the size it is to reach.
	name       string
	target     uint64
	releasedAt uint64
}

// check is a free-space check in progress.
type check struct {
	r       *reservations
	key     string
	startAt uint64
	done    bool
}

// reportedSize returns the size of a volume in the report a check uses, and
// whether the report has the volume.
type reportedSize func(name string) (uint64, bool)

func newReservations() *reservations {
	return &reservations{pools: map[string]*poolReservations{}}
}

// reservationKey returns the key of the capacity a device class allocates
// from: its thin pool, or its volume group for thick volumes.
func reservationKey(dc *lvmdTypes.DeviceClass) string {
	if dc.Type == lvmdTypes.TypeThin && dc.ThinPoolConfig != nil {
		return "thin:" + dc.VolumeGroup + "/" + dc.ThinPoolConfig.Name
	}
	return "vg:" + dc.VolumeGroup
}

func (r *reservations) pool(key string) *poolReservations {
	p, ok := r.pools[key]
	if !ok {
		p = &poolReservations{inflight: map[*reservation]struct{}{}, open: map[uint64]int{}}
		r.pools[key] = p
	}
	return p
}

// begin opens a check. Call it before taking the report that the check uses,
// and end the check with reserve or close.
func (r *reservations) begin(key string) *check {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seq++
	r.pool(key).open[r.seq]++
	return &check{r: r, key: key, startAt: r.seq}
}

// unreported returns how much of a volume's target size the report does not show.
func unreported(name string, target uint64, reported reportedSize) uint64 {
	size, ok := reported(name)
	switch {
	case !ok:
		return target
	case size >= target:
		return 0
	default:
		return target - size
	}
}

// reserve reserves capacity for the volume name to reach target bytes. free is
// the free capacity in the report taken after begin, and reported gives the
// sizes of the volumes in that report. Changes that other calls may have in
// progress are subtracted from free as far as the report does not show them.
// reserve closes the check. It returns the reservation, or nil and the
// capacity it considered available.
func (c *check) reserve(free uint64, name string, target uint64, reported reportedSize) (*reservation, uint64) {
	r := c.r
	r.mu.Lock()
	defer r.mu.Unlock()
	p := r.pool(c.key)

	var pending uint64
	for res := range p.inflight {
		pending += unreported(res.name, res.target, reported)
	}
	for _, res := range p.released {
		if res.releasedAt > c.startAt {
			pending += unreported(res.name, res.target, reported)
		}
	}

	available := uint64(0)
	if free > pending {
		available = free - pending
	}
	var res *reservation
	if unreported(name, target, reported) <= available {
		res = &reservation{key: c.key, name: name, target: target}
		p.inflight[res] = struct{}{}
	}
	// Close the check only now: closing it can prune the pool's entry, which
	// must not happen before the reservation is in it.
	c.closeLocked(p)
	return res, available
}

// close closes the check without reserving. It does nothing if the check was
// closed already, so it can be deferred.
func (c *check) close() {
	r := c.r
	r.mu.Lock()
	defer r.mu.Unlock()
	if c.done {
		return
	}
	c.closeLocked(r.pool(c.key))
}

func (c *check) closeLocked(p *poolReservations) {
	if c.done {
		return
	}
	c.done = true
	if p.open[c.startAt]--; p.open[c.startAt] <= 0 {
		delete(p.open, c.startAt)
	}
	c.r.pruneLocked(c.key, p)
}

// release releases a reservation once the change it was made for has
// finished, successfully or not. A nil reservation is ignored.
func (r *reservations) release(res *reservation) {
	if res == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	p := r.pool(res.key)
	delete(p.inflight, res)
	r.seq++
	res.releasedAt = r.seq
	p.released = append(p.released, res)
	r.pruneLocked(res.key, p)
}

// pruneLocked forgets released reservations that no open check needs to
// count any more, and the pool's entry once nothing is left in it.
func (r *reservations) pruneLocked(key string, p *poolReservations) {
	oldestOpen := uint64(0)
	for startAt := range p.open {
		if oldestOpen == 0 || startAt < oldestOpen {
			oldestOpen = startAt
		}
	}
	kept := p.released[:0]
	for _, res := range p.released {
		if oldestOpen != 0 && res.releasedAt > oldestOpen {
			kept = append(kept, res)
		}
	}
	clear(p.released[len(kept):])
	p.released = kept
	if len(p.inflight) == 0 && len(p.released) == 0 && len(p.open) == 0 {
		delete(r.pools, key)
	}
}
