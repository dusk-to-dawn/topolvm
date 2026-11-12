package lvmd

import (
	"fmt"
	"math/rand"
	"slices"
	"testing"
)

// A randomized model of concurrent capacity changes: it interleaves them step
// by step (each step is atomic: one reservation call, one LVM report, or one
// LVM change) and checks after every step that no pool's committed size
// exceeds its capacity. Each schedule runs up to 40 operations over three
// pools (two thin pools and a thick volume group): creations, resizes,
// two-step snapshots (lvcreate -s at the source's size, then a resize, or a
// removal after a failure), failures before any change, and removals of idle
// volumes.
//
// Schedules are deterministic per seed. Negative controls, run when this test
// was written: closing the check before recording the reservation (the
// lone-check bug) fails at seed 6 (103 > 100); ignoring released reservations
// fails at seed 0 (99 > 80).

type modelPool struct {
	key       string
	capacity  uint64
	committed map[string]uint64
}

// names returns the pool's volumes in a fixed order, so that a seed always
// produces the same schedule.
func (p *modelPool) names() []string {
	names := make([]string, 0, len(p.committed))
	for name := range p.committed {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

func (p *modelPool) used() uint64 {
	var s uint64
	for _, v := range p.committed {
		s += v
	}
	return s
}

type opKind int

const (
	opCreate opKind = iota
	opResize
	opSnapshot // lvcreate -s at srcSize, then lvresize to target; may fail after the first step and remove
)

type modelOp struct {
	kind    opKind
	pool    *modelPool
	name    string
	target  uint64
	srcSize uint64
	phase   int
	chk     *check
	free    uint64
	sizes   map[string]uint64
	res     *reservation
	failMid bool
	failAll bool
}

func TestReservationsModelNoOvercommit(t *testing.T) {
	for seed := int64(0); seed < 20000; seed++ {
		runReservationsModel(t, seed)
	}
}

// runReservationsModel runs one random schedule.
func runReservationsModel(t *testing.T, seed int64) {
	t.Helper()
	rng := rand.New(rand.NewSource(seed))
	r := newReservations()
	pools := []*modelPool{
		{key: "thin:vg/p1", capacity: 100, committed: map[string]uint64{}},
		{key: "thin:vg/p2", capacity: 60, committed: map[string]uint64{}},
		{key: "vg:vg2", capacity: 80, committed: map[string]uint64{}},
	}
	// some pre-existing volumes
	for i, p := range pools {
		p.committed[fmt.Sprintf("pre%d", i)] = uint64(10 + rng.Intn(20))
	}
	var active []*modelOp
	busy := map[string]bool{}
	started := 0
	n := 0
	for steps := 0; steps < 400; steps++ {
		if started < 40 && (len(active) == 0 || rng.Intn(3) == 0) {
			op := newModelOp(rng, pools[rng.Intn(len(pools))], busy, n)
			if op == nil {
				continue
			}
			n++
			started++
			busy[op.pool.key+"/"+op.name] = true
			active = append(active, op)
			continue
		}
		if len(active) == 0 {
			continue
		}
		// occasionally remove an idle volume (lvremove needs no reservation)
		if rng.Intn(12) == 0 {
			p := pools[rng.Intn(len(pools))]
			for _, name := range p.names() {
				if !busy[p.key+"/"+name] {
					delete(p.committed, name)
					break
				}
			}
		}
		i := rng.Intn(len(active))
		op := active[i]
		done := op.step(r)
		for _, q := range pools {
			if q.used() > q.capacity {
				t.Fatalf("seed %d step %d: pool %s overcommitted: %d > %d (op %v %s phase %d)", seed, steps, q.key, q.used(), q.capacity, op.kind, op.name, op.phase)
			}
		}
		op.phase++
		if done {
			busy[op.pool.key+"/"+op.name] = false
			active = append(active[:i], active[i+1:]...)
		}
	}
	// drain
	for _, op := range active {
		if op.phase <= 2 && op.chk != nil {
			op.chk.close()
		}
		if op.phase > 2 {
			r.release(op.res)
		}
	}
}

// newModelOp picks a random operation on pool p, or returns nil when the
// pool has no volume for it.
func newModelOp(rng *rand.Rand, p *modelPool, busy map[string]bool, n int) *modelOp {
	op := &modelOp{pool: p}
	switch k := rng.Intn(10); {
	case k < 6:
		op.kind = opCreate
		op.name = fmt.Sprintf("c%d", n)
		op.target = uint64(1 + rng.Intn(30))
	case k < 8:
		// resize an existing, idle volume
		cands := make([]string, 0, len(p.committed))
		for _, name := range p.names() {
			if !busy[p.key+"/"+name] {
				cands = append(cands, name)
			}
		}
		if len(cands) == 0 {
			return nil
		}
		op.kind = opResize
		op.name = cands[rng.Intn(len(cands))]
		op.target = p.committed[op.name] + uint64(1+rng.Intn(20))
	default:
		cands := p.names()
		if len(cands) == 0 {
			return nil
		}
		op.kind = opSnapshot
		op.name = fmt.Sprintf("s%d", n)
		op.srcSize = p.committed[cands[rng.Intn(len(cands))]]
		op.target = op.srcSize + uint64(rng.Intn(15))
		op.failMid = rng.Intn(5) == 0
	}
	op.failAll = rng.Intn(10) == 0
	return op
}

// step runs the operation's next atomic step and reports whether it is done.
func (op *modelOp) step(r *reservations) bool {
	p := op.pool
	switch op.phase {
	case 0:
		op.chk = r.begin(p.key)
	case 1: // the report
		op.sizes = map[string]uint64{}
		for k, v := range p.committed {
			op.sizes[k] = v
		}
		used := p.used()
		if used < p.capacity {
			op.free = p.capacity - used
		}
	case 2:
		op.res, _ = op.chk.reserve(op.free, op.name, op.target, reportedAs(op.sizes))
		op.chk.close()
		return op.res == nil
	case 3: // first LVM change
		if op.failAll {
			break
		}
		switch op.kind {
		case opCreate, opResize:
			p.committed[op.name] = op.target
		case opSnapshot:
			p.committed[op.name] = op.srcSize
		}
	case 4: // second LVM change (snapshot resize, or removal after failure)
		if op.kind == opSnapshot && !op.failAll {
			if op.failMid {
				delete(p.committed, op.name)
			} else {
				p.committed[op.name] = op.target
			}
		}
	case 5:
		r.release(op.res)
		return true
	}
	return false
}
