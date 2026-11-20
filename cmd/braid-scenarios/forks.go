package main

import (
	"encoding/json"
	"fmt"
	"slices"
	"time"
)

// holds waits until every peer of ps holds at least data tentative data entries.
func holds(c *Cluster, what string, ps []*Proc, data uint64) {
	c.Until(what, wait, func() (bool, string) {
		ss, err := c.Statuses(ps)
		if err != nil {
			return false, err.Error()
		}
		ok := true
		for _, s := range ss {
			ok = ok && s.TentativeData >= data
		}
		return ok, Summary(ps, ss)
	})
}

// waitHalted waits until every peer of ps reports that its current config settles no more.
func waitHalted(c *Cluster, ps []*Proc) time.Duration {
	return c.Until("settlement halts", wait, func() (bool, string) {
		ss, err := c.Statuses(ps)
		if err != nil {
			return false, err.Error()
		}
		ok := true
		for _, s := range ss {
			ok = ok && s.SettlementHalted != ""
		}
		return ok, Summary(ps, ss)
	})
}

// samePrefix fails the scenario unless the settled prefixes of ps agree up to the shortest one; it returns that length.
func samePrefix(c *Cluster, ps []*Proc) uint64 {
	ss := mustStatuses(c, ps)
	n := ss[0].Settled
	for _, s := range ss {
		n = min(n, s.Settled)
	}
	var want string
	for _, p := range ps {
		var r struct{ Order string }
		c.Must(json.Unmarshal(c.Do(p, "prefix", fmt.Sprint(n)), &r))
		if want == "" {
			want = r.Order
		}
		c.Check(r.Order == want, "%s settled a different prefix of %d positions", p.Name, n)
	}
	return n
}

func writerFork(c *Cluster) {
	ps := c.Grow(5, "f")
	seats, w := ps[:4], ps[4]
	one, two := seats[:2], seats[2:]
	c.Do(w, "put", "first", "1")
	c.Wait("settle the first write of the forking writer", ps, 0, wait)
	base := c.Data(ps[0])
	c.Cut(one, two)
	c.Isolate(w, ps)
	w, dir := c.Fork(w, ps)
	ps[4] = w
	c.Place(w, one...)
	c.Do(w, "putn", "h1", "5")
	holds(c, "half one holds history 1", one, base+5)
	c.Isolate(w, ps)
	w.Stop(c.ctx)
	clone := c.Clone("w-clone", dir)
	c.Place(clone, two...)
	c.Do(clone, "putn", "h2", "5")
	holds(c, "half two holds history 2", two, base+5)
	clone.Stop(c.ctx)
	c.Heal(seats...)
	d := c.Until("seats agree after the fork", wait, func() (bool, string) {
		ok, detail := c.Converged(seats, 0)()
		if !ok {
			return false, detail
		}
		for _, s := range mustStatuses(c, seats) {
			if slices.Contains(s.SettledMembers, w.Key) || s.Events["equivocation"] == 0 {
				return false, "evidence not ordered yet" + detail
			}
		}
		return true, ""
	})
	c.Time("agree on both histories", d)
	for _, s := range mustStatuses(c, seats) {
		c.Check(s.SettlementHalted == "" && s.Halted == "", "%s: settlement halted by a fork of a member that is not a seat: %q %q", s.Name, s.SettlementHalted, s.Halted)
	}
	n := c.Data(seats[0])
	c.Do(seats[2], "put", "after", "fork")
	c.Wait("settle after the fork", seats, n+1, wait)
}

func seatForkTwoThirds(c *Cluster) {
	ps := c.Grow(4, "t")
	honest, s := ps[:3], ps[3]
	one, two := honest[:2], honest[2:]
	c.Do(ps[0], "putn", "before", "10")
	c.Wait("settle before the fork", ps, 0, wait)
	c.Cut(one, two)
	c.Isolate(s, ps)
	s, dir := c.Fork(s, ps)
	ps[3] = s
	base := c.Data(ps[0])
	c.Place(s, one...)
	c.Do(s, "putn", "h1", "5")
	holds(c, "half one holds history 1", one, base+5)
	c.Isolate(s, ps)
	s.Stop(c.ctx)
	clone := c.Clone("seat-clone", dir)
	c.Place(clone, two...)
	c.Do(clone, "putn", "h2", "5")
	holds(c, "half two holds history 2", two, base+5)
	clone.Stop(c.ctx)
	c.Heal(honest...)
	d := waitHalted(c, honest)
	c.Time("settlement halted everywhere", d)
	n := samePrefix(c, honest)
	c.Note("no conflicting prefix: the three honest peers agree on the first %d settled positions", n)
	before := mustStatuses(c, honest)
	c.Do(honest[0], "putn", "after", "5")
	holds(c, "write after the halt is tentative", honest[:1], before[0].TentativeData+5)
	for i, s := range mustStatuses(c, honest) {
		c.Check(s.Settled == before[i].Settled, "%s settled past the halt: %d -> %d", honest[i].Name, before[i].Settled, s.Settled)
	}
}

// twoCopies runs a settler and a copy of its data directory, both confirmed against the same honest peers.
func twoCopies(c *Cluster) (honest []*Proc, s, copyOf *Proc) {
	ps := c.Grow(4, "k")
	honest, s = ps[:3], ps[3]
	c.Do(ps[0], "putn", "before", "10")
	c.Wait("settle before the copy", ps, 0, wait)
	s, dir := c.Fork(s, ps)
	ps[3] = s
	c.Place(s, honest...)
	copyOf = c.Spawn("seat-copy", "--dir", dir)
	c.attachTap(copyOf)
	c.Place(copyOf, honest...)
	c.Until("both copies confirmed", wait, func() (bool, string) {
		a, b := c.Stat(s), c.Stat(copyOf)
		return !a.Unconfirmed && !b.Unconfirmed, fmt.Sprintf("%v %v", a.Unconfirmed, b.Unconfirmed)
	})
	return honest, s, copyOf
}

// cloneHalt: two devices run one settler's data directory and both write cut off from the honest peers; on reconnect the
// second history halts settlement on every honest peer and their settled prefixes agree.
func cloneHalt(c *Cluster) {
	honest, s, copyOf := twoCopies(c)
	c.Cut([]*Proc{s, copyOf}, honest)
	// the second history is longer: a head no longer than what a peer holds is not looked at
	c.Do(s, "putn", "dev1", "5")
	c.Do(copyOf, "putn", "dev2", "12")
	c.Place(s, honest...)
	c.Place(copyOf, honest...)
	d := waitHalted(c, honest)
	c.Time("settlement halted everywhere", d)
	n := samePrefix(c, honest)
	c.Note("two running copies of one settler: settlement halted on all 3 honest peers, which agree on the first %d settled positions", n)
}
