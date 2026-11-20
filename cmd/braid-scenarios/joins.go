package main

import (
	"fmt"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"time"
)

// rssKiB returns the resident memory of a process in KiB, or 0 when it cannot be read.
func rssKiB(p *Proc) int64 {
	out, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(p.cmd.Process.Pid)).Output()
	if err != nil {
		return 0
	}
	n, _ := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
	return n
}

// dirKiB returns the size of the files under a directory in KiB.
func dirKiB(dir string) int64 {
	out, err := exec.Command("du", "-sk", dir).Output()
	if err != nil {
		return 0
	}
	n, _ := strconv.ParseInt(strings.Fields(string(out) + " ")[0], 10, 64)
	return n
}

// lateJoiner: a peer joins after 10,000 entries and settles the same prefix.
func lateJoiner(c *Cluster) {
	ps := c.Grow(3, "l")
	base := c.Data(ps[0])
	const n = 10000
	t0 := time.Now()
	c.Parallel(ps, func(i int, p *Proc) error {
		_, err := p.Do(c.ctx, "putn", fmt.Sprintf("e%d", i), fmt.Sprint(n/len(ps)+1), "64")
		return err
	})
	total := uint64(len(ps) * (n/len(ps) + 1))
	c.Time("write 10,000 entries on 3 peers", time.Since(t0))
	c.Wait("settle 10,000 entries", ps, base+total, 10*time.Minute)
	j := c.Spawn("late", "--join")
	t1 := time.Now()
	r := admitReq(c, ps[0], j)
	c.Do(j, "complete", r.Welcome)
	c.attachTap(j)
	c.Time("admission: Admit and Complete", time.Since(t1))
	live := append(slices.Clone(ps), j)
	c.Mesh(live...)
	d := c.Until("late joiner catches up", 10*time.Minute, c.Converged(live, base+total))
	c.Time("late joiner links and settles the same prefix", d)
	s := c.Stat(j)
	c.Note("late joiner after %d entries: %d settled positions, %d replica commits, %d KiB stored", total, s.Settled, s.ReplicaCommits, dirKiB(j.Dir))
}

// spam: a member floods the braid with entries, announces heads that do not verify and erases its own entries.
func spam(c *Cluster) {
	f := c.Founder("h0")
	honest := []*Proc{f}
	for i := 1; i < 4; i++ {
		p := c.Join(f, fmt.Sprintf("h%d", i))
		honest = append(honest, p)
		c.Mesh(honest...)
		c.Until("joined", wait, c.Converged(honest, 0))
	}
	c.GrowSeats(honest, 4)
	s := c.JoinWith(f, "spammer", []string{"--role", "hostile"}, "plain")
	all := append(slices.Clone(honest), s)
	c.Mesh(all...)
	c.Until("spammer joined", wait, c.Converged(all, 0))
	base := c.Data(f)
	const flood, junk = 1500, 1000
	c.Do(s, "put", "mark", "1")
	t0 := time.Now()
	errs := make(chan error, 2)
	go func() { errs <- c.DoErr(s, "putn", "spam", fmt.Sprint(flood), "100") }()
	go func() { errs <- c.DoErr(s, "junk", fmt.Sprint(junk), "200") }()
	c.Parallel(honest, func(i int, p *Proc) error {
		_, err := p.Do(c.ctx, "putn", fmt.Sprintf("honest%d", i), "20")
		return err
	})
	c.Must(<-errs)
	c.Must(<-errs)
	want := base + 1 + flood + 80
	c.Wait("settle the flood and the honest writes", all, want, 5*time.Minute)
	c.Time("flood of 2,500 entries from one member", time.Since(t0))
	maxTail := 2048
	for _, p := range all {
		st := c.Stat(p)
		c.Check(st.Tail <= maxTail, "%s tail %d is over MaxTail %d", p.Name, st.Tail, maxTail)
		c.Note("%s: tail %d, %d KiB on disk, %d KiB resident", p.Name, st.Tail, dirKiB(p.Dir), rssKiB(p))
	}
	for _, h := range honest {
		var r struct{ Closed bool }
		c.Must(unmarshal(c.Do(s, "badhead", h.Target()), &r))
		c.Check(r.Closed, "%s kept a connection that announced a head that does not verify", h.Name)
	}
	c.Note("announced a head that does not verify to %d peers: each closed the connection", len(honest))
	// erase own entries that only half of the peers hold, then let the others pull from the spammer
	a, b := honest[:2], honest[2:]
	c.Cut([]*Proc{s}, b)
	c.Do(s, "putn", "gone", "40")
	holds(c, "half of the peers hold the entries", a, want+40)
	c.Do(s, "deleteown", "tail", "10")
	c.Heal(all...)
	late := c.Join(honest[0], "late-after-delete")
	live := append(slices.Clone(honest), late)
	c.Mesh(live...)
	c.Wait("honest peers and a late joiner agree after the deletes", live, want+40, 5*time.Minute)
	c.Note("the spammer erased 10 of its entries that half of the peers held: every honest peer and a late joiner settled all of them")
}

// formerSettler: a replaced settler signs a late second history of its old strand while a new peer joins; everyone,
// the joiner included, settles the same prefix and settlement does not stop.
func formerSettler(c *Cluster) {
	ps := c.Grow(5, "q")
	x := ps[3]
	c.Do(x, "put", "before", "1")
	c.Wait("settle before the copy", ps, 0, wait)
	x.Stop(c.ctx)
	backup := c.CopyDir(x, "x-copy")
	x = c.Restart(x, ps)
	ps[3] = x
	c.Relink(x, ps)
	c.Until("confirmed", wait, func() (bool, string) { return !c.Stat(x).Unconfirmed, "" })
	c.Do(x, "putn", "after", "5")
	c.Wait("settle the writes after the copy", ps, 0, wait)
	keys := []string{ps[0].Key, ps[1].Key, ps[2].Key, ps[4].Key}
	c.Do(ps[0], "settlers", append([]string{"majority"}, keys...)...)
	c.Until("another settler config replaces the former seat", wait, func() (bool, string) {
		ss := mustStatuses(c, ps)
		done := true
		for _, s := range ss {
			done = done && !has(s.SeatKeys, x.Key) && has(s.SeatKeys, ps[4].Key) && len(s.Pending) == 0
		}
		if !done {
			c.Do(ps[0], "put", "grow", "1")
		}
		return done, Summary(ps, ss)
	})
	c.Wait("settle in the new config", ps, 0, wait)
	j := c.Spawn("newcomer", "--join")
	forger := c.Clone("x-forger", backup)
	c.Do(forger, "putn", "late", "30")
	r := admitReq(c, ps[0], j)
	c.Do(j, "complete", r.Welcome)
	c.attachTap(j)
	all := append(slices.Clone(ps), j)
	c.Mesh(all...)
	c.Place(forger, all...)
	live := slices.DeleteFunc(slices.Clone(all), func(p *Proc) bool { return p == x })
	d := c.Until("everyone settles the same prefix, the joiner too", wait, func() (bool, string) {
		ok, detail := c.Converged(live, 0)()
		if !ok {
			return false, detail
		}
		for _, p := range live[:len(live)-1] {
			if c.Stat(p).Events["equivocation"] == 0 {
				return false, "no evidence yet at " + p.Name
			}
		}
		return true, ""
	})
	c.Time("joiner and old members agree", d)
	for _, s := range mustStatuses(c, live) {
		c.Check(s.SettlementHalted == "" && s.Halted == "", "%s: halted by a former seat's second history: %q %q", s.Name, s.SettlementHalted, s.Halted)
	}
	st := c.Stat(j)
	c.Note("the joiner saw settlementHalted events %q and settles on; former seat is fenced: %v", j.EventErrs("settlementHalted"), !has(st.SettledMembers, x.Key))
	n := c.Data(ps[0])
	c.Do(ps[0], "put", "after", "forger")
	c.Wait("settle after the late second history", live, n+1, wait)
}

// discovery: peers find each other through a kolo-dht entry node and a kolo-swarm topic, with no addresses handed out.
func discovery(c *Cluster) {
	boot := c.DHT().String()
	f := c.Founder("d0", "--bootstrap", boot)
	ps := []*Proc{f}
	for i := 1; i < 4; i++ {
		p := c.JoinWith(f, fmt.Sprintf("d%d", i), []string{"--bootstrap", boot})
		ps = append(ps, p)
		c.Until(fmt.Sprintf("d%d found its peers", i), wait, func() (bool, string) {
			ok, detail := c.Converged(ps, 0)()
			for _, q := range ps {
				ok = ok && has(c.Stat(q).SettledMembers, p.Key)
			}
			return ok, detail
		})
	}
	c.GrowSeats(ps, 4)
	base := c.Data(ps[0])
	want := base + writeAll(c, ps, "s", 20)
	c.Wait("settle through the swarm", ps, want, wait)
	for _, p := range ps {
		c.Check(c.Stat(p).Links >= 1, "%s has no links", p.Name)
	}
}
