package main

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/ciranda-dev/kolo-playground/internal/braidpeer"
)

var scenarios = []Scenario{
	{Name: "convergence", Desc: "3, 4 and 5 peers write at once and settle the same prefix", Run: convergence},
	{Name: "one-device", Desc: "a one-device braid reopens and writes", Run: oneDevice},
	{Name: "partition-heal", Desc: "3/2 partition: the side with a quorum settles, the other cannot; heal and agree", Run: partitionHeal},
	{Name: "pulsegap-mixed", Desc: "settlers with PulseGap from 10ms to 1s settle the same prefix", Run: pulseGapMixed},
	{Name: "settlers-stopped", Desc: "two of three settlers stopped, one back: settlement resumes", Run: settlersStopped},
	{Name: "writer-fork", Desc: "a writer signs two histories and sends one to each half: the braid keeps settling", Run: writerFork},
	{Name: "seat-fork-twothirds", Desc: "a seat signs two histories in a TwoThirds braid: settlement halts everywhere, no conflicting prefix", Group: []string{"--quorum", "twothirds"}, Run: seatForkTwoThirds},
	{Name: "clone-halt", Desc: "a settler's data directory run twice with four settlers: settlement halts, no conflicting prefix", Run: cloneHalt},
	{Name: "admins-concurrent", Desc: "AddMember and RemoveMember from two admins at once", Run: adminsConcurrent},
	{Name: "mutual-removal", Desc: "two admins remove each other at once: exactly one stays", Run: mutualRemoval},
	{Name: "restore-behind", Desc: "a device restored from an old copy is only behind: it catches up and writes", Run: restoreBehind},
	{Name: "restore-inconsistent", Desc: "a restored copy that wrote offline is refused writes; the member recovers with a new strand", Run: restoreInconsistent},
	{Name: "kill9", Desc: "kill -9 in the middle of a settled batch, restart, agree", Run: kill9},
	{Name: "spam", Desc: "a member floods the braid, announces bad heads and erases its own entries", Run: spam},
	{Name: "late-joiner-10k", Desc: "a peer joins after 10,000 entries and settles the same prefix", Long: true, Run: lateJoiner},
	{Name: "former-settler-join", Desc: "a replaced settler signs a late second history while a peer joins", Run: formerSettler},
	{Name: "discovery", Desc: "four peers find each other through a kolo-dht entry node and a kolo-swarm topic", Run: discovery},
	{Name: "new-settler-offline", Desc: "a new settler that never comes online does not stop settlement", Run: newSettlerOffline},
}

const wait = 90 * time.Second

// writeAll makes every peer of ps write n entries at once under its own key prefix and returns n times len(ps).
func writeAll(c *Cluster, ps []*Proc, tag string, n int) uint64 {
	c.Parallel(ps, func(i int, p *Proc) error {
		_, err := p.Do(c.ctx, "putn", fmt.Sprintf("%s-%s", tag, p.Name), fmt.Sprint(n))
		return err
	})
	return uint64(n * len(ps))
}

func convergence(c *Cluster) {
	for _, n := range []int{3, 4, 5} {
		ps := c.Grow(n, fmt.Sprintf("n%d-", n))
		base := c.Data(ps[0])
		want := base + writeAll(c, ps, "w", 25)
		c.Wait(fmt.Sprintf("settle %d peers x25", n), ps, want, wait)
		for _, p := range ps {
			p.Stop(c.ctx)
		}
	}
}

func oneDevice(c *Cluster) {
	p := c.Founder("solo")
	c.Do(p, "putn", "a", "10")
	c.Wait("settle before reopen", []*Proc{p}, 10, wait)
	before := c.Stat(p)
	p.Stop(c.ctx)
	p = c.Restart(p, nil)
	after := c.Stat(p)
	c.Check(after.Order == before.Order && after.Settled >= before.Settled, "reopen changed the settled prefix: %.8s -> %.8s", before.Order, after.Order)
	c.Check(!after.Unconfirmed && !after.StrandLost && after.Seats == 1, "reopened state: unconfirmed=%v lost=%v seats=%d", after.Unconfirmed, after.StrandLost, after.Seats)
	c.Do(p, "putn", "b", "10")
	d := c.Wait("settle after reopen", []*Proc{p}, 20, wait)
	c.Note("one device reopened and wrote 10 more entries, settled in %v", d.Round(time.Millisecond))
}

func partitionHeal(c *Cluster) {
	ps := c.Grow(5, "p")
	a, b := ps[:3], ps[3:]
	base := c.Data(ps[0])
	c.Cut(a, b)
	writeAll(c, a, "a", 30)
	writeAll(c, b, "b", 30)
	c.Wait("majority side settles during the partition", a, base+90, wait)
	c.Until("minority holds its writes tentatively", wait, func() (bool, string) {
		ss, err := c.Statuses(b)
		if err != nil {
			return false, err.Error()
		}
		ok := true
		for _, s := range ss {
			ok = ok && s.TentativeData >= base+60
		}
		return ok, Summary(b, ss)
	})
	ss, err := c.Statuses(b)
	c.Must(err)
	for i, s := range ss {
		c.Check(s.SettledData == base, "%s settled %d data entries without a quorum (had %d)", b[i].Name, s.SettledData, base)
	}
	c.Heal(ps...)
	c.Wait("agree after heal", ps, base+150, wait)
}

func pulseGapMixed(c *Cluster) {
	gaps := []string{"10ms", "50ms", "250ms", "1s"}
	f := c.Founder("g0", "--pulse-gap", gaps[0])
	ps := []*Proc{f}
	for i := 1; i < len(gaps); i++ {
		p := c.JoinWith(f, fmt.Sprintf("g%d", i), []string{"--pulse-gap", gaps[i]})
		ps = append(ps, p)
		c.Mesh(ps...)
		c.Until("joined", wait, c.Converged(ps, 0))
	}
	c.GrowSeats(ps, 4)
	base := c.Data(ps[0])
	want := base + writeAll(c, ps, "m", 20)
	c.Wait("settle with mixed PulseGap "+strings.Join(gaps, ","), ps, want, wait)
	c.Do(ps[3], "put", "slow", "1")
	c.Wait("settle one write of the 1s peer", ps, want+1, wait)
	c.Do(ps[0], "put", "fast", "1")
	c.Wait("settle one write of the 10ms peer", ps, want+2, wait)
}

func settlersStopped(c *Cluster) {
	ps := c.Grow(3, "s")
	base := c.Data(ps[0])
	ps[1].Stop(c.ctx)
	ps[2].Stop(c.ctx)
	c.Do(ps[0], "putn", "lone", "10")
	s := c.Stat(ps[0])
	c.Check(s.Seats == 3 && s.Quorum == 2, "config: %d seats, quorum %d", s.Seats, s.Quorum)
	c.Until("write is tentative", wait, func() (bool, string) { return c.Stat(ps[0]).TentativeData == base+10, "" })
	c.Check(c.Data(ps[0]) == base, "one of three settlers settled by itself")
	ps[1] = c.Restart(ps[1], ps)
	c.Connect(ps[1], ps[0])
	c.Wait("settle with one settler back", ps[:2], base+10, wait)
	ps[2] = c.Restart(ps[2], ps)
	c.Mesh(ps...)
	c.Wait("third settler back", ps, base+10, wait)
}

func newSettlerOffline(c *Cluster) {
	ps := c.Grow(3, "o")
	base := c.Data(ps[0])
	j := c.Spawn("never", "--join")
	var r struct{ Welcome string }
	c.Must(unmarshal(c.Do(ps[0], "admit", j.Request), &r))
	j.Kill()
	written := writeAll(c, ps, "x", 20)
	want := base + written
	c.Wait("settle without the new settler", ps, want, wait)
	for _, s := range mustStatuses(c, ps) {
		c.Check(s.Seats == 3 && slices.Contains(s.Pending, j.Key), "pending config of %s: seats %d pending %v", s.Name, s.Seats, s.Pending)
	}
	c.Note("the config with the offline seat stays pending; the 3-seat config settled %d more entries", written)
}

func mustStatuses(c *Cluster, ps []*Proc) []braidpeer.Status {
	ss, err := c.Statuses(ps)
	c.Must(err)
	return ss
}
