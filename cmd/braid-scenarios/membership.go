package main

import (
	"encoding/json"
	"slices"
)

// sameMembers fails the scenario unless every peer of ps settled the same roster; it returns it.
func sameMembers(c *Cluster, ps []*Proc) []string {
	ss := mustStatuses(c, ps)
	want := slices.Sorted(slices.Values(ss[0].SettledMembers))
	for _, s := range ss {
		c.Check(slices.Equal(slices.Sorted(slices.Values(s.SettledMembers)), want), "%s settled another roster than %s", s.Name, ps[0].Name)
	}
	return want
}

func has(list []string, key string) bool { return slices.Contains(list, key) }

// adminsConcurrent: one admin adds a member while another removes one, on two sides of a partition; the roster is the same on
// every peer after the heal.
func adminsConcurrent(c *Cluster) {
	ps := c.Grow(5, "a")
	joiner := c.Spawn("newcomer", "--join")
	adder, remover, victim := ps[1], ps[2], ps[4]
	side1, side2 := []*Proc{ps[0], adder, victim}, []*Proc{remover, ps[3]}
	c.Cut(side1, side2)
	c.Do(victim, "putn", "v", "10")
	var r struct{ Welcome string }
	c.Must(json.Unmarshal(c.Do(adder, "admit", joiner.Request), &r))
	c.Must(c.DoErr(remover, "removemember", victim.Key))
	c.Do(joiner, "complete", r.Welcome)
	c.attachTap(joiner)
	c.Place(joiner, side1...)
	live := append(slices.Clone(ps[:4]), joiner)
	c.Heal(append(slices.Clone(ps), joiner)...)
	d := c.Until("agree after concurrent AddMember and RemoveMember", wait, func() (bool, string) {
		ok, detail := c.Converged(live, 0)()
		if !ok {
			return false, detail
		}
		for _, s := range mustStatuses(c, ps[:4]) {
			if !has(s.SettledMembers, joiner.Key) || has(s.SettledMembers, victim.Key) {
				return false, "membership not settled yet" + detail
			}
		}
		return true, ""
	})
	c.Time("agree on add and remove", d)
	roster := sameMembers(c, live)
	c.Note("one admin added a member while another removed one: all %d peers settled the same roster of %d members", len(live), len(roster))
	n := c.Data(ps[0])
	c.Do(ps[0], "put", "after", "membership")
	c.Wait("settle after the membership changes", live, n+1, wait)
}

// mutualRemoval: two admins remove each other on the two sides of a partition; the removal that comes first in the braid order
// takes effect and the other is void, so exactly one of them stays.
func mutualRemoval(c *Cluster) {
	ps := c.Grow(4, "m")
	a, b := ps[1], ps[2]
	c.Cut([]*Proc{ps[0], a}, []*Proc{b, ps[3]})
	c.Must(c.DoErr(a, "removemember", b.Key))
	c.Must(c.DoErr(b, "removemember", a.Key))
	c.Heal(ps...)
	observers := []*Proc{ps[0], ps[3]}
	c.Until("one of the two admins is removed", wait, func() (bool, string) {
		ss := mustStatuses(c, observers)
		for _, s := range ss {
			if has(s.SettledMembers, a.Key) == has(s.SettledMembers, b.Key) {
				return false, Summary(observers, ss)
			}
		}
		return true, ""
	})
	removed := b
	if !has(mustStatuses(c, observers[:1])[0].SettledMembers, a.Key) {
		removed = a
	}
	rest := slices.DeleteFunc(slices.Clone(ps), func(p *Proc) bool { return p == removed })
	d := c.Until("agree after the mutual removal", wait, c.Converged(rest, 0))
	c.Time("agree after mutual removal", d)
	roster := sameMembers(c, rest)
	kept := a
	if removed == a {
		kept = b
	}
	c.Check(has(roster, kept.Key) && !has(roster, removed.Key), "the roster after the mutual removal is wrong")
	c.Note("mutual removal: %s stayed, %s was removed, on every peer that kept settling", kept.Name, removed.Name)
	n := c.Data(rest[0])
	c.Do(rest[0], "put", "after", "removal")
	c.Wait("settle after the removal", rest, n+1, wait)
}
