// Command braid-scenarios runs the multi-peer kolo-braid scenarios: every peer is a braid-peer process on 127.0.0.1.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"regexp"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/ciranda-dev/kolo-playground/internal/braidpeer"
)

// Scenario is one multi-peer scenario.
type Scenario struct {
	Name string
	Desc string
	// Long marks a scenario that takes minutes or many entries; it runs only with -long or when named by -run.
	Long bool
	// Group are the founder's flags (the group config), Limits replace the default peer limits when set.
	Group  []string
	Limits []string
	Run    func(c *Cluster)
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == peerArg {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		code := braidpeer.Run(ctx, os.Args[2:], os.Stdin, os.Stdout, os.Stderr)
		stop()
		os.Exit(code)
	}
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("braid-scenarios", flag.ContinueOnError)
	fs.SetOutput(stderr)
	list := fs.Bool("list", false, "list the scenarios and exit")
	only := fs.String("run", "", "run the scenarios whose name matches this `regexp` (default: every short one)")
	long := fs.Bool("long", false, "include the long scenarios")
	timeout := fs.Duration("timeout", 15*time.Minute, "limit of one scenario")
	fs.StringVar(&peerBin, "peer-bin", "", "braid-peer `binary` to run instead of this one")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	var re *regexp.Regexp
	if *only != "" {
		var err error
		if re, err = regexp.Compile(*only); err != nil {
			fmt.Fprintf(stderr, "braid-scenarios: -run: %v\n", err)
			return 2
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	failed, ran := 0, 0
	for _, sc := range scenarios {
		if re != nil && !re.MatchString(sc.Name) || re == nil && sc.Long && !*long && !*list {
			continue
		}
		if *list {
			long := ""
			if sc.Long {
				long = " (long)"
			}
			fmt.Fprintf(stdout, "%-24s %s%s\n", sc.Name, sc.Desc, long)
			continue
		}
		ran++
		rep := Execute(ctx, sc, *timeout, func(f string, a ...any) { fmt.Fprintf(stderr, f+"\n", a...) })
		rep.Print(stdout)
		if rep.Err != nil {
			failed++
		}
	}
	if *list {
		return 0
	}
	fmt.Fprintf(stdout, "%d scenarios, %d failed\n", ran, failed)
	if failed > 0 || ran == 0 {
		return 1
	}
	return 0
}

// Execute runs sc on a fresh cluster and returns its report; every process is killed when it returns.
func Execute(parent context.Context, sc Scenario, limit time.Duration, logf func(string, ...any)) (rep *Report) {
	ctx, cancel := context.WithTimeout(parent, limit)
	defer cancel()
	rep = &Report{Name: sc.Name}
	t0 := time.Now()
	logf("== %s", sc.Name)
	c, err := newCluster(ctx, logf)
	if err != nil {
		rep.Err = err
		return rep
	}
	if sc.Limits != nil {
		c.limits = sc.Limits
	}
	defer func() {
		if r := recover(); r != nil {
			if f, ok := r.(failure); ok {
				rep.Err = fmt.Errorf("%s", f.msg)
			} else {
				rep.Err = fmt.Errorf("panic: %v\n%s", r, debug.Stack())
			}
			c.dump(logf)
		}
		rep.Duration = time.Since(t0)
		rep.Costs = c.costs()
		c.mu.Lock()
		rep.Timings = append(rep.Timings, c.times...)
		rep.Notes = append(rep.Notes, c.notes...)
		c.mu.Unlock()
		c.Close()
	}()
	if err := c.start(sc.Group...); err != nil {
		panic(failure{err.Error()})
	}
	sc.Run(c)
	return rep
}

// dump logs the last status and the stderr tail of every process.
func (c *Cluster) dump(logf func(string, ...any)) {
	c.mu.Lock()
	all := append([]*Proc(nil), c.all...)
	c.mu.Unlock()
	for _, p := range all {
		if p.Alive() {
			p.Status(context.Background())
		}
		l := p.Last()
		logf("-- %s alive=%v settled=%d data=%d/%d order=%.8s seats=%d halted=%q sethalt=%q lost=%v unconf=%v links=%d events=%v\n%s", p.Name, p.Alive(), l.Settled, l.SettledData, l.TentativeData, l.Order, l.Seats, l.Halted, l.SettlementHalted, l.StrandLost, l.Unconfirmed, l.Links, l.Events, p.Stderr())
	}
}
