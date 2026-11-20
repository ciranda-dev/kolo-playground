// Package braidpeer is the process behind cmd/braid-peer: one kolo-braid member that prints JSON lines and takes commands on
// stdin.
package braidpeer

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	kolobraid "github.com/ciranda-dev/kolo-braid"
	"github.com/ciranda-dev/kolo-braid/kv"
	kolodht "github.com/ciranda-dev/kolo-dht"
	koloid "github.com/ciranda-dev/kolo-id"
	kololog "github.com/ciranda-dev/kolo-log"
	kolosecure "github.com/ciranda-dev/kolo-secure"
	kolostore "github.com/ciranda-dev/kolo-store"
	koloswarm "github.com/ciranda-dev/kolo-swarm"
	kolotransport "github.com/ciranda-dev/kolo-transport"
)

// AppTag is the application tag of every braid the playground makes.
const AppTag = "kolo-playground/braid/v0"

type config struct {
	name, dir, listen, identity, role string
	first, join                       bool
	peers                             []string
	bootstrap                         []netip.AddrPort
	pins                              []ed25519.PublicKey
	appendUnconfirmed, rebuild        bool
	statusEvery                       time.Duration
	group                             kolobraid.GroupConfig
	limits                            kolobraid.Limits
}

type listFlag []string

func (l *listFlag) String() string     { return strings.Join(*l, ",") }
func (l *listFlag) Set(s string) error { *l = append(*l, strings.Split(s, ",")...); return nil }

func parse(args []string, stderr io.Writer) (config, error) {
	var c config
	var peers, boot, pins listFlag
	var quorum string
	fs := flag.NewFlagSet("braid-peer", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&c.name, "name", "", "label in logs and status")
	fs.StringVar(&c.dir, "dir", "", "data directory; empty: a new temporary directory removed on exit")
	fs.StringVar(&c.listen, "listen", "127.0.0.1:0", "UDP `127.0.0.1:PORT` to listen on")
	fs.StringVar(&c.identity, "identity", "", "identity file; default DIR/identity, created if missing")
	fs.StringVar(&c.role, "role", "honest", "honest or hostile (enables badhead and deleteown)")
	fs.BoolVar(&c.first, "first", false, "create the braid; the first peer")
	fs.BoolVar(&c.join, "join", false, "make a strand for a braid to join; print the join request")
	fs.Var(&peers, "peer", "`IP:PORT@KEY` to dial once the braid is open (repeatable, comma-separated)")
	fs.Var(&boot, "bootstrap", "`IP:PORT` of kolo-dht entry nodes (repeatable, comma-separated): find peers through a kolo-swarm instead of --peer")
	fs.Var(&pins, "pin", "pin public key in hex (repeatable)")
	fs.BoolVar(&c.appendUnconfirmed, "append-unconfirmed", false, "write before the own strand is confirmed")
	fs.DurationVar(&c.statusEvery, "status-every", 0, "print a status line this often (0: only on the status command)")
	fs.BoolVar(&c.rebuild, "rebuild", false, "replay the view from genesis at open")
	fs.StringVar(&quorum, "quorum", "majority", "majority or twothirds (with --first)")
	fs.IntVar(&c.group.MaxMembers, "max-members", 0, "group MaxMembers (with --first)")
	fs.IntVar(&c.group.MaxSettlers, "max-settlers", 0, "group MaxSettlers (with --first)")
	fs.IntVar(&c.group.MinSettlers, "min-settlers", 0, "group MinSettlers (with --first)")
	fs.IntVar(&c.group.MaxBody, "max-body", 0, "group MaxBody (with --first)")
	fs.IntVar(&c.group.CheckpointEvery, "checkpoint-every", 0, "group CheckpointEvery (with --first)")
	fs.IntVar(&c.limits.MaxTail, "max-tail", 0, "Limits.MaxTail")
	fs.IntVar(&c.limits.MaxPullAhead, "max-pull-ahead", 0, "Limits.MaxPullAhead")
	fs.DurationVar(&c.limits.PulseGap, "pulse-gap", 0, "Limits.PulseGap")
	fs.DurationVar(&c.limits.PivotWait, "pivot-wait", 0, "Limits.PivotWait")
	fs.DurationVar(&c.limits.AnnounceGap, "announce-gap", 0, "Limits.AnnounceGap")
	fs.DurationVar(&c.limits.ConfirmWait, "confirm-wait", 0, "Limits.ConfirmWait")
	if err := fs.Parse(args); err != nil {
		return c, err
	}
	var err error
	switch quorum {
	case "majority":
		c.group.Quorum = kolobraid.Majority
	case "twothirds":
		c.group.Quorum = kolobraid.TwoThirds
	default:
		err = errors.New("--quorum must be majority or twothirds")
	}
	if c.first && c.join {
		err = errors.New("--first and --join exclude each other")
	}
	if c.role != "honest" && c.role != "hostile" {
		err = errors.New("--role must be honest or hostile")
	}
	if ap, perr := netip.ParseAddrPort(c.listen); perr != nil || !ap.Addr().Is4() || ap.Addr() != netip.MustParseAddr("127.0.0.1") {
		err = errors.New("--listen must be 127.0.0.1:PORT")
	}
	c.peers = peers
	for _, s := range boot {
		ap, perr := netip.ParseAddrPort(s)
		if perr != nil || !ap.Addr().Is4() || !ap.Addr().IsLoopback() {
			err = fmt.Errorf("--bootstrap %q must be a 127.0.0.1:PORT", s)
		}
		c.bootstrap = append(c.bootstrap, ap)
	}
	for _, s := range pins {
		k, derr := parseKey(s)
		if derr != nil {
			err = derr
		}
		c.pins = append(c.pins, k)
	}
	if err == nil && fs.NArg() > 0 {
		err = errors.New("unexpected argument")
	}
	c.group.App = AppTag
	return c, err
}

type peer struct {
	cfg  config
	ctx  context.Context
	wr   sync.Mutex
	out  io.Writer
	errw io.Writer
	id   *koloid.Identity
	raw  kolostore.Backend
	be   kolostore.Backend
	ctr  counters
	opts kolobraid.Options
	kvs  *kv.Store
	addr string
	sl   *kolosecure.Listener
	sw   *koloswarm.Swarm
	wg   sync.WaitGroup

	cleanup   func()
	cancelAll context.CancelFunc

	mu         sync.Mutex
	b          *kolobraid.Braid
	joining    *kolobraid.Joining
	strand     kololog.LogID
	haveStrand bool
	conns      map[string]*kolosecure.Conn
	blocked    map[string]bool
	events     map[string]int
	oc         orderCache
	admitNanos int64
	started    bool
}

// Run runs one peer until stdin ends, a quit command arrives or ctx ends, and returns the exit code.
func Run(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	cfg, err := parse(args, stderr)
	if errors.Is(err, flag.ErrHelp) {
		return 0
	}
	if err != nil {
		fmt.Fprintf(stderr, "braid-peer: %v\n", err)
		return 2
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	p := &peer{cfg: cfg, ctx: ctx, out: stdout, errw: stderr, kvs: kv.New([]byte("kv/")), conns: map[string]*kolosecure.Conn{},
		blocked: map[string]bool{}, events: map[string]int{}, cancelAll: cancel}
	if err := p.open(); err != nil {
		fmt.Fprintf(stderr, "braid-peer: %v\n", err)
		p.close()
		return 1
	}
	defer p.close()
	if cfg.statusEvery > 0 {
		p.wg.Go(func() { p.statusLines(cfg.statusEvery) })
	}
	p.control(stdin, cancel)
	return 0
}

// apply is the application: kv entries, plus a count of the effective ones.
func (p *peer) apply(tx *kolobraid.Tx, e *kolobraid.Entry) error {
	if e.Kind != kolobraid.KindData {
		return nil
	}
	handled, err := p.kvs.Apply(tx, e)
	if err != nil || !handled {
		return err
	}
	var n uint64
	if v, err := tx.Get(counterKey); err == nil && len(v) == 8 {
		n = binary.BigEndian.Uint64(v)
	}
	return tx.Put(counterKey, binary.BigEndian.AppendUint64(nil, n+1))
}

func (p *peer) open() (err error) {
	c := &p.cfg
	if c.dir == "" {
		if c.dir, err = os.MkdirTemp("", "braid-peer-"); err != nil {
			return err
		}
		defer func() {
			if err != nil {
				os.RemoveAll(c.dir)
			}
		}()
		p.cleanup = func() { os.RemoveAll(c.dir) }
	}
	path := c.identity
	if path == "" {
		path = filepath.Join(c.dir, "identity")
	}
	if p.id, err = koloid.Load(path); errors.Is(err, os.ErrNotExist) {
		if p.id, err = koloid.Generate(); err == nil {
			err = p.id.SaveNew(path)
		}
	}
	if err != nil {
		return fmt.Errorf("identity: %w", err)
	}
	if p.raw, err = kolostore.OpenPebble(filepath.Join(c.dir, "db")); err != nil {
		return err
	}
	p.be = &backend{Backend: p.raw, own: p.id.PublicKey(), c: &p.ctr}
	p.opts = kolobraid.Options{Backend: p.be, App: AppTag, Apply: p.apply, Pins: c.pins, AppendUnconfirmed: c.appendUnconfirmed,
		Rebuild: c.rebuild}
	if c.limits != (kolobraid.Limits{}) {
		l := c.limits
		p.opts.Limits = &l
	}
	if err := p.listen(); err != nil {
		return err
	}
	ready := map[string]any{"ev": "ready", "name": c.name, "key": hexOf(p.id.PublicKey()), "addr": p.addr, "dir": c.dir}
	switch {
	case c.first:
		b, err := kolobraid.Create(p.ctx, p.id, c.group, p.opts)
		if err != nil {
			return fmt.Errorf("create: %w", err)
		}
		p.setBraid(b)
	case c.join:
		j, err := kolobraid.NewJoin(p.ctx, p.id, p.opts)
		if err != nil {
			return fmt.Errorf("join: %w", err)
		}
		req, err := j.Request([]byte(c.name))
		if err != nil {
			return err
		}
		p.joining = j
		ready["request"] = hexOf(req)
	default:
		b, err := kolobraid.Open(p.ctx, p.id, p.opts)
		if err != nil {
			return fmt.Errorf("open: %w", err)
		}
		p.setBraid(b)
	}
	if b := p.braid(); b != nil {
		g := b.GroupID()
		ready["group"] = hexOf(g[:])
	}
	p.emit(ready)
	return nil
}

func (p *peer) listen() error {
	c := &p.cfg
	if len(c.bootstrap) > 0 {
		sc := &koloswarm.Config{LookupInterval: time.Second, RetryMin: 100 * time.Millisecond, RetryMax: 2 * time.Second,
			MaxHandshakes: 32, MaxHandshakesPerIP: 32, Allow: func(_ ed25519.PublicKey, a netip.AddrPort) bool { return !p.isBlocked(a.String()) },
			DHT: &kolodht.Config{Secure: &kolosecure.Config{Transport: &kolotransport.Config{MaxConnsPerIP: 64}}}}
		s, err := koloswarm.New(p.ctx, c.listen, p.id, sc)
		if err != nil {
			return err
		}
		p.sw = s
		bctx, cancel := context.WithTimeout(p.ctx, 15*time.Second)
		defer cancel()
		if err := s.Bootstrap(bctx, c.bootstrap); err != nil {
			return fmt.Errorf("bootstrap: %w", err)
		}
		p.addr = s.Addr().String()
		return nil
	}
	pc, err := net.ListenPacket("udp4", c.listen)
	if err != nil {
		return err
	}
	tl, err := kolotransport.ListenConn(p.ctx, &packetConn{PacketConn: pc, c: &p.ctr}, &kolotransport.Config{MaxConnsPerIP: 64, MaxConns: 256})
	if err != nil {
		return err
	}
	if p.sl, err = kolosecure.NewListener(tl, p.id, nil); err != nil {
		return err
	}
	p.addr = p.sl.Addr().String()
	return nil
}

func (p *peer) braid() *kolobraid.Braid {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.b
}

// setBraid takes a created, opened or completed braid and starts what serves it, once.
func (p *peer) setBraid(b *kolobraid.Braid) {
	p.mu.Lock()
	p.b = b
	first := !p.started
	p.started = true
	p.mu.Unlock()
	p.wg.Go(func() { p.drain(b) })
	if !first {
		return
	}
	switch {
	case p.sl != nil:
		p.wg.Go(p.acceptLoop)
		for _, s := range p.cfg.peers {
			if _, err := p.link(s); err != nil {
				fmt.Fprintf(p.errw, "braid-peer: %s: link %s: %v\n", p.cfg.name, s, err)
			}
		}
	case p.sw != nil:
		topic := b.Topic()
		if err := p.sw.Join(p.ctx, topic, nil); err != nil {
			fmt.Fprintf(p.errw, "braid-peer: %s: join topic: %v\n", p.cfg.name, err)
			return
		}
		p.wg.Go(p.swarmLoop)
	}
}

var eventNames = map[kolobraid.EventKind]string{
	kolobraid.EventApplied: "applied", kolobraid.EventReorg: "reorg", kolobraid.EventSettled: "settled",
	kolobraid.EventCertified: "certified", kolobraid.EventMemberAdded: "memberAdded", kolobraid.EventMemberRemoved: "memberRemoved",
	kolobraid.EventRemoved: "removed", kolobraid.EventEquivocation: "equivocation", kolobraid.EventSettlementHalted: "settlementHalted",
	kolobraid.EventStrandLost: "strandLost", kolobraid.EventStrandCaughtUp: "strandCaughtUp", kolobraid.EventHalted: "halted",
	kolobraid.EventDiverged: "diverged",
}

// drain counts the braid's events and prints the rare ones.
func (p *peer) drain(b *kolobraid.Braid) {
	for {
		ev, err := b.Next(p.ctx)
		if err != nil {
			return
		}
		name := eventNames[ev.Kind]
		p.mu.Lock()
		p.events[name]++
		p.mu.Unlock()
		switch ev.Kind {
		case kolobraid.EventApplied, kolobraid.EventSettled, kolobraid.EventCertified, kolobraid.EventReorg:
		default:
			m := map[string]any{"ev": "braid", "kind": name, "position": ev.Position}
			if ev.Member != nil {
				m["member"] = hexOf(ev.Member)
			}
			if ev.Err != nil {
				m["err"] = ev.Err.Error()
			}
			p.emit(m)
		}
	}
}

func (p *peer) isBlocked(addr string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.blocked[addr]
}

func (p *peer) acceptLoop() {
	for {
		c, err := p.sl.Accept(p.ctx)
		if err != nil {
			return
		}
		p.serve(c)
	}
}

func (p *peer) swarmLoop() {
	for {
		ev, err := p.sw.Next(p.ctx)
		if err != nil {
			return
		}
		if ev.Kind != koloswarm.Connected {
			continue
		}
		if c, ok := ev.Peer.Conn().(*kolosecure.Conn); ok {
			p.serve(c)
		}
	}
}

// serve runs the braid on a connection until it ends; blocked and duplicate addresses are closed.
func (p *peer) serve(c *kolosecure.Conn) {
	addr := c.RemoteAddr().String()
	b := p.braid()
	p.mu.Lock()
	if b == nil || p.blocked[addr] || p.conns[addr] != nil {
		p.mu.Unlock()
		c.Close()
		return
	}
	p.conns[addr] = c
	p.mu.Unlock()
	p.wg.Go(func() {
		err := b.ServeConn(p.ctx, c)
		c.Close()
		p.mu.Lock()
		if p.conns[addr] == c {
			delete(p.conns, addr)
		}
		p.mu.Unlock()
		if err != nil && p.ctx.Err() == nil && !errors.Is(err, kolobraid.ErrClosed) {
			fmt.Fprintf(p.errw, "braid-peer: %s: connection %s: %v\n", p.cfg.name, addr, err)
		}
	})
}

func parseKey(s string) (ed25519.PublicKey, error) {
	k, err := hex.DecodeString(s)
	if err != nil || len(k) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("bad public key %q", s)
	}
	return k, nil
}

// link dials ADDR@KEY from the listener's socket and serves the connection.
func (p *peer) link(s string) (string, error) {
	if p.sl == nil {
		return "", errors.New("links are made by the swarm")
	}
	addr, k, ok := strings.Cut(s, "@")
	ap, err := netip.ParseAddrPort(addr)
	if !ok || err != nil || ap.Addr() != netip.MustParseAddr("127.0.0.1") {
		return "", fmt.Errorf("want 127.0.0.1:PORT@KEY, got %q", s)
	}
	key, err := parseKey(k)
	if err != nil {
		return "", err
	}
	if p.isBlocked(addr) {
		return "", errors.New("address blocked")
	}
	p.mu.Lock()
	have := p.conns[addr] != nil
	p.mu.Unlock()
	if have {
		return addr, nil
	}
	ctx, cancel := context.WithTimeout(p.ctx, 15*time.Second)
	defer cancel()
	c, err := p.sl.Dial(ctx, ap, key)
	if err != nil {
		return "", err
	}
	p.serve(c)
	return addr, nil
}

// closeConns closes every connection; withAddrs limits it to those addresses.
func (p *peer) closeConns(addrs ...string) int {
	p.mu.Lock()
	var cs []*kolosecure.Conn
	for a, c := range p.conns {
		if len(addrs) == 0 || contains(addrs, a) {
			cs = append(cs, c)
		}
	}
	p.mu.Unlock()
	for _, c := range cs {
		c.Close()
	}
	return len(cs)
}

func contains(l []string, s string) bool {
	for _, x := range l {
		if x == s {
			return true
		}
	}
	return false
}

func (p *peer) close() {
	if p.cancelAll != nil {
		p.cancelAll()
	}
	p.closeConns()
	if b := p.braid(); b != nil {
		b.Close()
	}
	if p.sw != nil {
		p.sw.Close()
	}
	if p.sl != nil {
		p.sl.Close()
	}
	p.wg.Wait()
	if p.raw != nil {
		p.raw.Close()
	}
	if p.cleanup != nil {
		p.cleanup()
	}
}

// statusLines prints the status as a line of its own every d.
func (p *peer) statusLines(d time.Duration) {
	t := time.NewTicker(d)
	defer t.Stop()
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-t.C:
			p.emit(struct {
				Ev string `json:"ev"`
				Status
			}{"status", p.status()})
		}
	}
}

func (p *peer) emit(v any) {
	line, err := json.Marshal(v)
	if err != nil {
		return
	}
	p.wr.Lock()
	defer p.wr.Unlock()
	p.out.Write(append(line, '\n'))
}

// maxCommands is how many commands run at once; the next line is read when one ends.
const maxCommands = 16

// control reads commands, one per line as "ID COMMAND ARGS...", and answers each with one reply line; commands run
// concurrently so that status stays answerable during a long write.
func (p *peer) control(in io.Reader, stop context.CancelFunc) {
	p.commands(in, stop, p.exec, maxCommands)
}

// commands is control with the command runner and the number of concurrent commands as parameters.
func (p *peer) commands(in io.Reader, stop context.CancelFunc, run func(cmd string, args []string) (any, error), slots int) {
	sc := bufio.NewScanner(in)
	sc.Buffer(make([]byte, 0, 64<<10), 8<<20)
	var wg sync.WaitGroup
	defer wg.Wait()
	free := make(chan struct{}, slots)
	for sc.Scan() {
		f := strings.Fields(sc.Text())
		if len(f) < 2 {
			continue
		}
		id, cmd, args := f[0], f[1], f[2:]
		if cmd == "quit" {
			p.emit(map[string]any{"ev": "reply", "id": id, "ok": true})
			stop()
			return
		}
		free <- struct{}{}
		wg.Go(func() {
			defer func() { <-free }()
			data, err := run(cmd, args)
			r := map[string]any{"ev": "reply", "id": id, "ok": err == nil, "data": data}
			if err != nil {
				r["err"] = err.Error()
			}
			p.emit(r)
		})
	}
	stop()
}
