package braidpeer

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"strconv"
	"strings"
	"time"

	kolobraid "github.com/ciranda-dev/kolo-braid"
	kololog "github.com/ciranda-dev/kolo-log"
	kolosecure "github.com/ciranda-dev/kolo-secure"
	kolostore "github.com/ciranda-dev/kolo-store"

	"github.com/ciranda-dev/kolo-playground/internal/braidwire"
)

// contentSum hashes a settled position and every view pair in key order, so peers can compare their settled views.
func contentSum(pos uint64, snap *kolobraid.Snapshot) []byte {
	h := sha256.New()
	h.Write(binary.AppendUvarint(nil, pos))
	for k, v := range snap.Scan(nil, nil) {
		h.Write(binary.AppendUvarint(nil, uint64(len(k))))
		h.Write(k)
		h.Write(binary.AppendUvarint(nil, uint64(len(v))))
		h.Write(v)
	}
	return h.Sum(nil)
}

// commandTimeout bounds one command that talks to the braid.
const commandTimeout = 5 * time.Minute

// maxValueSize bounds the SIZE of putn and junk: 3 MiB is the largest group MaxBody kolo-braid accepts.
const maxValueSize = 3 << 20

func (p *peer) status() Status {
	s := Status{Name: p.cfg.name, Key: hexOf(p.id.PublicKey()), Addr: p.addr, Events: map[string]int{}, Members: []string{},
		SettledMembers: []string{}, Commits: p.ctr.commits.Load(), ReplicaCommits: p.ctr.replicaCommits.Load(),
		ReplicaBytes: p.ctr.replicaBytes.Load(), OwnCommits: p.ctr.ownCommits.Load(), UDPInBytes: p.ctr.udpInBytes.Load(),
		UDPOutBytes: p.ctr.udpOutBytes.Load(), UDPIn: p.ctr.udpIn.Load(), UDPOut: p.ctr.udpOut.Load()}
	p.mu.Lock()
	maps.Copy(s.Events, p.events)
	s.Links = len(p.conns)
	s.AdmitNanos = p.admitNanos
	if p.haveStrand {
		s.Strand = hexOf(p.strand[:])
	}
	oc := p.oc
	p.mu.Unlock()
	b := p.braid()
	if b == nil {
		return s
	}
	s.HasBraid = true
	g := b.GroupID()
	s.Group = hexOf(g[:])
	var st kolobraid.Status
	var snap *kolobraid.Snapshot
	for range 8 {
		st = b.Status()
		snap = b.Settled()
		if b.Status().Settled == st.Settled {
			break
		}
		snap.Close()
		snap = nil
	}
	if snap == nil {
		st = b.Status()
		snap = b.Settled()
	}
	defer snap.Close()
	s.Settled, s.Tentative, s.Tail = st.Settled, st.Tentative, st.Tail
	s.Config, s.Seats, s.Quorum = st.Config, st.Seats, st.Quorum
	s.Unconfirmed, s.StrandLost, s.EventsDropped = st.Unconfirmed, st.StrandLost, st.EventsDropped
	if st.SettlementHalted != nil {
		s.SettlementHalted = st.SettlementHalted.Error()
	}
	if st.Halted != nil {
		s.Halted = st.Halted.Error()
	}
	for _, m := range st.PendingConfig {
		s.Pending = append(s.Pending, hexOf(m.Key))
	}
	for _, m := range st.AwaitingReadiness {
		s.Awaiting = append(s.Awaiting, hexOf(m.Key))
	}
	s.SettledData = dataCount(snap)
	for _, m := range snap.Members() {
		s.SettledMembers = append(s.SettledMembers, hexOf(m.Key))
	}
	v := b.View()
	s.TentativeData = dataCount(v)
	for _, m := range v.Members() {
		s.Members = append(s.Members, hexOf(m.Key))
		if m.Seat {
			s.SeatKeys = append(s.SeatKeys, hexOf(m.Key))
		}
	}
	v.Close()
	s.Content = hexOf(contentSum(st.Settled, snap))
	if oc2, err := orderTo(b, oc, st.Settled); err == nil {
		s.Order, s.OrderAt = hexOf(oc2.sum[:]), oc2.pos
		p.mu.Lock()
		if oc2.pos > p.oc.pos {
			p.oc = oc2
		}
		p.mu.Unlock()
	}
	return s
}

func (p *peer) exec(cmd string, args []string) (any, error) {
	if cmd == "status" {
		return p.status(), nil
	}
	ctx, cancel := context.WithTimeout(p.ctx, commandTimeout)
	defer cancel()
	b := p.braid()
	need := func(n int) error {
		if len(args) < n {
			return fmt.Errorf("%s needs %d arguments", cmd, n)
		}
		return nil
	}
	switch cmd {
	case "complete":
		if err := need(1); err != nil {
			return nil, err
		}
		return p.complete(ctx, args[0])
	case "link":
		if err := need(1); err != nil {
			return nil, err
		}
		a, err := p.link(args[0])
		return map[string]any{"addr": a}, err
	case "unlink":
		return map[string]any{"closed": p.closeConns(args...)}, nil
	case "block", "unblock":
		p.mu.Lock()
		for _, a := range args {
			if cmd == "block" {
				p.blocked[a] = true
			} else {
				delete(p.blocked, a)
			}
		}
		p.mu.Unlock()
		if cmd == "block" {
			return map[string]any{"closed": p.closeConns(args...)}, nil
		}
		return nil, nil
	}
	if b == nil {
		return nil, errors.New("no braid yet")
	}
	switch cmd {
	case "put":
		if err := need(2); err != nil {
			return nil, err
		}
		return nil, p.kvs.Put(ctx, b, []byte(args[0]), []byte(args[1]))
	case "del":
		if err := need(1); err != nil {
			return nil, err
		}
		return nil, p.kvs.Delete(ctx, b, []byte(args[0]))
	case "get":
		if err := need(1); err != nil {
			return nil, err
		}
		return p.get(b, args[0])
	case "putn":
		return p.putn(ctx, b, args)
	case "junk":
		return p.junk(ctx, b, args)
	case "prefix":
		if err := need(1); err != nil {
			return nil, err
		}
		n, err := strconv.ParseUint(args[0], 10, 64)
		if err != nil {
			return nil, err
		}
		oc, err := orderTo(b, orderCache{}, n)
		return map[string]any{"order": hexOf(oc.sum[:]), "at": oc.pos}, err
	case "admit":
		return p.admit(ctx, b, args)
	case "removemember":
		if err := need(1); err != nil {
			return nil, err
		}
		k, err := parseKey(args[0])
		if err != nil {
			return nil, err
		}
		_, err = b.RemoveMember(ctx, k)
		return nil, err
	case "settlers":
		return p.settlers(ctx, b, args)
	case "badhead", "deleteown":
		if p.cfg.role != "hostile" {
			return nil, errors.New("only a hostile peer runs " + cmd)
		}
		if cmd == "badhead" {
			if err := need(1); err != nil {
				return nil, err
			}
			return p.badHead(ctx, b, args[0])
		}
		return p.deleteOwn(ctx, b, args)
	case "reopen":
		return nil, p.reopen(ctx, b, nil)
	}
	return nil, fmt.Errorf("unknown command %q", cmd)
}

// ownStrand finds the own strand from an entry this peer wrote.
func (p *peer) ownStrand(b *kolobraid.Braid) (kololog.LogID, bool) {
	p.mu.Lock()
	id, ok := p.strand, p.haveStrand
	p.mu.Unlock()
	if ok {
		return id, true
	}
	me := p.id.PublicKey()
	for o, err := range b.Entries(0) {
		if err != nil {
			break
		}
		if o.Writer.Equal(me) && o.Kind == kolobraid.KindData {
			p.mu.Lock()
			p.strand, p.haveStrand = o.Strand, true
			p.mu.Unlock()
			return o.Strand, true
		}
	}
	return id, false
}

func (p *peer) get(b *kolobraid.Braid, key string) (any, error) {
	out := map[string]any{}
	for name, s := range map[string]*kolobraid.Snapshot{"settled": b.Settled(), "view": b.View()} {
		v, err := p.kvs.Get(s, []byte(key))
		s.Close()
		if err == nil {
			out[name] = string(v)
		}
	}
	return out, nil
}

func (p *peer) putn(ctx context.Context, b *kolobraid.Braid, args []string) (any, error) {
	if len(args) < 2 {
		return nil, errors.New("putn PREFIX COUNT [SIZE]")
	}
	n, err := strconv.Atoi(args[1])
	if err != nil || n < 0 {
		return nil, errors.New("bad count")
	}
	size := 32
	if len(args) > 2 {
		if size, err = strconv.Atoi(args[2]); err != nil || size < 0 || size > maxValueSize {
			return nil, fmt.Errorf("bad size, at most %d", maxValueSize)
		}
	}
	val := []byte(strings.Repeat("v", size))
	t0 := time.Now()
	for i := range n {
		if err := p.kvs.Put(ctx, b, fmt.Appendf(nil, "%s/%d", args[0], i), val); err != nil {
			return map[string]any{"written": i}, err
		}
	}
	return map[string]any{"written": n, "nanos": time.Since(t0).Nanoseconds()}, nil
}

// junk appends data entries that are not kv operations: valid for the braid, ignored by the application.
func (p *peer) junk(ctx context.Context, b *kolobraid.Braid, args []string) (any, error) {
	if len(args) < 2 {
		return nil, errors.New("junk COUNT SIZE")
	}
	n, err1 := strconv.Atoi(args[0])
	size, err2 := strconv.Atoi(args[1])
	if err1 != nil || err2 != nil || n < 0 || size < 1 || size > maxValueSize {
		return nil, fmt.Errorf("bad count or size, size at most %d", maxValueSize)
	}
	for i := range n {
		body := make([]byte, size)
		rand.Read(body)
		body[0] = 0xff
		if _, err := b.Append(ctx, body); err != nil {
			return map[string]any{"written": i}, err
		}
	}
	return map[string]any{"written": n}, nil
}

func (p *peer) admit(ctx context.Context, b *kolobraid.Braid, args []string) (any, error) {
	if len(args) < 1 {
		return nil, errors.New("admit REQUEST [default|plain|admin] [replace]")
	}
	req, err := hex.DecodeString(args[0])
	if err != nil {
		return nil, err
	}
	jr, err := kolobraid.ParseJoinRequest(req)
	if err != nil {
		return nil, err
	}
	h, err := kololog.DecodeLogHeader(jr.Header)
	if err != nil {
		return nil, err
	}
	opts := &kolobraid.AdmitOptions{}
	grow := len(args) == 1
	for _, a := range args[1:] {
		switch a {
		case "default":
			grow = true
		case "plain":
			r := kolobraid.Role(0)
			opts.Roles = &r
		case "admin":
			r := kolobraid.RoleAdmin
			opts.Roles = &r
		case "replace":
			opts.Replace = true
		default:
			return nil, fmt.Errorf("unknown admit option %q", a)
		}
	}
	// without roles a joiner is a plain member since braid v0.1.1; the default grows the braid toward MinSettlers seats
	opts.GrowSettlers = grow && opts.Roles == nil
	t0 := time.Now()
	// the harness trusts its driver; an application passes the joiner key kolo-invite authenticated (Requested.Joiner)
	w, err := b.Admit(ctx, h.LogKey, req, opts)
	d := time.Since(t0)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	p.admitNanos = d.Nanoseconds()
	p.mu.Unlock()
	return map[string]any{"welcome": hexOf(w), "nanos": d.Nanoseconds()}, nil
}

func (p *peer) complete(ctx context.Context, welcome string) (any, error) {
	p.mu.Lock()
	j := p.joining
	p.mu.Unlock()
	if j == nil {
		return nil, errors.New("not a joining peer")
	}
	w, err := hex.DecodeString(welcome)
	if err != nil {
		return nil, err
	}
	t0 := time.Now()
	b, err := j.Complete(ctx, w)
	if err != nil {
		return nil, err
	}
	p.setBraid(b)
	p.mu.Lock()
	p.joining = nil
	p.mu.Unlock()
	return map[string]any{"nanos": time.Since(t0).Nanoseconds()}, nil
}

func (p *peer) settlers(ctx context.Context, b *kolobraid.Braid, args []string) (any, error) {
	if len(args) < 2 {
		return nil, errors.New("usage: settlers majority|twothirds KEY, one or more keys")
	}
	rule := kolobraid.Majority
	if args[0] == "twothirds" {
		rule = kolobraid.TwoThirds
	}
	var keys []ed25519.PublicKey
	for _, a := range args[1:] {
		k, err := parseKey(a)
		if err != nil {
			return nil, err
		}
		keys = append(keys, k)
	}
	_, err := b.SetSettlers(ctx, keys, rule)
	return nil, err
}

// badHead announces from a fresh socket a head of the own strand that does not verify; the victim must close the link.
func (p *peer) badHead(ctx context.Context, b *kolobraid.Braid, target string) (any, error) {
	strand, ok := p.ownStrand(b)
	if !ok {
		return nil, errors.New("write an entry first")
	}
	addr, k, _ := strings.Cut(target, "@")
	key, err := parseKey(k)
	if err != nil {
		return nil, err
	}
	conn, err := kolosecure.Dial(ctx, addr, p.id, key, nil)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	h := kololog.Head{LogKey: p.id.PublicKey(), LogID: strand, Size: 1 << 20, Signature: make([]byte, 64)}
	rand.Read(h.Root[:])
	rand.Read(h.Signature)
	frame := braidwire.AnnounceFrame(b.GroupID(), []kololog.Head{h})
	sctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	st, err := conn.OpenUniStream(sctx)
	if err != nil {
		return nil, err
	}
	st.Write(frame)
	st.Close()
	select {
	case <-conn.Context().Done():
		return map[string]any{"closed": true}, nil
	case <-sctx.Done():
		return map[string]any{"closed": false}, nil
	}
}

// reopen closes the braid and opens it again, after edit if given; connections end with it and the caller links again.
func (p *peer) reopen(ctx context.Context, old *kolobraid.Braid, edit func() error) error {
	p.closeConns()
	if err := old.Close(); err != nil {
		return err
	}
	if edit != nil {
		if err := edit(); err != nil {
			return err
		}
	}
	b, err := kolobraid.Open(ctx, p.id, p.opts)
	if err != nil {
		return err
	}
	p.setBraid(b)
	return nil
}

// deleteOwn erases entries of the own strand from the disk and reopens; the arguments are entry indexes or "tail N".
func (p *peer) deleteOwn(ctx context.Context, b *kolobraid.Braid, args []string) (any, error) {
	strand, ok := p.ownStrand(b)
	if !ok || len(args) < 1 {
		return nil, errors.New("write an entry first, then deleteown INDEX... or deleteown tail N")
	}
	tail := 0
	var idx []uint64
	if args[0] == "tail" && len(args) == 2 {
		n, err := strconv.Atoi(args[1])
		if err != nil || n < 1 {
			return nil, errors.New("bad count")
		}
		tail = n
	} else {
		for _, a := range args {
			i, err := strconv.ParseUint(a, 10, 64)
			if err != nil {
				return nil, err
			}
			idx = append(idx, i)
		}
	}
	err := p.reopen(ctx, b, func() error {
		db := kolobraid.PrefixBackend(p.be, append([]byte{strandPrefix}, p.id.PublicKey()...))
		st := kolostore.New(db)
		defer st.Close()
		l, err := st.Open(p.id, strand, kololog.WithValueEncoding(kololog.Bytes))
		if err != nil {
			return err
		}
		defer l.Close()
		for i := range uint64(tail) {
			idx = append(idx, l.Len()-1-i)
		}
		return l.Delete(idx...)
	})
	return map[string]any{"deleted": len(idx)}, err
}
