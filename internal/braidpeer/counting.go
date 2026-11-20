package braidpeer

import (
	"bytes"
	"net"
	"sync/atomic"

	kolostore "github.com/ciranda-dev/kolo-store"
)

// strandPrefix starts the keys of every strand log in a braid's backend (followed by the writer key).
const strandPrefix = 'l'

// counters holds what a peer measures about itself.
type counters struct {
	udpInBytes, udpOutBytes, udpIn, udpOut atomic.Int64
	commits, replicaCommits, ownCommits    atomic.Int64
	replicaBytes                           atomic.Int64
}

// packetConn counts the datagrams that cross the peer's UDP socket.
type packetConn struct {
	net.PacketConn
	c *counters
}

func (p *packetConn) ReadFrom(b []byte) (int, net.Addr, error) {
	n, a, err := p.PacketConn.ReadFrom(b)
	if err == nil {
		p.c.udpIn.Add(1)
		p.c.udpInBytes.Add(int64(n))
	}
	return n, a, err
}

func (p *packetConn) WriteTo(b []byte, a net.Addr) (int, error) {
	n, err := p.PacketConn.WriteTo(b, a)
	if err == nil {
		p.c.udpOut.Add(1)
		p.c.udpOutBytes.Add(int64(n))
	}
	return n, err
}

// backend counts storage commits; one that writes a followed strand's log is a pull chunk stored.
type backend struct {
	kolostore.Backend
	own []byte
	c   *counters
}

func (b *backend) Put(key, value []byte) error {
	err := b.Backend.Put(key, value)
	if err == nil {
		b.c.commits.Add(1)
	}
	return err
}

func (b *backend) NewBatch() kolostore.Batch {
	return &batch{Batch: b.Backend.NewBatch(), b: b}
}

type batch struct {
	kolostore.Batch
	b              *backend
	replica, owned bool
	size           int
}

func (x *batch) Put(key, value []byte) {
	if len(key) >= 33 && key[0] == strandPrefix {
		if bytes.Equal(key[1:33], x.b.own) {
			x.owned = true
		} else {
			x.replica = true
			x.size += len(key) + len(value)
		}
	}
	x.Batch.Put(key, value)
}

func (x *batch) Commit() error {
	err := x.Batch.Commit()
	if err != nil {
		return err
	}
	c := x.b.c
	c.commits.Add(1)
	if x.replica {
		c.replicaCommits.Add(1)
		c.replicaBytes.Add(int64(x.size))
	}
	if x.owned {
		c.ownCommits.Add(1)
	}
	return nil
}
