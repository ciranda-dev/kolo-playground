# kolo-playground

A private test tool for Kolo: peers find each other through a kolo-node and ping each other.

[![status: experimental](https://img.shields.io/badge/status-experimental-orange)](#status)
[![license: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue)](#license)
[![go: 1.26+](https://img.shields.io/badge/go-1.26%2B-00ADD8)](go.mod)

`kolo-peer` joins a topic with [kolo-swarm](https://github.com/ciranda-dev/kolo-swarm),
using one or more [kolo-node](https://github.com/ciranda-dev/kolo-node)
bootstrap servers. It connects to the other peers on the topic, pings each
one every 5 seconds and logs one line per event. With `--relay`, a peer
that hole punching cannot reach is dialed through a
[kolo-relay](https://github.com/ciranda-dev/kolo-relay) server. This
repository is not a library and is not public. Nothing in it goes into the
Kolo libraries.

## Install

Every dependency is a published Kolo library. Build both binaries into a
directory you choose, with kolo-node checked out next to this repository:

```sh
bin=$(mktemp -d)
(cd ../kolo-node && go build -o "$bin/kolo-node" ./cmd/kolo-node)
go build -o "$bin/kolo-peer" ./cmd/kolo-peer
echo "$bin"
```

In every other terminal, set `bin` to the directory that printed. For
another machine, cross-compile, for example with
`GOOS=linux GOARCH=amd64 CGO_ENABLED=0`, and copy the two binaries.

## Usage

### One machine, three terminals

Terminal 1, the bootstrap node:

```sh
dir=$(mktemp -d)
"$bin/kolo-node" run -listen 127.0.0.1:7447 -identity "$dir/state/node.key"
```

Terminal 2, then terminal 3:

```sh
"$bin/kolo-peer" --bootstrap 127.0.0.1:7447 --topic demo --allow-loopback
```

Terminal 3, the second peer, finds the first one and connects:

```
09:21:03 id 33c3ee27
09:21:03 bootstrap reached
09:21:03 joined topic "demo"
09:21:03 peer found ee8e66a5
09:21:03 peer connected ee8e66a5 direct
09:21:08 ping ee8e66a5 rtt 0.5ms
09:21:13 ping ee8e66a5 rtt 0.6ms
```

Terminal 2 shows `peer connected 33c3ee27 direct` and its own pings. Its
`peer found 33c3ee27` line comes only at its next lookup, 10 to 12 minutes
after it joined, since the other peer found it first. Press Ctrl-C in
terminal 3:

```
09:21:15 stopping
09:21:15 stopped
```

and terminal 2 prints:

```
09:21:15 peer lost 33c3ee27: koloswarm: connection lost
```

Without `--allow-loopback`, kolo-peer exits with
`kolo-peer: --bootstrap 127.0.0.1:7447 is a loopback address; add --allow-loopback to run peers on one machine`.

### Two machines on the same Wi-Fi

On machine A, find its address (`ipconfig getifaddr en0` on macOS,
`hostname -I` on Linux), here `192.168.1.20`, and start the node:

```sh
dir=$(mktemp -d)
"$bin/kolo-node" run -listen :7447 -identity "$dir/state/node.key"
```

On machine B, check that the node answers:

```sh
"$bin/kolo-node" check 192.168.1.20:7447
```

It prints `ok 192.168.1.20:7447`. Then start a peer in a second terminal
on A, and one on B:

```sh
"$bin/kolo-peer" --bootstrap 192.168.1.20:7447 --topic demo
```

The lines are the same as on one machine. On A too, use the LAN address,
not `127.0.0.1`: kolo-dht gives out loopback addresses only from a
loopback node, and B cannot reach those. If macOS asks whether kolo-node
or kolo-peer may accept incoming connections, allow it. If `check` prints
`failed`, the Wi-Fi may keep its clients apart (guest networks often do)
or a firewall drops UDP port 7447.

### Two different networks

The node needs a machine with a public IP address, here `203.0.113.10`,
and UDP port 7447 open. On that machine:

```sh
dir=$(mktemp -d)
"$bin/kolo-node" run -listen :7447 -identity "$dir/state/node.key"
```

Open the port in the host firewall, with ufw `sudo ufw allow 7447/udp`,
and in the provider's firewall or security group if it has one. "Running
on a VPS" in the kolo-node README sets up a permanent node.

From each peer's network, check the port:

```sh
"$bin/kolo-node" check 203.0.113.10:7447
```

`ok 203.0.113.10:7447` means the UDP path works.
`failed 203.0.113.10:7447: no kolo-dht server answered` means it does not:
the firewall, the address or the node is wrong. The peers need no open
port. Start one on each machine:

```sh
"$bin/kolo-peer" --bootstrap 203.0.113.10:7447 --topic demo
```

When hole punching works, the lines are the same as above, with larger
round trip times. When it fails, the peer that dials prints this line,
again and again at growing intervals while the swarm retries:

```
09:30:07 peer unreachable ee8e66a5: hole punching failed, no relay, retrying
```

Two peers behind NATs that pick a new port for each destination (random,
or symmetric, mappings, common on mobile networks) cannot reach each other
this way: kolo-dht returns `ErrUnreachable` at once and kolo-swarm reports
`Unreachable`. With only one such peer, kolo-dht tries a birthday punch;
when it fails, `Unreachable` comes after about 45s, and the next attempt
waits a cooldown of 2 to 16 minutes. A peer whose punched connections are
full answers try later instead, for as long as they last, and is never
reported unreachable. There is no automatic fallback: without `--relay`
such a pair does not connect. Add `--verbose` to see the addresses
involved.

### With a relay

kolo-relay has no binary, so kolo-peer runs the relay with
`--serve-relay`. On the machine with the public address, next to the
node, open UDP port 7448 as well (`sudo ufw allow 7448/udp`) and run:

```sh
"$bin/kolo-peer" --serve-relay --listen :7448 --key "$dir/state/relay.key"
```

`--key` keeps the relay's key across restarts. The relay prints its key,
which the peers need, and its listen address:

```
09:29:00 id 5e6f7a8b
09:29:00 relay ready key 5e6f7a8b9c0d1e2f30415263748596a7b8c9dae0f1021324354657687980a1b2 listen 0.0.0.0:7448
```

With `--listen :7448` the address is `0.0.0.0:7448`, so the peers use the
machine's public IP with that port, here `203.0.113.10:7448`.

This relay serves any key that reaches it, with the kolo-relay defaults:
64 reservations, 16 sessions (4 per key), 1 MiB/s per client connection and
1 GiB per session in each direction. It has no allow-list, so anyone who can
reach the port can relay traffic through it within those limits: it is a
relay for tests, not for permanent use. Open the port only while you test and
stop the relay with Ctrl-C afterwards.

Start each peer with the relay's address and key, joined by `@`:

```sh
"$bin/kolo-peer" --bootstrap 203.0.113.10:7447 --topic demo \
  --relay 203.0.113.10:7448@5e6f7a8b9c0d1e2f30415263748596a7b8c9dae0f1021324354657687980a1b2
```

Each peer reserves a slot on the relay before it joins the topic. When
hole punching fails, the peer that dials connects through the relay:

```
09:30:02 id 33c3ee27
09:30:02 bootstrap reached
09:30:02 relay reserved
09:30:02 joined topic "demo"
09:30:02 peer found ee8e66a5
09:30:07 peer unreachable ee8e66a5: hole punching failed, trying the relay
09:30:07 peer connected ee8e66a5 relayed
09:30:12 ping ee8e66a5 rtt 48.3ms
```

The other peer prints `peer connected 33c3ee27 relayed`. A peer that hole
punching reaches still connects directly. Both peers must name the same
relay, since a kolo-dht record does not say where a peer reserved. If the
other peer has no reservation there, the dial fails, and the next
unreachable line tries again:

```
09:30:07 relay ee8e66a5 failed: kolorelay: target unavailable through the relay
```

A relay dial and its hello hold one slot for at most 30s, a deliberate
cap: kolo-relay's own bounds add up to about 60s (10s and 10s to connect
to the relay, 20s for the Connect, 10s and 10s for the end-to-end
handshake) and the hello up to 10s more, while a working relay needs a few
round trips. A dial that hits the cap fails with:

```
09:30:37 relay ee8e66a5 failed: context deadline exceeded
```

At most `--max-peers` relay dials run at once. While they all run, an
unreachable peer waits for the next try:

```
09:30:08 peer unreachable 5d6e7f80: hole punching failed, relay busy, retrying
```

The relay forwards end-to-end encrypted packets it cannot read. It ends a
reservation after 1 hour; both peers then print
`relay reservation ended: kolorelay: reservation ended: kolotransport: stream reset by peer (code 2)`.
Connected peers stay connected, but kolo-peer does not reserve again. The
relay also ends each session after 1 hour (kolo-relay's
`MaxSessionDuration`). Once a peer's reservation is gone, no peer can
reach it through the relay, so a pair that lost its relayed connection
connects again only after a restart. To try it on one machine, run the
relay with `--listen 127.0.0.1:7448` and the peers with
`--relay 127.0.0.1:7448@KEY --allow-loopback`.

## API

kolo-peer has no importable package. Its interface is the command line,
the log lines and the exit codes.

```
kolo-peer --bootstrap IP:PORT --topic NAME [flags]
kolo-peer --serve-relay --listen IP:PORT [--key FILE] [--verbose]
```

| Flag | Default | Meaning |
|---|---|---|
| `--bootstrap` | required | `IP:PORT` of a kolo-dht bootstrap node such as kolo-node; repeat it for more, at most 8 |
| `--topic` | required | topic name, 1 to 256 bytes of UTF-8 (see Formats) |
| `--listen` | `:0` | UDP address for kolo-dht and QUIC, or for the relay with `--serve-relay`: `IP:PORT` or `:PORT` |
| `--key` | empty | identity file, loaded, or created with mode 0600 (its directory 0700) if missing; empty means a new identity in a new temporary directory, removed at the first signal or on exit |
| `--relay` | empty | `IP:PORT@KEY` of a kolo-relay server, KEY its public key in 64 hex digits: reserve there, and dial through it each peer kolo-swarm reports unreachable, at most `--max-peers` dials at once |
| `--serve-relay` | off | run a kolo-relay server on `--listen`, which it needs, instead of a peer; only `--listen`, `--key` and `--verbose` go with it |
| `--allow-loopback` | off | allow a loopback relay, bootstrap nodes and peers, to run peers on one machine |
| `--max-peers` | 4 | most peers connected or being dialed, 1 to 32 (kolo-swarm `MaxPeers`); relay dials, which kolo-swarm does not see, have the same bound of their own |
| `--verbose` | off | add addresses, the full key and the topic hash to the log lines |

Addresses are IPv4: IPv6 input is refused, the IPv4-mapped form
included, with exit code 2.

Without `--allow-loopback`, a loopback `--bootstrap` or `--relay` is an
error, and the swarm's `Allow` hook refuses loopback addresses: it dials no loopback
record and closes every connection from a loopback address before its
hello. Private and link-local addresses are always allowed.

Log lines go to stdout, each prefixed with the local time. A peer is shown
by the first 8 hex digits of its key. With `--verbose`, `id` adds the full
key and the listen address, `bootstrap reached` adds the bootstrap
addresses, `relay reserved` adds the relay's address, `joined topic` adds
the topic hash, and the `peer` lines add ` at IP:PORT` after the ID, except
for a relayed peer, which has no address of its own.

| Line | When |
|---|---|
| `id 1a2b3c4d` | at start |
| `bootstrap reached` | a bootstrap node answered |
| `joined topic "demo"` | our record is stored and the first lookup runs |
| `peer found 1a2b3c4d` | a lookup returned a peer |
| `relay ready key 5e6f... listen 0.0.0.0:7448` | with `--serve-relay`, the relay listens; always with its full key and address, its public identity |
| `relay reserved` | with `--relay`, the relay holds our slot |
| `peer connected 1a2b3c4d direct` | a connection is up; `relayed` instead of `direct` when it goes through the relay |
| `ping 1a2b3c4d rtt 0.5ms` | an echo came back |
| `ping 1a2b3c4d failed: timeout` | no echo within 4s (or `bad reply`, or a kolo-transport error) |
| `peer unreachable 1a2b3c4d: hole punching failed, no relay, retrying` | kolo-dht could not connect to a peer found |
| `peer unreachable 1a2b3c4d: hole punching failed, trying the relay` | the same with `--relay`, once per relay dial |
| `peer unreachable 1a2b3c4d: hole punching failed, relay busy, retrying` | the same, while `--max-peers` relay dials run |
| `relay 1a2b3c4d failed: kolorelay: target unavailable through the relay` | the relay dial or the hello failed (or a kolo-relay, kolo-swarm or kolo-transport error, or `context deadline exceeded` after 30s); the next unreachable tries again |
| `relay reservation ended: kolorelay: reservation ended: ...` | the relay ended our slot; the reason is a kolo-relay error |
| `peer lost 1a2b3c4d: koloswarm: connection lost` | a connection ended; the reason is a kolo-swarm error |
| `stopping`, `stopped` | after SIGINT, SIGTERM or SIGHUP (unless SIGHUP was ignored at start, as `nohup` does) |

Errors go to stderr as one `kolo-peer: ...` line, such as
`kolo-peer: bootstrap: kolodht: no server answered`.

Exit codes: 0 after SIGINT, SIGTERM or SIGHUP once everything is closed;
1 if the identity, the socket, the bootstrap, the relay reservation or the
join fails, or the shutdown takes over 10s; 2 for an invalid flag. A
second signal ends the process at once.

## Formats

The topic is `SHA-256("kolo-playground/topic/v0" ‖ name)`, with the name
in UTF-8. To compute it:

```sh
printf '%s%s' kolo-playground/topic/v0 demo | shasum -a 256
```

`demo` gives
`1f8c1d5e6d722c95994fdad6fef7ab2f646428813b3ea58d0e4f4e8e89386241`.

A ping is one bidirectional stream. The pinging side sends 9 bytes and
ends the stream:

    uvarint(version 0) ‖ nonce (8 random bytes)

The other side sends the same 9 bytes back and ends the stream. Anything
else, more than 9 bytes, or nothing within 4s resets the stream with code
1. There is no JSON form.

## Design

- One swarm, one topic, joined with announce and lookup.
- A new stream per ping, since a QUIC stream costs no round trip. One ping
  is in flight per peer, and the answering side serves one ping at a time
  and reads at most 10 bytes; further streams wait, capped by
  kolo-transport's `MaxIncomingStreams` (default 16). Inbound pings are not
  logged, so a peer cannot fill the log.
- The round trip time runs from the write of the ping to the end of the
  echo.
- Short IDs, and no address or full key unless `--verbose`, except on the
  relay's `relay ready` line.
- The default identity is new on each run, in a temporary directory that
  is removed at the first signal, so a second signal leaves nothing, or on
  exit. After SIGKILL the directory stays in the system's temporary
  folder.
- The topic hash has its own context, so a playground topic never matches
  another application's topic of the same name. Anyone who knows the name
  can join.
- A relay only with `--relay`, and only for a peer kolo-swarm reports
  unreachable: kolo-peer dials it through kolo-relay and hands the
  connection to kolo-swarm with `Attach`. Without `--relay` there is no
  fallback.
- Shutdown closes the swarm and waits, up to 10s, for every goroutine
  kolo-peer started, so the reservation is closed before the process
  exits.
- One reservation per run, kept after the relay ends it, since closing it
  would close the peers it brought.
- IPv4 only, as the Kolo libraries: IPv6 input is refused.
- No config file and no DNS names: the bootstrap list is IP addresses.

## Braid scenarios

`braid-peer` is one [kolo-braid](https://github.com/ciranda-dev/kolo-braid) member as its own process, and `braid-scenarios`
starts several of them on 127.0.0.1, each in a temporary directory, and runs multi-peer scenarios for kolo-braid. Each
scenario ends with the same settled digests on every honest peer, or with the halt it expects, and prints a cost report.

List the scenarios:

```sh
go run ./cmd/braid-scenarios -list
```

Run one:

```sh
go run ./cmd/braid-scenarios -run partition-heal
```

Run one peer by hand, as the founder of a new braid in a temporary directory (it reads commands on stdin):

```sh
go run ./cmd/braid-peer --first
```

`braid-scenarios` with no flag runs every short scenario in turn; `-run REGEXP` picks scenarios by name, `-long` adds the long
one (`late-joiner-10k`), `-timeout` bounds one scenario (15 minutes) and `-peer-bin FILE` runs that `braid-peer` instead of
the scenario binary itself. It exits 1 if a scenario failed. Every peer runs in a temporary directory and in its own process
group, and all of them are killed when a scenario ends, also when it fails; a peer also exits when its stdin closes.

| Scenario | What it checks |
|---|---|
| `convergence` | 3, 4 and 5 peers write at once and settle the same prefix |
| `discovery` | 4 peers find each other through a kolo-dht entry node and a kolo-swarm topic, with no address handed out |
| `one-device` | a one-device braid reopens and writes |
| `partition-heal` | 3/2 partition of 5 peers: the side with a quorum settles, the other settles nothing; after the heal all agree |
| `admins-concurrent` | one admin adds a member while another removes one, on two sides of a partition |
| `mutual-removal` | two admins remove each other at once: exactly one stays, on every peer |
| `writer-fork` | a writer signs two histories and sends one to each half: the braid keeps settling and fences the writer |
| `seat-fork-twothirds` | a seat does so in a TwoThirds braid: settlement halts on every honest peer and their settled prefixes agree |
| `clone-halt` | a settler's data directory run twice with four settlers, both writing while cut off: settlement halts, no conflicting prefix |
| `former-settler-join` | a replaced settler signs a late second history while a peer joins: the joiner settles the same prefix |
| `spam` | a member floods 2,500 entries, announces heads that do not verify and erases its own entries |
| `kill9` | a peer is killed up to three times while it settles a run of 2,000 entries, restarted each time, and agrees in the end |
| `restore-behind` | a device restored from an old copy is only behind: it catches up and writes |
| `restore-inconsistent` | a restored copy that wrote offline is refused writes; the member joins again with a new strand |
| `settlers-stopped` | two of three settlers stopped, one back: settlement resumes |
| `new-settler-offline` | a new settler that never comes online does not stop settlement |
| `pulsegap-mixed` | settlers with a PulseGap from 10ms to 1s settle the same prefix |
| `late-joiner-10k` | a peer joins after 10,000 entries and settles the same prefix (long) |

The cost report has the settlement times, the ANNOUNCE messages and bytes the peers sent to a pin run by `braid-scenarios`
(the wire is encrypted, so they are counted there), the storage commits and bytes of pulls, the UDP bytes of the peers'
sockets (not in `discovery`) and the admission times.

`braid-peer` takes flags and, on stdin, commands of the form `ID COMMAND ARGS`; it prints one JSON object per line: a `ready`
line, one `reply` per command, and a `braid` line for each notable event (not for applied or settled).

| Flag | Meaning |
|---|---|
| `--dir DIR` | data directory (a new temporary one when empty); the identity is `DIR/identity` |
| `--listen 127.0.0.1:PORT` | UDP address, port 0 by default |
| `--first` / `--join` | create the braid / make a strand to join and print the join request; neither opens the braid in `--dir` |
| `--peer ADDR@KEY` | dial this peer once the braid is open (repeatable) |
| `--bootstrap ADDR` | kolo-dht entry nodes: find peers through a kolo-swarm topic instead of dialing |
| `--pin KEY` | a pin (repeatable) |
| `--role honest\|hostile` | a hostile peer also runs `badhead` and `deleteown` |
| `--quorum`, `--max-members`, `--max-settlers`, `--min-settlers`, `--max-body`, `--checkpoint-every` | the group config, with `--first` |
| `--pulse-gap`, `--pivot-wait`, `--announce-gap`, `--confirm-wait`, `--max-tail`, `--max-pull-ahead` | the braid's limits |
| `--append-unconfirmed`, `--rebuild` | the braid's options of the same names |
| `--status-every D` | also print a `status` line every D (off by default) |

Commands: `status`, `put K V`, `del K`, `get K`, `putn PREFIX COUNT [SIZE]`, `junk COUNT SIZE`, `prefix N` (the order digest of
the first N settled positions), `admit REQUEST [default|plain|admin] [replace]`, `complete WELCOME`, `removemember KEY`,
`settlers majority|twothirds KEY...`, `link ADDR@KEY`, `unlink ADDR...`, `block ADDR...`, `unblock ADDR...`, `reopen`, `quit`, and
for a hostile peer `badhead ADDR@KEY` and `deleteown INDEX...` or `deleteown tail N`. The welcome and the join request travel
through the commands, which stand in for the invite channel; `Admit` and `Complete` are the real calls.

Test the harness and every short scenario:

```sh
go test ./...
```

Add the long scenario:

```sh
go test -count=1 ./cmd/braid-scenarios -run TestScenarios/late-joiner-10k -long -timeout 30m
```

## References

- [kolo-swarm](https://github.com/ciranda-dev/kolo-swarm), [kolo-node](https://github.com/ciranda-dev/kolo-node), [kolo-dht](https://github.com/ciranda-dev/kolo-dht), [kolo-id](https://github.com/ciranda-dev/kolo-id), [kolo-relay](https://github.com/ciranda-dev/kolo-relay), [kolo-transport](https://github.com/ciranda-dev/kolo-transport)

## Status

Private test tool, experimental.

## License

Licensed under either of the [Apache License, Version 2.0](LICENSE) or the [MIT license](LICENSE-MIT), at your option.
