# Distribution

This page explains how lockstep's actors work across nodes: how messages become bytes, how
remoting carries them between systems, how a cluster tracks its members, and how virtual actors
find a home among them. The [README](../README.md#send-messages-across-nodes) shows the API with
small programs; this page describes what happens underneath, what is guaranteed, and how to test
all of it in a simulation. The [architecture page](architecture.md) describes the runtime that
these packages build on.

Three packages and a nested module do the work:

- `lockstep/codec` maps Go types to wire names and turns values into bytes with a codec. Its
  `codec.JSON` codec is the default.
- `lockstep/codec/protobuf`, a module of its own, is a codec for protocol buffer messages.
- `lockstep/remote` connects systems over TCP and carries sends, asks, replies, lookups and death
  watches between them.
- `lockstep/cluster` runs the membership protocol and hosts virtual actors, called grains.

## Codecs and wire names

A message to an actor on another node travels as a wire name and the bytes of its value. The wire
name tells the receiving node which Go type to decode the bytes into. A `codec.Registry` holds the
mapping in both directions:

```go
reg := codec.NewRegistry()
codec.RegisterIn[Ping](reg, "orders.Ping")

name, data, err := reg.Encode(codec.JSON{}, Ping{Seq: 1})
if err != nil {
	panic(err)
}
fmt.Println(name, string(data))

// ... on the receiving node:
v, err := reg.Decode(codec.JSON{}, name, data)
if err != nil {
	panic(err)
}
fmt.Printf("%T %+v\n", v, v)
```

```text
orders.Ping {"Seq":1}
codec_test.Ping {Seq:1}
```

Here `Ping` is `struct{ Seq int }`. `codec.Default()` returns the registry of the process, which
`codec.Register` adds to, and which `remote` and `cluster` use unless their `Config` names another.
`codec.NewRegistry` returns a private one, for tests or for a system that must not share its types,
and `codec.RegisterIn` adds to it. A registry is safe for concurrent use. Register a type before
the first message of it is sent, typically from an `init` function or a package-level variable.

The mapping is one to one and never changes. A name holds one type and a type holds one name.
Registering the same name and type again does nothing, and any other reuse panics, because it is a
programming error that would otherwise show only when the first message is sent. Type identity is
exact: a struct `T`, its pointer `*T` and a named type declared over `T` are three types, and each
needs a name of its own.

A wire name is 1 to 255 bytes long. Its first byte is an ASCII letter, and the rest are ASCII
letters, digits, `.`, `_`, `/` and `-`. Names are case-sensitive. A prefix such as `orders.` or
`orders/v2.` keeps the names of different packages apart. Names that start with `lockstep.` are
reserved for lockstep's own types, such as the built-in types, except names that start with
`lockstep.cluster.`, which can be registered: package `cluster` registers the messages of the
membership protocol and the grain hosts under them.

Every registry starts with 19 built-in types, so a plain value, such as the `int64` answer to an
ask, crosses nodes without registration: `bool`, `string`, `int`, `int8` to `int64`, `uint`,
`uint8` to `uint64`, `float32`, `float64`, `[]byte`, `struct{}`, `time.Duration`, `time.Time` and
`lockstep.Address`. `uintptr` is not one of them. A `time.Time`
keeps only its instant: the monotonic clock reading and the location are lost, so compare a
received time with `Time.Equal`. `PID` and `ReplyTo` are generic, so no built-in covers them; they
cross nodes as fields of registered messages.

### Codecs

A `codec.Codec` has a name, such as `json` or `protobuf`, and four methods. `Marshal` must give
equal bytes for equal values, and `Unmarshal` must not keep the bytes it decodes after it returns,
because remoting reuses its read buffers. `Supports` reports whether the codec can encode a type,
and `Registry.Check` asks it about every registered type at once. `remote.Enable` calls `Check`, so
a type that the codec cannot encode fails when the system starts, not when its first message is
sent:

```go
reg := codec.NewRegistry()
codec.RegisterIn[Ping](reg, "orders.Ping")
codec.RegisterIn[Order](reg, "orders.Order")

fmt.Println(reg.Check(codec.JSON{}))
```

```text
codec: unsupported type: json cannot encode codec_test.Order: field Note: interface type error
```

Here `Order` has a field `Note` of type `error`, an interface that `encoding/json` cannot decode.

`codec.JSON` is built on `encoding/json`. It writes struct fields in declaration order and map keys
sorted, so equal values give identical bytes. It does not escape `<`, `>` and `&`, and its bytes do
not end in a newline. It decodes with `json.Unmarshal`, so a field that the receiving type does not
have is ignored: a node can add a field to a message type before its peers know it. Addresses, PIDs
and `ReplyTo` values encode as strings of their text forms.

`JSON.Supports` rejects a type that `encoding/json` would encode lossily or not at all: interface,
channel, function and complex types anywhere in the type, a struct with an unexported field, a
marshal method without its unmarshal method, map keys that would not read back the same, and two
fields that get the same JSON name. The error names the path to the rejected field. Some loss
depends on the value, and no check of the type can find it: invalid UTF-8 in a string becomes
U+FFFD.

### Protocol buffers

The nested module `github.com/hmdsefi/lockstep/codec/protobuf` holds `protobuf.Codec`, for messages
that are already protocol buffers. It lives in its own module so that the lockstep module never
requires `google.golang.org/protobuf`:

```sh
go get github.com/hmdsefi/lockstep/codec/protobuf@v0.3.0
```

A generated message type is registered as its pointer type and decodes as a new value of it:

```go
reg := codec.NewRegistry()
codec.RegisterIn[*wrapperspb.StringValue](reg, "orders.Note")
if err := reg.Check(protobuf.Codec{}); err != nil {
	panic(err)
}

name, data, err := reg.Encode(protobuf.Codec{}, wrapperspb.String("hello"))
if err != nil {
	panic(err)
}
fmt.Printf("%s % x\n", name, data)

// ... on the receiving node:
v, err := reg.Decode(protobuf.Codec{}, name, data)
if err != nil {
	panic(err)
}
note, ok := v.(*wrapperspb.StringValue)
fmt.Printf("%T %t %s\n", v, ok, note.GetValue())
```

```text
orders.Note 0a 05 68 65 6c 6c 6f
*wrapperspb.StringValue true hello
```

`wrapperspb.StringValue` stands in for a generated message type of your own. The codec marshals
deterministically, with map entries in key order. The built-in types travel as the well-known
wrapper messages, such as `google.protobuf.Int64Value` and `google.protobuf.Timestamp`, so another
protocol buffer implementation can read them, and decoding into a narrower type checks the range.
A struct that is not a message, or a named type over a built-in type, is not supported. A nil
message pointer encodes as an empty message and decodes as a new empty message, not as nil.

Every node that talks to another uses the same codec: the connection handshake compares the codec
names, and two nodes with different codecs refuse to connect. A cluster member keeps `codec.JSON`
for remoting, because the messages of package `cluster` are plain structs, and can use
`protobuf.Codec` for its grains' messages; [Membership](#membership) explains why.

## Remoting

`remote.Enable(sys, cfg)` switches remoting on for a system built by `lockstep.New`. From then on,
a message to an address whose node is not the local node goes through remoting: the sender's call
encodes the message and puts it in a bounded queue for that peer, and the receiving node decodes it
and delivers it to the local actor as if a local sender had sent it.

`Enable` checks, in order, that remoting is not enabled on the system already, that the `Config`
is valid, that `Listen` is set, that the codec supports every registered type, and that it can
listen on `Listen`. It returns the first error and leaves nothing open. It returns an error that
wraps `lockstep.ErrStopped` on a system that is shutting down.

`remote.Config` holds the settings of one node. A zero field takes its default, and
`Config.Validate` fills the defaults in and checks the result:

| Field | Default | Meaning |
|---|---|---|
| `Listen` | none, required | the `host:port` this node listens on |
| `Advertise` | the listener's address | the `host:port` peers dial back |
| `Peers` | none | node name to `host:port` |
| `Resolve` | none | asked for a node that is not in `Peers` |
| `TLS` | plain TCP | a `*tls.Config` for both the listener and the dialer |
| `Registry` | `codec.Default()` | the message types that cross nodes |
| `Codec` | `codec.JSON{}` | how messages are encoded |
| `MaxFrame` | 1 MiB | the largest frame, from 4 KiB to 64 MiB |
| `OutboundQueue` | 1024 | frames per peer not yet written |
| `Heartbeat` | 1s | the interval between pings |
| `IdleTimeout` | 10s | how long a connection may stay silent; at least twice `Heartbeat` |
| `DialTimeout` | 5s | the bound on one dial and its handshake |
| `RedialMin`, `RedialMax` | 100ms, 10s | the bounds of the redial base, see below |

A node finds a peer's address in `Peers`, else asks `Resolve`, else uses the address the peer
announced when it last connected. `Resolve` is called while remoting holds a lock that every send
to another node takes, so it must return at once, must not block on the network, and must never
send to the system. A listener on every interface, such as `0.0.0.0:7000`, has no address to
announce, so such a node sets `Advertise`. A message to a node with no address becomes a dead
letter.

### Connections

A node opens one outgoing connection to each peer, when it first sends to that peer. Messages flow
only on outgoing connections, so two nodes that send to each other hold two connections, one dialed
by each. The incoming side of a connection writes only the answers to the handshake and the pings,
and the message that closes it.

The dialer opens with a hello that carries its node name, the name of the node it expects to
reach, the incarnation of its process, the largest frame it takes, its codec's name and its
advertised address. The acceptor answers with the same fields, or refuses the connection when the
protocol version differs, when the hello is malformed, when it comes from the acceptor's own node
name, when it names another node, or when the codecs differ. Every frame carries a version byte,
`remote.ProtocolVersion`, and a length, and a frame longer than the receiver's `MaxFrame` closes the
connection.

An outgoing connection that has written nothing, or received nothing, for one `Heartbeat` writes a
ping, and every ping gets an answer. A connection that has received nothing for `IdleTimeout`
closes. A failed dial, a refused handshake or a dial that takes longer than `DialTimeout` is
retried after a wait. After `f` failures in a row, the base is `b = min(RedialMax, RedialMin <<
(f-1))`, and the wait is drawn uniformly from `[b/2, b]` from remoting's own random stream for
this node and process, so in a simulation the seed decides it. A node redials only while it has
messages queued for the peer or watches of the peer's actors.

### Order, loss and dead letters

Frames are numbered on each connection. The receiver accepts the next number, accepts a higher one
after it records the gap, and discards a lower one. So messages from one sender to one receiver
arrive in send order and at most once, and a broken connection can lose some of them. Remoting
never retries a message; [Message delivery](delivery.md) describes how actors handle duplicates and
loss themselves.

A send to another node fails at once, and nothing is queued, when the message's type is not
registered, when the codec cannot encode it, or when its frame is larger than this node's
`MaxFrame` (the error wraps `remote.ErrFrameTooLarge`). Other problems turn the message into a
dead letter on the sending node, with a reason:

| Reason | When |
|---|---|
| `unknown-node` | no address is known for the peer |
| `queue-full` | the peer's queue already holds `OutboundQueue` frames |
| `too-large-for-peer` | the frame is larger than the limit the peer announced |
| `peer-restarted` | the frame was queued for a process of the peer that has restarted |
| `shutdown` | the frame was still queued when the system shut down |
| `wrong-type` | on the receiving node, the actor there does not take the message's type |

An ask whose request becomes a dead letter on the sending node fails, with an error that depends
on the reason:

- `unknown-node` and `queue-full`: an error that wraps `remote.ErrUnreachable`.
- `too-large-for-peer`: the frame size error, which wraps `remote.ErrFrameTooLarge`.
- `peer-restarted`: the restart error, which matches both `remote.ErrPeerRestarted` and
  `lockstep.ErrTerminated`, as for every ask that waits on the old process.
- `shutdown`: the system's shutdown fails its pending asks with `lockstep.ErrStopped`.

Queued messages wait across a reconnect.

### Asks, lookups and watches

An ask to an actor on another node carries the address of the reply, on the asking node, and the
reply travels back as a reply frame or a failure frame. A failure that the other node reports
arrives as a `*remote.Error`. Its `Code` says what kind of failure it was, and `errors.Is` finds the
matching lockstep error through it:

```go
var err error = &remote.Error{Node: "b", Code: remote.CodeNotFound, Message: "no actor at /user/greeter"}

var re *remote.Error
if errors.As(err, &re) {
	fmt.Println(re.Node, re.Code)
}
fmt.Println(errors.Is(err, lockstep.ErrNotFound))
fmt.Println(err)
```

```text
b not-found
true
remote: b: no actor at /user/greeter
```

The codes are `CodeTimeout`, `CodeTerminated`, `CodeNotFound`, `CodeWrongType`, `CodeMailboxFull`,
`CodeOverloaded` and `CodeStopped`, each standing for the lockstep error of the same name, and
`CodeOther`, which stands for none. The ask's timeout runs on the asking node. A request whose
target is not there fails with an error that matches `lockstep.ErrTerminated`, as on one node. A
message, a request or a reply of a type that the receiver does not take is discarded, and an ask
then fails with an error that wraps `lockstep.ErrWrongType`. In a simulation, such a message also
fails the seed with `lockstep/misuse`.

`remote.Lookup[M](ctx, sys, node, path)` asks a node for the actor registered at a path and returns
its PID, with its uid, when its message type is exactly `M`. Otherwise it returns a `*remote.Error`
with `CodeNotFound` or `CodeWrongType`. It waits as `Reply.Await` does, with the deadline of `ctx`
as its timeout. On the system's own node it is `lockstep.Lookup`, and returns that function's
errors, `lockstep.ErrNotFound` and `lockstep.ErrWrongType`, without a `*remote.Error`.
`lockstep.PIDAt` needs no lookup: a path PID reaches whichever actor is registered at its path when
a message arrives.

`lockstep.Watch` of an actor on another node registers the watch with that node, and the
`Terminated` signal comes back with the actor's stop reason. `Unwatch` cancels it. Watches are sent
again on every new connection, so they outlive a broken one. A watch of an actor that is not alive
ends at once with the reason `NoActor`. One peer may hold at most 65,536 watches on a node; a watch
beyond that ends at once with a failure.

### Restarts and shutdown

Each connection's handshake names the incarnation of the peer's process, so a node sees that a peer
restarted when a new incarnation connects. It then ends what it held for the old process: the
frames queued for it become dead letters `peer-restarted`, the asks waiting on it fail with an error
that matches both `remote.ErrPeerRestarted` and `lockstep.ErrTerminated`, the watches of its actors
end with a failure, and the old process's other connections close. An ask sent just before the
restart was seen fails this way too, so a caller that expects a peer to restart can ask once more
on `remote.ErrPeerRestarted`, as the README's simulation tests do.

A message queued while this node does not yet know the peer's incarnation, because it has never
connected to it, is kept. If the peer restarts before the first connection comes up, that message
reaches the new process.

`Shutdown` stops remoting after the last actor has stopped. It tells each peer and closes every
connection, turns the messages still queued into dead letters `shutdown`, and returns once the
goroutines of remoting have exited.

### Logs and metrics

`Enable` logs `remote: listening` at Info with the address it bound. With `WithMetrics`, it
registers seven more series, each with the `node` label:

| Name | Kind | Help |
|---|---|---|
| `lockstep_remote_frames_sent_total` | counter | `Remoting frames written to peers.` |
| `lockstep_remote_frames_received_total` | counter | `Remoting frames received from peers.` |
| `lockstep_remote_bytes_sent_total` | counter | `Remoting bytes written to peers, length prefixes included.` |
| `lockstep_remote_bytes_received_total` | counter | `Remoting bytes received from peers, length prefixes included.` |
| `lockstep_remote_reconnects_total` | counter | `Remoting sessions that came up to a peer that had one before.` |
| `lockstep_remote_gaps_total` | counter | `Remoting sequence gaps seen on incoming sessions.` |
| `lockstep_remote_sessions` | gauge | `Open remoting sessions.` |

The dead letters of the table above count in `lockstep_deadletters_total` under their reasons.

## Membership

`cluster.Join(sys, cfg)` makes a system a member of a cluster and returns a `*cluster.Membership`
at once. It copies the `Config`, fills in its defaults and checks it, then starts the system actor
`/system/cluster`, which keeps the member table. Until the node has joined, the actor asks every
other seed in `Seeds` for its table, at once and then every `JoinRetryInterval`, and the first
answer joins it. A node whose seeds name no other node is a cluster of one, which others may join,
and has joined at once. A system joins at most once: a second `Join` returns
`cluster.ErrAlreadyJoined`, also after the node has left.

The members talk through remoting. On a production system, enable remoting before `Join`; in a
simulation, remoting is always on. Package `cluster` registers its own messages, those of the
membership protocol and of the grain hosts, in `codec.Default()` under names that start with
`lockstep.cluster.`. They are plain structs. So remoting on a member uses the default registry and
a codec that encodes plain structs, such as `codec.JSON`: with `protobuf.Codec`, `remote.Enable`
fails its codec check on these types, and a private registry does not hold them. The messages of
the grains themselves can still use another codec and registry, through `cluster.Config.Codec` and
`cluster.Config.Registry`.

`cluster.Config` holds the settings, and a zero field takes its default:

| Field | Default | Meaning |
|---|---|---|
| `Seeds` | none: a cluster of one | the nodes to join through; may include this node |
| `ProbeInterval` | 1s | the time between two probes |
| `ProbeTimeout` | 500ms | how long a probe waits before other members are asked; less than `ProbeInterval` |
| `IndirectProbes` | 3 | how many members are asked to probe a member that did not answer |
| `SuspicionTimeout` | 5s | how long a member stays suspected before it is dead; at least `ProbeInterval` |
| `SyncInterval` | 30s | the time between two exchanges of the whole table with a random member |
| `JoinRetryInterval` | 1s | the time between two attempts to reach the seeds |
| `RetransmitMult` | 4 | scales how many messages carry each update |
| `MaxPiggyback` | 8 | the most updates on one message |
| `DeadRetention` | 60s | how long a dead or departed member stays in the table |
| `Kinds` | none | the grain kinds this node hosts |
| `MaxHops` | 3 | how many times a grain message may be forwarded |
| `Registry`, `Codec` | `codec.Default()`, `codec.JSON{}` | how grain messages for other nodes are encoded |

`Config.Validate` checks the fields after it fills in the defaults, so the limits between fields
hold against the defaults of the fields left zero: setting only `ProbeInterval` to 10s fails,
because the default `SuspicionTimeout` is 5s. Every error wraps `cluster.ErrInvalidConfig` and
names the field.

### Members and incarnations

A member is known by its node name and its ID, the incarnation of the process that runs the node
(`System.Incarnation()`). A restarted process is a new member under the old name: the old ID goes
down and the new one comes up. A member is in one of four states. `Alive` and `Suspect` members are
up; `Dead` and `Left` members are down.

Each member also has an incarnation number, which only the member itself raises. News about a
member wins over what a node believes when it says more: `Left` beats everything, `Suspect` beats
`Alive` and `Dead` beats both at the same incarnation number, and `Alive` beats `Suspect` or `Dead`
only with a higher one.
So a member that hears it is suspected raises its own number and announces itself `Alive` again,
which refutes the suspicion everywhere. Nothing brings back a member that left under the same ID.
A dead or departed member stays in the table for `DeadRetention` and is then removed. A record of
the removed entry stays behind, so old news about that process does not add it again.

### Probes and suspicion

Every `ProbeInterval`, a node probes one other up member. It takes them in a new random order each
round, so while the members stay the same, it probes each of the other N-1 members at least once
in any `2*(N-1)` consecutive intervals. A probe without an answer
after `ProbeTimeout` is sent again through up to `IndirectProbes` other `Alive` members, which
probe the target and pass its answer back. A member that has answered neither by the next interval
becomes `Suspect`. It becomes `Dead` `SuspicionTimeout` later, unless it refuted the suspicion
meanwhile.

At every interval, a node also tells each member it suspects, other than the one it probes, that it
suspects it. A member that was only cut off, for less than `SuspicionTimeout` minus
`ProbeInterval`, hears of the suspicion within about one interval of the heal and refutes it before
it is declared dead. In a cluster of N nodes, a crashed node is `Dead` in the view of every other
node within about `2*(N-1)*ProbeInterval + ProbeTimeout + SuspicionTimeout + ProbeInterval`: 10.5
seconds for three nodes with the defaults.

### Gossip and state exchange

News about members travels on the probe traffic: each probe, indirect probe and answer carries up
to `MaxPiggyback` queued updates, those sent the fewest times first. Each update goes out on
`RetransmitMult` times as many messages as the member count has bits, 8 for three members with
the defaults, and is then dropped from the queue. A message also carries the sender's belief about
the recipient when that belief is `Suspect` or `Dead`, or the recipient's entry was removed, and
this update does not count against `MaxPiggyback`. So a suspected member learns of it from the next
message it gets.

Members also send each other their whole tables: when a node joins, every `SyncInterval` to one
`Alive` member chosen at random (never a `Suspect` one), and at the same interval to every dead
member and to every seed that is not up. That last exchange is how the two sides of a healed
partition learn of each other again.

The membership actor reads time only from `ctx.Now` and its timers, and randomness only from
`ctx.Rand`, so a simulation replays every probe order and every choice of helper exactly.

### Views and signals

`Membership.View` returns the last published view: an immutable snapshot of the member table, sorted
by node name, with a `Version` that grows by one with every published view and the node's own entry
in `Self`. A turn of the membership actor that changes the table publishes one view, however many
entries it changed. Reading it sends no message. `View.Up` lists the up members, and `View.Member`
finds one by name. The first view, published before `Join` returns, holds only the node itself.

`Membership.Subscribe(addr)` makes a local actor a subscriber. Its `Signal` method receives a
`lockstep.NodeUp` for every other member that is up when the subscription takes effect, in node
name order, then a `NodeUp` whenever a view shows another member come up and a `NodeDown` whenever
one goes down. A member that restarted gives a `NodeDown` of its old ID, then a `NodeUp` of its new
one. Every subscriber gets the signals in the order of the views, so for a subscriber that stays
running, the signals about each node alternate, starting with `NodeUp`. A subscriber that restarts
keeps its subscription, but a signal that reaches it while it is restarting is lost. So is one
that reaches it while it is suspended after a failure, unless its supervisor resumes it rather
than restarting or stopping it. After a restart, a subscriber reads `View` for the current members.

When a member goes down, every death watch that a local actor holds on an actor of that node ends
with a failure that wraps `cluster.ErrNodeDown`, and a new watch of an actor there ends at once,
until the node comes up again.

### Leaving

`Membership.Leave` makes the node leave gracefully: it marks itself `Left`, tells every up member,
publishes the view and stops the membership actor. The other members see it as `Left`, not `Dead`,
and their subscribers get `NodeDown` with `Left` set. `Shutdown` leaves the same way, and so does a
failure that stops the membership actor for good. After `Leave`, `Subscribe` and `Grain.Send`
return errors that wrap `cluster.ErrLeft`, and the system cannot join again.

When the membership actor fails and restarts, it rebuilds its table from the last published view
and keeps its subscribers. A `Shutdown` while the actor is between two incarnations does not leave
the cluster, because no incarnation runs to send the news: the other members see the node `Dead`.

## Virtual actors

A grain is a virtual actor in the style of [Orleans][orleans]: it is named by a kind and a key and
exists whenever a message is sent to it. The running actor of one grain is its activation. No code
spawns or stops activations; the hosts do.

[orleans]: https://www.microsoft.com/en-us/research/publication/orleans-distributed-virtual-actors-for-programmability-and-scalability/

### Kinds and keys

`cluster.NewKind[M](name, factory, opts...)` describes one kind. The name is 1 to 64 bytes of
`a-z`, `0-9`, `.`, `_` and `-`, unique among the kinds of a node. The factory is called with the
grain's key every time an activation starts, and the activation receives messages of type `M`. The
options and their defaults:

- `WithIdleTimeout(d)`: how long an activation may go without a message before it is stopped. The
  default is 10 minutes.
- `WithHandoffTimeout(d)`: how long a new owner waits for the previous owners to hand a grain over
  before it activates the grain anyway. The default is 5 seconds.
- `WithBuffer(n)`: how many messages a node holds for the grains of the kind while it cannot
  deliver them yet. The default is 1024.
- `WithActivationOptions(...)` and `WithHostOptions(...)`: the spawn options of every activation
  and of the kind's host.

`NewKind` checks nothing; `Join` reports an invalid kind with an error that wraps
`cluster.ErrInvalidConfig`. A node lists the kinds it hosts in `Config.Kinds`. A node with none is a
client: it sends to grains and hosts none.

A key is 1 to 256 bytes, any bytes. The activation's actor name is the key with every byte other
than `A-Z`, `a-z`, `0-9`, `-`, `.` and `_` written as `%XX`, and the same escaped form appears in
errors and trace records.

### Placement

The owner of a grain is chosen among the hosting members: the up members, `Suspect` ones included,
that list the grain's kind. Each of them gets a rendezvous score, `cluster.Score(node, kind, key)`:
the 64-bit FNV-1a hash of the kind, a zero byte, the key, a zero byte and the node name, passed
through the 64-bit finalizer of MurmurHash3. The member with the highest score owns the grain, and
equal scores go to the lowest node name. The ranking is done by the package `core` of
[tenure](https://github.com/hmdsefi/tenure). `cluster.Owner(view, kind, key)` returns the owner in a
view.

Every node computes the same owner from the same view, with no message. When a member joins, it
takes over only the keys it now scores highest for; when a member goes down, only its keys move,
each to its runner-up. The member's ID plays no part, so a node that restarts gets its keys back.

### Hosts and activation

`Join` starts the system actor `/system/grains` and, under it, one host per kind at
`/system/grains/<kind>`. A host runs the activations of the grains of its kind that this node owns,
as its children. The host's mailbox holds 1024 messages and refuses more, unless the kind's host
options set another mailbox. A host that fails restarts empty, and the activations it ran stop with
it.

`Grain.Send` and `cluster.Ask` send the message to the owner's host: as it is when this node is the
owner, else encoded with `Config.Registry` and `Config.Codec`. A host that receives a message for a
key follows the first rule that applies:

1. Before the host has seen a view in which the node has joined, it holds the message.
2. When the key was handed over to another node that this host's view does not show yet, it
   forwards the message there.
3. While the key is waiting for a handoff or its activation is stopping, it holds the message.
4. When another node owns the key in its view, it forwards the message there.
5. When the key is active, it delivers the message to the activation.
6. When this node owns the key, it activates the key and delivers the message to the new
   activation at once when it has nobody to ask, or else first asks the nodes that may still run
   the key to hand it over, and holds the message until the activation runs.
7. Otherwise no member hosts the kind, and it drops the message.

All the messages that a host holds for a kind count against the kind's buffer, and a message that
does not fit is dropped. Every drop is traced as a `lockstep.grain.drop` record with its reason.
`Send` cannot report a drop that happens later on a host, so a dropped request of `cluster.Ask`
resolves at its timeout.

`Membership.Host(kind)` returns the `HostInfo` of the kind's host on this node, which the host
publishes after every turn that changed it: the version of the last view it applied, whether the
node had joined then, the keys with a running activation, the keys whose activation is stopping,
the keys waiting for a handoff, and the number of messages it holds.

### Passivation

An activation that gets no message for the kind's idle timeout is passivated: the host stops it
with `lockstep.Stop`. The host also passivates the activations of keys that this node no longer
owns after a view change, and the activations that another node asks it to hand over. When the
activation has stopped, the host routes the messages it held for the key again, so a message that
arrived while the key was stopping starts a new activation, or goes to the new owner.

The stop runs before the messages already in the activation's mailbox, as every system message
does. Those messages become dead letters with the reason `stopped`, and the host records no grain
drop for them. This matters most when ownership moves: messages that the old owner had passed to
the activation, but that the activation had not processed yet, are lost.

An activation that stops by itself is treated the same way, with no passivation record. One that
fails and is restarted by the host's supervision stays active.

### Forwarding

While the views of two nodes disagree about a grain's owner, a host forwards a message to the
owner it sees. A forwarded message stays encoded, so a host that only forwards does not need the
message's type registered. Each forward adds one to the message's hop count, and a host drops,
instead of forwarding, a message that has already made `Config.MaxHops` hops.

A message from another node that finds the host's mailbox full is lost: the node counts and traces
a full mailbox, but records no grain drop, and the sender's `Send` has already returned nil. A
local `Send` to a full host returns an error that wraps `lockstep.ErrMailboxFull`.

### Handoff

When the members change, the owners of some keys change too. A node that becomes the owner of a key
it has no activation for first asks the nodes that may still run the key to hand it over. Who it
asks depends on the view its host applied before the current one:

- none, or one in which the node had not joined: the key's runner-up in the current view;
- one in which this node already owned the key: nobody;
- otherwise: the key's owner in that earlier view and its runner-up in the current one.

Nodes that are down or absent in the current view, or that do not host the kind, are left out.
With nobody to ask, the key is activated at once. Otherwise the node holds the key's messages, and
activates the key when every node it asked has answered, or when the kind's handoff timeout has
passed.

What an asked node does depends on its own view of the requester's process. When that view shows
the requester down, it answers at once and changes nothing. Otherwise it sends the messages it
holds for the key to the requester, stops its activation of the key if it has one, and answers once
the activation has stopped; a key that it was itself waiting to take over is given up and answered
at once. While its view has no entry for the requester's process, it also forwards every later
message for the key to the requester.

A handoff can reorder one sender's messages. The new owner holds the messages that reach it first,
and the messages that the old owner held are forwarded to it later and queue behind them. The
messages of one sender reach the activation in send order only while the owner does not change and
nothing is dropped.

### The guarantee and its limits

Once the views of the nodes agree and every host has applied its node's latest view, no key is in
the active set (`HostInfo.Active`) of two nodes. A host applies a view a turn after the membership
actor publishes it, so the guarantee starts only then. It covers running activations that are not
stopping: an activation that a view change passivated may still be stopping, and if the new
owner's handoff timeout passes first, the new activation runs beside it until the stop completes.

lockstep's own simulation tests check the active sets after every event, whenever the views agree
and every host has applied its view, over random crashes, restarts, partitions and pauses. A stop
takes no virtual time in a simulation, so these tests cannot see an old activation that is still
stopping. For nodes that join one at a time with no faults, the tests also check that the
activations of a key on different nodes never overlap in the trace.

While the views disagree, nothing prevents two activations of one key. A partition is the clearest
case: each side sees the members of the other side go down and activates their keys, while those
members keep running the activations they had. The handoff timeout has the same effect when a
previous owner is alive but cannot answer. Code that must never run one key on two nodes at once
needs more than grains, such as a fencing token that the store the grain writes to checks.

Within the guarantee, delivery is still at most once. An activation's state is in memory, so the
crash of its owner loses it, and a message sent into a cut or to a node that is down is lost.
[Limitations](../README.md#limitations) lists the remaining limits of remoting, membership and
grains.

## Testing in simulation

In `lockstep/sim`, every lockstep node runs the same remoting code as in production, and its frames
travel as datagrams on faultline's simulated network. Partitions, loss and delay apply to the
datagrams; each link keeps its datagrams in order and never duplicates them, so remoting's order
and its loss behave as over TCP. A paused node's datagrams wait until it resumes.

Every message between nodes is encoded, so each type sent, asked or replied with across nodes is
registered. `sim.Options.Remote` holds the protocol settings, the codec and the registry of every
node. Its network fields, `Listen`, `Advertise`, `Peers`, `Resolve` and `TLS`, are ignored, and its
zero value uses `codec.Default()` with JSON. `Simulate` checks the settings and the codec against
the registry once, and fails the test before any seed when either is wrong. A node's setup may
call `remote.Enable` as production code does; it replaces that node's protocol settings.

A lost handshake costs `DialTimeout`, 5 seconds by default, before the redial. Short runs set a
smaller `DialTimeout` and redial delays in `sim.Options.Remote`.

The following test runs a greeter on node `b`, asks it from node `a` once a second while faultline
partitions the network, and checks that it answers once the faults have stopped:

```go
func TestGreeterUnderPartitions(t *testing.T) {
	opts := sim.Options{Options: faultline.Options{Seeds: 20, Duration: 20 * time.Second}}
	sim.Simulate(t, opts, func(s *sim.Sim) {
		w := s.World()
		s.Node("b", func(sys *lockstep.System) {
			if _, err := lockstep.Spawn(sys, "greeter", newGreeter); err != nil {
				panic(err)
			}
		})
		s.Node("a", func(*lockstep.System) {})
		w.Plan(&fault.Random{Rules: []fault.Rule{
			{Kind: fault.KindPartition, Every: 3 * time.Second, MaxFor: 2 * time.Second},
		}})

		greeter := lockstep.PIDAt[Greet]("b", "/user/greeter")
		answered := 0
		for range 10 {
			w.RunFor(time.Second)
			if _, err := sim.Ask(s, "a", greeter, greet("a"), lockstep.WithTimeout(500*time.Millisecond)); err == nil {
				answered++
			}
		}
		w.RunFor(w.RecoveryStart().Sub(w.Sim.Now()) + time.Second) // a second after the faults stop
		answer, err := sim.Ask(s, "a", greeter, greet("a"))
		w.Final("the greeter answers after the faults", func() error {
			if err != nil {
				return fmt.Errorf("%w (%d of 10 asks answered during the faults)", err, answered)
			}
			if answer != "hello a from b" {
				return fmt.Errorf("answer %q", answer)
			}
			return nil
		})
	})
}
```

`Greet`, `newGreeter` and `greet` are the README's
[two-node program](../README.md#send-messages-across-nodes). Both nodes are servers, because
faultline's planners target servers. `w.RecoveryStart` is where the planner stops starting faults
and ends the ones still running.

`cluster.Join` runs unchanged in a simulation. The next test runs four nodes that join each other
while faultline crashes them and cuts the network, and checks at the end of the run that every node
sees the same up members, with the same IDs:

```go
func TestViewsAgree(t *testing.T) {
	opts := sim.Options{Options: faultline.Options{Seeds: 20, Duration: 80 * time.Second}}
	sim.Simulate(t, opts, func(s *sim.Sim) {
		w := s.World()
		nodes := []string{"n1", "n2", "n3", "n4"}
		members := make(map[string]*cluster.Membership)
		for _, name := range nodes {
			s.Node(name, func(sys *lockstep.System) {
				m, err := cluster.Join(sys, cluster.Config{Seeds: nodes, SyncInterval: 5 * time.Second})
				if err != nil {
					panic(err)
				}
				members[name] = m // the Membership of the node's current process
			})
		}
		w.Plan(&fault.Random{MaxDown: 1, Quiet: 40 * time.Second, Rules: []fault.Rule{
			{Kind: fault.KindCrash, Every: 10 * time.Second, MaxFor: 5 * time.Second},
			{Kind: fault.KindPartition, Every: 10 * time.Second, MaxFor: 10 * time.Second},
		}}) // faults stop at 40s

		w.Final("every node sees the same up members", func() error {
			want := upMembers(members["n1"].View())
			for _, name := range nodes[1:] {
				if got := upMembers(members[name].View()); !slices.Equal(got, want) {
					return fmt.Errorf("%s sees %v, n1 sees %v", name, got, want)
				}
			}
			if len(want) != len(nodes) {
				return fmt.Errorf("up members %v, want all %d nodes", want, len(nodes))
			}
			return nil
		})
	})
}

// upMembers returns the up members of v as "node:id", sorted by node.
func upMembers(v *cluster.View) []string {
	var out []string
	for _, m := range v.Up() {
		out = append(out, fmt.Sprintf("%s:%d", m.Node, m.ID))
	}
	return out
}
```

Each boot of a node runs its setup again, so `members` always holds the `Membership` of the node's
current process. The shorter `SyncInterval` lets the two sides of a long partition find each other
again well before the end of the run.

The README's [grain test](../README.md#the-same-cluster-in-simulation) runs a grain on three nodes
under crashes and partitions. Two rules of the simulation shape such tests. `Reply.Await` never
waits: on a pending reply it fails the seed, so a test runs the world past an ask's timeout before
it reads a `cluster.Ask` reply, or uses `sim.Ask` for an actor's PID. And `remote.Lookup` of another
node from a test body fails the seed for the same reason; a test addresses the actor with
`lockstep.PIDAt`, or calls `lockstep.Lookup` on the system of the node that holds it.

Remoting, membership and the grain hosts write trace records, which go into `trace.jsonl` and the
trace hash like the rest of the run: `lockstep.remote.*` for connections, sends, gaps and restarts,
`lockstep.cluster.*` for probes, suspicions, refutations, views and signals, and `lockstep.grain.*`
for activations, passivations, forwards, handoffs and drops. Run the tests with
`LOCKSTEP_CHECK_DETERMINISM=1` to run every seed twice and compare the traces.

## Next steps

- [README](../README.md#send-messages-across-nodes): the two-node program, a cluster, and a grain.
- [Message delivery](delivery.md): duplicates and reordering that application code must handle.
- [Architecture](architecture.md): the runtime, its engines, and how simulation maps onto
  faultline.
- [Package documentation](https://pkg.go.dev/github.com/hmdsefi/lockstep): the full API.
