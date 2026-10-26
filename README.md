# RO4R - Remote Objects for Ruby 2.7+

RO4R is a Ruby RPC library that has been in production use since 2007. A server
exports one object over TCP; clients call methods on it, on the objects it
returns, and on the blocks they pass, as if everything were local.

## Supported Ruby versions

Ruby 2.7 up to and including Ruby 4.0, in any combination of client and server
versions. (2.7 is required because remote object references are tracked in an
`ObjectSpace::WeakMap` keyed by integer ids.) A `.ruby-version` file pins 4.0.6
for rbenv and similar tools.

For older Rubies use the git history: the revision preceding the Ruby 4.0
update supports 2.4 to 2.6 (it still used `ObjectSpace._id2ref`), and release
1.0.0 supports 1.8 to 2.3.

## Installation

```
  gem build ro4r.gemspec
  gem install ro4r-*.gem
```

or, from a checkout, add `-Ilib` to the Ruby command line. The library is
loaded with `require 'ro4r'`. (`RO4R.rb` at the top of the checkout is a shim
so that the old `ruby -I. -r RO4R` still works.)

## Quick start

Server:

```ruby
  require 'ro4r'
  shared= {}
  RO4R::Server.new( shared).join   # listens on 127.0.0.1:4044
```

Client:

```ruby
  require 'ro4r'
  r= RO4R::Connection.new( 'localhost:4044').root
  r[:counter]= 1
  r.each{ |k, v| puts "#{k}=#{v}" }   # the block runs here, called from the server
```

## Security

RO4R is designed for use between mutually trusting processes on a trusted network.
It is **not** safe to expose to untrusted peers:

* Messages are deserialized with `Marshal.load`, which can instantiate arbitrary
  objects from the wire.
* A peer can call any public method on an exported object, except for a blocklist
  of obviously dangerous ones (`send`, `instance_eval`, `system`, ... - see
  `Object::INSECURE_METHODS`) and any method whose name starts with `_`.
  A blocklist is not a sandbox; if you need stricter control, pass an allowlist
  to the server (see "Restricting methods" below) or override `__secure_method?`
  in your exported classes.
* Classes you define are passed by reference (see "Passing objects" below), and
  `class` is a public method of every exported object. Through such a class
  proxy a peer can reach `superclass`, `const_get` and `new`, i.e. every
  constant and class loaded in the process, not only the exported object.
* A peer can make the receiver create as many worker threads as it has calls
  in flight, and can send messages up to `max_frame` bytes (256 MiB unless
  configured otherwise).
* Methods are invoked with `public_send`, so private methods stay private even
  if the exported class overrides `respond_to?`.
* The server listens on `127.0.0.1` by default. Pass `"0.0.0.0:4044"` (or
  `"[::]:4044"`) to `Server.new` to listen on all interfaces.
* The `safe` argument to `Connection.new` and `Server.new` is ignored, since
  Ruby 3.0 removed `$SAFE`. It is kept only for API compatibility.

## Wire format

Each message is a 13-byte header followed by the marshalled payload. The header
holds the payload length (32-bit unsigned), the message kind (one byte: 1 call,
2 return, 3 yield, 4 release) and the id of the call the message concerns
(64-bit signed): the sender's call id for a call or yield, the receiver's for a
return. All fields are little-endian. The header lets a receiver that cannot
decode a payload (an object of a class it does not define, a frame larger than
`max_frame`) answer that one call with an error instead of dropping the
connection.

Messages are marshalled as arrays of their fields (`marshal_dump`), and object
ids inside references are 64-bit signed little-endian. Each side's root object
has id 0. A `Release` message carries the number of references being released.
Sockets are set to `TCP_NODELAY` and `SO_KEEPALIVE`. `Connection#read(io,
maxlen)` is the single point through which bytes enter and may be overridden;
it must return between 1 and `maxlen` bytes or raise `EOFError`.

**Both peers must run the same generation of RO4R.** The 3.0 format is
incompatible with earlier releases (which had wider messages, native-endian
32-bit ids, and a root id that differed between 32- and 64-bit hosts).

## How to see it work

1. Check out the files:

```
  git clone https://github.com/crystallabs/RO4R.git
```

1. In one terminal, start an example server. It will listen on port 4044, export one Ruby Hash variable, and provide the main loop:

```
  cd RO4R
  ruby -Ilib examples/srv.rb
```

1. In another terminal, start an example client. It will attach to the server and shared Hash, iterate a counter 10_000 times, and then print the benchmark statistics and the final contents of the Hash:

```
  cd RO4R
  ruby -Ilib examples/cli.rb
```

When the client disconnects, the server silently drops the connection and keeps
serving other clients.

### Benchmarks explanation

When you run the above, it will print statistics such as:

```
0.390000   0.390000   0.780000 (  1.451144)
{1=>A, 2=>#<A:0xb7741e38>, :counter=>10001}
```

The first line is the benchmark output, showing user, system, total and real times. (That's benchmark for the 10,000 iterations that the example client does).

The second line are the contents of the shared Hash object. In it you see the :counter that was created on the client side, and two keys that were initialized by the example server.

### Testing

The automated test suite (minitest, bundled with Ruby) runs with:

```
  rake test
```

or `ruby -Ilib test/test_ro4r.rb`. It starts a server on an ephemeral port
inside the test process and covers calls, keyword arguments, blocks, exception
propagation, the method blocklist and allowlist, reference counting, timeouts,
undecodable messages, connection tracking and shutdown. A few tests start a
second server process (`test/peer_server.rb`) whose classes the test process
does not define. Any RO4R thread dying with an unhandled exception fails the
run. The GitHub Actions workflow runs it on every supported Ruby.

To test RO4R manually, you can create two processes (client and server), and then display and modify variables and invoke remote functions.

1. To conveniently test modifying data, run the basic benchmark under IRB, then modify some hash keys on the example shared object `$r`:

```
$ cd RO4R
$ irb -Ilib -r examples/cli.rb

# ... statistics will be printed ...

> $r[:counter]
10001
> $r[:test_value]= 717
#-> 717
```

1. To run defined functions, simply call them:

On the server side, in the example we have initialized an example method 'm' that returns value of 1; you can run it:

```
> $r[2].m
#-> 1
```

1. Multiple clients:

You can also open a new/third terminal in which you can query all the changes:

```
$ cd RO4R
$ irb -Ilib -r examples/cli.rb

> $r
{1=>A, 2=>#<A:0xb7745e34>, :counter=>20001, :test_value=>717}
```

## Benchmarks

Benchmarks for 10,000 RPC invocations:

```
Date        Server   Client     Stats
Jun 2018:   2.6.0p2   2.6.0p2   0.192000   0.100000   0.292000 (  0.517264)
Oct 2015:   2.2.3     2.2.3     0.196000   0.076000   0.272000 (  0.481286)

Jun 2018 setup was: i7-4790K CPU @ 4.00GHz on Linux 3.16.0-4-amd64
Oct 2015 setup was: i7-4790K CPU @ 4.00GHz on Linux 4.2.0-040200-lowlatency

Older Benchmarks from 2008:

            Server   Client     Stats
            1.9.2     1.9.2     0.390000   0.390000   0.780000 (  1.451144)
            1.9.2     1.8.7     1.000000   0.280000   1.280000 (  2.009202)
            1.8.7     1.9.2     0.520000   0.300000   0.820000 (  1.914454)
            1.8.7     1.8.7     0.960000   0.260000   1.220000 (  2.472942)
```

## Programming notes

### Addresses

`Connection.new` takes `"host"`, `"host:port"`, an IPv6 address such as
`"::1"` or `"[::1]:4044"`, or an already connected IO. `Server.new` takes a
port number, `"port"`, `"address:port"`, `"[v6address]:port"`, or a listening
socket.

### Options

Both constructors accept keyword options; `Server.new` passes them on to every
connection it accepts:

```ruby
  RO4R::Server.new( object, 4044, allow: [ :find, :store], timeout: 30)
  RO4R::Connection.new( 'host:4044', timeout: 5, connect_timeout: 3)
```

* `timeout:` seconds to wait for a reply before raising `RO4R::TimeoutError`
  (default: wait forever). `Connection#call( object, method, args, block,
  byref, timeout: t)` overrides it for one call.
* `connect_timeout:` seconds to wait when connecting.
* `poolsize:` idle worker threads kept per connection (default 5).
* `logger:` a logger for this connection (default: `RO4R.logger`).
* `max_frame:` largest message, in bytes, sent or accepted (default 256 MiB).
* `allow:` an allowlist of methods, see "Restricting methods".
* `on_close:` a callable invoked with the connection once it is closed.

The third positional argument (`safe`) is ignored, since Ruby 3.0 removed
`$SAFE`; it is kept only for API compatibility.

### Restricting methods

`allow:` limits what a peer may call on **any** object exported over the
connection, including objects returned by other calls. It is an Array of method
names, or a callable that receives the object and the method name:

```ruby
  RO4R::Server.new( object, 4044, allow: [ :find, :store])
  RO4R::Server.new( object, 4044, allow: proc{ |obj, name| obj.is_a?( Store) })
```

Calls to other methods raise `SecurityError` on the caller's side. The blocklist
(`Object::INSECURE_METHODS`, names starting with `_`) still applies.

### Shutting down

`RO4R::Server#close` stops accepting connections, disconnects every client and
waits for their handler threads to finish. `RO4R::Connection#close` disconnects
a single client. Calls made on a connection whose peer has gone away, and calls
that were waiting for a reply when it went away, raise `RO4R::ConnectionError`
(a `RuntimeError`, message "Unable to reach peer").

### Errors

All RO4R errors are `RuntimeError`s:

* `RO4R::ConnectionError`: the peer is gone. The connection is dead.
* `RO4R::TimeoutError`: no reply within the call's timeout. The connection stays
  usable; the peer may still be executing the call, and its eventual reply is
  discarded.
* `RO4R::ProtocolError`: a message could not be decoded by its receiver, on
  either side (typically an object of a class the receiver does not define, see
  "Passing objects" below), or exceeded `max_frame`. The connection stays
  usable.

Exceptions raised by the remote method itself are re-raised as they are.

### Concurrency

Each incoming call is executed on a worker thread belonging to the connection,
so exported methods must be thread-safe if clients may call them concurrently.
Workers are created on demand and up to five idle ones are kept per
connection (`Connection#poolsize`). There is deliberately no upper bound: a
remote method may call back into the client, which may call the server again
before the first call has returned.

Each waiting call has its own reply queue; the thread that reads the socket
never blocks on anything but the socket, so a peer that is slow to read cannot
stall the delivery of replies.

### Keyword arguments

Keyword arguments work across the wire: `r.find( name: 'x')` calls
`find( name: 'x')` remotely, and a block may be yielded keywords. A Hash passed
positionally stays positional (and, like every Hash, is passed by reference).

### Blocks

A block passed to a remote method runs on the caller's side, once per `yield`.
An exception raised in the block is raised by the `yield` in the remote method.
If the block exits early (`break`, `return`, `throw`, or an exception that is
not a `StandardError`), or the caller gave up waiting (timeout, interrupt), the
remote method's `yield` raises `RO4R::BlockExit`, which is an `Exception` but
not a `StandardError`: `rescue => e` does not catch it, `ensure` blocks run,
and the method unwinds instead of waiting forever for a block result that will
never come. Nothing is sent back to the caller in that case, since it is gone.
If the block's result cannot be sent (it is larger than `max_frame`, or cannot
be marshalled), the remote method's `yield` raises `RO4R::ProtocolError`
instead, and the caller gets whatever the method then returns or raises.

### Passing objects: by reference or by value

Objects are passed by reference unless their class is by value. By value are
`Array`, `String`, `Symbol`, `Integer`, `Float`, `Range`, `Regexp`, `Time`,
`Exception`, `nil`, `true` and `false`. The class objects of the by-value
classes, of `Hash`, `IO`, `Struct`, `Proc`, `Thread` and a few others, and of
their subclasses, are passed by name (`Class`, `Module` and `Kernel` too). Any
other class, including every class you define, is passed by reference like an
ordinary object, so the peer can call `new` on it.
Everything else, **including `Hash`**, is passed by reference: the receiver
gets a `RO4R::RemoteObject` proxy and every method call on it goes back to the
owner. To pass your own class by value, `include ByVal` in it (and give it
`_dump`/`_load` or `marshal_dump`/`marshal_load` if the default object dump
is not suitable).

By-reference objects that are passed directly, in Arrays, or as keyword
arguments are replaced by references before marshalling, so the peer need not
know their classes. Objects nested deeper inside a by-value object (an instance
variable of an Exception, say) are converted by a Marshal hook that writes the
class name, so those classes must exist on both sides. A by-value object whose
class the receiver does not define (a custom Exception class, for instance)
makes that one call raise `RO4R::ProtocolError`.

### References and garbage collection

An object that has been passed by reference is kept alive by its owner until
the peer's proxy for it is garbage collected. Proxies are reused for the same
remote object, and the owner counts how many references it handed out, so an
object passed again while an earlier proxy is being collected stays exported.
A proxy's `__refs` shows the count it will release.

### Logging

By default RO4R prints only warnings and errors to `$stderr`. Exceptions raised
inside remotely invoked methods are returned to the caller and only logged at
debug level, i.e. discarded. To see them, or to route messages elsewhere,
assign any object responding to `debug`, `warn` and `error` (a standard
`Logger` works) to `RO4R.logger`:

```ruby
  require 'logger'
  RO4R.logger= Logger.new( $stdout)
```

### Marshal and by-value classes

RO4R works by hooking `Marshal`: while it serializes a message, objects that are
passed by reference answer `respond_to?(:_dump)` with true and dump themselves
as a reference. The hook is active only on the thread and for the duration of
RO4R's own `Marshal.dump`, so other uses of `Marshal` in the same process are
not affected. Classes passed by value keep their own `_dump`/`_load` if they
define one (e.g. `Time`).

### Method return values

When writing methods, you usually don't care about unused return
values because Ruby simply discards them.

However, on methods that are invoked remotely, RO4R will pass the return
value back to the client,
so pay attention to exit a method with an explicit 'nil' if its return
value is not needed. This is cleaner and can also save you from errors
if the (unnecessary) return value would be a weird object that Ruby
marshaller can't serialize and pass back to the client.

### Return by reference

`Connection#byref( object, method, args)` asks the peer to return the result by
reference even if it is a by-value object. The peer honours this only after
`RO4R::RefCall.enable` has been called there; otherwise it is an ordinary call.

### Code stability

RO4R has been in production use with Ruby 1.8, 1.9, and later since 2007.
Verified on Ruby 3.1 and 4.0 with `ruby -w -W:deprecated` producing no warnings.

Note for users of Ruby 3.1 and later: exceptions raised by a remote method are
now correctly returned to the caller by value. Previously they leaked through as
remote references, and re-raising them on the caller side recursed until
`SystemStackError`. Exceptions that cannot be marshalled (e.g. ones with singleton
methods) are returned as a `RuntimeError` carrying the original class and message.

## Changes in 3.0

See CHANGELOG.md for the full list. (The v2.0.0 tag from 2018 predates all of
this; the version number skips to 3.0 so that the tag stays unique.) In short:

* Keyword arguments are passed correctly under Ruby 3 (they used to arrive as
  a positional Hash and raise `ArgumentError`).
* Callers waiting for a reply when the connection drops always get
  `ConnectionError`; two races could previously leave them hanging or kill the
  reader thread.
* A block that exits early no longer leaves the remote method suspended
  forever; a message the peer cannot decode no longer drops the connection.
* Call timeouts, connect timeouts, a method allowlist, per-connection loggers
  and a maximum message size (all keyword options).
* Exported objects are reference counted, closing a race between a proxy being
  collected and the same object being sent again.
* The server binds to loopback by default.
* Messages are smaller on the wire (a typical call went from 71 to about 50
  bytes).
* IPv6 addresses are accepted.
* Packaged as a gem (`lib/ro4r.rb`, `require 'ro4r'`); `RO4R.rb` remains as a
  shim. The Ruby 1.8 C marshaller (`rmarshal/`) has been removed.

## License

RO4R is free software, licensed under the GNU Affero General Public License,
version 3 or (at your option) any later version. See the LICENSE file.
