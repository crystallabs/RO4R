# Changelog

## 3.1.0

Protocol version 1: the wire format is incompatible with 3.0, and both peers
must run this release or later.

### Changed

* Each connection opens with a 10-byte handshake (magic, protocol version,
  the sender's `max_frame`). A peer that sends anything else, or another
  version, is refused with a `ConnectionError` naming the reason; 3.0 peers
  used to hang or drop the connection silently. Once the handshake is read, a
  message larger than the peer accepts is refused locally.
* Payloads are plain Arrays dispatched on the header's kind byte: no class or
  instance variable names, and no call id repeated from the header. Keyword
  arguments are a field of the call or yield record instead of a `Kwargs`
  object. A call returning by reference is its own kind (5).
* Exported objects get per-connection sequential ids, encoded as
  BER-compressed integers, instead of 64-bit object ids. A reference is 8
  bytes instead of 15.
* Releases queued together (a GC run collecting many proxies) are sent as one
  message, up to 1000 at a time.
* A call with one small argument is 28 bytes on the wire instead of 42, its
  return 20 instead of 37.
* `Connection#call` requires a `RemoteObject` of that connection. Passing a
  local object used to send its object id, which meant nothing to the peer.
* `ConnectionError` messages say why the connection closed.
* A method returning its own root object yields the caller's root proxy, not
  a second proxy for the same object.

### Fixed

* `nil.__nil?` was false (the alias pointed at `Kernel#nil?`, which `NilClass`
  overrides), so a connection exporting no object did not report "No root
  object exported" and a by-reference request for `nil` was honoured.
* An error while setting up an accepted connection (now including a client
  that vanished before the handshake was written) ended the server's accept
  loop.

* A block whose result could not be sent back (larger than `max_frame`, or
  not marshallable) left the remote method suspended in its `yield` forever,
  holding a worker thread. The `yield` now raises `RO4R::ProtocolError`.
* A failed `accept` (`ECONNABORTED` when a peer reset the connection before it
  was accepted, `EMFILE`, ...) silently ended the server's accept loop; later
  clients connected but were never served. The failure is now logged and the
  server keeps accepting.
* README: classes you define are passed by reference, not by name; only the
  class objects of the by-value classes and a few others are.

## 3.0.2

* Release workflow: the gem is attached to the GitHub release.

## 3.0.1

### Fixed

* `Server#close` hung on macOS (and the BSDs), where `shutdown` does not wake
  a thread blocked in `accept` on a listening socket. The accept loop is now
  woken through a pipe.

## 3.0.0

Requires Ruby 2.7 or later. The wire format is incompatible with 1.x; both
peers must run 3.x. (The v2.0.0 tag from 2018 predates all of this; the
version skips to 3.0 so that the tag stays unique.)

### Fixed

* A block that exits early (`break`, `return`, `throw`, or a non-StandardError
  exception) left the remote method suspended in its `yield` forever, holding a
  worker thread. The method now sees `RO4R::BlockExit` and unwinds.
* A message the receiver could not decode (an object of a class it does not
  define, a failing `_load`, an unknown reference) closed the whole connection.
  It now fails only that call, with `RO4R::ProtocolError`, on whichever side
  was waiting.
* By-reference objects passed in Arrays or as keyword arguments no longer
  require the peer to know their class.
* A reply arriving after its call was abandoned (timeout, `Timeout.timeout`,
  `Thread#raise`) could be delivered to the caller's next call. Calls are now
  identified by a per-connection sequence instead of the thread id.
* A yield for an abandoned call is answered with `RO4R::BlockExit`, so the
  remote method unwinds instead of waiting forever for the block.
* Keyword arguments arrived as a positional Hash under Ruby 3.
* Exceptions raised by a remote method are returned by value; they used to
  leak through as remote references and recurse on re-raise.
* Callers waiting for a reply when the connection dropped could hang.
* An object passed again while an earlier proxy was being collected could be
  released prematurely; exported objects are now reference counted.
* `Time` (and any by-value class with its own `_dump`) can be passed.
* RO4R's Marshal hook affected other users of Marshal in the same process.

### Added

* Keyword options on `Connection.new` and `Server.new`: `timeout:`,
  `connect_timeout:`, `poolsize:`, `logger:`, `max_frame:`, `allow:`,
  `on_close:`. `Connection#call` and `#byref` take `timeout:` per call.
* `RO4R::TimeoutError`, `RO4R::ProtocolError`, `RO4R::BlockExit`,
  `RO4R::ConnectionError`.
* Method allowlist (`allow:`) applying to every object exported over a
  connection.
* `RO4R.logger`, `Connection#logger`, `Connection#closed?`.
* `Server#close`, `Server#connections`, `Connection#close`.
* IPv6 addresses.
* `SO_KEEPALIVE` and `TCP_NODELAY` on TCP sockets.
* Test suite (`rake test`) and GitHub Actions CI across Ruby 2.7 to 4.0.

### Changed

* Frame header is 13 bytes (length, kind, call id) instead of a 4-byte length;
  messages are marshalled as arrays of fields and ids are 64-bit
  little-endian. A typical call is about 50 bytes.
* The server binds to 127.0.0.1 by default.
* Packaged as a gem: `lib/ro4r.rb`, `require 'ro4r'`. `RO4R.rb` at the top of
  the checkout is a compatibility shim.
* The `safe` argument is ignored (Ruby 3.0 removed `$SAFE`).

### Removed

* The Ruby 1.8 C marshaller (`rmarshal/`).
* Support for Ruby before 2.7 (use release 1.0.0 for 1.8 to 2.3, or the
  revision before the Ruby 4.0 update for 2.4 to 2.6).

## 1.0.0 (2015-10-27)

Last release supporting Ruby 1.8 to 2.3.
