# RO4R - Remote Objects for Ruby 2.7+

Git export of RO4R, a Ruby RPC library that has been in production use since 2007.

## Supported Ruby versions

A `.ruby-version` file pins 4.0.6 for rbenv and similar tools.

Supports all Ruby versions from 2.7 up to and including Ruby 4.0, in all combinations of
client and server versions. (2.7 is required because remote object references are now
tracked in an `ObjectSpace::WeakMap` keyed by integer ids.)

For support on older versions, please see below under "Other Ruby versions".

## Security

RO4R is designed for use between mutually trusting processes on a trusted network.
It is **not** safe to expose to untrusted peers:

* Messages are deserialized with `Marshal.load`, which can instantiate arbitrary
  objects from the wire.
* A peer can call any public method on an exported object, except for a blocklist
  of obviously dangerous ones (`send`, `instance_eval`, `system`, ... - see
  `Object::INSECURE_METHODS`) and any method whose name starts with `_`.
  A blocklist is not a sandbox; if you need stricter control, override
  `__secure_method?` in your exported classes with an allowlist.
* The `safe` argument to `Connection.new` and `Server.new` is ignored, since
  Ruby 3.0 removed `$SAFE`. It is kept only for API compatibility.

## Wire format

Each message is a 32-bit little-endian length prefix followed by the marshalled
payload; object ids inside references are 64-bit signed little-endian.

**This is incompatible with releases before the Ruby 4.0 update**, which used
native-endian 32-bit object ids: both peers must run the same generation of RO4R.
The change removes a silent wraparound of object ids past 2^31 in long-running
processes, and lets big-endian hosts interoperate.

## How to see it work

1. Check out the files:

```
  git clone git://github.com/crystallabs/RO4R.git
```

1. In one terminal, start an example server. It will listen on port 4044, export one Ruby Hash variable, and provide the main loop:

```
  cd RO4R
  ruby -I. examples/srv.rb
```

1. In another terminal, start an example client. It will attach to the server and shared Hash, iterate a counter 10_000 times, and then print the benchmark statistics and the final contents of the Hash:

```
  cd RO4R
  ruby -I. examples/cli.rb
```

When the client disconnects, the server silently drops the connection and keeps
serving other clients.

### Benchmarks explanation

When you run the above, it will print statistics such as:

```
0.390000   0.390000   0.780000 (  1.451144)
{:counter=>9999, 1=>A, 2=>#<A:0xb7741e38>}
```

The first line is the benchmark output, showing user, system, total and real times. (That's benchmark for the 10,000 iterations that the example client does).

The second line are the contents of the shared Hash object. In it you see the :counter that was created on the client side, and two keys that were initialized by the example server.

### Testing

The automated test suite (minitest, bundled with Ruby) runs with:

```
  ruby -I. test/test_ro4r.rb
```

It starts a server on an ephemeral port inside the test process and covers
calls, blocks, exception propagation, the method blocklist, connection
tracking and shutdown, and reference handling.

To test RO4R manually, you can create two processes (client and server), and then display and modify variables and invoke remote functions.

1. To conveniently test modifying data, run the basic benchmark under IRB, then modify some hash keys on the example shared object `$r`:

```
$ cd RO4R
$ irb -I. -r examples/cli.rb

# ... statistics will be printed ...

> $r[:counter]
9999
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
$ irb -I. -r examples/cli.rb

> $r
{:counter=>9999, 1=>A, 2=>#<A:0xb7745e34>, :test_value=>717}
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

### Shutting down

`RO4R::Server#close` stops accepting connections, disconnects every client and
waits for their handler threads to finish. `RO4R::Connection#close` disconnects
a single client. Calls made on a connection whose peer has gone away raise
`RO4R::ConnectionError` (a `RuntimeError`, message "Unable to reach peer").

### Concurrency

Each incoming call is executed on a worker thread belonging to the connection,
so exported methods must be thread-safe if clients may call them concurrently.
Workers are created on demand and up to five idle ones are kept per
connection (`Connection#poolsize`). There is deliberately no upper bound: a
remote method may call back into the client, which may call the server again
before the first call has returned.

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

### Shared classes

Generally, you will share instances of classes that are known to both the server and the clients.

But it is also possible to share objects that clients know nothing about.

Included in the distribution is rmarshal/, a modification of
Ruby marshaller that you need to compile and 'require' into
your Ruby app on the server side (the one that exports the object),
without any other changes.

With that, the clients will transparently be able to use the remote
object even though they don't know its Class.

NOTE: rmarshal.c is currently out of date, and the version included works with Ruby 1.8.

### Method return values

When writing methods, you usually don't care about unused return
values because Ruby simply discards them.

However, on methods that are invoked remotely, RO4R will pass the return
value back to the client,
so pay attention to exit a method with an explicit 'nil' if its return
value is not needed. This is cleaner and can also save you from errors
if the (unnecessary) return value would be a weird object that Ruby
marshaller can't serialize and pass back to the client.

### Code stability

RO4R has been in production use with Ruby 1.8, 1.9, and later since 2007.
Verified on Ruby 4.0 with `ruby -w -W:deprecated` producing no warnings.

Note for users of Ruby 3.1 and later: exceptions raised by a remote method are
now correctly returned to the caller by value. Previously they leaked through as
remote references, and re-raising them on the caller side recursed until
`SystemStackError`. Exceptions that cannot be marshalled (e.g. ones with singleton
methods) are returned as a `RuntimeError` carrying the original class and message.

## Other Ruby versions

For Ruby 2.4 to 2.6, use the git revision preceding the Ruby 4.0 update (the one
still using `ObjectSpace._id2ref`).

For use with lower versions (Ruby 1.8 to Ruby 2.3) , please either use RO4R release 1.0.0
or edit RO4R.rb to replace "safe= 1" with "safe= 3" and uncomment the line saying
"Bignum, Fixnum".

The C marshaller (rmarshall) is operational only for Ruby 1.8 versions.

## TODO

* Add rmarshall for newer Ruby versions
