# Started by test_ro4r.rb in a separate process: exports an object whose
# methods return and raise objects of classes the test process does not
# define. Prints the port it listens on and serves until killed.
require 'ro4r'

# decode failures are expected here; don't clutter the test output
class NullLogger
	def debug( m= nil) end
	def warn( m= nil) end
	def error( m= nil) end
end
RO4R.logger= NullLogger.new

class PeerOnly
	def hi() :hi end
end
class PeerOnlyError< StandardError; end
# by value, so the test process must decode it and cannot
class PeerOnlyValue
	include ByVal
end

class Peer
	def top() PeerOnly.new end
	def nested() [ PeerOnly.new, [ PeerOnly.new]] end
	def kw_values( **k) k.to_a end
	def echo( x) x end
	def value() PeerOnlyValue.new end
	def fail_unknown() raise PeerOnlyError, "peer only" end
	def has_hi?( x) x.hi== :hi end
end

listener= TCPServer.new '127.0.0.1', 0
$stdout.puts listener.addr[1]
$stdout.flush
RO4R::Server.new( Peer.new, listener).join
