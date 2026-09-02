# Run with:  ruby -Ilib test/test_ro4r.rb  (or: rake test)
require 'minitest/autorun'
require 'socket'
require 'ro4r'

# No RO4R thread may die with an unhandled exception; make that fail the run
# instead of just printing a backtrace.
Thread.abort_on_exception= true

class Exported
	def ok() 42 end
	def boom() raise "plain boom" end
	def custom() raise CustomError, "custom" end
	def unmarshalable
		e= RuntimeError.new "has a singleton"
		def e.extra() end # singleton methods cannot be marshalled
		raise e
	end
	def each_twice() yield 1; yield 2; :done end
	def each_kw() yield x: 1 end
	def obj() Object.new end
	def shared() @shared||= Exported.new end
	def echo( x) x end
	def epoch() Time.at( 0).utc end
	def kw( a:, b: 2) [ a, b] end
	def mixed( x, *rest, k: nil, **opts) [ x, rest.size, k, opts.to_a] end
	def sleep_for( s) sleep s; :woke end
	# Claims to respond to everything; must still not expose private methods.
	def respond_to?( m, priv= false) true end
	private
	def secret() :leaked end
end
# A by-value class with its own Marshal format.
class Point
	include ByVal
	attr_reader :x, :y
	def initialize( x, y) @x, @y= x, y end
	def _dump( depth) [ @x, @y].pack( 'l<l<') end
	def self._load( str) new( *str.unpack( 'l<l<')) end
	def ==( o) o.is_a?( Point) && x== o.x && y== o.y end
end
class CustomError< StandardError; end

class CapturingLogger
	attr_reader :lines
	def initialize() @lines= [] end
	def debug( m= nil) @lines<< [:debug, m || yield] end
	def warn( m= nil) @lines<< [:warn, m || yield] end
	def error( m= nil) @lines<< [:error, m || yield] end
end

class TestRO4R< Minitest::Test
	def setup
		@listener= TCPServer.new '127.0.0.1', 0
		@port= @listener.addr[1]
		@server= RO4R::Server.new Exported.new, @listener
		@conn= connect
		@r= @conn.root
	end
	def teardown
		@server.close
	end
	def connect
		RO4R::Connection.new "127.0.0.1:#{@port}"
	end
	def wait_until( timeout= 2)
		t0= Time.now
		sleep 0.01 until yield or Time.now- t0> timeout
		assert yield, "condition not met within #{timeout}s"
	end
	def ro4r_threads
		Thread.list.select{ |t| t!= Thread.current and
			Array( t.backtrace).any?{ |l| l.include? 'ro4r.rb'} }
	end

	def test_basic_call
		assert_equal 42, @r.ok
		assert_equal [1, 'a', :b], @r.echo( [1, 'a', :b])
	end

	def test_keyword_arguments
		assert_equal [1, 2], @r.kw( a: 1)
		assert_equal [1, 3], @r.kw( a: 1, b: 3)
		assert_equal [1, 1, :k, [[ :z, 9]]], @r.mixed( 1, 2, k: :k, z: 9)
		assert_equal [1, 1, nil, []], @r.mixed( 1, { a: 1}) # a Hash stays positional
		assert_equal [1, 0, nil, []], @r.mixed( 1, **{}) # no keywords at all
		assert_equal [:k, 1], @r.echo( [:k, 1]) # and so does a Hash inside an Array
		assert_raises( ArgumentError) { @r.kw }
	end

	def test_keyword_arguments_to_blocks
		seen= nil
		@r.each_kw{ |x:| seen= x}
		assert_equal 1, seen
	end

	def test_exception_returned_by_value
		e= assert_raises( RuntimeError) { @r.boom }
		assert_equal "plain boom", e.message
		assert_kind_of Array, e.backtrace
		e= assert_raises( CustomError) { @r.custom }
		assert_equal "custom", e.message
	end

	def test_unmarshalable_exception_becomes_runtime_error
		e= assert_raises( RuntimeError) { @r.unmarshalable }
		assert_equal "RuntimeError: has a singleton", e.message
	end

	def test_block_yield
		seen= []
		assert_equal :done, @r.each_twice{ |x| seen<< x}
		assert_equal [1, 2], seen
	end

	def test_insecure_methods_are_blocked
		assert_raises( SecurityError) { @r.send :ok}
		assert_raises( SecurityError) { @r.instance_eval "1"}
		assert_raises( SecurityError) { @r.instance_variable_get :@x}
	end

	def test_private_methods_stay_private_despite_respond_to_override
		assert @r.respond_to?( :secret) # the exported class lies
		assert_raises( NoMethodError) { @r.secret }
		assert_raises( NoMethodError) { @r.no_such_method }
	end

	def test_underscore_calls_are_forbidden
		e= assert_raises( NoMethodError) { @conn.call @r, :_dump, []}
		assert_equal "Call forbidden", e.message
	end

	def test_unknown_reference_reports_its_id
		bogus= RO4R::RemoteObject.new 123456, @conn
		e= assert_raises( TypeError) { @conn.call bogus, :ok, []}
		assert_includes e.message, "123456"
	end

	def test_remote_objects_are_proxies
		o= @r.obj
		assert_equal RO4R::RemoteObject, o.__class # kind_of? would go remote
		assert_kind_of Integer, o.__remote_id
		assert_same @conn, o.__connection
		assert_equal RO4R::ROOT_ID, @r.__remote_id
	end

	def test_remote_objects_round_trip
		o= @r.shared
		assert_same o, @r.echo( o) # RID back to the server, LID back to us
		assert_same o, @r.shared # same id, proxy reused
		assert_equal 42, @r.echo( o).ok
	end

	def test_exported_objects_are_reference_counted
		o= @r.shared
		o2= @r.shared
		assert_same o, o2
		assert_equal 2, o.__refs[0]
		id= o.__remote_id
		release= lambda do |n|
			@conn.__send__ :send, RO4R::Release.new( id, n)
			sleep 0.05
		end
		release.call 1 # one of the two references dropped: still exported
		assert_equal 42, o.ok
		release.call 1 # both dropped: gone
		e= assert_raises( TypeError) { o.ok }
		assert_includes e.message, "not found"
		assert_equal 42, @r.ok # the root is unaffected
	end

	def test_server_tracks_and_reaps_connections
		wait_until{ @server.connections.size== 1}
		c2= connect
		wait_until{ @server.connections.size== 2}
		c2.close
		wait_until{ @server.connections.size== 1}
		assert_equal 42, @r.ok # the other connection is unaffected
	end

	def test_client_notices_dead_server
		@server.close
		e= assert_raises( RO4R::ConnectionError) { @r.ok }
		assert_match(/Unable to reach peer/, e.message)
		assert_kind_of RuntimeError, e # for callers rescuing the old error
	end

	def test_waiting_callers_are_released_on_disconnect
		results= Queue.new
		threads= 5.times.map do
			Thread.new{ results<< ( @r.sleep_for( 0.5) rescue $!)}
		end
		sleep 0.1 # let the calls go out
		@server.close
		threads.each{ |t| assert t.join( 3), "a caller hung after disconnect"}
		5.times{ assert_kind_of RO4R::ConnectionError, results.pop}
		wait_until{ ro4r_threads.empty?} # the sleeping workers exit too
	end

	def test_repeated_connect_call_close
		# a reply may be sent before the server-side constructor returns
		30.times do
			c= connect
			assert_equal 42, c.root.ok
			c.close
		end
		wait_until{ @server.connections.size== 1}
	end

	def test_server_close_stops_accepting
		@server.close
		assert_raises( Errno::ECONNREFUSED) { connect }
		assert_empty @server.connections
	end

	def test_released_remote_objects_do_not_break_the_connection
		20.times{ @r.obj }
		GC.start
		sleep 0.2
		assert_equal 42, @r.ok
	end

	def test_server_listens_on_loopback_by_default
		s= RO4R::Server.new Exported.new, 0
		assert_equal '127.0.0.1', s.server.addr[3]
		s.close
	end

	def test_string_port_forms
		s= RO4R::Server.new Exported.new, "127.0.0.1:0"
		assert_equal '127.0.0.1', s.server.addr[3]
		s.close
		s= RO4R::Server.new Exported.new, "0"
		assert_equal RO4R::Server::DefaultAddress, s.server.addr[3]
		s.close
	end

	def test_parse_address
		d= RO4R::Server::DefaultPort
		assert_equal [ 'host', d], RO4R.parse_address( 'host', d)
		assert_equal [ 'host', 7], RO4R.parse_address( 'host:7', d)
		assert_equal [ '::1', d], RO4R.parse_address( '::1', d)
		assert_equal [ '::1', d], RO4R.parse_address( '[::1]', d)
		assert_equal [ 'fe80::1', 7], RO4R.parse_address( '[fe80::1]:7', d)
	end

	def test_ipv6
		begin
			l= TCPServer.new '::1', 0
		rescue SystemCallError
			skip "no IPv6 loopback"
		end
		s= RO4R::Server.new Exported.new, l
		c= RO4R::Connection.new "[::1]:#{l.addr[1]}"
		assert_equal 42, c.root.ok
		c.close
		s.close
	end

	def test_logger_receives_remote_call_errors
		old= RO4R.logger
		RO4R.logger= log= CapturingLogger.new
		assert_raises( RuntimeError) { @r.boom }
		wait_until{ log.lines.any? }
		level, msg= log.lines.first
		assert_equal :debug, level
		assert_includes msg, "plain boom"
	ensure
		RO4R.logger= old
	end

	def test_nested_call_from_within_a_block
		# The block runs on the client while the server is still inside
		# each_twice, and calls the server again from there.
		seen= []
		@r.each_twice{ |x| seen<< @r.echo( x* 10)}
		assert_equal [10, 20], seen
	end

	def test_close_leaves_no_helper_threads
		10.times{ @r.obj } # exercise workers and the release path
		refute_empty ro4r_threads
		@server.close
		wait_until{ ro4r_threads.empty?}
	end

	def test_plain_marshal_is_unaffected_in_a_thread_that_made_calls
		@r.ok
		h= { a: 1, obj: Object.new, err: RuntimeError.new( "x")}
		copy= Marshal.load( Marshal.dump( h))
		assert_equal 1, copy[:a]
		assert_instance_of Object, copy[:obj]
		assert_equal "x", copy[:err].message
	end

	def test_by_value_objects_with_custom_dump
		assert_equal Time.at( 0).utc, @r.epoch
		assert_equal Point.new( 3, -4), @r.echo( Point.new( 3, -4))
	end

	def test_byref_calls_are_ignored_unless_enabled
		assert_instance_of String, @conn.byref( @r, :echo, [ "s"])
		RO4R::RefCall.enable
		assert_equal RO4R::RemoteObject, @conn.byref( @r, :echo, [ "s"]).__class
	ensure
		RO4R::RefCall.disable
	end

	def test_messages_are_marshalled_compactly
		data= Marshal.dump RO4R::Call.new( 1, 2, :m, [ 3], false)
		refute_includes data, "@tid"
		msg= Marshal.load data
		assert_equal [ 1, 2, :m, [ 3], false], [ msg.tid, msg.oid, msg.nme, msg.arg, msg.blk]
		assert_operator data.bytesize, :<, 40
	end

	def test_reference_ids_are_64_bit
		conn= Object.new
		def conn.make_object( ref) ref end
		Thread.current[:__conn]= conn
		big= 2**40+ 5
		assert_equal big, Marshal.load( Marshal.dump( RID.new( big))).oid
	ensure
		Thread.current[:__conn]= nil
	end
end
