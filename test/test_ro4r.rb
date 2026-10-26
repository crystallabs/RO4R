# Run with:  ruby -Ilib test/test_ro4r.rb  (or: rake test)
require 'minitest/autorun'
require 'socket'
require 'rbconfig'
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
	def me() self end
	def echo( x) x end
	def epoch() Time.at( 0).utc end
	def kw( a:, b: 2) [ a, b] end
	def mixed( x, *rest, k: nil, **opts) [ x, rest.size, k, opts.to_a] end
	def sleep_for( s) sleep s; :woke end
	# Yields once; records whether the method ran to completion or unwound.
	def yield_once
		$outcomes<< :yielding
		r= yield 1
		$outcomes<< [ :completed, r]
		r
	ensure
		$outcomes<< :ensure
	end
	def slow_yield() sleep 0.3; yield 1; :done ensure $outcomes<< :ensure end
	def bad_value() BadLoad.new end
	def echo_yield( x) yield x end
	def big( n) 'x'* n end
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
# A by-value class that the receiver cannot decode.
class BadLoad
	include ByVal
	def _dump( depth) '' end
	def self._load( str) raise "cannot load" end
end

class CapturingLogger
	attr_reader :lines
	def initialize() @lines= [] end
	def debug( m= nil) @lines<< [:debug, m || yield] end
	def warn( m= nil) @lines<< [:warn, m || yield] end
	def error( m= nil) @lines<< [:error, m || yield] end
end

class TestRO4R< Minitest::Test
	def setup
		# decode failures are logged as warnings; keep the test output clean
		@old_logger= RO4R.logger
		RO4R.logger= CapturingLogger.new
		$outcomes= Queue.new
		@listener= TCPServer.new '127.0.0.1', 0
		@port= @listener.addr[1]
		@server= RO4R::Server.new Exported.new, @listener
		@conn= connect
		@r= @conn.root
	end
	def teardown
		@server.close
		@peer_conn.close if @peer_conn
		if @peer
			Process.kill 'TERM', @peer.pid
			@peer.close
		end
		RO4R.logger= @old_logger
	end
	def connect **options
		RO4R::Connection.new "127.0.0.1:#{@port}", **options
	end
	# A server in another process, which does not share our class table.
	def connect_to_peer
		@peer= IO.popen [ RbConfig.ruby, "-I#{File.expand_path( '../lib', __dir__)}",
			File.join( __dir__, 'peer_server.rb')]
		port= @peer.gets.to_i
		@peer_conn= RO4R::Connection.new "127.0.0.1:#{port}"
	end
	def outcomes
		list= []
		list<< $outcomes.pop until $outcomes.empty?
		list
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
			@conn.__send__ :transmit, RO4R::Release.new( id, n)
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

	# A listening socket whose first accept fails, as accept(2) does with
	# ECONNABORTED when the peer resets before being accepted, or EMFILE.
	class FlakyListener< TCPServer
		def accept_nonblock( **opts)
			unless @failed
				@failed= true
				raise Errno::ECONNABORTED
			end
			super
		end
	end
	def test_accept_loop_survives_a_failed_accept
		l= FlakyListener.new '127.0.0.1', 0
		s= RO4R::Server.new Exported.new, l
		c= RO4R::Connection.new "127.0.0.1:#{l.addr[1]}", timeout: 5
		assert_equal 42, c.root.ok # the connection the failed accept concerned
		c.close
		c= RO4R::Connection.new "127.0.0.1:#{l.addr[1]}", timeout: 5
		assert_equal 42, c.root.ok # and the server still accepts after it
		c.close
		assert RO4R.logger.lines.any?{ |level, msg| level== :warn && msg.include?( "accept failed") }
		s.close
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
	rescue Minitest::Assertion
		ro4r_threads.each{ |t| $stderr.puts "leftover #{t.inspect} #{t.status}", t.backtrace.first( 5) }
		raise
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
		# payloads are plain Arrays dispatched on the header's kind: no class
		# names, no instance variable names, no call id
		msg= RO4R::Call.new( 1, 2, :m, [ 3], false)
		data= Marshal.dump msg.payload
		refute_includes data, "RO4R"
		refute_includes data, "@"
		assert_operator data.bytesize, :<, 20
		back= RO4R::Msg.decode msg.kind, msg.hid, Marshal.load( data)
		assert_instance_of RO4R::Call, back
		assert_equal [ 1, 2, :m, [ 3], false], [ back.tid, back.oid, back.nme, back.arg, back.blk]
		assert_instance_of RO4R::RefCall, RO4R::Msg.decode( RO4R::Msg::REFCALL, 1, msg.payload)
		# keywords are a field of the record, not a Kwargs object on the wire
		msg= RO4R::Call.new( 1, 2, :m, [ 3, RO4R::Kwargs.new( a: 1)], false)
		data= Marshal.dump msg.payload
		refute_includes data, "Kwargs"
		back= RO4R::Msg.decode msg.kind, msg.hid, Marshal.load( data)
		assert_equal [[ 3], { a: 1}], RO4R::Kwargs.unpack( back.arg)
		y= RO4R::Yield.new 7, 9, [ RO4R::Kwargs.new( x: 1)]
		back= RO4R::Msg.decode y.kind, y.hid, Marshal.load( Marshal.dump( y.payload))
		assert_equal [ 7, 9, [[], { x: 1}]], [ back.tid, back.yid, RO4R::Kwargs.unpack( back.arg)]
		assert_operator Marshal.dump( RO4R::Return.new( 5, 42, false).payload).bytesize, :<, 10
		assert_operator Marshal.dump( LID.new( 5)).bytesize, :<=, 10 # varint id
	end

	def test_malformed_payloads_are_rejected
		assert_raises( RO4R::ProtocolError) { RO4R::Msg.decode 99, 1, []}
		assert_raises( RO4R::ProtocolError) { RO4R::Msg.decode RO4R::Msg::CALL, 1, "x"}
		assert_raises( RO4R::ProtocolError) { RO4R::Msg.decode RO4R::Msg::CALL, 1, [ 0, :m]}
		assert_raises( RO4R::ProtocolError) { RO4R::Msg.decode RO4R::Msg::CALL, 1, [ 0, :m, 3, nil, false]} # args not an Array
		assert_raises( RO4R::ProtocolError) { RO4R::Msg.decode RO4R::Msg::RELEASE, 0, [ 1]}
		assert_raises( RO4R::ProtocolError) { RO4R::Msg.decode RO4R::Msg::RELEASE, 0, [ 1, "x"]}
	end

	def test_releases_merge
		r= RO4R::Release.new( 5, 2).merge( RO4R::Release.new( 6))
		assert_equal [ 5, 2, 6, 1], r.payload
		back= RO4R::Msg.decode r.kind, r.hid, Marshal.load( Marshal.dump( r.payload))
		assert_equal [[ 5, 2], [ 6, 1]], back.pairs
		assert_operator Marshal.dump( r.payload).bytesize, :<=, 12
	end

	def test_releases_queued_together_are_sent_as_one_message
		sent= []
		@conn.define_singleton_method( :transmit) do |msg|
			sent<< msg if msg.__kind_of? RO4R::Release
			super( msg)
		end
		outbox= @conn.instance_variable_get :@outbox
		100.times{ |i| outbox.push RO4R::Release.new( 100_000+ i) } # ids the server never gave out
		wait_until{ sent.sum( &:size)== 100}
		assert_operator sent.size, :<, 100, "releases were not merged"
		assert_equal 42, @r.ok
	end

	def test_exported_ids_are_per_connection_counters
		assert_same @r, @r.me # the root object comes back as the root proxy
		o= @r.obj
		assert_operator o.__remote_id, :<, 1000 # a counter, not an object id
		o2= @r.obj
		assert_equal o.__remote_id+ 1, o2.__remote_id
		assert_equal o2.__remote_id, @r.echo( o2).__remote_id # the same object keeps its id
		assert_raises( ArgumentError) { @conn.call Object.new, :ok } # only remote objects can be called
	end

	def test_nil_is_recognized_locally
		assert nil.__nil?
		refute Object.new.__nil?
		refute @r.__nil? # a proxy answers locally, no remote call
	end

	### Handshake ###

	def test_handshake_rejects_other_protocols
		# a 3.0 frame (13-byte header, no handshake) or anything else is refused
		[ [ "\x1d\x00\x00\x00\x01\x01\x00\x00\x00\x00\x00\x00\x00", "handshake"],
			[ [ 'RO4R', 99, 0].pack( RO4R::Connection::HELLO), "protocol 99"] ].each do |bytes, expected|
			l= TCPServer.new '127.0.0.1', 0
			t= Thread.new{ s= l.accept; s.write bytes; sleep 2; s.close }
			c= RO4R::Connection.new "127.0.0.1:#{l.addr[1]}"
			e= assert_raises( RO4R::ConnectionError) { c.root.ok }
			assert_includes e.message, expected
			c.close
			t.kill
			l.close
		end
		# and the server refuses such a client
		s= TCPSocket.new '127.0.0.1', @port
		s.write "GET / HTTP/1.0\r\n\r\n"
		data= s.read # the server's handshake, then EOF
		assert_equal RO4R::Connection::MAGIC, data[ 0, 4]
		assert_equal RO4R::Connection::HELLO_SIZE, data.bytesize
		s.close
		wait_until{ RO4R.logger.lines.any?{ |level, msg| level== :error && msg.include?( "handshake") } }
		assert_equal 42, @r.ok # other connections are unaffected
	end

	def test_peer_max_frame_is_learned_from_the_handshake
		s= RO4R::Server.new Exported.new, 0, max_frame: 200
		c= RO4R::Connection.new "127.0.0.1:#{s.server.addr[1]}"
		assert_equal 42, c.root.ok # the reply came after the server's handshake
		e= assert_raises( RO4R::ProtocolError) { c.root.echo 'x'* 1000 } # refused locally
		assert_includes e.message, "the peer's max_frame (200 bytes)"
		assert_equal 42, c.root.ok
		c.close
		s.close
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

	### Blocks that exit early ###

	def test_break_from_block_unwinds_the_server_method
		assert_equal :broke, @r.yield_once{ break :broke }
		wait_until{ $outcomes.size>= 2}
		assert_equal [ :yielding, :ensure], outcomes # not :completed
		wait_until{ ro4r_threads.none?{ |t| t.backtrace.any?{ |l| l.include? 'remote_yield'} } }
		assert_equal 42, @r.ok
	end

	def test_throw_and_non_standard_errors_from_block_unwind_the_server_method
		assert_equal :thrown, catch( :out) { @r.yield_once{ throw :out, :thrown } }
		wait_until{ $outcomes.size>= 2}
		assert_equal [ :yielding, :ensure], outcomes
		assert_raises( Interrupt) { @r.yield_once{ raise Interrupt } }
		wait_until{ $outcomes.size>= 2}
		assert_equal [ :yielding, :ensure], outcomes
		assert_equal 42, @r.ok
	end

	def test_standard_errors_from_block_are_raised_in_the_server_method
		assert_raises( CustomError) { @r.yield_once{ raise CustomError, "in block" } }
		wait_until{ $outcomes.size>= 2}
		assert_equal [ :yielding, :ensure], outcomes # the method saw the error
	end

	def test_block_return_value_is_converted
		o= @r.shared
		assert_same o, @r.yield_once{ |x| o } # a RID goes back, an LID returns
		assert_equal [ :yielding, [ :completed, Exported], :ensure], # the server got its own object
			outcomes.map{ |x| x.is_a?( Array) ? [ x[0], x[1].__class] : x }
		mine= Object.new
		assert_same mine, @r.yield_once{ |x| mine }
		assert_equal [ :yielding, [ :completed, RO4R::RemoteObject], :ensure],
			outcomes.map{ |x| x.is_a?( Array) ? [ x[0], x[1].__class] : x }
	end

	### Undecodable messages ###

	def test_undecodable_reply_raises_protocol_error
		e= assert_raises( RO4R::ProtocolError) { @r.bad_value }
		assert_includes e.message, "cannot load"
		assert_equal 42, @r.ok # the connection survives
	end

	def test_undecodable_argument_raises_protocol_error
		e= assert_raises( RO4R::ProtocolError) { @r.echo BadLoad.new }
		assert_includes e.message, "cannot load"
		assert_equal 42, @r.ok
	end

	def test_unknown_reference_in_arguments_raises_protocol_error
		bogus= RO4R::RemoteObject.new 123456, @conn
		e= assert_raises( RO4R::ProtocolError) { @r.echo [ bogus]}
		assert_includes e.message, "123456"
		assert_equal 42, @r.ok
	end

	def test_unsendable_block_result_unwinds_the_server_method
		# Our block's result cannot be sent: the server's yield must raise, not
		# wait forever for a result that never comes.
		c= connect max_frame: 4000
		e= assert_raises( RO4R::ProtocolError) { c.root.yield_once{ 'x'* 10_000 } }
		assert_includes e.message, "block result cannot be sent"
		assert_includes e.message, "max_frame"
		wait_until{ $outcomes.size>= 2}
		assert_equal [ :yielding, :ensure], outcomes # not :completed
		assert_equal 42, c.root.ok
		c.close
		e= assert_raises( RO4R::ProtocolError) { @r.yield_once{ Struct.new( :a) } } # unmarshallable
		assert_includes e.message, "block result cannot be sent"
		wait_until{ $outcomes.size>= 2}
		assert_equal [ :yielding, :ensure], outcomes
		wait_until{ ro4r_threads.none?{ |t| t.backtrace.any?{ |l| l.include? 'remote_yield'} } }
		assert_equal 42, @r.ok
	end

	def test_undecodable_block_argument_unwinds_the_server_method
		# The server's yield fails on our side; it must see an error, not hang.
		called= false
		e= assert_raises( RO4R::ProtocolError) { @r.echo_yield( BadLoad.new) { called= true } }
		assert_includes e.message, "cannot load"
		refute called
		assert_equal 42, @r.ok
	end

	### Objects of classes the peer does not define ###

	def test_nested_references_do_not_need_the_class_on_the_peer
		r= connect_to_peer.root
		assert_equal :hi, r.top.hi
		a= r.nested
		assert_equal :hi, a[0].hi
		assert_equal :hi, a[1][0].hi
		assert_equal :hi, r.kw_values( k: r.top)[0][1].hi
		# our objects, unknown to the peer, in arguments
		mine= Object.new
		assert_same mine, r.echo( mine)
		assert_same mine, r.echo( [ mine])[0]
		assert_same mine, r.echo( [[ mine]])[0][0]
		assert_same mine, r.kw_values( k: mine)[0][1]
		# and our proxies of its objects, passed back nested
		assert r.has_hi?( r.top)
		assert_equal :hi, r.echo( [ r.top])[0].hi
	end

	def test_unknown_classes_do_not_break_the_connection
		r= connect_to_peer.root
		e= assert_raises( RO4R::ProtocolError) { r.fail_unknown }
		assert_includes e.message, "PeerOnlyError"
		e= assert_raises( RO4R::ProtocolError) { r.value }
		assert_includes e.message, "PeerOnlyValue"
		e= assert_raises( RO4R::ProtocolError) { r.echo Point.new( 1, 2) }
		assert_includes e.message, "Point"
		assert_equal 1, r.echo( 1)
	end

	### Timeouts ###

	def test_call_timeout
		e= assert_raises( RO4R::TimeoutError) { @conn.call @r, :sleep_for, [ 0.3], timeout: 0.05}
		assert_match(/0.05/, e.message)
		assert_equal 42, @r.ok # usable right away
		sleep 0.4 # the late reply is discarded
		assert_equal 42, @r.ok
	end

	def test_connection_timeout_option
		c= connect timeout: 0.05
		assert_equal 0.05, c.timeout
		assert_raises( RO4R::TimeoutError) { c.root.sleep_for 0.3}
		assert_equal :woke, c.call( c.root, :sleep_for, [ 0.1], timeout: nil) # per-call override
		c.close
	end

	def test_late_yield_after_timeout_unwinds_the_server_method
		assert_raises( RO4R::TimeoutError) { @conn.call @r, :slow_yield, [], proc{}, timeout: 0.05}
		wait_until{ !$outcomes.empty? }
		assert_equal [ :ensure], outcomes
		assert_equal 42, @r.ok
	end

	def test_interrupted_call_does_not_receive_a_stale_reply
		require 'timeout'
		assert_raises( Timeout::Error) { Timeout.timeout( 0.05) { @r.sleep_for 0.3 } }
		assert_equal :fresh, @r.echo( :fresh)
		sleep 0.4
		assert_equal :fresh, @r.echo( :fresh)
	end

	def test_connect_timeout_is_accepted
		c= connect connect_timeout: 1
		assert_equal 42, c.root.ok
		c.close
	end

	### Options ###

	def test_allowlist
		s= RO4R::Server.new Exported.new, 0, allow: [ :ok, 'echo']
		c= RO4R::Connection.new "127.0.0.1:#{s.server.addr[1]}"
		assert_equal 42, c.root.ok
		assert_equal 1, c.root.echo( 1)
		e= assert_raises( SecurityError) { c.root.obj }
		assert_includes e.message, "obj"
		assert_raises( SecurityError) { c.root.send :ok } # the blocklist still applies
		c.close
		s.close
		s= RO4R::Server.new Exported.new, 0, allow: proc{ |obj, m| obj.is_a?( Exported) && m== :ok }
		c= RO4R::Connection.new "127.0.0.1:#{s.server.addr[1]}"
		assert_equal 42, c.root.ok
		assert_raises( SecurityError) { c.root.echo 1 }
		c.close
		s.close
	end

	def test_poolsize_and_logger_options
		log= CapturingLogger.new
		s= RO4R::Server.new Exported.new, 0, poolsize: 1, logger: log
		c= RO4R::Connection.new "127.0.0.1:#{s.server.addr[1]}", poolsize: 2
		assert_equal 2, c.poolsize
		wait_until{ s.connections.size== 1}
		assert_equal 1, s.connections.first.poolsize
		assert_raises( RuntimeError) { c.root.boom }
		wait_until{ log.lines.any? } # went to the connection's logger
		assert_equal :debug, log.lines.first[0]
		c.close
		s.close
	end

	def test_on_close_option
		closed= Queue.new
		s= RO4R::Server.new Exported.new, 0, on_close: proc{ |conn| closed<< conn }
		c= RO4R::Connection.new "127.0.0.1:#{s.server.addr[1]}", on_close: proc{ |conn| closed<< :client }
		assert_equal 42, c.root.ok
		c.close
		seen= [ closed.pop, closed.pop]
		assert_includes seen, :client
		assert_kind_of RO4R::Connection, seen.find{ |x| x!= :client }
		s.close
	end

	def test_max_frame_on_send
		c= connect max_frame: 200
		e= assert_raises( RO4R::ProtocolError) { c.root.echo 'x'* 1000 }
		assert_includes e.message, "max_frame"
		assert_equal 42, c.root.ok
		c.close
	end

	def test_max_frame_on_receive
		s= RO4R::Server.new Exported.new, 0, max_frame: 200
		c= RO4R::Connection.new "127.0.0.1:#{s.server.addr[1]}"
		e= assert_raises( RO4R::ProtocolError) { c.root.echo 'x'* 1000 } # skipped by the server
		assert_includes e.message, "max_frame"
		assert_equal 42, c.root.ok
		# a reply too large for us to send is reported, not silently dropped
		e= assert_raises( RuntimeError) { c.root.big( 1000) }
		assert_includes e.message, "max_frame"
		c.close
		s.close
	end

	def test_frame_header_carries_kind_and_call_id
		assert_equal [ RO4R::Msg::CALL, 7], [ RO4R::Call.new( 7, 0, :m, [], false).kind, RO4R::Call.new( 7, 0, :m, [], false).hid]
		assert_equal [ RO4R::Msg::RETURN, 7], [ RO4R::Return.new( 7, nil, false).kind, RO4R::Return.new( 7, nil, false).hid]
		assert_equal [ RO4R::Msg::YIELD, 9], [ RO4R::Yield.new( 7, 9, []).kind, RO4R::Yield.new( 7, 9, []).hid]
		assert_equal [ RO4R::Msg::RELEASE, 0], [ RO4R::Release.new( 5).kind, RO4R::Release.new( 5).hid]
		assert_equal [ RO4R::Msg::REFCALL, 7], [ RO4R::RefCall.new( 7, 0, :m, [], false).kind, RO4R::RefCall.new( 7, 0, :m, [], false).hid]
	end

	def test_cyclic_arrays_still_travel
		a= [ 1]
		a<< a
		b= @r.echo a
		assert_equal 1, b[0]
		assert_same b, b[1]
	end

	def test_gc_churn_keeps_references_consistent
		o= @r.shared
		200.times do |i|
			@r.obj
			@r.echo [ @r.obj, o]
			GC.start if i% 25== 0
		end
		GC.start
		sleep 0.1
		assert_equal 42, o.ok
		assert_equal 42, @r.ok
	end

end
