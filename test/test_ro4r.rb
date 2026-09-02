# Run with:  ruby -I. test/test_ro4r.rb
require 'minitest/autorun'
require 'socket'
require 'RO4R'

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
	def obj() Object.new end
	def echo( x) x end
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

	def test_basic_call
		assert_equal 42, @r.ok
		assert_equal [1, 'a', :b], @r.echo( [1, 'a', :b])
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

	def test_string_port_forms
		s= RO4R::Server.new Exported.new, "127.0.0.1:0"
		assert_equal '127.0.0.1', s.server.addr[3]
		s.close
		s= RO4R::Server.new Exported.new, "0"
		assert_equal RO4R::Server::DefaultAddress, s.server.addr[3]
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
		ro4r_threads= lambda do
			Thread.list.select{ |t| t!= Thread.current and
				Array( t.backtrace).any?{ |l| l.include? 'RO4R.rb'} }
		end
		refute_empty ro4r_threads.call
		@server.close
		wait_until{ ro4r_threads.call.empty?}
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
