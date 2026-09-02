# "Remote Objects for Ruby" (RO4R) Copyright 2007-2010 Nenad Ocelic
# Based on "Ruby RPC" Copyright 2005 Brian Ollenberger
# This program is licensed under the terms of the GNU Affero General Public
# License, version 3 or later; see LICENSE.

require 'socket'
require_relative 'ro4r/version'

module ByVal
	def __by_ref?() false end
end
module ByRef
	def __by_ref?() true end
	# Enforce passing an object by ref:
	def byref!
		unless __by_ref? or nil.eql? self
			# nil is always passed by value
			instance_eval{ def __by_ref?() true end }
		end
		self
	end
end

class Object
	# Save before redefining in the Ref::Remote class:
	alias_method :__class, :class
	alias_method :__kind_of?, :kind_of?
	alias_method :__nil?, :nil?
	# Default to passing objects by reference:
	include ByRef
	def self._load str
		Marshal.load str
	end
	alias_method :__respond_to?, :respond_to?
	# Marshal asks respond_to?(:_dump) for every object it dumps; answering
	# true for by-reference objects makes it call our _dump below, which
	# replaces the object with a reference. This only happens while RO4R
	# itself is marshalling (Thread.current[:__ro4r_marshal] is set), so other
	# users of Marshal in the process are unaffected. Otherwise, and for
	# by-value objects, a class's own _dump (e.g. Time's) is honoured.
	#
	# Defined in a prepended module rather than directly on Object, since Ruby
	# warns about redefining Object#respond_to? (possible infinite loop). It is
	# also prepended to by-value classes that define their own respond_to?
	# (e.g. Exception), which would otherwise bypass this override.
	module RespondTo
		# class -> does it define _dump itself, rather than inherit ours?
		CUSTOM_DUMP= Hash.new do |h, klass|
			h[ klass]= klass.instance_method( :_dump).owner!= RespondTo
		end
		def respond_to? method, priv= false
			if :_dump== method
				if Thread.current[:__ro4r_marshal] && __by_ref?
					true
				else
					CUSTOM_DUMP[ __class]
				end
			else
				super
			end
		end
		def _dump depth= -1
			conn= Thread.current[:__conn]
			Marshal.dump conn.make_reference( self, true), depth
		end
	end
	prepend RespondTo
	def __remote_call method, arguments, block
		m= method.to_sym
		if __respond_to? m, false
			unless __secure_method? m
				raise SecurityError,
					"insecure method '#{method}' called" # for #{self.inspect}"
			end
			# public_send, not __send__: visibility is enforced at call time
			# even if the class overrides respond_to?.
			args, kw= RO4R::Kwargs.unpack arguments
			if kw
				public_send m, *args, **kw, &block
			else
				public_send m, *args, &block
			end
		else
			raise NoMethodError,
				"undefined or private remote method '#{m}' called" # for #{self.inspect}"
		end
	end
	# Methods that must not be invoked by a remote peer on an exported object.
	# NOTE: this is a blocklist, not a sandbox; see "Security" in README.md.
	# Names starting with '_' (e.g. __send__, _dump) are rejected separately.
	INSECURE_METHODS= [
		:send, :public_send, :method, :public_method, :singleton_method,
		:eval, :instance_eval, :instance_exec, :class_eval, :class_exec,
		:module_eval, :module_exec, :define_method, :define_singleton_method,
		:instance_variable_get, :instance_variable_set, :remove_instance_variable,
		:extend, :singleton_class, :system, :exec, :spawn, :fork, :exit, :exit!,
		:abort, :load, :require, :require_relative, :binding,
	].freeze
	def __secure_method? method
		## intended to be redefined in subclasses, e.g. with an allowlist
		not INSECURE_METHODS.include? method
	end
end

module RO4R
	# Id under which each side exports its root object; no real object has it.
	ROOT_ID= 0

	# Minimal default logger: warnings and errors go to $stderr, debug output
	# (e.g. exceptions raised by remote calls, which are returned to the caller
	# anyway) is discarded. Assign any Logger-compatible object, i.e. one that
	# responds to debug/warn/error and accepts a message or a block, to
	# RO4R.logger to change this; e.g. RO4R.logger= Logger.new( $stdout).
	class StderrLogger
		def debug( msg= nil) end
		def warn( msg= nil) $stderr.puts "RO4R: #{msg || yield}" end
		def error( msg= nil) $stderr.puts "RO4R: #{msg || yield}" end
	end
	@logger= StderrLogger.new
	class << self
		attr_accessor :logger
	end

	# Raised when a message cannot be sent because the peer is gone.
	class ConnectionError< RuntimeError; end

	# Keyword arguments in transit. A call made with keywords collects them
	# in a trailing Hash flagged by ruby2_keywords; since Hashes travel by
	# reference, that Hash is replaced by a Kwargs (by value, as an Array of
	# pairs) before sending, and turned back into keywords by the receiver.
	class Kwargs
		include ByVal
		def initialize hash
			@pairs= hash.to_a
		end
		def to_h() @pairs.to_h end
		def marshal_dump() @pairs end
		def marshal_load ary; @pairs= ary end
		# args with a trailing flagged Hash replaced by a Kwargs
		def self.pack args
			last= args.last
			if last.__kind_of?( Hash) && Hash.ruby2_keywords_hash?( last)
				args[ 0...-1]<< new( last)
			else
				args
			end
		end
		# [positional args, keywords Hash or nil]
		def self.unpack args
			if args.last.__kind_of?( self)
				[ args[ 0...-1], args.last.to_h]
			else
				[ args, nil]
			end
		end
	end

	# Split "host", "host:port", "v6addr" (more than one colon), "[v6addr]" or
	# "[v6addr]:port" into [host, port]; port is default_port when absent.
	def self.parse_address str, default_port
		case str
		when /\A\[([^\]]*)\](?::(\d+))?\z/
			[ $1, ( $2 ? $2.to_i : default_port)]
		when /\A([^:]*):(\d+)\z/
			[ $1, $2.to_i]
		else
			[ str, default_port]
		end
	end

	# Transparent reference to a remote object:
	class RemoteObject
		for m in instance_methods
			undef_method m unless m[0..1]== '__' || m== :object_id
		end

		# refs: a one-element Array counting the references received from the
		# peer for this id; shared with the finalizer, which reports the count
		# back so that the peer can drop the object once all are released.
		def initialize id, connection, refs= [ 0]
			@id = id
			@conn= connection
			@refs= refs
		end
		def __connection
			@conn
		end
		def __remote_id
			@id
		end
		def __refs
			@refs
		end
		def _dump *arg
			super
		end
		private
		# ruby2_keywords: keyword arguments arrive as a flagged trailing Hash,
		# which Kwargs.pack turns into something that can travel by value.
		ruby2_keywords def method_missing name, *args, &block
			if name.to_s[0..0]== '_' # keep 'underscore calls' local
				super
			else
				@conn.call self, name, Kwargs.pack( args), block
			end
		end
		def __remote_call method, arguments, block
			# need not the check the args (will be done remotely)
			__send__ method, *arguments, &block
		end
	end

	### Serializable references ###
	class Ref
		def initialize id
			@id= id
		end
		def oid() @id end
		def self._load str
			id= str.unpack( 'q<').first
			Thread.current[:__conn].make_object self.new( id)
		end
		def _dump depth
			[ @id].pack( 'q<')
		end
	end
end

## The following objects belog to RO4R, but are defined at global scope
## to save some bandwidth (LID vs. RO4R::LID)

# module RO4R
	### Serializable references ###
	# local object of the sender
	class LID< RO4R::Ref
	end
	# remote for the sender (local for the receiver)
	class RID< RO4R::Ref
	end
#end

module RO4R
	### Connection to a remote machine (server or client) ###
	class Connection
		# safe: unused since Ruby 3.0 removed $SAFE; kept for API compatibility.
		# The optional block is invoked with the connection once it is closed.
		def initialize host='localhost', object= nil, safe= nil, &on_close
			@safe= safe
			@on_close= on_close
			@closed= false
			@send_mutex= Mutex.new
			@recv_mutex= Mutex.new
			@local_mutex= Mutex.new
			@object= object # local object (exported over connection)
			@receiver= {} # thread id -> Queue of the call awaiting a reply
			@remote= ObjectSpace::WeakMap.new # remote_id -> RemoteObject (to reuse it)
			@local= {} # local_id -> local Object (must not disappear while in use remotely)
			@local_refs= Hash.new( 0) # local_id -> references handed to the peer
			case host
			when String
				host, port= RO4R.parse_address host, Server::DefaultPort
				@io= TCPSocket.new host, port
			else
				@io= host
			end
			# request/response traffic: don't let Nagle hold back small writes
			@io.setsockopt( :TCP, :NODELAY, 1) if @io.is_a?( TCPSocket)
			@local[ ROOT_ID]= @object
			@pool= [] # mailboxes (Queues) of idle worker threads for remote calls
			@poolsize= 5 # how many idle workers to keep
			@release_q= Queue.new # [id, count] of collected RemoteObjects, to release remotely
			@root= RemoteObject.new ROOT_ID, self # remote object exported to us
			# Start the threads last: a reply to the peer's first call may be
			# sent before this constructor returns.
			@thread= Thread.new{ listen } # Wait for incoming messages
			@release_thread= Thread.new{ release_loop }
		end
		attr_reader :object, :root, :thread, :poolsize

		def inspect
			 "<#{self.to_s} @safe=#{@safe}>"
		end

		def join() @thread.join end
		def close()
			if @io.respond_to? 'shutdown'
				@io.shutdown
			else
				@io.close
			end
		rescue IOError, SystemCallError
			# already closed
		end

		private
		def add_remote id
			### TODO: check whether to allow foreign objects or not
			refs= [ 0]
			ro= RemoteObject.new id, self, refs
			@remote[ id]= ro # weak; the entry disappears when ro is collected
			ObjectSpace.define_finalizer ro, make_finalizer( id, refs)
			ro
		end
		def make_finalizer id, refs
			# Finalizers may run on any thread, including one that holds
			# @send_mutex, so they must not send directly; queue the id for
			# release_loop instead. refs must not be read before the object
			# is dead, since references may still arrive for it until then.
			q= @release_q
			proc{ |oid| q.push [ id, refs[0]] rescue nil } # ClosedQueueError once disconnected
		end
		# Tell the peer that we no longer hold references to its objects.
		def release_loop
			while ( r= @release_q.pop)
				send Release.new( *r)
			end
		rescue ConnectionError, ClosedQueueError
			# peer gone; nothing more to release
		end
		def local_reference object
			# this ref will be handed over, local object must not disappear
			id= object.__id__
			@local_mutex.synchronize do
				@local[ id]= object
				@local_refs[ id]+= 1
			end
			LID.new id
		end
		# The peer releases as many references as it received for the id (it
		# may have received some again after an earlier proxy was collected),
		# so the object stays exported until the counts match.
		def release_local id, count
			return if id== ROOT_ID
			@local_mutex.synchronize do
				n= @local_refs[ id]- count
				if n> 0
					@local_refs[ id]= n
				else
					@local_refs.delete id
					@local.delete id
				end
			end
		end
		def remote_object rid
			ro= @remote[ rid] || add_remote( rid) # reuse or create
			ro.__refs[0]+= 1
			ro
		end
		public

		def has_local? id
			@local.has_key? id
		end
		def make_object reference # after receiving, from sender POV
			return reference unless reference.__kind_of? Ref
			if reference.kind_of? RID	 # our local object
				unless has_local? reference.oid  # was that ref ever given?
					raise TypeError, "No reference given for object id #{reference.oid}"
				end
				@local[ reference.oid]
			else # kind_of LID, we got a remote object
				remote_object reference.oid
			end
		end
		def make_reference object, forceref= nil # before sending, from our POV
			if object.__kind_of? RemoteObject
				if object.__connection== self
					RID.new object.__remote_id
				else
					local_reference object
				end
			else
				if forceref or object.__by_ref?
					local_reference object
				else # by_val, i.e. copy
					object
				end
			end
		end
		def call object, name, args=[], block= nil, byref= false
			oid= if object.__kind_of? RemoteObject
				object.__remote_id
			else
				object.__id__
			end
			tid= Thread.current.__id__
			cls= byref ? RefCall : Call
			msg= cls.new tid, oid, name, args, !block.nil?
			await_return msg, &block
		end
		def byref object, name, args=[], block= nil
			call object, name, args, block, true
		end

		private
		def remote_yield tid, args
			msg= Yield.new tid, Thread.current.__id__, args
			await_return msg
		end
		# Send msg and wait for the peer's reply on a Queue registered under
		# our thread id, which the peer echoes back. Yields from the peer are
		# served in between; the Queue is unregistered while the block runs,
		# so a nested call from the block can register its own.
		def await_return msg
			tid= Thread.current.__id__
			q= Queue.new
			while true
				@recv_mutex.synchronize do
					raise ConnectionError, "Unable to reach peer" if @closed
					@receiver[ tid]= q
				end
				begin
					send msg
					msg= q.pop
				ensure
					@recv_mutex.synchronize{ @receiver.delete tid}
				end
				case msg
				when Return # got our answer
					if msg.err  # error, propagate
						raise msg.obj
					else # return value
						return msg.obj
					end
				when Yield  # need to call block
					begin
						args, kw= Kwargs.unpack msg.arg
						r= kw ? yield( *args, **kw) : yield( *args)
						msg= Return.new msg.yid, r, false
					rescue => e
						msg= Return.new msg.yid, e, true
					end
				when Exception
					raise msg
				else
					raise RuntimeError, "Cannot interpret message #{msg}"
				end
			end
		end
		# Hand an incoming Call to an idle worker thread, or a new one. Workers
		# are unbounded on purpose: a remote call may call back into the peer,
		# which may call us again while the first call is still running.
		def local_call message
			mbox= @recv_mutex.synchronize{ @pool.pop}
			mbox||= spawn_worker
			mbox.push message
		end
		# A worker executes calls handed to it through its own Queue. When done
		# it returns to the idle pool, or exits if the pool is full or the
		# connection is closed.
		def spawn_worker
			mbox= Queue.new
			Thread.new do
				while ( msg= mbox.pop)
					begin
						handle_call msg
					rescue ConnectionError
						break # peer gone, nothing to reply to
					end
					keep= @recv_mutex.synchronize do
						!@closed && @pool.size< @poolsize && @pool.push( mbox)
					end
					break unless keep
				end
			end
			mbox
		end
		def handle_call msg
			if @local.has_key? msg.oid
				obj= @local[ msg.oid]
				if obj.__nil?
					raise TypeError, "No root object exported"
				end
			else
				raise TypeError, "Receiver ##{msg.oid} not found"
			end
			if msg.nme
				if msg.nme.to_s[0..0]=='_'
					raise NoMethodError, "Call forbidden"
				end
				blk= if msg.blk
					proc { | *arg| remote_yield msg.tid, Kwargs.pack( arg)}.ruby2_keywords
				end
				r= obj.__remote_call msg.nme, msg.arg, blk
			else
				r= obj
			end
			# Explicit conversion here so that return-by-ref call can be
			# honored (but ignore byref request for nil)
			forceref= msg.kind_of?( RefCall) && RefCall.enabled? && !r.__nil?
			send Return.new( msg.tid, make_reference( r, forceref), false)
		rescue Exception=> e
			RO4R.logger.debug do
				"error in remote call '#{msg.nme}': #{e.class}: #{e.message}\n" +
					Array( e.backtrace).join( "\n")
			end
			send_error msg.tid, e
		end

		# Return an exception to the caller. If it cannot be marshalled (e.g. it
		# holds a Proc or an IO), send a plain RuntimeError instead, so that the
		# remote caller is not left waiting forever.
		def send_error tid, e
			send Return.new( tid, e, true)
		rescue TypeError, ArgumentError
			send Return.new( tid, RuntimeError.new( "#{e.class}: #{e.message}"), true)
		end

		# Read up to maxlen bytes, at least one; raise EOFError when the peer is
		# gone. Intended to be redefined if necessary.
		def read io, maxlen
			io.readpartial maxlen
		end
		READ_CHUNK= 65536
		# Return the payload of the next length-prefixed frame. `buf` holds bytes
		# read ahead of the frame boundary and is reused across calls.
		def read_frame buf
			buf<< read( @io, READ_CHUNK) while buf.bytesize< 4
			len= buf.unpack1( "L<")
			buf<< read( @io, READ_CHUNK) while buf.bytesize< 4+ len
			frame= buf.byteslice( 4, len)
			buf[ 0, 4+ len]= ''
			frame
		end

		def listen
			Thread.current[:__conn]= self
			@io.binmode
			begin
				buf= String.new( capacity: READ_CHUNK)
				while true
					msg= Marshal.load read_frame( buf)
					case msg
					when Call
						local_call msg
					when Return, Yield
						q= @recv_mutex.synchronize{ @receiver[ msg.tid]}
						if q
							q.push msg
						else
							RO4R.logger.warn "no thread waiting for #{msg.__class} #{msg.tid}"
						end
					when Release
						release_local msg.oid, msg.count
					else
						RO4R.logger.warn "ignoring unexpected #{msg.__class} message"
					end
				end
			rescue Exception=> e
				# EOFError / ECONNRESET just mean the peer disconnected; don't
				# report those, but still run the cleanup below so that threads
				# waiting on this connection are woken up with the error.
				unless e.is_a?( EOFError) or e.is_a?( Errno::ECONNRESET)
					RO4R.logger.error{ "connection closed: #{e.inspect}"}
				end
				@io.close
				# Wake up callers waiting for a reply. @closed is set under
				# @recv_mutex so that no caller can register after this.
				err= ConnectionError.new "Unable to reach peer (#{e.class}: #{e.message})"
				@recv_mutex.synchronize do
					@closed= true
					@receiver.each_value{ |q| q.push err}
				end
			ensure
				# purge finalizers
				@remote.each_value do |ro|
					ObjectSpace.undefine_finalizer ro
				end
				@remote= ObjectSpace::WeakMap.new # useless after the connection is broken
				@local_mutex.synchronize{ @local.clear; @local_refs.clear} # free local objects
				# stop the helper threads
				@closed= true
				@release_q.close
				@recv_mutex.synchronize{ @pool.each( &:close); @pool.clear}
				@on_close.call self if @on_close
			end
		end

		def send msg
			raise ConnectionError, "Unable to reach peer" if @closed
			RO4R.logger.warn "sending a #{msg.__class} instead of a Msg" unless msg.__kind_of? Msg
			t= Thread.current
			t[:__conn]= self
			t[:__ro4r_marshal]= true
			begin
				data= Marshal.dump msg
			ensure
				t[:__ro4r_marshal]= nil
			end
			@send_mutex.synchronize do
				@io.write [ data.bytesize].pack( "L<"), data # one writev, no concatenation
			rescue IOError, SystemCallError=> e
				raise ConnectionError, "Unable to reach peer (#{e.message})"
			end
			nil
		end
	end

	### Serializable messages ###
	# Messages travel by value as an Array of their fields (marshal_dump), so
	# that the wire carries no instance variable names.
	class Msg
		include ByVal
	end
	# method call
	class Call< Msg
		def initialize thread_id, object_id, method_name, args, block=nil
			@tid, @oid, @nme= thread_id, object_id, method_name
			@arg, @blk= args, block
		end
		attr_reader :tid, :oid, :nme, :arg, :blk
		def marshal_dump
			[ @tid, @oid, @nme, @arg, @blk]
		end
		def marshal_load ary
			@tid, @oid, @nme, @arg, @blk= ary
		end
	end
	# method call whose result is to be returned by reference; honoured by the
	# receiver only when enabled there (it is disabled by default).
	class RefCall< Call
		@enabled= false
		class << self
			def enable() @enabled= true end
			def disable() @enabled= false end
			def enabled?() @enabled end
		end
	end
	# yield to remote block
	class Yield< Msg
		def initialize threadid, yield_threadid, args
			@tid, @yid, @arg = threadid, yield_threadid, args
		end
		attr_reader :tid, :yid, :arg
		def marshal_dump
			[ @tid, @yid, @arg]
		end
		def marshal_load ary
			@tid, @yid, @arg= ary
		end
	end
	# return value or exception return message
	# (also for returning values from blocks after yield)
	class Return< Msg
		def initialize thread_id, object, exception
			@tid, @obj, @err= thread_id, object, exception
		end
		attr_reader :tid, :obj, :err
		def marshal_dump
			[ @tid, @obj, @err]
		end
		def marshal_load ary
			@tid, @obj, @err= ary
		end
	end
	# release `count` references to the sender's RemoteObject for the
	# receiver's local object `oid`
	class Release< Msg
		def initialize id, count= 1
			@oid, @count= id, count
		end
		attr_reader :oid, :count
		def marshal_dump
			[ @oid, @count]
		end
		def marshal_load ary
			@oid, @count= ary
		end
	end

	# A server that exports a single object on a socket.
	class Server
		DefaultPort= 4044
		# Loopback only: RO4R is not safe to expose to untrusted peers. Pass
		# "0.0.0.0:port" (or "[::]:port") to listen on all interfaces.
		DefaultAddress= '127.0.0.1'
		attr_reader :connections, :server # server: the listening socket

		# safe: unused since Ruby 3.0 removed $SAFE; kept for API compatibility.
		def initialize object= Hash.new, port= DefaultPort, safe= nil
			### root object must be passed by ref:
			raise ArgumentError, "#{object} must be sent by reference" unless object.__by_ref?
			@server= case port
			when Integer
				TCPServer.new DefaultAddress, port
			when /\A\d+\z/ # "port"
				TCPServer.new DefaultAddress, port.to_i
			when String # "address:port", "[v6addr]:port"
				TCPServer.new( *RO4R.parse_address( port, DefaultPort))
			else
				port # probably an IO
			end
			# Automatically accept incoming connections; a connection removes
			# itself from the list once its peer disconnects.
			@connections= []
			@mutex= Mutex.new
			@thread= Thread.new do
				begin
					while true
						io= @server.accept
						@mutex.synchronize do
							@connections << Connection.new( io, object, safe) do |c|
								@mutex.synchronize{ @connections.delete c}
							end
						end
					end
				rescue IOError, SystemCallError # listening socket closed
				end
			end
		end

		def join() @thread.join end

		# Stop accepting connections and disconnect all clients.
		def close
			# Wake the accept loop with shutdown and let it exit before closing
			# the socket: closing it while a thread is blocked in accept makes
			# Ruby raise IOError in that thread even if accept(2) has just
			# returned, which leaks the accepted descriptor and leaves that
			# client hanging with an open, unserved connection.
			if @server.respond_to? :shutdown
				@server.shutdown rescue nil
				@thread.join
			end
			@server.close unless @server.closed?
			@thread.join # no connection can be added after the accept loop ends
			@mutex.synchronize{ @connections.dup}.each do |c|
				c.close rescue nil
				c.thread.join 5 # wait for its listen thread to clean up
			end
			nil
		end
	end

	# Classes # to be passed by value (serialized):
	[ Array,
		#Complex,
		Exception,
		FalseClass, Float,
		Integer,
		MatchData,
		NilClass,	Numeric,
		Range, Regexp,
		String, Symbol,
		Time, TrueClass,
		# Exception backtraces are held in a Thread::Backtrace object, which
		# Marshal converts to an Array via a compat hook. That hook is only
		# reached if Object#_dump does not claim the object, so it must be
		# by-value or exceptions cannot be returned to the caller.
		(Thread::Backtrace if defined? Thread::Backtrace),
	].compact.each do |c|
		c.class_eval{ extend ByVal; include ByVal}
		# Exception defines its own respond_to?, which would hide the override
		# in Object::RespondTo and make exceptions travel by reference:
		unless c.instance_method( :respond_to?).owner== Object::RespondTo
			c.prepend Object::RespondTo
		end
	end
	# Objects # to be passed by value (serialized):
	[	Binding,
		Class,
		Comparable,
		Enumerable,
		File,
		Hash,
		Kernel,
		IO,
		Math, Method, Module,
		Proc, Process,
		#Object,
		Struct,
		Thread
	].each do |o|
		o.extend ByVal
	end

end
