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
	# Connection#make_reference replaces by-reference objects in the message
	# itself, in Arrays and in keyword arguments before Marshal sees them, so
	# this hook is only reached for objects nested inside other by-value
	# objects (e.g. an instance variable of an Exception). Since Marshal
	# writes the class name next to the _dump data, the peer must know the
	# class of any object that travels this way.
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
	# A connection may also be given its own logger (Connection.new logger:).
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
	# Raised when the peer does not reply within the call's timeout. The
	# connection stays usable; the peer may still be executing the call.
	class TimeoutError< RuntimeError; end
	# Raised when a message cannot be decoded by its receiver (e.g. it holds
	# an object of a class the receiver does not define) or exceeds the
	# maximum frame size. The connection stays usable.
	class ProtocolError< RuntimeError; end
	# Raised inside a method that is yielding to a remote block when the
	# block exits early (break, return, throw, or an exception that is not a
	# StandardError) or when the caller has abandoned the call (timeout).
	# It is not a StandardError, so that a bare rescue in the method does not
	# swallow it; ensure blocks still run. It is not returned to the caller,
	# who is gone already.
	class BlockExit< Exception; end

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
		# Replace every key and value by the block's result.
		def convert!
			@pairs.map!{ |k, v| [ yield( k), yield( v)] }
			self
		end
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
			last= args.last
			if last.__kind_of?( self)
				[ args[ 0...-1], last.to_h]
			elsif last.__kind_of?( RemoteObject)
				# Ruby 2.7 takes a trailing argument as keywords if the method
				# accepts them, probing it with to_hash: on a RemoteObject that
				# is a remote call, and what comes back is another RemoteObject,
				# never a Hash. Passing empty keywords keeps it positional.
				[ args, {}]
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

	# Open a TCP connection, with an optional connect timeout in seconds.
	def self.tcp_connect host, port, timeout= nil
		if RUBY_VERSION>= '3.0'
			TCPSocket.new host, port, connect_timeout: timeout
		elsif timeout
			Socket.tcp host, port, connect_timeout: timeout
		else
			TCPSocket.new host, port
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
		# Largest message accepted or sent, in bytes, unless configured
		# otherwise (max_frame:).
		MAX_FRAME= 1<< 28 # 256 MiB
		# Does Queue#pop accept a timeout (Ruby 3.2+)?
		QUEUE_TIMEOUT= begin
			Queue.new.pop( timeout: 0)
			true
		rescue ArgumentError, ThreadError # older: the Hash is taken as non_block
			require 'timeout'
			false
		end
		class PopTimeout< Exception; end # private_constant below

		# host: "host", "host:port", "[v6addr]:port" or a connected IO.
		# object: the local object exported to the peer as its root.
		# safe: unused since Ruby 3.0 removed $SAFE; kept for API compatibility.
		# Options:
		# poolsize: idle worker threads kept per connection (default 5)
		# logger: a Logger-compatible object for this connection (default:
		#   RO4R.logger)
		# timeout: seconds to wait for a reply before raising TimeoutError
		#   (default nil: wait forever); Connection#call can override it
		# connect_timeout: seconds to wait when connecting to a host
		# max_frame: largest message, in bytes, sent or accepted
		# allow: methods the peer may call on any object exported over this
		#   connection: an Array of names, or a callable receiving (object,
		#   method name) and returning true to allow the call. The blocklist
		#   (Object::INSECURE_METHODS, names starting with '_') still applies.
		# on_close: called with the connection once it is closed; the block
		#   is an alternative way to pass it.
		def initialize host= 'localhost', object= nil, safe= nil,
				poolsize: 5, logger: nil, timeout: nil, connect_timeout: nil,
				max_frame: MAX_FRAME, allow: nil, on_close: nil, &on_close_block
			@safe= safe
			@on_close= on_close || on_close_block
			@logger= logger
			@timeout= timeout
			@max_frame= max_frame
			@allow= if allow.respond_to?( :call) or allow.nil?
				allow
			else
				allow.map( &:to_sym)
			end
			@closed= false
			@send_mutex= Mutex.new
			@recv_mutex= Mutex.new
			@local_mutex= Mutex.new
			@object= object # local object (exported over connection)
			@seq= 0 # last call id handed out
			@receiver= {} # call id -> Queue of the call awaiting a reply
			@remote= ObjectSpace::WeakMap.new # remote_id -> RemoteObject (to reuse it)
			@local= {} # local_id -> local Object (must not disappear while in use remotely)
			@local_refs= Hash.new( 0) # local_id -> references handed to the peer
			case host
			when String
				host, port= RO4R.parse_address host, Server::DefaultPort
				@io= RO4R.tcp_connect host, port, connect_timeout
			else
				@io= host
			end
			if @io.__kind_of?( BasicSocket) and @io.local_address.ip?
				# request/response traffic: don't let Nagle hold back small writes
				@io.setsockopt :TCP, :NODELAY, 1
				# notice a peer that vanished without closing the connection
				@io.setsockopt :SOCKET, :KEEPALIVE, 1
			end
			@local[ ROOT_ID]= @object
			@pool= [] # mailboxes (Queues) of idle worker threads for remote calls
			@poolsize= poolsize # how many idle workers to keep
			# Messages to be sent by the outbox thread, on behalf of threads
			# that must not block in a send: finalizers and the listen thread.
			@outbox= Queue.new
			@root= RemoteObject.new ROOT_ID, self # remote object exported to us
			# Start the threads last: a reply to the peer's first call may be
			# sent before this constructor returns.
			@thread= Thread.new{ listen } # Wait for incoming messages
			@outbox_thread= Thread.new{ outbox_loop }
		end
		attr_reader :object, :root, :thread, :poolsize, :timeout, :max_frame
		attr_writer :logger

		def logger() @logger || RO4R.logger end
		def closed?() @closed end

		def inspect
			 "#<#{self.__class} #{@closed ? 'closed' : 'open'}>"
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
			# The id may belong to an object the peer itself received from a
			# third connection; calls on it are forwarded by the peer.
			refs= [ 0]
			ro= RemoteObject.new id, self, refs
			@remote[ id]= ro # weak; the entry disappears when ro is collected
			ObjectSpace.define_finalizer ro, make_finalizer( id, refs)
			ro
		end
		def make_finalizer id, refs
			# Finalizers may run on any thread, including one that holds
			# @send_mutex, so they must not send directly; queue the id for
			# the outbox thread instead. refs must not be read before the
			# object is dead, since references may still arrive for it until
			# then.
			q= @outbox
			proc{ |oid| q.push Release.new( id, refs[0]) rescue nil } # ClosedQueueError once disconnected
		end
		# Send the messages queued by finalizers and the listen thread.
		def outbox_loop
			while ( msg= @outbox.pop)
				transmit msg
			end
		rescue ConnectionError, ClosedQueueError
			# peer gone; nothing more to send
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
		# Convert an outgoing object, from our POV: by-reference objects become
		# references, by-value objects are returned as they are, except that
		# Arrays and keyword arguments are searched for by-reference objects
		# (without the class name that Marshal would write for them, so the
		# peer need not know their classes). seen guards against cyclic
		# Arrays, which are left to Marshal.
		def make_reference object, forceref= nil, seen= nil
			if object.__kind_of? RemoteObject
				if object.__connection== self
					RID.new object.__remote_id
				else
					local_reference object
				end
			elsif object.__kind_of? Ref # converted already
				object
			elsif forceref or object.__by_ref?
				local_reference object
			elsif object.__kind_of? Array
				if seen
					return object if seen[ object.__id__]
					seen[ object.__id__]= true
				end
				out= nil
				i= 0
				while i< object.size
					o= object[ i]
					c= if o.__kind_of?( Array) # only nested Arrays can form a cycle
						make_reference o, nil, ( seen||= { object.__id__=> true})
					else
						make_reference o, nil, seen
					end
					unless c.__id__== o.__id__
						out||= object.dup # keeps the class and instance variables
						out[ i]= c
					end
					i+= 1
				end
				out || object
			elsif object.__kind_of? Kwargs
				object.convert!{ |o| make_reference o, nil, seen}
			else # by_val, i.e. copy
				object
			end
		end
		# Call `name` on the peer's `object` (a RemoteObject, or a local object
		# the peer holds a reference to) and return the result. timeout
		# overrides the connection's; nil waits forever.
		def call object, name, args=[], block= nil, byref= false, timeout: @timeout
			oid= if object.__kind_of? RemoteObject
				object.__remote_id
			else
				object.__id__
			end
			cls= byref ? RefCall : Call
			msg= cls.new next_id, oid, name, make_reference( args), !block.nil?
			await_return msg, timeout, &block
		end
		def byref object, name, args=[], block= nil, timeout: @timeout
			call object, name, args, block, true, timeout: timeout
		end

		private
		def next_id
			@recv_mutex.synchronize{ @seq+= 1}
		end
		def remote_yield tid, args
			msg= Yield.new tid, next_id, make_reference( args)
			await_return msg, nil
		end
		# Wait up to timeout seconds for something to be pushed on q; nil
		# when nothing was.
		def pop q, timeout
			if QUEUE_TIMEOUT
				q.pop timeout: timeout
			elsif timeout
				Timeout.timeout( timeout, PopTimeout) { q.pop }
			else
				q.pop
			end
		rescue PopTimeout
			nil
		end
		# Send msg and wait for the peer's reply on a Queue registered under
		# the message's call id, which the peer echoes back. Yields from the
		# peer are served in between.
		def await_return msg, timeout
			id= msg.hid
			q= Queue.new
			while true
				@recv_mutex.synchronize do
					raise ConnectionError, "Unable to reach peer" if @closed
					@receiver[ id]= q
				end
				begin
					transmit msg
					msg= pop q, timeout
				ensure
					@recv_mutex.synchronize{ @receiver.delete id}
				end
				case msg
				when Return # got our answer
					if msg.err  # error, propagate
						raise msg.obj
					else # return value
						return msg.obj
					end
				when Yield  # need to call block
					yid= msg.yid
					reply= nil
					begin
						args, kw= Kwargs.unpack msg.arg
						r= kw ? yield( *args, **kw) : yield( *args)
						reply= Return.new yid, make_reference( r), false
					rescue StandardError=> e
						reply= Return.new yid, e, true
					ensure
						# break, return, throw or a non-StandardError exception
						# left the block: the peer is still inside its yield,
						# waiting for the result; make it unwind.
						unless reply
							e= BlockExit.new "remote block exited early"
							transmit( Return.new( yid, e, true)) rescue nil
						end
					end
					msg= reply
				when Exception
					raise msg
				when nil
					raise TimeoutError, "no reply within #{timeout}s"
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
		def allowed? obj, name
			if @allow.respond_to? :call
				@allow.call obj, name
			else
				@allow.include? name
			end
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
				name= msg.nme.to_sym
				if name.to_s[0..0]=='_'
					raise NoMethodError, "Call forbidden"
				end
				if @allow and not allowed?( obj, name)
					raise SecurityError, "method '#{name}' is not allowed"
				end
				blk= if msg.blk
					proc { | *arg| remote_yield msg.tid, Kwargs.pack( arg)}.ruby2_keywords
				end
				r= obj.__remote_call name, msg.arg, blk
			else
				r= obj
			end
			# Explicit conversion here so that return-by-ref call can be
			# honored (but ignore byref request for nil)
			forceref= msg.kind_of?( RefCall) && RefCall.enabled? && !r.__nil?
			transmit Return.new( msg.tid, make_reference( r, forceref), false)
		rescue BlockExit=> e
			# the caller is gone (its block exited early, or it gave up
			# waiting); nobody to reply to
			logger.debug{ "remote call '#{msg.nme}' abandoned: #{e.message}"}
		rescue Exception=> e
			logger.debug do
				"error in remote call '#{msg.nme}': #{e.class}: #{e.message}\n" +
					Array( e.backtrace).join( "\n")
			end
			send_error msg.tid, e
		end

		# Return an exception to the caller. If it cannot be marshalled (e.g. it
		# holds a Proc or an IO) or is too large, send a plain RuntimeError
		# instead, so that the remote caller is not left waiting forever.
		def send_error tid, e
			transmit Return.new( tid, e, true)
		rescue TypeError, ArgumentError, ProtocolError
			transmit Return.new( tid, RuntimeError.new( "#{e.class}: #{e.message}"), true)
		end

		# Read up to maxlen bytes, at least one; raise EOFError when the peer is
		# gone. Intended to be redefined if necessary.
		def read io, maxlen
			io.readpartial maxlen
		end
		READ_CHUNK= 65536
		# Frame header: payload length, message kind, and the id of the call
		# the message concerns (see Msg#hid).
		HEADER= 'L<Cq<'
		HEADER_SIZE= 13
		def fill buf, n
			buf<< read( @io, READ_CHUNK) while buf.bytesize< n
		end
		# Return [kind, id, payload] of the next frame. `buf` holds bytes read
		# ahead of the frame boundary and is reused across calls. A frame
		# larger than max_frame is skipped and returned with a nil payload.
		def read_frame buf
			fill buf, HEADER_SIZE
			len, kind, id= buf.unpack HEADER
			buf[ 0, HEADER_SIZE]= ''
			if @max_frame and len> @max_frame
				while len> 0
					fill buf, 1
					n= [ len, buf.bytesize].min
					buf[ 0, n]= ''
					len-= n
				end
				return [ kind, id, nil]
			end
			fill buf, len
			frame= buf.byteslice( 0, len)
			buf[ 0, len]= ''
			[ kind, id, frame]
		end
		# Called from the listen thread when a frame cannot be turned into a
		# message: report the failure to whoever is waiting on it, on either
		# side, instead of dropping the connection.
		def reject kind, id, error
			desc= "#{Msg::KINDS.key( kind) || kind} message: #{error.class}: #{error.message}"
			logger.warn "cannot decode #{desc}"
			err= ProtocolError.new "peer could not decode #{desc}"
			case kind
			when Msg::CALL, Msg::YIELD # the peer's thread is waiting for us
				enqueue Return.new( id, err, true)
			when Msg::RETURN # one of ours is waiting for the peer
				deliver Return.new( id, ProtocolError.new( "cannot decode #{desc}"), true)
			end
		end
		# Queue msg for the outbox thread.
		def enqueue msg
			@outbox.push msg
		rescue ClosedQueueError
			# connection closing; nothing more to send
		end
		# Hand a Return or Yield to the call waiting for it.
		def deliver msg
			q= @recv_mutex.synchronize{ @receiver[ msg.tid]}
			if q
				q.push msg
			elsif msg.__kind_of? Yield
				# The call was abandoned (timeout, interrupt) after the peer
				# started serving it; its method is now waiting inside a
				# yield, so make it unwind.
				logger.debug{ "yield for abandoned call #{msg.tid}"}
				e= BlockExit.new "the caller abandoned the call"
				enqueue Return.new( msg.yid, e, true)
			else
				logger.debug{ "reply for abandoned call #{msg.tid}"}
			end
		end

		def listen
			Thread.current[:__conn]= self
			@io.binmode
			begin
				buf= String.new( capacity: READ_CHUNK)
				while true
					kind, id, frame= read_frame buf
					unless frame
						reject kind, id, ProtocolError.new( "larger than max_frame (#{@max_frame} bytes)")
						next
					end
					begin
						msg= Marshal.load frame
					rescue StandardError=> e # unknown class, _load failure, bad reference, ...
						reject kind, id, e
						next
					end
					case msg
					when Call
						local_call msg
					when Return, Yield
						deliver msg
					when Release
						release_local msg.oid, msg.count
					else
						logger.warn "ignoring unexpected #{msg.__class} message"
					end
				end
			rescue Exception=> e
				# EOFError / ECONNRESET just mean the peer disconnected; don't
				# report those, but still run the cleanup below so that threads
				# waiting on this connection are woken up with the error.
				unless e.is_a?( EOFError) or e.is_a?( Errno::ECONNRESET)
					logger.error{ "connection closed: #{e.inspect}"}
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
				@outbox.close
				@recv_mutex.synchronize{ @pool.each( &:close); @pool.clear}
				@on_close.call self if @on_close
			end
		end

		# Marshal msg and write it to the peer as one frame.
		def transmit msg
			raise ConnectionError, "Unable to reach peer" if @closed
			logger.warn "sending a #{msg.__class} instead of a Msg" unless msg.__kind_of? Msg
			t= Thread.current
			t[:__conn]= self
			t[:__ro4r_marshal]= true
			begin
				data= Marshal.dump msg
			ensure
				t[:__ro4r_marshal]= nil
			end
			if @max_frame and data.bytesize> @max_frame
				raise ProtocolError, "message of #{data.bytesize} bytes exceeds max_frame (#{@max_frame} bytes)"
			end
			header= [ data.bytesize, msg.kind, msg.hid].pack( HEADER)
			@send_mutex.synchronize do
				@io.write header, data # one writev, no concatenation
			rescue IOError, SystemCallError=> e
				raise ConnectionError, "Unable to reach peer (#{e.message})"
			end
			nil
		end
		private_constant :PopTimeout
	end

	### Serializable messages ###
	# Messages travel by value as an Array of their fields (marshal_dump), so
	# that the wire carries no instance variable names. The frame header
	# repeats the kind of the message and the id of the call it concerns
	# (`hid`: the sender's call id for Call and Yield, the receiver's for
	# Return), so that a message the receiver cannot decode can still be
	# answered with an error.
	class Msg
		include ByVal
		CALL, RETURN, YIELD, RELEASE= 1, 2, 3, 4
		KINDS= { call: CALL, return: RETURN, yield: YIELD, release: RELEASE}.freeze
		def hid() 0 end
	end
	# method call
	class Call< Msg
		# thread_id: an id unique among the caller's pending calls (a call
		# id; called tid for historical reasons), echoed in the Return
		def initialize thread_id, object_id, method_name, args, block=nil
			@tid, @oid, @nme= thread_id, object_id, method_name
			@arg, @blk= args, block
		end
		attr_reader :tid, :oid, :nme, :arg, :blk
		def kind() CALL end
		def hid() @tid end
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
	# yield to remote block: tid is the caller's id of the call being served,
	# yid the id under which the yielding side waits for the block's result
	class Yield< Msg
		def initialize threadid, yield_threadid, args
			@tid, @yid, @arg = threadid, yield_threadid, args
		end
		attr_reader :tid, :yid, :arg
		def kind() YIELD end
		def hid() @yid end
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
		def kind() RETURN end
		def hid() @tid end
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
		def kind() RELEASE end
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
		# options are passed to Connection.new for every accepted connection
		# (poolsize:, logger:, timeout:, max_frame:, allow:, on_close:).
		def initialize object= Hash.new, port= DefaultPort, safe= nil, **options
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
			on_close= options.delete :on_close
			# Automatically accept incoming connections; a connection removes
			# itself from the list once its peer disconnects.
			@connections= []
			@mutex= Mutex.new
			# Closing this pipe wakes the accept loop (see #close).
			@stop_r, @stop_w= IO.pipe
			@thread= Thread.new do
				begin
					while true
						ready,= IO.select [ @server, @stop_r]
						break if ready.include? @stop_r
						io= @server.accept_nonblock exception: false
						next if io== :wait_readable
						@mutex.synchronize do
							@connections << Connection.new( io, object, safe, **options) do |c|
								@mutex.synchronize{ @connections.delete c}
								on_close.call c if on_close
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
			# Wake the accept loop and let it exit before closing the socket:
			# closing it while a thread is blocked in accept makes Ruby raise
			# IOError in that thread even if accept(2) has just returned, which
			# leaks the accepted descriptor and leaves that client hanging with
			# an open, unserved connection. The loop is woken through a pipe
			# since shutdown does not wake accept on a listening socket on
			# every platform (it fails with ENOTCONN on macOS and the BSDs).
			@stop_w.close unless @stop_w.closed?
			@thread.join # no connection can be added after the accept loop ends
			@server.close unless @server.closed?
			@stop_r.close unless @stop_r.closed?
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
