# "Remote Objects for Ruby" (RO4R) Copyright 2007-2010 Nenad Ocelic
# Based on "Ruby RPC" Copyright 2005 Brian Ollenberger
# This program is licensed under the terms of BSD license.

require 'socket'

MARSHAL= if defined? RMarshal then RMarshal else Marshal end
#p MARSHAL

module ByVal
	def __by_ref?() false end
end
module ByRef
	def __by_ref?() true end
	# Enforce passing an object by ref:
	def byref!
		unless __by_ref? or nil.eql? self
		# never pass a ref to nil because ref-to-nil == root-obj.
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
		MARSHAL.load str
	end
	alias_method :__respond_to?, :respond_to?
	# Defined in a prepended module rather than directly on Object, since Ruby
	# warns about redefining Object#respond_to? (possible infinite loop). It is
	# also prepended to by-value classes that define their own respond_to?
	# (e.g. Exception), which would otherwise bypass this override.
	module RespondTo
		def respond_to? method, priv= false
			if :_dump== method || :_dump_as== method ## cheat on Marshal.dump
				 # use _dump if we have to turn the object into a reference
				 # otherwise Marshal must do it on its own, possibly with the aid of
				 # marshal_dump and marshal_load methods exposed by the object
				__by_ref?
			else
				super
			end
		end
	end
	prepend RespondTo
	def _dump depth= -1
		conn= Thread.current[:__conn]
		MARSHAL.dump conn.make_reference( self, true), depth
	end
	def _dump_as
		conn= Thread.current[:__conn]
		conn.make_reference self, true
	end
	def __remote_call method, arguments, block
		m= method.to_sym
		if __respond_to? m, false
			unless __secure_method? m
				raise SecurityError,
					"insecure method '#{method}' called" # for #{self.inspect}"
			end
			__send__ m, *arguments, &block
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

	# Transparent reference to a remote object:
	class RemoteObject
		for m in instance_methods
			undef_method m unless m[0..1]== '__' || m== :object_id
		end

		def initialize id, connection
			@id = id
			@conn= connection
		end
		def __connection
			@conn
		end
		def __remote_id
			@id
		end
		def _dump *arg
			super
		end
		private
		def method_missing name, *args, &block
			if name.to_s[0..0]== '_' # keep 'underscore calls' local
				super
			else
				@conn.call self, name, args, block
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
		def respond_to? method, priv= false
			if :_dump_as== method
				false
			else
				__respond_to? method, priv
			end
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
			@send_mutex= Mutex.new
			@recv_mutex= Mutex.new
			@recv_cond= ConditionVariable.new
			@object= object # local object (exported over connection)
			@receiver= {}
			@remote= ObjectSpace::WeakMap.new # remote_id -> RemoteObject (to reuse it)
			@local= {} # local_id -> local Object (must not disappear while in use remotely)
			case host
			when String
				host, port= *host.split(':')
				@io= TCPSocket.new host, port || Server::DefaultPort
			else
				@io= host
			end
			@local[ nil.__id__]= @local[ @object.__id__]= @object
			@pool= [] # mailboxes (Queues) of idle worker threads for remote calls
			@poolsize= 5 # how many idle workers to keep
			@release_q= Queue.new # ids of collected RemoteObjects, to release remotely
			@thread= Thread.new do
				listen # Wait for incoming messages
			end
			@release_thread= Thread.new{ release_loop }
			@root= RemoteObject.new nil.__id__, self # remote object exported to us
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
		end

		private
		def add_local object
			@local[ object.__id__]= object
		end
		def delete_local id
			@local.delete id
		end
		def add_remote id
			### TODO: check whether to allow foreign objects or not
			ro= RemoteObject.new id, self
			@remote[ id]= ro # weak; the entry disappears when ro is collected
			ObjectSpace.define_finalizer ro, make_finalizer( id)
			ro
		end
		def make_finalizer id
			# Finalizers may run on any thread, including one that holds
			# @send_mutex, so they must not send directly; queue the id for
			# release_loop instead.
			q= @release_q
			proc{ |lid| q.push id rescue nil } # ClosedQueueError once disconnected
		end
		# Tell the peer that we no longer hold references to its objects.
		def release_loop
			while ( id= @release_q.pop)
				send Release.new( id)
			end
		rescue ConnectionError, ClosedQueueError
			# peer gone; nothing more to release
		end
		def local_reference object
			# this ref will be handed over, local object must not disappear
			add_local object
			LID.new object.__id__
		end
		def remote_object rid
			@remote[ rid] || add_remote( rid) # reuse or create
		end
		public

		def has_local? id
			@local.has_key? id
		end
		def make_object reference # after receiving, from sender POV
			return reference unless reference.__kind_of? Ref
			#$stderr.puts "make_object: #{reference.inspect}"
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
					# always pass nil by value, since remote ref. to nil means root object
					local_reference object
				else # by_val, i.e. copy
					#$stderr.puts "Warning: returning object instead of reference"
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
		def await_return msg
			t= Thread.current
			tid= t.__id__
			while true
				@recv_mutex.synchronize{ @receiver[ tid]= t}
				#$stderr.puts "Awaiting return"
				send msg, true # also wait there for the response
				@recv_mutex.synchronize{ msg= @receiver.delete tid}
				#$stderr.puts "Awaited message received: #{msg.class}"
				case msg
				when Return # got our answer
					if msg.err  # error, propagate
						raise msg.obj
					else # return value
						return msg.obj
					end
				when Yield  # need to call block
					begin
						arg= msg.arg
						r= yield(*arg)
						msg= Return.new msg.yid, r, false
					rescue => e
						msg= Return.new msg.yid, e, true
					end
				when Exception
					raise msg
				else # shouldn't happen, but with YARV it occasionally does.
					if msg== t
						#$stderr.puts @receiver
						raise RuntimeError, "Message not passed before wake-up"
					else
						raise RuntimeError, "Cannot interpret message #{msg}"
					end
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
					proc { | *arg| remote_yield msg.tid, arg}
				end
				r= obj.__remote_call msg.nme, msg.arg, blk
			else
				r= obj
			end
			# Explicit conversion here so that return-by-ref call can be
			# honored (but ignore byref request for nil)
			forceref= msg.kind_of?( RefCall) && !r.__nil?
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

		def __time( label='', res=nil)
			#tm= Time.now
			ret= yield
			#puts "#{label}: #{1e3*(Time.now-tm)} ms => #{res||ret}"
			ret
		end

		def read io, len # intended to be redefined if necessary
			@io.read( len) or raise EOFError, "peer disconnected"
		end

		def listen
			Thread.current[:__conn]= self
			@io.binmode
			begin
				len= data= msg= nil
				while true
					#__time 'sel1', 'OK' {
					#IO::select [@io], nil, nil
					#}
					#?! why does this cause a >50x slow-down? :
					#__time 'read1' {
					data= String.new
					buflen= 4
					while buflen> 0
						aux= read @io, buflen
						buflen-= aux.length
						data<< aux
					end
					#GC.disable
					len= data.unpack("L<").first
					#GC.enable
					#}
					#__time 'sel2', 'OK' {
					#IO::select [@io], nil, nil
					#}
					#__time 'read2' {
					data= String.new
					while len> 0
						aux= read @io, len
						len-= aux.length
						data<< aux
					end
					#}
					#__time 'load', 'OK' {
					msg= MARSHAL.load data
					#}
					#msg= MARSHAL.load @io
					###msg= MARSHAL.load( @io, proc{ |o|
						###$stderr.print "#{o.__class}:"
					###})
					msg= Msg._load msg if msg.__kind_of? Array
					#$stderr.puts "Recevied message #{msg.__class}" #unless msg.__kind_of? Msg
					case msg
					when Call
						local_call msg
					when Return, Yield
						t= @receiver[ msg.tid]
						#$stderr.puts "Receiver: #{t}"
						if t and t.kind_of? Thread
							@send_mutex.synchronize do
								@recv_mutex.synchronize{ @receiver[ msg.tid]= msg}
								#$stderr.puts "Running thread #{t}"
								t[:recv_cond].signal
							end
						else
							RO4R.logger.warn "no thread waiting for #{msg.__class} #{msg.tid}"
						end
					when Release
						@local.delete msg.oid
					end
				end
			rescue Exception=> e
				# EOFError / ECONNRESET just mean the peer disconnected; don't
				# report those, but still run the cleanup below so that threads
				# waiting on this connection are woken up with the error.
				unless e.is_a?( EOFError) or e.is_a?( Errno::ECONNRESET)
					RO4R.logger.error{ "connection closed: #{e.inspect}"}
				end
				Thread.current[:err]= e
				@closed= true
				@io.close
				# Wake up callers waiting for a reply. Their condition variables are
				# bound to @send_mutex, so it must be held here or the signal can be
				# lost between their check and their wait.
				err= ConnectionError.new "Unable to reach peer (#{e.class}: #{e.message})"
				@send_mutex.synchronize do
					@recv_mutex.synchronize do
						@receiver.values.grep( Thread).each do |t|
							@receiver[ t.__id__]= err
							t[:recv_cond].signal
						end
					end
				end
			ensure
				#r.puts "listen() exiting"
				# purge finalizers
				@remote.each_value do |ro|
					ObjectSpace.undefine_finalizer ro
				end
				@remote= ObjectSpace::WeakMap.new # useless after the connection is broken
				@local.clear # free local objects
				# stop the helper threads
				@closed= true
				@release_q.close
				@recv_mutex.synchronize{ @pool.each( &:close); @pool.clear}
				@on_close.call self if @on_close
			end
		end

		def send msg, wait= false
			raise ConnectionError, "Unable to reach peer" if @closed or not @thread.alive?
			RO4R.logger.warn "sending a #{msg.__class} instead of a Msg" unless msg.__kind_of? Msg
			t= Thread.current
			t[:__conn]= self
			#data= len= c= tm= nil
			#__time 'dump' {
				data= MARSHAL.dump msg
				len= data.size #bytesize
			#}
			#__time 'sync' {
			@send_mutex.synchronize do
				c=( t[:recv_cond]||= ConditionVariable.new)
				#__time 'write' {
				begin
					@io.write [len].pack("L<") << data
				rescue IOError, SystemCallError=> e
					raise ConnectionError, "Unable to reach peer (#{e.message})"
				end
				#@io.write data
				#}
				#__time 'wait' {
				if wait # wait for response
					#t0= Time.now
					c.wait @send_mutex while @receiver[ t.__id__]== t
					#?! c.wait seems to occasionally return before cond. signal!
					#td= Time.now- t0
					#p td if td<2e-5
				end
				#}
			end
			#}
		end
	end

	### Serializable messages ###
	# method call
	class Msg
		def respond_to? method, priv= false
			if :_dump== method
				false
			else
				__respond_to? method, priv
			end
		end
		MSGMAP=[]
		def self._load ary
			cls= ary.shift
			MSGMAP[ cls]._load ary
		end
	end
	# method call
	class Call< Msg
		Msg::MSGMAP[ N= Msg::MSGMAP.size]= self
		def initialize thread_id, object_id, method_name, args, block=nil
			@tid, @oid, @nme= thread_id, object_id, method_name
			@arg, @blk= args, block
		end
		attr_reader :tid, :oid, :nme, :arg, :blk
		def _dump_as
			[ N, @tid, @oid, @nme, @arg, @blk]
		end
		def self._load ary
			new(*ary)
		end
	end
	# method call
	class RefCall< Call
		Msg::MSGMAP[ N= Msg::MSGMAP.size]= Call # disabled by default
		def self.enable
			Msg::MSGMAP[ N]= self
		end
		def self.disable
			Msg::MSGMAP[ N]= Call # divert to ordinary Call
		end
		def _dump_as
			[ N, @tid, @oid, @nme, @arg, @blk]
		end
		def self._load ary
			Msg::MSGMAP[ N].new(*ary)
		end
	end
	# yield to remote block
	class Yield< Msg
		Msg::MSGMAP[ N= Msg::MSGMAP.size]= self
		def initialize threadid, yield_threadid, args
			@tid, @yid, @arg = threadid, yield_threadid, args
		end
		attr_reader :tid, :yid, :arg
		def _dump_as
			[ N, @tid, @yid, @arg]
		end
		def self._load ary
			new(*ary)
		end
	end
	# return value or exception return message
	# (also for returning values from blocks after yield) (-if feasible)
	class Return< Msg
		Msg::MSGMAP[ N= Msg::MSGMAP.size]= self
		def initialize thread_id, object, exception
			@tid, @obj, @err= thread_id, object, exception
			#~ p "returning: ", object._connection.nil?, "\n"
		end
		attr_reader :tid, :obj, :err
		def _dump_as
			[ N, @tid, @obj, @err]
		end
		def self._load ary
			new(*ary)
		end
	end
	# release the reference on the remote side protecting a local RemoteObject
	class Release< Msg
		Msg::MSGMAP[ N= Msg::MSGMAP.size]= self
		def initialize id
			@oid= id
		end
		attr_reader :oid
		def _dump_as()
			[ N, @oid]
		end
		def self._load ary
			new(*ary)
		end
	end

	# A server that exports a single object on a socket.
	class Server
		DefaultPort= 4044
		DefaultAddress= '0.0.0.0'
		attr_reader :connections, :server # server: the listening socket

		# safe: unused since Ruby 3.0 removed $SAFE; kept for API compatibility.
		def initialize object= Hash.new, port= DefaultPort, safe= nil
			### root object must be passed by ref:
			raise ArgumentError, "#{object} must be sent by reference" unless object.__by_ref?
			@server= case port
			when Integer
				TCPServer.new DefaultAddress, port
			when String # "port" or "address:port"
				a, p= *port.split(':')
				a, p= DefaultAddress, a unless p
				TCPServer.new a, p.to_i
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
		#Bignum, Fixnum,
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

