# "Remote Objects for Ruby" (RO4R) Copyright 2007-2010 Nenad Ocelic
# Based on "Ruby RPC" Copyright 2005 Brian Ollenberger
# This program is licensed under the terms of BSD license.

require 'socket'
require 'thread'

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
	def respond_to? method, priv= false
		if :_dump== method || :_dump_as== method ## cheat on Marshal.dump
			 # use _dump if we have to turn the object into a reference
			 # otherwise Marshal must do it on its own, possibly with the aid of
			 # marshal_dump and marshal_load methods exposed by the object
			__by_ref?
		else
			__respond_to? method, priv
		end
	end
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
	def __secure_method? method
		## intended to be redefined in subclasses e.g. like this:
		not [ :send].include? method
	end
end

module RO4R
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
			id= str.unpack( 'l').first
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
			[ @id].pack( 'l')
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
		def initialize host='localhost', object= nil, safe= 3
			@safe= safe.to_i
			@send_mutex= Mutex.new
			@recv_mutex= Mutex.new
			@recv_cond= ConditionVariable.new
			@object= object # local object (exported over connection)
			@receiver= {}
			@remote= {} # remote_id -> local_id mapping (to reuse RemoteObject)
			@local= {} # local_id -> local Object (must not disappear while in use remotely)
			case host
			when String
				host, port= *host.split(':')
				@io= TCPSocket.new host, port || Server::DefaultPort
			else
				@io= host
			end
			@local[ nil.__id__]= @local[ @object.__id__]= @object
			@pool= [] # thread pool for executing remote calls
			@poolsize= 5 # how many threads to keep idle
			@thread= Thread.new do
				listen # Wait for incoming messages
				#$stderr.puts "listen thread exited"
			end
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
			#$stderr.print "New remote_object with local id: #{ro.__id__} "
			@remote[ id]= ro.__id__
			ObjectSpace.define_finalizer ro, make_finalizer( id)
			#$stderr.puts " + defined finalizer."
			ro
		end
		def delete_remote id
			@remote.delete id
		end
		def make_finalizer id
			proc{ |lid| Thread.new {
				begin
					#$stderr.print "Finalizer for #{lid}"
					delete_remote id
					send Release.new( id)
				rescue
					# ignore, since the RemoteObject is already gone, anyway.
				end
			} }
		end
		def local_reference object
			# this ref will be handed over, local object must not disappear
			add_local object
			LID.new object.__id__
		end
		def remote_object rid
			#$stderr.print "remote_object: #{rid} "
			if @remote.has_key? rid # reuse
				lid= @remote[ rid]
				#$stderr.puts "(already seen)"
				ObjectSpace._id2ref lid
			else # create
				#$stderr.puts "(new)"
				add_remote rid
			end
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
					raise TypeError, "No reference given for object id #{@id}"
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
						r= yield *arg
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
		def local_call message
			#$stderr.puts "Received local call: #{message.class} : #{message.nme}"
			# TODO: optionally disable RefCalls for security reasons
			lct= nil
			@recv_mutex.synchronize{ lct= @pool.pop}
			lct||= Thread.new do
				while true
					Thread.stop
					begin
						t= Thread.current
						msg= t[ :msg]
						if @local.has_key? msg.oid
							obj= @local[ msg.oid]
							#$stderr.puts "... upon a #{obj.class}"
							if obj.__nil?
								raise TypeError, "No root object exported"
							end
						else
							raise TypeError, "Receiver ##{msg.oid} not found"
						end
						#GC.disable
						if msg.nme
							if msg.nme.to_s[0..0]=='_'
								raise NoMethodErrorError, "Call forbidden"
							end
							arg= msg.arg
							blk= if msg.blk
								blk= proc { | *arg| remote_yield msg.tid, arg}
							else nil
							end
							### TODO: check if the method is public (or just wait for Ruby 2)
							r= obj.__remote_call msg.nme, arg, blk
						else
							r= obj
						end
						#GC.enable
						# Explicit conversion here so that return-by-ref call can be
						#	honored ( but ingnore byref request for nil )
						forceref= msg.kind_of?( RefCall) && !r.__nil?
						ref= make_reference r, forceref
						send Return.new( msg.tid, ref, false)
					rescue Exception=> e
						$stderr.puts "Error in local call: #{e}"
						$stderr.puts e.backtrace
						#$stderr.puts "@local: #{@local.inspect}"
						send Return.new( msg.tid, e, true)
					end
					if @pool.size< @poolsize then @pool.push t else Thread.exit end
				end
			end
			Thread.pass until lct.stop?
			lct[ :msg]= message
			lct.run if lct.alive?
		end

		def __time( label='', res=nil)
			#tm= Time.now
			ret= yield
			#puts "#{label}: #{1e3*(Time.now-tm)} ms => #{res||ret}"
		end

		def read io, len # intended to be redefined if necessary 
			@io.read len
		end 

		def listen
			Thread.current[:__conn]= self
			@io.binmode
#			@io.nonblock= true
			$SAFE= @safe
			begin
				len= data= msg= nil
				while true
					#__time 'sel1', 'OK' {
					#IO::select [@io], nil, nil
					#}
					#?! why does this cause a >50x slow-down? :
					#__time 'read1' {
					data= ''
					buflen= 4
					while buflen> 0
						aux= read @io, buflen
						buflen-= aux.length
						data<< aux
					end
					#GC.disable
					len= data.unpack("L").first
					#GC.enable
					#}
					#__time 'sel2', 'OK' {
					#IO::select [@io], nil, nil
					#}
					#__time 'read2' {
					data= ''
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
							$stderr.puts "Thread #{t} not found"
							# local warinig would be in order, but no action required
						end
					when Release
						@local.delete msg.oid
					end
				end
			# rescue EOFError, Errno::ECONNRESET => e
				# Ignore this error, as it is just a client disconnecting.
				# Remote references will be invalidated due to the
				# remote host disconnecting, so we don't need
				# to track it here.
			rescue Exception=> e
				$stderr.puts e.inspect
				Thread.current[:err]= e
				@io.close
				@recv_mutex.synchronize do
					rec= @receiver.values.grep Thread
					rec.each { |t|
						@receiver[t.__id__]= e;
						#### t.run if t.alive?
						t[:recv_cond].signal
					}
				end
			ensure
				#r.puts "listen() exiting"
				# purge finalizers
				@remote.each_value do |id|
					ObjectSpace.undefine_finalizer ObjectSpace._id2ref( id)
				end
				@remote.clear # useless after the connection is broken
				@local.clear # free local objects
			end
		end

		def send msg, wait= false
			raise "Unable to reach peer" unless @thread.alive?
			$stderr.puts "Sending #{msg.__class}" unless msg.__kind_of? Msg
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
				@io.write [len].pack("L") << data
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
			new *ary
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
			Msg::MSGMAP[ N].new *ary
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
			new *ary
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
			new *ary
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
			new *ary
		end
	end

	# A server that exports a single object on a socket.
	class Server
		DefaultPort= 4044
		DefaultAddress= '0.0.0.0'
		attr_reader :connections

		def initialize object= Hash.new, port= DefaultPort, safe= 3
			### root object must be passed by ref:
			raise ArgumentError, "#{object} must be sent by reference" unless object.__by_ref?
			server= case port
			when Integer
				TCPServer.new DefaultAddress, port
			when String
				a, p= *port.split(':')
				TCPServer.new a, p.to_i
			else
				port # probably an IO
			end
			# Automatically accept incoming connections
			@connections= []
			@thread= Thread.new do
				@connections << Connection.new( server.accept, object, safe) while true
			end
		end

		def join() @thread.join end
	end

	# Classes # to be passed by value (serialized):
	[ Array,
		Bignum,
		#Complex,
		Exception,
		FalseClass, Fixnum, Float,
		Integer,
		MatchData,
		NilClass,	Numeric,
		Range, Regexp,
		String, Symbol,
		Time, TrueClass
	].each do |c|
		c.class_eval{ extend ByVal; include ByVal}
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

