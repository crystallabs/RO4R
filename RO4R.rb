# Compatibility shim for `ruby -I. -r RO4R`: the library lives in lib/ro4r.rb
# and is loaded with require 'ro4r' when installed as a gem or with -Ilib.
require_relative 'lib/ro4r'
