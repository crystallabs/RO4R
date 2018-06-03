#!/usr/bin/env ruby

require 'benchmark'
require 'RO4R'

$c= RO4R::Connection.new( ARGV.first || 'localhost')
$r= $c.root

$r[:counter]||= 1
puts Benchmark.measure { 10000.times do |n| $r[:counter]+=1 end}

puts $r.inspect
