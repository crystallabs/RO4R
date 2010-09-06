puts ARGV.first
$c= RO4R::Connection.new( ARGV.first || 'localhost')
$r= $c.root
