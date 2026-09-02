require_relative 'lib/ro4r/version'

Gem::Specification.new do |s|
	s.name= 'ro4r'
	s.version= RO4R::VERSION
	s.summary= 'Remote Objects for Ruby: transparent RPC with by-reference objects'
	s.description= 'RO4R exports a Ruby object over TCP and lets clients call '\
		'methods on it, on the objects it returns, and on the blocks they pass, '\
		'as if they were local. Intended for mutually trusting processes.'
	s.authors= [ 'Nenad Ocelic', 'Davor Ocelic']
	s.email= 'docelic@crystallabs.io'
	s.homepage= 'https://github.com/crystallabs/RO4R'
	s.license= 'AGPL-3.0-or-later' # see LICENSE
	s.required_ruby_version= '>= 2.7'
	s.files= Dir[ 'lib/**/*.rb'] + %w[ README.md CHANGELOG.md LICENSE]
	s.require_paths= [ 'lib']
	s.metadata= {
		'source_code_uri'=> s.homepage,
		'bug_tracker_uri'=> "#{s.homepage}/issues",
		'changelog_uri'=> "#{s.homepage}/blob/master/CHANGELOG.md",
	}
	s.add_development_dependency 'minitest', '~> 5.0'
	s.add_development_dependency 'rake', '~> 13.0'
end
