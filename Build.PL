# Build.PL
use Module::Build;

my $build = Module::Build->new(
	# look up Module::Build::API for the info!
	'dynamic_config'	=> 0,
	'module_name'		=> 'POE::Component::SimpleDBI',
	'license'		=> 'perl',

	'dist_abstract'		=> 'Asynchronous non-blocking DBI calls in POE made simple',
	'dist_author'		=> 'Apocalypse <APOCAL@cpan.org>',

	'create_packlist'	=> 1,
	'create_makefile_pl'	=> 'traditional',
	'create_readme'		=> 1,

	'test_files'		=> 't/*.t',

	'add_to_cleanup'	=> [ 'META.yml', 'Makefile.PL', 'README' ],	# automatically generated

	'requires'		=> {
		# POE Stuff
		'POE'				=> 0,

		# FIXME POE stuff that Test::Dependencies needs to see
		'POE::Session'			=> 0,
		'POE::Filter::Line'		=> 0,
		'POE::Filter::Reference'	=> 0,
		'POE::Wheel::Run'		=> 0,

		# DB access
		'DBI'				=> '1.30',

		# error handling
		'Error'				=> '0.15',
	},

	'recommends'		=> {
		# Test stuff
		'Test::More'			=> 0,
	},

	# FIXME wishlist...
#	'test_requires'		=> {
#		# Test stuff
#		'Test::Compile'		=> 0,
#		'Test::Perl::Critic'	=> 0,
#		'Test::Dependencies'	=> 0,
#		'Test::Distribution'	=> 0,
#		'Test::Fixme'		=> 0,
#		'Test::HasVersion'	=> 0,
#		'Test::Kwalitee'	=> 0,
#		'Test::CheckManifest'	=> 0,
#		'Test::MinimumVersion'	=> 0,
#		'Test::Pod::Coverage'	=> 0,
#		'Test::Spelling'	=> 0,
#		'Test::Pod'		=> 0,
#		'Test::Prereq'		=> 0,
#		'Test::Strict'		=> 0,
#		'Test::UseAllModules'	=> 0,
#	},
);

# all done!
$build->create_build_script;

