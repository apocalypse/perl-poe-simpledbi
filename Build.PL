# Build.PL
use strict; use warnings;
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
	'create_license'	=> 1,
	'sign'			=> 0,

	'test_files'		=> 't/*.t',

	'add_to_cleanup'	=> [ 'META.yml', 'Makefile.PL', 'README', 'Makefile', 'LICENSE' ], # automatically generated

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

		# minimum perl version
		'perl'				=> '5.006',
	},
);

# all done!
$build->create_build_script;

