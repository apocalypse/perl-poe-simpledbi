use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
	'NAME'		=> 'POE::Component::SimpleDBI',
	'VERSION_FROM'	=> 'lib/POE/Component/SimpleDBI.pm', # finds $VERSION
	'PREREQ_PM'	=> {
		'POE'				=>	'0.20',
		'POE::Filter::Line'		=>	0,
		'POE::Filter::Reference'	=>	0,
		'POE::Session'			=>	0,
		'POE::Wheel::Run'		=>	0,

		'DBI'		=>	'1.30',
		'Error'		=>	'0.15',

		# test stuff
		'Test::More'		=> 0,
		'Test::Distribution'	=> 0,
		'Test::Strict'		=> 0,
		'Test::HasVersion'	=> 0,
		'Test::CheckManifest'	=> 0,
		'Test::Kwalitee'	=> 0,
		'Test::MinimumVersion'	=> 0,
		'Test::Pod'		=> 0,
		'Test::Pod::Coverage'	=> 0,
		'Test::UseAllModules'	=> 0,
	},
	(	$] >= 5.005 ?		# Add new keywords
		(
			'ABSTRACT_FROM'	=> 'lib/POE/Component/SimpleDBI.pm', # retrieve abstract from module
			'AUTHOR'	=> 'Apocalypse <APOCAL@cpan.org>',
			'LICENSE'	=> 'perl',
		) : ()
	),
);
