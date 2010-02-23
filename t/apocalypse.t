#!/usr/bin/perl
use strict; use warnings;

use Test::More;
eval "use Test::Apocalypse";
if ( $@ ) {
	plan skip_all => 'Test::Apocalypse required for validating the distribution';
} else {
	require Test::NoWarnings; require Test::Pod; require Test::Pod::Coverage;	# lousy hack for kwalitee
	is_apocalypse_here( {
		# Add PERL_APOCALYSPE env var so we can test everything when needed...
		! $ENV{PERL_APOCALYPSE} ? ( deny => qr/^(?:OutdatedPrereqs|ModuleUsed|Strict|Pod_(?:Spelling|Coverage))$/, ) : (),
	} );
}
