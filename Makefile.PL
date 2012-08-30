#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

use File::Basename;
use ExtUtils::MakeMaker qw( prompt );

my %config = (
    NAME                        => 'Redis::JobQueue',
    VERSION_FROM                => 'lib/Redis/JobQueue.pm',
# more to the list $Module::CoreList::version{5.010}
    PREREQ_PM                   => {
        'Data::UUID'            => 1.217,
        'Mouse'                 => 0.97,
        'Params::Util'          => 1.07,
        'Redis'                 => 1.926,
        },
    BUILD_REQUIRES              => {
        'Test::Distribution'    => 2.00,
        'Test::Exception'       => 0.31,
        'Test::Fixme'           => 0.07,
        'Test::Kwalitee'        => 1.01,
        'Test::Perl::Critic'    => 1.02,
        'Test::Pod'             => 1.45,
        'Test::Pod::Coverage'   => 1.08,
        'Test::RedisServer'     => 0.04,
        'Test::TCP'             => 1.17,
        },
    ( $] >= 5.005 ?
        (
        ABSTRACT        => 'Redis::JobQueue simple job queue with Redis server capabilities.',
        AUTHOR          => 'TrackingSoft LLC <sgladkov@trackingsoft.com>'
        ) : ()
    ),
    dist        => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean       => { FILES => "Debian_CPANTS.txt Redis-*.tar.gz" },
    LICENSE     => 'perl',
    NO_MYMETA   => 1,
);

my @patterns = qw( t/*.t t/*/*.t );
$config{test} = { TESTS => join ' ', map { glob } @patterns };

ExtUtils::MakeMaker::WriteMakefile( %config );
