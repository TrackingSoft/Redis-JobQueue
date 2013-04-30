#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

use File::Basename;
use ExtUtils::MakeMaker qw( prompt );

my %config = (
    NAME                        => 'Redis::JobQueue',
    VERSION_FROM                => 'lib/Redis/JobQueue.pm',
    MIN_PERL_VERSION            => '5.010',
# more to the list $Module::CoreList::version{5.010}
    PREREQ_PM                   => {
        'Data::UUID'            => 1.217,
        'Digest::SHA1'          => 2.13,
        'List::MoreUtils'       => 0.33,
        'Mouse'                 => 0.97,
        'Params::Util'          => 1.07,
        'Redis'                 => 1.926,
        'Storable'              => 2.39,
        },
    BUILD_REQUIRES              => {
        'Net::EmptyPort'        => 0,
        'Test::Deep'            => 0.109,
        'Test::Exception'       => 0.31,
        'Test::NoWarnings'      => 1.04,
        'Test::RedisServer'     => 0.04,
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

if( $ENV{AUTHOR_TESTS} ) {
    @patterns = qw( xt/author/*.t xt/author/*/*.t );
    $config{test}->{TESTS} .= ' '.join ' ', map { glob } @patterns;
}

if( $ENV{RELEASE_TESTS} ) {
    @patterns = qw( xt/release/*.t xt/release/*/*.t );
    $config{test}->{TESTS} .= ' '.join ' ', map { glob } @patterns;
}

ExtUtils::MakeMaker::WriteMakefile( %config );
