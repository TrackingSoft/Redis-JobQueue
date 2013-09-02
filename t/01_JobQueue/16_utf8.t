#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

use lib 'lib';

# WARNING: global file scope
#use utf8;
#use bytes;

use Test::More;
plan "no_plan";
use Test::NoWarnings;

BEGIN {
    eval "use Test::Exception";                 ## no critic
    plan skip_all => "because Test::Exception required for testing" if $@;
}

BEGIN {
    eval "use Test::RedisServer";               ## no critic
    plan skip_all => "because Test::RedisServer required for testing" if $@;
}

BEGIN {
    eval "use Net::EmptyPort";                  ## no critic
    plan skip_all => "because Net::EmptyPort required for testing" if $@;
}

use Data::Dumper;
$Data::Dumper::Sortkeys = 1;
$Data::Dumper::Terse = 1;
use Redis::JobQueue qw(
    DEFAULT_SERVER
    DEFAULT_PORT
    DEFAULT_TIMEOUT
    );
use Redis::JobQueue::Job qw(
    STATUS_CREATED
    STATUS_WORKING
    STATUS_COMPLETED
    STATUS_FAILED
    );

my $redis_server;
my $redis;
my $port = Net::EmptyPort::empty_port( 32637 ); # 32637-32766 Unassigned
my $exists_real_redis = 1;
if ( !$redis )
{
    $exists_real_redis = 0;
    $redis_server = eval { Test::RedisServer->new( conf => { port => $port }, timeout => 3 ) };
    if ( $redis_server )
    {
        $redis = Redis->new( server => DEFAULT_SERVER.":".$port );
    }
}

my $skip_msg;
$skip_msg = "Redis server is unavailable" unless ( !$@ and $redis and $redis->ping );

SKIP: {
    diag $skip_msg if $skip_msg;
    skip( "Redis server is unavailable", 1 ) unless ( !$@ and $redis and $redis->ping );

$redis->quit;

#-- just testing ---------------------------------------------------------------

# WARNING: In a test mode 'encoding' of the object '$redis' during writing and reading the same

#-- Controlling server connection

$redis = Redis->new(
    server      => DEFAULT_SERVER.":".$port,
#    encoding    => undef,
#    encoding    => 'utf8',
    );
isa_ok( $redis, 'Redis' );

# default encoding
is $redis->{encoding}, 'utf8', "default encoding = 'utf8'";

# encoding is setable in new
$redis = Redis->new(
    server      => DEFAULT_SERVER.":".$port,
    encoding    => undef,
#    encoding    => 'utf8',
    );
is $redis->{encoding}, undef, "encoding = undef";

$redis = Redis->new(
    server      => DEFAULT_SERVER.":".$port,
#    encoding    => undef,
    encoding    => 'utf8',
    );
is $redis->{encoding}, 'utf8', "encoding = 'utf8'";

# encoding being set inside
$redis->{encoding} = undef;
is $redis->{encoding}, undef, "encoding = undef";
$redis->{encoding} = 'utf8';
is $redis->{encoding}, 'utf8', "encoding = 'utf8'";

#-- The behavior of the server itself
# Do not depend on the current:
#   - setting of 'use utf8;' or 'use bytes;'
#   - the place in which data is generated

my $file_euro       = "\x{20ac}";
#my $file_bin        = pack "H8", "5065726c";
my $file_bin        = "\x61\xE2\x98\xBA\x62";

{
    # utf8 - Perl pragma to enable/disable UTF-8 (or UTF-EBCDIC) in source code
    use utf8;                                   # in the current lexical scope

    my $euro    = "\x{20ac}";
#    my $bin     = pack "H8", "5065726c";
    my $bin     = "\x61\xE2\x98\xBA\x62";

    foreach my $mode ( (
        [],
        [ encoding => 'utf8' ],
        [ encoding => undef ]
        ) )
    {
        $redis = Redis->new(
            server      => DEFAULT_SERVER.":".$port,
            @$mode,
            );

        if ( $redis->{encoding} )
        {
            lives_ok { $redis->set( utf8 => $file_euro ) } 'set utf8';
            is_deeply $redis->get( 'utf8' ), $file_euro, 'get utf8';
            lives_ok { $redis->set( utf8 => $euro ) } 'set utf8';
            is_deeply $redis->get( 'utf8' ), $euro, 'get utf8';
        }
        else
        {
# At 'encoding => undef' you can not write data 'utf8'
            dies_ok { $redis->set( utf8 => $file_euro ) } 'not set utf8';
            dies_ok { $redis->set( utf8 => $euro ) } 'not set utf8';
        }
        ok $redis->set( bin => $file_bin ), 'set bin';
        is_deeply $redis->get( 'bin' ), $bin, 'get bin';
    }
}

{
    # disables character semantics for the rest of the lexical scope
    use bytes;

    my $euro    = "\x{20ac}";
#    my $bin     = pack "H8", "5065726c";
    my $bin     = "\x61\xE2\x98\xBA\x62";

    foreach my $mode ( (
        [],
        [ encoding => 'utf8' ],
        [ encoding => undef ]
        ) )
    {
        $redis = Redis->new(
            server      => DEFAULT_SERVER.":".$port,
            @$mode,
            );

        if ( $redis->{encoding} )
        {
            lives_ok { $redis->set( utf8 => $file_euro ) } 'set utf8';
            is_deeply $redis->get( 'utf8' ), $file_euro, 'get utf8';
            lives_ok { $redis->set( utf8 => $euro ) } 'set utf8';
            is_deeply $redis->get( 'utf8' ), $euro, 'get utf8';
        }
        else
        {
# At 'encoding => undef' you can not write data 'utf8'
            dies_ok { $redis->set( utf8 => $file_euro ) } 'not set utf8';
            dies_ok { $redis->set( utf8 => $euro ) } 'not set utf8';
        }
        ok $redis->set( bin => $file_bin ), 'set bin';
        is_deeply $redis->get( 'bin' ), $bin, 'get bin';
    }
}

#-- The behavior of the Redis::JobQueue

# Checking the ordinary fields
{
    # utf8 - Perl pragma to enable/disable UTF-8 (or UTF-EBCDIC) in source code
    use utf8;                                   # in the current lexical scope

    for my $data ( (
        $file_euro,
        $file_bin,
        ))
    {

        foreach my $mode ( (
            [],
            [ encoding => 'utf8' ],
            [ encoding => undef ]
            ) )
        {
            $redis = Redis->new(
                server      => DEFAULT_SERVER.":".$port,
                @$mode,
                );
            my $pre_job = {
                queue       => 'lovely_queue',
                job         => 'strong_job',
                expire      => 12*60*60,
                status      => $data,
                };

            my $jq = Redis::JobQueue->new( $redis );
            my $added_job;
            if ( $redis->{encoding} or $data eq $file_bin )
            {
                lives_ok { $added_job = $jq->add_job( $pre_job ) } 'set utf8';

                my $status = $jq->get_job_data( $added_job, 'status' );
                is_deeply $status, $pre_job->{status}, 'correct loaded status';

                my $new_job = $jq->load_job( $added_job );
                is_deeply $new_job->status, $pre_job->{status}, 'correct loaded status';
            }
            else
            {
                dies_ok { $added_job = $jq->add_job( $pre_job ) } 'not set utf8';
            }
        }
    }
}

{
    # disables character semantics for the rest of the lexical scope
    use bytes;

    for my $data ( (
        $file_euro,
        $file_bin,
        ))
    {

        foreach my $mode ( (
            [],
            [ encoding => 'utf8' ],
            [ encoding => undef ]
            ) )
        {
            $redis = Redis->new(
                server      => DEFAULT_SERVER.":".$port,
                @$mode,
                );
            my $pre_job = {
                queue       => 'lovely_queue',
                job         => 'strong_job',
                expire      => 12*60*60,
                status      => $data,
                };

            my $jq = Redis::JobQueue->new( $redis );
            my $added_job;
            if ( $redis->{encoding} or $data eq $file_bin )
            {
                lives_ok { $added_job = $jq->add_job( $pre_job ) } 'set utf8';

                my $status = $jq->get_job_data( $added_job, 'status' );
                is_deeply $status, $pre_job->{status}, 'correct loaded status';

                my $new_job = $jq->load_job( $added_job );
                is_deeply $new_job->status, $pre_job->{status}, 'correct loaded status';
            }
            else
            {
                dies_ok { $added_job = $jq->add_job( $pre_job ) } 'not set utf8';
            }
        }
    }
}

# Checking the serialized fields
{
    # utf8 - Perl pragma to enable/disable UTF-8 (or UTF-EBCDIC) in source code
    use utf8;                                   # in the current lexical scope

    for my $data ( (
        $file_euro,
        $file_bin,
        ))
    {

        foreach my $mode ( (
            [],
            [ encoding => 'utf8' ],
            [ encoding => undef ]
            ) )
        {
            $redis = Redis->new(
                server      => DEFAULT_SERVER.":".$port,
                @$mode,
                );
            my $pre_job = {
                queue       => 'lovely_queue',
                job         => 'strong_job',
                expire      => 12*60*60,
                result      => \$data,
                meta_data   => {
                    foo     => $data,
                    },
                };

            my $jq = Redis::JobQueue->new( $redis );
            my $added_job;

            lives_ok { $added_job = $jq->add_job( $pre_job ) } 'set utf8';

            my $foo = $jq->get_job_data( $added_job, 'foo' );
            is_deeply $foo, $pre_job->{meta_data}->{foo}, 'correct loaded foo';

            my $new_job = $jq->load_job( $added_job );
            is_deeply $new_job->result, $pre_job->{result}, 'correct loaded result';
            is_deeply $new_job->meta_data( 'foo' ), $pre_job->{meta_data}->{foo}, 'correct loaded foo';
        }
    }
}

{
    # disables character semantics for the rest of the lexical scope
    use bytes;

    for my $data ( (
        $file_euro,
        $file_bin,
        ))
    {

        foreach my $mode ( (
            [],
            [ encoding => 'utf8' ],
            [ encoding => undef ]
            ) )
        {
            $redis = Redis->new(
                server      => DEFAULT_SERVER.":".$port,
                @$mode,
                );
            my $pre_job = {
                queue       => 'lovely_queue',
                job         => 'strong_job',
                expire      => 12*60*60,
                result      => \$data,
                meta_data   => {
                    foo     => $data,
                    },
                };

            my $jq = Redis::JobQueue->new( $redis );
            my $added_job;

            lives_ok { $added_job = $jq->add_job( $pre_job ) } 'set utf8';

            my $foo = $jq->get_job_data( $added_job, 'foo' );
            is_deeply $foo, $pre_job->{meta_data}->{foo}, 'correct loaded foo';

            my $new_job = $jq->load_job( $added_job );
            is_deeply $new_job->result, $pre_job->{result}, 'correct loaded result';
            is_deeply $new_job->meta_data( 'foo' ), $pre_job->{meta_data}->{foo}, 'correct loaded foo';
        }
    }
}

#-- Everything is working correctly, if the data is "protected"

# Checking the ordinary fields
{
    # utf8 - Perl pragma to enable/disable UTF-8 (or UTF-EBCDIC) in source code
    use utf8;                                   # in the current lexical scope

    for my $data ( (
        $file_euro,
        $file_bin,
        ))
    {

        foreach my $mode ( (
            [],
            [ encoding => 'utf8' ],
            [ encoding => undef ]
            ) )
        {
            $redis = Redis->new(
                server      => DEFAULT_SERVER.":".$port,
                @$mode,
                );

            # data is "protected"
            my $status = $data;
            utf8::encode( $status );

            my $pre_job = {
                queue       => 'lovely_queue',
                job         => 'strong_job',
                expire      => 12*60*60,
                status      => $status,
                };

            my $jq = Redis::JobQueue->new( $redis );
            my $added_job;

            lives_ok { $added_job = $jq->add_job( $pre_job ) } 'set utf8';

            my $new_status = $jq->get_job_data( $added_job, 'status' );
            utf8::decode( $new_status );
            is_deeply $new_status, $data, 'correct loaded status';

            my $new_job = $jq->load_job( $added_job );
            $new_status = $new_job->status;
            utf8::decode( $new_status );
            is_deeply $new_status, $data, 'correct loaded status';
        }
    }
}

# Checking the ordinary fields
{
    # disables character semantics for the rest of the lexical scope
    use bytes;
    use Encode;

    for my $data ( (
        $file_euro,
        $file_bin,
        ))
    {

        foreach my $mode ( (
            [],
            [ encoding => 'utf8' ],
            [ encoding => undef ]
            ) )
        {
            $redis = Redis->new(
                server      => DEFAULT_SERVER.":".$port,
                @$mode,
                );

            # data is "protected"
# The "problem" applies only to text fields 'status', 'message'
            my $status = $redis->{encoding} ? $data : Encode::encode_utf8( $data );

            my $pre_job = {
                queue       => 'lovely_queue',
                job         => 'strong_job',
                expire      => 12*60*60,
                status      => $status,
                };

            my $jq = Redis::JobQueue->new( $redis );
            my $added_job;

            lives_ok { $added_job = $jq->add_job( $pre_job ) } 'set utf8';

            my $new_status = $jq->get_job_data( $added_job, 'status' );
            $new_status = $redis->{encoding} ? $new_status : Encode::decode_utf8( $new_status );
            is_deeply $new_status, $data, 'correct loaded status';

            my $new_job = $jq->load_job( $added_job );
            $new_status = $new_job->status;
            is_deeply( ( utf8::is_utf8( $new_status ) ? $new_status : Encode::decode_utf8( $new_job->status ) ), $data, 'correct loaded status' );
        }
    }
}

{
    # disables character semantics for the rest of the lexical scope
    use bytes;

    for my $data ( (
        $file_euro,
        $file_bin,
        ))
    {

        foreach my $mode ( (
            [],
            [ encoding => 'utf8' ],
            [ encoding => undef ]
            ) )
        {
            $redis = Redis->new(
                server      => DEFAULT_SERVER.":".$port,
                @$mode,
                );

            # data is "protected"
            my $status = $data;
            utf8::encode( $status );

            my $pre_job = {
                queue       => 'lovely_queue',
                job         => 'strong_job',
                expire      => 12*60*60,
                status      => $status,
                };

            my $jq = Redis::JobQueue->new( $redis );
            my $added_job;

            lives_ok { $added_job = $jq->add_job( $pre_job ) } 'set utf8';

            my $new_status = $jq->get_job_data( $added_job, 'status' );
            utf8::decode( $new_status );
            is_deeply $new_status, $data, 'correct loaded status';

            my $new_job = $jq->load_job( $added_job );
            $new_status = $new_job->status;
            utf8::decode( $new_status );
            is_deeply $new_status, $data, 'correct loaded status';
        }
    }
}

# For non-serialized fields: UTF8 can not be transferred to the server Redis in mode of 'encoding => undef'
{
    # utf8 - Perl pragma to enable/disable UTF-8 (or UTF-EBCDIC) in source code
    use utf8;                                   # in the current lexical scope

    foreach my $data ( (
        [ status    => $file_euro ],
        [ message   => $file_euro ],
        ) )
    {
        $redis = Redis->new(
            server      => DEFAULT_SERVER.":".$port,
            encoding    => undef,
            );

        my $pre_job = {
            queue       => 'lovely_queue',
            job         => 'strong_job',
            expire      => 12*60*60,
            @$data,
            };

        my $jq = Redis::JobQueue->new( $redis );
        my $added_job;

        dies_ok { $added_job = $jq->add_job( $pre_job ) } 'an attempt set utf8';
        like $@, qr/Invalid argument \(utf8 in \w+\)/, 'correct exception';
    }
}

{
    # disables character semantics for the rest of the lexical scope
    use bytes;

    foreach my $data ( (
        [ status    => $file_euro ],
        [ message   => $file_euro ],
        ) )
    {
        $redis = Redis->new(
            server      => DEFAULT_SERVER.":".$port,
            encoding    => undef,
            );

        my $pre_job = {
            queue       => 'lovely_queue',
            job         => 'strong_job',
            expire      => 12*60*60,
            @$data,
            };

        my $jq = Redis::JobQueue->new( $redis );
        my $added_job;

        dies_ok { $added_job = $jq->add_job( $pre_job ) } 'an attempt set utf8';
        like $@, qr/Invalid argument \(utf8 in \w+\)/, 'correct exception';
    }
}

};
