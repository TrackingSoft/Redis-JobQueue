#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

use lib 'lib';

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

use Redis::JobQueue qw(
    DEFAULT_SERVER
    DEFAULT_PORT
    DEFAULT_TIMEOUT
    NAMESPACE

    E_NO_ERROR
    E_MISMATCH_ARG
    E_DATA_TOO_LARGE
    E_NETWORK
    E_MAX_MEMORY_LIMIT
    E_JOB_DELETED
    E_REDIS
    );

use Redis::JobQueue::Job qw(
    STATUS_CREATED
    STATUS_WORKING
    STATUS_COMPLETED
    STATUS_FAILED
    );

$| = 1;

my $redis;
my $real_redis;
my $port = Net::EmptyPort::empty_port( 32637 ); # 32637-32766 Unassigned

eval { $real_redis = Redis->new( server => DEFAULT_SERVER.":".DEFAULT_PORT ) };
if ( !$real_redis )
{
    $redis = eval { Test::RedisServer->new( conf => { port => $port }, timeout => 3 ) };
    if ( $redis )
    {
        eval { $real_redis = Redis->new( server => DEFAULT_SERVER.":".$port ) };
    }
}
my $skip_msg;
$skip_msg = "Redis server is unavailable" unless ( !$@ && $real_redis && $real_redis->ping );

SKIP: {
    diag $skip_msg if $skip_msg;
    skip( "Redis server is unavailable", 1 ) unless ( !$@ && $real_redis && $real_redis->ping );
$real_redis->quit;

my ( $jq, $job, @jobs, $maxmemory, $vm, $policy );
my $pre_job = {
    id           => '4BE19672-C503-11E1-BF34-28791473A258',
    queue        => 'lovely_queue',
    job          => 'strong_job',
    expire       => 60,
    status       => 'created',
    workload     => \'Some stuff up to 512MB long',
    result       => \'JOB result comes here, up to 512MB long',
    };

sub new_connect {
    # For real Redis:
#    $real_redis = Redis->new( server => DEFAULT_SERVER.":".DEFAULT_PORT );
#    $redis = $real_redis;
#    isa_ok( $redis, 'Redis' );

    # For Test::RedisServer
    $redis = Test::RedisServer->new( conf =>
        {
            port                => Net::EmptyPort::empty_port( 32637 ),
            maxmemory           => $maxmemory,
#            "vm-enabled"        => $vm,
            "maxmemory-policy"  => $policy,
            "maxmemory-samples" => 100,
        },
# Test::RedisServer does not use timeout = 0
        timeout => 3,
        );
    isa_ok( $redis, 'Test::RedisServer' );

    $jq = Redis::JobQueue->new(
        $redis,
        );
    isa_ok( $jq, 'Redis::JobQueue');

    $jq->_call_redis( "DEL", $_ ) foreach $jq->_call_redis( "KEYS", "JobQueue:*" );
}

$maxmemory = 0;
$vm = "no";
$policy = "noeviction";
new_connect();

$jq->_call_redis( "DEL", $_ ) foreach $jq->_call_redis( "KEYS", "JobQueue:*" );

$job = $jq->add_job( $pre_job );
isa_ok( $job, 'Redis::JobQueue::Job');

@jobs = $jq->get_job_ids;
ok scalar( @jobs ), "jobs exists";

#-- E_NO_ERROR

is $jq->last_errorcode, E_NO_ERROR, "E_NO_ERROR";
note '$@: ', $@;

#-- E_MISMATCH_ARG

eval { $jq->load_job( undef ) };
is $jq->last_errorcode, E_MISMATCH_ARG, "E_MISMATCH_ARG";
note '$@: ', $@;

#-- E_DATA_TOO_LARGE

my $prev_max_datasize = $jq->max_datasize;
my $max_datasize = 100;
$pre_job->{result} .= '*' x ( $max_datasize + 1 );
$jq->max_datasize( $max_datasize );

$job = undef;
eval { $job = $jq->add_job( $pre_job ) };
is $jq->last_errorcode, E_DATA_TOO_LARGE, "E_DATA_TOO_LARGE";
note '$@: ', $@;
is $job, undef, "the job isn't changed";
$jq->max_datasize( $prev_max_datasize );

#-- E_NETWORK

$job = $jq->add_job( $pre_job );
isa_ok( $job, 'Redis::JobQueue::Job');

$jq->quit;

@jobs = ();
eval { @jobs = $jq->get_job_ids };
is $jq->last_errorcode, E_NETWORK, "E_NETWORK";
note '$@: ', $@;
ok !scalar( @jobs ), '@jobs is empty';
ok !$jq->_redis->ping, "server is not available";

new_connect();

#-- E_MAX_MEMORY_LIMIT

SKIP:
{
    skip( 'because Test::RedisServer required for that test', 1 ) if eval { $real_redis->ping };

    $maxmemory = 1024 * 1024;
    new_connect();
    my ( undef, $max_datasize ) = $jq->_call_redis( 'CONFIG', 'GET', 'maxmemory' );
    is $max_datasize, $maxmemory, "value is set correctly";

    $pre_job->{result} .= '*' x 1024;
    for ( my $i = 0; $i < 1000; ++$i )
    {
        eval { $job = $jq->add_job( $pre_job ) };
        if ( $@ )
        {
            is $jq->last_errorcode, E_MAX_MEMORY_LIMIT, "E_MAX_MEMORY_LIMIT";
            note "($i)", '$@: ', $@;
            last;
        }
    }
    $jq->_call_redis( "DEL", $_ ) foreach $jq->_call_redis( "KEYS", "JobQueue:*" );
}

#-- job was removed by maxmemory-policy (E_JOB_DELETED)

SKIP:
{
    skip( 'because Test::RedisServer required for that test', 1 ) if eval { $real_redis->ping };

#    $policy = "volatile-lru";       # -> remove the key with an expire set using an LRU algorithm
#    $policy = "allkeys-lru";        # -> remove any key accordingly to the LRU algorithm
#    $policy = "volatile-random";    # -> remove a random key with an expire set
    $policy = "allkeys-random";     # -> remove a random key, any key
#    $policy = "volatile-ttl";       # -> remove the key with the nearest expire time (minor TTL)
#    $policy = "noeviction";         # -> don't expire at all, just return an error on write operations

    $maxmemory = 2 * 1024 * 1024;
    new_connect();
    my ( undef, $max_datasize ) = $jq->_call_redis( 'CONFIG', 'GET', 'maxmemory' );
    is $max_datasize, $maxmemory, "value is set correctly";

    $pre_job->{result} .= '*' x ( 1024 * 10 );
    $pre_job->{expire} = 0;

    $jq->timeout( 1 );
    {
        do
        {
            eval { $job = $jq->add_job( $pre_job ) } for ( 1..1024 );
        } until ( $jq->_call_redis( "KEYS", "JobQueue:queue:*" ) );

        eval {
            while ( my $job = $jq->get_next_job(
                queue       => $pre_job->{queue},
                blocking    => 1,
                ) )
            {
                ;
            }
        };
        redo unless ( $jq->last_errorcode == E_JOB_DELETED );
    }
    ok $@, "exception";
    is $jq->last_errorcode, E_JOB_DELETED, "job was removed by maxmemory-policy";
    note '$@: ', $@;

    $jq->_call_redis( "DEL", $_ ) foreach $jq->_call_redis( "KEYS", "JobQueue:*" );
}

#-- E_JOB_DELETED

SKIP:
{
    skip( 'because Test::RedisServer required for that test', 1 ) if eval { $real_redis->ping };

    $policy = "noeviction";         # -> don't expire at all, just return an error on write operations

    $maxmemory = 1024 * 1024;
    new_connect();
    my ( undef, $max_datasize ) = $jq->_call_redis( 'CONFIG', 'GET', 'maxmemory' );
    is $max_datasize, $maxmemory, "value is set correctly";

# $jq->get_next_job after the jobs expired
    $pre_job->{result} .= '*' x 100;
    $pre_job->{expire} = 1;

    eval { $job = $jq->add_job( $pre_job ) } for ( 1..10 );
    my @jobs = $jq->get_job_ids;
    ok scalar( @jobs ), "the jobs added";
    $jq->delete_job( $_ ) foreach @jobs;
    sleep $pre_job->{expire} * 2;
    my @new_jobs = $jq->get_job_ids;
    ok !scalar( @new_jobs ), "the jobs expired";

    $jq->timeout( 1 );
    eval {
        while ( my $job = $jq->get_next_job(
            queue       => $pre_job->{queue},
            blocking    => 0
            ) )
        {
            ;
        }
    };
    is $@, "", "no exception";

# $jq->get_next_job before the jobs expired
    $pre_job->{expire} = 2;

    eval { $job = $jq->add_job( $pre_job ) } for ( 1..10 );
    @jobs = $jq->get_job_ids;
    ok scalar( @jobs ), "the jobs added";
    $jq->_call_redis( 'DEL', NAMESPACE.':'.$_ ) foreach @jobs;

    eval {
        while ( my $job = $jq->get_next_job(
            queue       => $pre_job->{queue},
            blocking    => 0
            ) )
        {
            ;
        }
    };
    ok $@, "exception";
    is $jq->last_errorcode, E_JOB_DELETED, "E_JOB_DELETED";
    note '$@: ', $@;

    $jq->_call_redis( "DEL", $_ ) foreach $jq->_call_redis( "KEYS", "JobQueue:*" );
}

#-- E_REDIS

eval { $jq->_call_redis( "BADTHING", "Anything" ) };
is $jq->last_errorcode, E_REDIS, "E_REDIS";
note '$@: ', $@;

#-- Closes and cleans up -------------------------------------------------------

$jq->_call_redis( "DEL", $_ ) foreach $jq->_call_redis( "KEYS", "JobQueue:*" );

ok $jq->_redis->ping, "server is available";
$jq->quit;
ok !$jq->_redis->ping, "no server";

};

exit;
