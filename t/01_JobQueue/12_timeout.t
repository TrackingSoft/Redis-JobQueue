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

    E_NO_ERROR
    );

use Redis::JobQueue::Job qw(
    STATUS_CREATED
    STATUS_WORKING
    STATUS_COMPLETED
    STATUS_FAILED
    );

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
$skip_msg = "Redis server is unavailable" unless ( !$@ and $real_redis and $real_redis->ping );

SKIP: {
    diag $skip_msg if $skip_msg;
    skip( "Redis server is unavailable", 1 ) unless ( !$@ and $real_redis and $real_redis->ping );
$real_redis->quit;

my ( $jq, $job, @jobs, $maxmemory, $vm, $policy, $timeout );
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
        $timeout ? ( timeout => $timeout ) : (),
        );
    isa_ok( $jq, 'Redis::JobQueue');

    $jq->_call_redis( "DEL", $_ ) foreach $jq->_call_redis( "KEYS", "JobQueue:*" );
}

$maxmemory = 0;
$policy = "noeviction";
$timeout = 3;
new_connect();

$jq->_call_redis( "DEL", $_ ) foreach $jq->_call_redis( "KEYS", "JobQueue:*" );

$job = $jq->add_job( $pre_job );
isa_ok( $job, 'Redis::JobQueue::Job');

@jobs = $jq->get_job_ids;
ok scalar( @jobs ), "jobs exists";

#-- E_NO_ERROR

is $jq->last_errorcode, E_NO_ERROR, "E_NO_ERROR";
note '$@: ', $@;

#-- timeout

my $tm = time;
$job = $jq->get_next_job(
    queue    => 'not_lovely_queue',
    blocking => 1,
    );
ok time - $tm >= $timeout, "timeout ok";

$tm = time;
$job = $jq->get_next_job(
    queue    => 'lovely_queue',
    blocking => 0,
    );
ok time - $tm == 0, "timeout ok";

#-- sample

new_connect();

my $queue = Redis::JobQueue->new(
    $redis,
    timeout => 3,
);

my @job_types = qw( foo bar );

say scalar localtime;
while( my $job = $queue->get_next_job(
    queue    => 'ts',
    blocking => 1,
))
{
    say "Got job: $job";
}
say scalar localtime;

#-- Closes and cleans up -------------------------------------------------------

$jq->_call_redis( "DEL", $_ ) foreach $jq->_call_redis( "KEYS", "JobQueue:*" );

ok $jq->_redis->ping, "server is available";
$jq->quit;
ok !$jq->_redis->ping, "no server";

};

exit;
