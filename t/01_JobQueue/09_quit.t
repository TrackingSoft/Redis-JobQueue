#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

use lib 'lib', 't/tlib';

use Test::More;
plan "no_plan";

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

use Test::NoWarnings;

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

use Redis::JobQueue::Test::Utils qw(
    get_redis
);

# options for testing arguments: ( undef, 0, 0.5, 1, -1, -3, "", "0", "0.5", "1", 9999999999999999, \"scalar", [] )

my $server = "127.5.5.5";   # for example
#my $port = 6379;
my $timeout = 1;

my $redis;
my $real_redis;
my $port = Net::EmptyPort::empty_port( DEFAULT_PORT );

#eval { $real_redis = Redis->new( server => "$server:$port" ) };
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

# For real Redis:
#$redis = $real_redis;
#isa_ok( $redis, 'Redis' );

# For Test::RedisServer
$real_redis->quit;
$port = Net::EmptyPort::empty_port( DEFAULT_PORT );
$redis = get_redis( conf => { port => $port } );
isa_ok( $redis, 'Test::RedisServer' );

my ( $jq, $job, @jobs );
my $pre_job = {
    id           => '4BE19672-C503-11E1-BF34-28791473A258',
    queue        => 'lovely_queue',
    job          => 'strong_job',
    expire       => 60,
    status       => 'created',
    workload     => \'Some stuff up to 512MB long',
    result       => \'JOB result comes here, up to 512MB long',
    };

$jq = Redis::JobQueue->new(
    $redis,
    timeout => $timeout,
    );
isa_ok( $jq, 'Redis::JobQueue');

#-- server
is $jq->server, $jq->_server, 'redis address OK';
like $jq->server, qr/:$port$/, 'redis address OK';

$jq->_call_redis( "DEL", $_ ) foreach $jq->_call_redis( "KEYS", "JobQueue:*" );

$job = $jq->add_job( $pre_job );
isa_ok( $job, 'Redis::JobQueue::Job');

@jobs = $jq->get_job_ids;
ok scalar( @jobs ), "jobs exists";

$jq->_call_redis( "DEL", $_ ) foreach $jq->_call_redis( "KEYS", "JobQueue:*" );

ok $jq->_redis->ping, "server is available";
$jq->quit;
ok !$jq->_redis->ping, "no server";

#-- ping

$jq = Redis::JobQueue->new(
    $redis,
    timeout => $timeout,
    );
isa_ok( $jq, 'Redis::JobQueue');

ok $jq->ping, "server is available";
$jq->quit;
ok !$jq->ping, "no server";

};

exit;
