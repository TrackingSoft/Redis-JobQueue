#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

use lib 'lib';

use Test::More;
plan "no_plan";

BEGIN {
    eval "use Test::Exception";
    plan skip_all => "because Test::Exception required for testing" if $@;
}

BEGIN {
    eval "use Test::RedisServer";
    plan skip_all => "because Test::RedisServer required for testing" if $@;
}

BEGIN {
    eval "use Test::TCP";
    plan skip_all => "because Test::RedisServer required for testing" if $@;
}

use Redis::JobQueue qw(
    DEFAULT_SERVER
    DEFAULT_PORT
    DEFAULT_TIMEOUT
    STATUS_CREATED
    STATUS_WORKING
    STATUS_COMPLETED
    STATUS_DELETED
    );

# options for testing arguments: ( undef, 0, 0.5, 1, -1, -3, "", "0", "0.5", "1", 9999999999999999, \"scalar", [] )

my $server = "127.0.0.1";
my $port = 6379;
my $timeout = 1;

my $redis;
my $real_redis;
eval { $real_redis = Redis->new( server => "$server:$port" ) };
SKIP: {
    skip( "Redis server is unavailable", 1 ) unless ( !$@ and $real_redis and $real_redis->ping );

# For real Redis:
#$redis = $real_redis;
#isa_ok( $redis, 'Redis' );

# For Test::RedisServer
$real_redis->quit;
$redis = Test::RedisServer->new( conf => { port => empty_port() } );
isa_ok( $redis, 'Test::RedisServer' );

my ( $jq, $job );
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

$jq->_call_redis( "DEL", $_ ) foreach $jq->_call_redis( "KEYS", "JobQueue:*" );

$job = $jq->add_job( $pre_job );
isa_ok( $job, 'Redis::JobQueue::Job');

ok $jq->load_job( $job->id ), "job does exist";
my $id = $job->id;
ok $jq->delete_job( $job ), "job deleted";
$job = $jq->load_job( $job->id );
isa_ok( $job, 'Redis::JobQueue::Job');
is $job->id,            $id,            "correct value";
is $job->queue,         undef,          "correct value";
is $job->job,           undef,          "correct value";
is $job->expire,        undef,          "correct value";
is $job->status,        STATUS_DELETED, "correct value";
is ${$job->workload},   undef,          "correct value";
is ${$job->result},     undef,          "correct value";

$job = $jq->add_job( $pre_job );
isa_ok( $job, 'Redis::JobQueue::Job');

ok $jq->load_job( $job->id ), "job does exist";
ok $jq->delete_job( $job->id ), "job deleted";
$job = $jq->load_job( $job->id );
isa_ok( $job, 'Redis::JobQueue::Job');

$job = $jq->add_job( $pre_job );
isa_ok( $job, 'Redis::JobQueue::Job');
foreach my $arg ( ( undef, "", \"scalar", [] ) )
{
    dies_ok { $jq->delete_job( $arg ) } "expecting to die: ".( $arg || "" );
}

$jq->_call_redis( "DEL", $_ ) foreach $jq->_call_redis( "KEYS", "JobQueue:*" );

};
