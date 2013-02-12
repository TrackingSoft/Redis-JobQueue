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
    plan skip_all => "because Test::TCP required for testing" if $@;
}

use Redis::JobQueue qw(
    DEFAULT_SERVER
    DEFAULT_PORT
    DEFAULT_TIMEOUT
    STATUS_CREATED
    STATUS_WORKING
    STATUS_COMPLETED
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

my ( $jq, $job, $resulting_job, $job2, $job3, $ret, @arr );
my $pre_job = {
    id           => '4BE19672-C503-11E1-BF34-28791473A258',
    queue        => 'lovely_queue',
    job          => 'strong_job',
    expire       => 30,
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

$job = Redis::JobQueue::Job->new(
    id           => $pre_job->{id},
    queue        => $pre_job->{queue},
    job          => $pre_job->{job},
    expire       => $pre_job->{expire},
    status       => $pre_job->{status},
    workload     => $pre_job->{workload},
    result       => $pre_job->{result},
    );
isa_ok( $job, 'Redis::JobQueue::Job');

$resulting_job = $jq->add_job(
    $pre_job,
    );
isa_ok( $resulting_job, 'Redis::JobQueue::Job');

is scalar( $job->modified_attributes ), scalar( keys %{$pre_job} ), "all fields are modified";

$resulting_job = $jq->add_job(
    $job,
    );
isa_ok( $resulting_job, 'Redis::JobQueue::Job');

$resulting_job = $jq->add_job(
    $job,
    LPUSH       => 1,
    );
isa_ok( $resulting_job, 'Redis::JobQueue::Job');

#-------------------------------------------------------------------------------

dies_ok { $resulting_job = $jq->add_job(
    ) } "expecting to die";

foreach my $arg ( ( undef, 0, 0.5, 1, -1, -3, "", "0", "0.5", "1", 9999999999999999, \"scalar", [] ) )
{
    dies_ok { $resulting_job = $jq->add_job(
        $arg,
        ) } "expecting to die: ".( $arg || "" );
}

#-------------------------------------------------------------------------------

#$jq->_call_redis( "flushall" );

$job = $jq->add_job(
    $pre_job,
    );

ok $ret = $jq->_call_redis( 'EXISTS', Redis::JobQueue::NAMESPACE.":".$job->id ), "key exists: $ret";
ok $ret = $jq->_call_redis( 'EXISTS', Redis::JobQueue::NAMESPACE.":queue:".$job->queue ), "key exists: $ret";

$job->queue( "zzz" );

$jq->_call_redis( 'DEL', Redis::JobQueue::NAMESPACE.":queue:".$job->queue );

$job2 = $jq->add_job(
    $job,
    );

ok $ret = $jq->_call_redis( 'EXISTS', Redis::JobQueue::NAMESPACE.":".$job2->id ), "key exists: $ret";
ok $ret = $jq->_call_redis( 'EXISTS', Redis::JobQueue::NAMESPACE.":queue:".$job2->queue ), "key exists: $ret";

$job3 = $jq->add_job(
    $job2,
    );

is scalar ( @arr = $jq->_call_redis( 'LRANGE', Redis::JobQueue::NAMESPACE.":queue:".$job2->queue, 0, -1 ) ), 2, "queue exists: @arr";
is scalar ( @arr = $jq->_call_redis( 'HGETALL', Redis::JobQueue::NAMESPACE.":".$job2->id ) ), ( scalar keys %{$pre_job} ) * 2, "right hash";

foreach my $field ( keys %{$pre_job} )
{
    if ( $field =~ /workload|result/ )
    {
        is $jq->_call_redis( 'HGET', Redis::JobQueue::NAMESPACE.":".$job2->id, $field ), ${$job2->$field}, "a valid value (".${$job2->$field}.")";
    }
    else
    {
        is $jq->_call_redis( 'HGET', Redis::JobQueue::NAMESPACE.":".$job2->id, $field ), $job2->$field, "a valid value (".$job2->$field.")";
    }
}

$jq->_call_redis( "DEL", $_ ) foreach $jq->_call_redis( "KEYS", "JobQueue:*" );

};
