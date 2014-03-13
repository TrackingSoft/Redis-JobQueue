#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

use lib 'lib', 't/tlib';

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

use Redis::JobQueue::Test::Utils qw(
    get_redis
);

my $redis;
my $real_redis;
my $port = Net::EmptyPort::empty_port( DEFAULT_PORT );
my $exists_real_redis = 1;
#    eval { $real_redis = Redis->new( server => DEFAULT_SERVER.":".DEFAULT_PORT ) };
if ( !$real_redis )
{
    $exists_real_redis = 0;
    $redis = eval { Test::RedisServer->new( conf => { port => $port }, timeout => 3 ) };
    if ( $redis )
    {
        eval { $real_redis = Redis->new( server => DEFAULT_SERVER.":".$port ) };
    }
}
my $redis_port = $exists_real_redis ? DEFAULT_PORT : $port;
my $redis_addr = DEFAULT_SERVER.":$redis_port";
my @redis_params = ( $exists_real_redis ? () : ( redis => $redis_addr ) );

my $skip_msg;
$skip_msg = "Redis server is unavailable" unless ( !$@ && $real_redis && $real_redis->ping );

SKIP: {
    diag $skip_msg if $skip_msg;
    skip( "Redis server is unavailable", 1 ) unless ( !$@ && $real_redis && $real_redis->ping );

$real_redis->quit;
#    $real_redis->flushall;
#    $redis = $real_redis;
# Test::RedisServer does not use timeout = 0
$redis = get_redis( conf => { port => Net::EmptyPort::empty_port( DEFAULT_PORT ) }, timeout => 3 ) unless $redis;
isa_ok( $redis, 'Test::RedisServer' );

#    my $jq = Redis::JobQueue->new();
my $jq = Redis::JobQueue->new( @redis_params );
isa_ok( $jq, 'Redis::JobQueue' );

my $pre_job = {
    queue       => 'lovely_queue',
    job         => 'strong_job',
    expire      => 12*60*60,
    };

my $job;
for ( 1..5 )
{
    note "$_ .. 5";
    $job = $jq->add_job( $pre_job );
    $job->started( time ) if $_ > 1;
    if ( $_ > 3 )
    {
        $job->failed( time );
    }
    elsif ( $_ > 2 )
    {
        $job->completed( time );
    }
    $jq->update_job( $job );
    sleep 1;
}
$jq->get_next_job( queue => $pre_job->{queue} );

foreach my $queue ( ( $pre_job->{queue}, $job ) )
{
    my $qstatus = $jq->queue_status( $queue );
    note "queue status = ", Dumper( $qstatus );

    is $qstatus->{length}, 4, 'correct length';
    is $qstatus->{all_jobs}, 5, 'correct all_jobs';
    ok $qstatus->{lifetime}, 'lifetime present';
    ok $qstatus->{max_job_age}, 'max_job_age present';
    ok $qstatus->{min_job_age}, 'min_job_age present';
}

$jq->delete_job( $job );

foreach my $queue ( ( $pre_job->{queue}, $job ) )
{
    my $qstatus = $jq->queue_status( $queue );
    note "queue status = ", Dumper( $qstatus );

    is $qstatus->{length}, 3, 'correct length';
    is $qstatus->{all_jobs}, 4, 'correct all_jobs';
    ok $qstatus->{lifetime}, 'lifetime present';
    ok $qstatus->{max_job_age}, 'max_job_age present';
    ok $qstatus->{min_job_age}, 'min_job_age present';
}

my $qstatus = $jq->queue_status( 'something_wrong' );
note "queue status = ", Dumper( $qstatus );
is $qstatus->{all_jobs}, 0, 'correct all_jobs';
is $qstatus->{length}, 0, 'correct length';
is scalar( keys %$qstatus ), 2, 'correct length';

dies_ok { $jq->queue_status } 'expecting to die - no args';

foreach my $queue ( ( undef, "", \"scalar", [] ) )
{
    dies_ok { $jq->queue_status( $queue ) } 'expecting to die ('.( $queue // '<undef>' ).')';
}

};
