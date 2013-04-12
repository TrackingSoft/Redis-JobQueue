#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

use lib 'lib';

use Test::More;
plan "no_plan";
use Test::NoWarnings;

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

my $redis;
my $real_redis;
my $port = Net::EmptyPort::empty_port( 32637 ); # 32637-32766 Unassigned
my $exists_real_redis = 1;
#eval { $real_redis = Redis->new( server => DEFAULT_SERVER.":".DEFAULT_PORT ) };
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
$skip_msg = "Redis server is unavailable" unless ( !$@ and $real_redis and $real_redis->ping );

SKIP: {
    diag $skip_msg if $skip_msg;
    skip( "Redis server is unavailable", 1 ) unless ( !$@ and $real_redis and $real_redis->ping );

$real_redis->quit;
# Test::RedisServer does not use timeout = 0
$redis = Test::RedisServer->new( conf => { port => Net::EmptyPort::empty_port( 32637 ) }, timeout => 3 ) unless $redis;
isa_ok( $redis, 'Test::RedisServer' );

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
    $job->completed( time ) if $_ > 2;
    $jq->update_job( $job );
    sleep 1;
}
$jq->get_next_job( queue => $pre_job->{queue} );
$jq->delete_job( $job );

my $qstatus = $jq->queue_status( $pre_job->{queue} );
note "queue status = ", Dumper( $qstatus );

#ok $qstatus->{avg_lifetime}, 'avg_lifetime present';
is $qstatus->{length}, 4, 'correct length';
is $qstatus->{all_job}, 4, 'correct all_job';
ok $qstatus->{lifetime}, 'lifetime present';
ok $qstatus->{max_job_age}, 'max_job_age present';
ok $qstatus->{min_job_age}, 'min_job_age present';
#ok( ( $qstatus->{avg_lifetime} > $qstatus->{min_job_age} and $qstatus->{avg_lifetime} < $qstatus->{max_job_age} ), 'correct times' );

# The correctness of the 'created', 'started', 'completed' more detailed checked by other tests

#is scalar( keys %{ $qstatus->{jobs} } ), 5, 'records of all the jobs are present';
#
#my ( $exists, $in_queue, $lifetime, $created, $started, $completed, $finishing, $processing );
#foreach my $id ( keys %{ $qstatus->{jobs} } )
#{
#    ++$exists       if $qstatus->{jobs}->{ $id }->{exists};
#    ++$in_queue     if $qstatus->{jobs}->{ $id }->{in_queue};
#    ++$lifetime     if $qstatus->{jobs}->{ $id }->{lifetime};
#    ++$created      if $qstatus->{jobs}->{ $id }->{created};
#    ++$started      if $qstatus->{jobs}->{ $id }->{started};
#    ++$completed    if $qstatus->{jobs}->{ $id }->{completed};
#    ++$finishing    if exists $qstatus->{jobs}->{ $id }->{finishing};
#    ++$processing   if exists $qstatus->{jobs}->{ $id }->{processing};
#}
#is $exists,     4, "the correct amount of 'exists'";
#is $in_queue,   4, "the correct amount of 'in_queue'";
#is $lifetime,   4, "the correct amount of 'lifetime'";
#is $created,    4, "the correct amount of 'created'";
#is $started,    3, "the correct amount of 'started'";
#is $completed,  2, "the correct amount of 'completed'";
#is $finishing,  2, "the correct amount of 'finishing'";
#is $processing, 2, "the correct amount of 'processing'";

};
