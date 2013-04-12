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

my $job = $jq->add_job( $pre_job );
isa_ok( $job, 'Redis::JobQueue::Job');

ok $jq->get_job_created( $job ),                    'created is set';
ok $jq->get_job_updated( $job ),                    'updated is set';
is $jq->get_job_started( $job ), 0,                 'started not set';
is $jq->get_job_completed( $job ), 0,               'completed not set';
is $jq->get_job_elapsed( $job ), undef,             'elapsed not set';

is $jq->get_job_progress( $job ), 0,                'progress not set';
is $jq->get_job_message( $job ), '',                'message not set';

$job->status( STATUS_WORKING );
ok $jq->update_job( $job ),                         'job updated';

ok $jq->get_job_started( $job ),                    'started is set';
is $jq->get_job_completed( $job ), 0,               'completed not set';
ok defined( $jq->get_job_elapsed( $job ) ),         'elapsed is set';

$job->progress( 0.5 );
$job->message( 'Hello, World!' );
ok $jq->update_job( $job ),                         'job updated';

is $jq->get_job_progress( $job ), 0.5,              'progress is set';
is $jq->get_job_message( $job ), 'Hello, World!',   'message is set';

foreach my $status ( ( STATUS_COMPLETED, STATUS_FAILED ) )
{
    $job = $jq->add_job( $pre_job );
    isa_ok( $job, 'Redis::JobQueue::Job');

    $job->status( $status );
    ok $jq->update_job( $job ),                     'job updated';

    is $jq->get_job_started( $job ), 0,             'started not set';
    ok $jq->get_job_completed( $job ),              'completed is set';
    is $jq->get_job_elapsed( $job ), undef,         'elapsed not set';
}

foreach my $status ( ( STATUS_COMPLETED, STATUS_FAILED ) )
{
    $job = $jq->add_job( $pre_job );
    isa_ok( $job, 'Redis::JobQueue::Job');

    $job->status( STATUS_WORKING );
    $job->status( $status );
    ok $jq->update_job( $job ),                     'job updated';

    ok $jq->get_job_started( $job ),                'started is set';
    ok $jq->get_job_completed( $job ),              'completed is set';
    ok defined( $jq->get_job_elapsed( $job ) ),     'elapsed is set';
}

};
