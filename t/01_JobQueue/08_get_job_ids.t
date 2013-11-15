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

use List::Util qw(
    shuffle
    );
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

# options for testing arguments: ( undef, 0, 0.5, 1, -1, -3, "", "0", "0.5", "1", 9999999999999999, \"scalar", [] )

my $server = "127.0.0.1";
#my $port = 6379;
my $timeout = 1;

my $redis;
my $real_redis;
my $port = Net::EmptyPort::empty_port( 32637 ); # 32637-32766 Unassigned

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
$redis = Test::RedisServer->new( conf => {
    port => Net::EmptyPort::empty_port( 32637 ),
    } );
isa_ok( $redis, 'Test::RedisServer' );

my ( $jq, $job, @jobs, @new_jobs );
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

#-------------------------------------------------------------------------------

#-- full list

my @statuses = (
    STATUS_CREATED,
    STATUS_WORKING,
    STATUS_COMPLETED,
    STATUS_FAILED,
    'something else'
    );

@jobs = $jq->get_job_ids;
is scalar( @jobs ), 0, 'there are no jobs';
@jobs = $jq->get_job_ids( queue => [ 'q1', 'q2' ], status => [ 's1', 's2', 's3' ] );
is scalar( @jobs ), 0, 'there are no jobs';

foreach my $status ( @statuses )
{
    $job = $jq->add_job( $pre_job );
    $job->status( $status );
    $jq->update_job( $job );
}
@jobs = $jq->get_job_ids;
is scalar( @jobs ), scalar( @statuses ), 'jobs added';

#-- Filter by 'status'

@jobs = $jq->get_job_ids( status => STATUS_COMPLETED );
is scalar( @jobs ), 1, 'job filtered ('.STATUS_COMPLETED.')';

@jobs = $jq->get_job_ids( status => 'something wrong' );
is scalar( @jobs ), 0, 'job not found';

@jobs = $jq->get_job_ids( status => \@statuses );
is scalar( @jobs ), scalar( @statuses ), 'jobs filtered';

my @heap = ( STATUS_CREATED, STATUS_FAILED, 'fake', [ 'bad thibg' ] );
for ( 1..100 )
{
    @jobs = $jq->get_job_ids( status => [ shuffle @heap ] );
    is scalar( @jobs ), 2, 'all jobs found';
}

#-- Filter by 'queue'

$pre_job->{queue} = 'next_queue';
foreach my $status ( @statuses )
{
    $job = $jq->add_job( $pre_job );
    $job->status( $status );
    $jq->update_job( $job );
}

for ( 1..100 )
{
    @jobs = $jq->get_job_ids( status => [ shuffle @heap ] );
    is scalar( @jobs ), 4, 'all jobs found';
}

# not queued
@jobs = $jq->get_job_ids( queue => 'next_queue' );
is scalar( @jobs ), scalar( @statuses ), 'jobs filtered';
@jobs = $jq->get_job_ids( queue => [ 'lovely_queue', 'next_queue' ] );
is scalar( @jobs ), scalar( @statuses ) * 2, 'jobs filtered';

# queued
@jobs = $jq->get_job_ids( queued => 1 );
is scalar( @jobs ), scalar( @statuses ) * 2, 'jobs filtered';
@jobs = $jq->get_job_ids( queued => 1, queue => 'next_queue' );
is scalar( @jobs ), scalar( @statuses ), 'jobs filtered';
@jobs = $jq->get_job_ids( queued => 1, queue => [ 'lovely_queue', 'next_queue' ] );
is scalar( @jobs ), scalar( @statuses ) * 2, 'jobs filtered';

# get one job
$jq->get_next_job( queue => 'lovely_queue' );

# not queued
@jobs = $jq->get_job_ids( queue => [ 'lovely_queue' ] );
is scalar( @jobs ), scalar( @statuses ), 'jobs filtered';

# queued
@jobs = $jq->get_job_ids( queued => 1, queue => [ 'lovely_queue' ] );
is scalar( @jobs ), scalar( @statuses ) - 1, 'jobs filtered';

#-- bad arguments

dies_ok { $jq->get_job_ids( 'something' ) } 'expecting to die (Odd number of elements in hash assignment)';

foreach my $arg ( ( undef, 0, 0.5, 1, -1, -3, "", "0", "0.5", "1", 9999999999999999, \"scalar", [] ) )
{
    dies_ok { $jq->get_job_ids( $arg ) } 'expecting to die ('.( $arg // '<undef>' ).')';
}

foreach my $val ( ( undef, 0, 0.5, 1, -1, -3, "", "0", "0.5", "1", 9999999999999999, \"scalar", [] ) )
{
    # keywords are different from 'queue', 'status' are ignored
    lives_ok { $jq->get_job_ids( anything => $val ) } 'expecting to die ('.( $val // '<undef>' ).')';
}

};
