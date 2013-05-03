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

BEGIN {                                         ## no critic
    eval "use Test::Deep";
    plan skip_all => "because Test::Deep required for testing" if $@;
}

BEGIN {
    eval "use Test::RedisServer";               ## no critic
    plan skip_all => "because Test::RedisServer required for testing" if $@;
}

BEGIN {
    eval "use Net::EmptyPort";                  ## no critic
    plan skip_all => "because Net::EmptyPort required for testing" if $@;
}

use List::MoreUtils qw(
    firstidx
    );
use Redis::JobQueue qw(
    DEFAULT_SERVER
    DEFAULT_PORT
    DEFAULT_TIMEOUT
    );
use Redis::JobQueue::Job;
use Storable;

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
    id          => '4BE19672-C503-11E1-BF34-28791473A258',
    queue       => 'lovely_queue',
    job         => 'strong_job',
    expire      => 12*60*60,
    status      => 'created',
    workload    => 'Some stuff up to 512MB long',
    result      => \'JOB result comes here, up to 512MB long',
    meta_data   => {
        foo     => 12,
        bar     => [ 13, 14, 15 ],
        other   => { a => 'b', c => 'd' },
        s_ref   => \'Hello, Deeply World',
        strble  => Storable::nfreeze( \'Data for Storable' ),
        rstrble => \Storable::nfreeze( \'Data for Storable' ),
        },
    };

# add_job
my $job = Redis::JobQueue::Job->new( $pre_job );
isa_ok( $job, 'Redis::JobQueue::Job');

$pre_job->{meta_data}->{obj} = $job;            # add an example of a complex object

my $added_job = $jq->add_job(
    $pre_job,
    );
isa_ok( $added_job, 'Redis::JobQueue::Job');

# load_job
my $new_job = $jq->load_job( $added_job );
isa_ok( $new_job, 'Redis::JobQueue::Job');
is_deeply $new_job->meta_data, $added_job->meta_data, 'correct loaded hash';

# get_next_job
lives_ok { $new_job = $jq->get_next_job(
    queue       => $pre_job->{queue},
    ) } 'expecting to live';
isa_ok( $new_job, 'Redis::JobQueue::Job');
is_deeply $new_job->meta_data, $added_job->meta_data, 'correct loaded hash';

# update_job
$added_job->clear_variability;
$added_job->meta_data( 'foo', \16 );
is $jq->update_job( $added_job ), 2, "meta_data & updated";
$new_job = $jq->load_job( $added_job );
isa_ok( $new_job, 'Redis::JobQueue::Job');
is_deeply $new_job->meta_data, $added_job->meta_data, 'correct loaded hash';

# get_job_ids
is scalar( $jq->get_job_ids ), 1, 'there is a single job';

# delete_job
my $key = Redis::JobQueue::NAMESPACE.':'.$added_job->id;
ok $jq->_call_redis( 'EXISTS', $key ), 'data hash exists';
ok $jq->_call_redis( 'EXISTS', $jq->_get_meta_data_key( $key ) ), 'metadata hash exists';
ok $jq->delete_job( $added_job ), "job deleted";
ok !$jq->_call_redis( 'EXISTS', $key ), 'data hash does not exists';
ok !$jq->_call_redis( 'EXISTS', $jq->_get_meta_data_key( $key ) ), 'metadata hash does not exists';

# get_job_ids
is scalar( $jq->get_job_ids ), 0, 'no job';

#-- get_job_meta_fields

$pre_job = {
    id          => '4BE19672-C503-11E1-BF34-28791473A258',
    queue       => 'lovely_queue',
    job         => 'strong_job',
    expire      => 12*60*60,
    status      => 'created',
    workload    => 'Some stuff up to 512MB long',
    result      => \'JOB result comes here, up to 512MB long',
    };

$job = Redis::JobQueue::Job->new( $pre_job );

dies_ok { $jq->get_job_meta_fields() } 'expecting to die - no args';

my @mfields = $jq->get_job_meta_fields( $job );
is scalar( @mfields ), 0, 'no metadata';

$pre_job->{meta_data} = {
        foo     => 12,
        bar     => [ 13, 14, 15 ],
        other   => { a => 'b', c => 'd' },
        s_ref   => \'Hello, Deeply World',
        strble  => Storable::nfreeze( \'Data for Storable' ),
        rstrble => \Storable::nfreeze( \'Data for Storable' ),
        };

$jq->_redis->flushall;
$job = $jq->add_job( $pre_job );
@mfields = sort $jq->get_job_meta_fields( $job );
my @arr_k = sort keys %{ $pre_job->{meta_data} };
ok scalar( @mfields ), 'metadata present';
is_deeply( \@mfields, \@arr_k, 'all meta fields present' );

foreach my $id_source ( ( undef, "", \"scalar", [] ) )
{
    dies_ok { $jq->get_job_meta_fields( $id_source ) } 'expecting to die ('.( $id_source // '<undef>' ).')';
}

};
