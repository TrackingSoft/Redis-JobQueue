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
    );

use Redis::JobQueue::Job qw(
    STATUS_CREATED
    STATUS_WORKING
    STATUS_COMPLETED
    STATUS_FAILED
    );

# options for testing arguments: ( undef, 0, 0.5, 1, -1, -3, "", "0", "0.5", "1", 9999999999999999, \"scalar", [] )

my $server = "127.5.5.5";   # for example
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
$redis = Test::RedisServer->new( conf => { port => Net::EmptyPort::empty_port( 32637 ) } );
isa_ok( $redis, 'Test::RedisServer' );

my ( $jq, $job, $new_job );
my $pre_job = {
    id           => '4BE19672-C503-11E1-BF34-28791473A258',
    queue        => 'lovely_queue',
    job          => 'strong_job',
    expire       => 600,
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

foreach my $field ( keys %{$pre_job} )
{
    $job = $jq->add_job( $pre_job );
    isa_ok( $job, 'Redis::JobQueue::Job');

    is scalar( $job->modified_attributes ), 0, "no modified fields";
    $job->$field( $job->$field );
    is scalar( $job->modified_attributes ), 1 + ( $field =~ /^status|^meta_data|^workload|^result|^progress|^message|^completed|^failed/ ? 1 : 0 ), "is modified field"; # because also changes 'updated'

    if ( $field eq 'id' )
    {
        $job->$field( $job->$field."wrong" );
        is scalar( $job->modified_attributes ), 1, "is modified field";
        ok !$jq->update_job( $job ), "(id) job not found";
    }
    elsif ( $field eq 'expire' )
    {
        my $key = Redis::JobQueue::NAMESPACE.":".$job->id;
        ok $jq->_call_redis( 'TTL', $key ), "EXPIRE is";
        $job->$field( 0 );
        is $job->$field, 0, "a valid value ($field = ".$job->$field.")";
        ok !$jq->update_job( $job ), "job not updated";
        ok $jq->_call_redis( 'TTL', $key ), "EXPIRE is";
        $new_job = $jq->load_job( $job->id );
        isnt $new_job->$field, $job->$field, "a valid value ($field = ".$job->$field.")";
    }
    elsif ( $field =~ /^workload|^result/ )
    {
        ok $jq->update_job( $job ), "successful update";
        $new_job = $jq->load_job( $job->id );
        is ${$new_job->$field}, ${$job->$field}, "a valid value ($field = ".${$new_job->$field}.")";
    }
    else
    {
        ok $jq->update_job( $job ), "successful update";
        $new_job = $jq->load_job( $job->id );
        is $new_job->$field, $job->$field, "a valid value ($field = ".$job->$field.")";
    }
}

$job = $jq->add_job( $pre_job );
isa_ok( $job, 'Redis::JobQueue::Job');
foreach my $arg ( ( undef, 0, 0.5, 1, -1, -3, "", "0", "0.5", "1", 9999999999999999, \"scalar", [] ) )
{
    dies_ok { $jq->update_job( $arg ) } "expecting to die: ".( $arg || "" );
}

#$jq->_call_redis( "DEL", $_ ) foreach $jq->_call_redis( "KEYS", "JobQueue:*" );

# Testing workload|result with valid data types
$jq = Redis::JobQueue->new(
    $redis,
    timeout => $timeout,
    );
isa_ok( $jq, 'Redis::JobQueue');

#$pre_job->{expire} = 600;
$job = $jq->add_job( $pre_job );
my $obj = $jq->add_job( $pre_job );

foreach my $field ( qw( workload result ) )
{
    foreach my $val (
        'Some stuff',
        \'Any stuff',
        { first => 'First stuff', second => 'Second stuff' },
        [ 'Foo', 'Bar' ],
        $obj,
        )
    {
        $job->$field( $val );
        is $jq->update_job( $job ), 2, 'job updated';   # because also changes 'updated'
        my $retrieved_job = $jq->load_job( $job );
        is_deeply $retrieved_job->$field, $job->$field, "$field is correct";
        is $jq->update_job( $retrieved_job ), 0, 'job not updated';
    }
}

$jq->_call_redis( "DEL", $_ ) foreach $jq->_call_redis( "KEYS", "JobQueue:*" );

};
