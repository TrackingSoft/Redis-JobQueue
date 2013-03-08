#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

use lib 'lib';

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
$skip_msg = "Redis server is unavailable" unless ( !$@ and $real_redis and $real_redis->ping );

SKIP: {
    diag $skip_msg if $skip_msg;
    skip( "Redis server is unavailable", 1 ) unless ( !$@ and $real_redis and $real_redis->ping );

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

foreach my $field ( keys %{$pre_job} )
{
#    next if $field =~ /expire|id/;

    $job = $jq->add_job( $pre_job );
    isa_ok( $job, 'Redis::JobQueue::Job');

    is scalar( $job->modified_attributes ), 0, "no modified fields";
    $job->$field( $job->$field );
    is scalar( $job->modified_attributes ), 1, "is modified field";

    if ( $field =~ /id/ )
    {
        $job->$field( $job->$field."wrong" );
        is scalar( $job->modified_attributes ), 1, "is modified field";
        ok !$jq->update_job( $job ), "job not found";
    }
    elsif ( $field =~ /expire/ )
    {
        my $key = Redis::JobQueue::NAMESPACE.":".$job->id;
        ok $jq->_call_redis( 'TTL', $key ), "EXPIRE is";
        $job->$field( 0 );
        is $job->$field, 0, "a valid value (".$job->$field.")";
        ok !$jq->update_job( $job ), "job not updated";
        ok $jq->_call_redis( 'TTL', $key ), "EXPIRE is";
        $new_job = $jq->load_job( $job->id );
        isnt $new_job->$field, $job->$field, "a valid value (".$job->$field.")";
    }
    elsif ( $field =~ /workload|result/ )
    {
        ok $jq->update_job( $job ), "successful update";
        $new_job = $jq->load_job( $job->id );
        is ${$new_job->$field}, ${$job->$field}, "a valid value (".${$new_job->$field}.")";
    }
    else
    {
        ok $jq->update_job( $job ), "successful update";
        $new_job = $jq->load_job( $job->id );
        is $new_job->$field, $job->$field, "a valid value (".$job->$field.")";
    }
}

$job = $jq->add_job( $pre_job );
isa_ok( $job, 'Redis::JobQueue::Job');
foreach my $arg ( ( undef, 0, 0.5, 1, -1, -3, "", "0", "0.5", "1", 9999999999999999, \"scalar", [] ) )
{
    dies_ok { $jq->update_job( $arg ) } "expecting to die: ".( $arg || "" );
}

$jq->_call_redis( "DEL", $_ ) foreach $jq->_call_redis( "KEYS", "JobQueue:*" );

};
