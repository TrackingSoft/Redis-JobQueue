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
    verify_redis
);

# options for testing arguments: ( undef, 0, 0.5, 1, -1, -3, "", "0", "0.5", "1", 9999999999999999, \"scalar", [] )

my $server = DEFAULT_SERVER;
my $timeout = 1;

my $redis_error = "Unable to create test Redis server";
my ( $redis, $skip_msg, $port ) = verify_redis();

my $redis_addr = "$server:$port";
my @redis_params = ( redis => $redis_addr );

SKIP: {
    diag $skip_msg if $skip_msg;
    skip( $skip_msg, 1 ) if $skip_msg;

# Test::RedisServer does not use timeout = 0
$redis = get_redis( $redis, conf => { port => Net::EmptyPort::empty_port( DEFAULT_PORT ) }, timeout => 3 ) unless $redis;
skip( $redis_error, 1 ) unless $redis;
isa_ok( $redis, 'Test::RedisServer' );

my ( $jq, $next_jq );
my $msg = "attribute is set correctly";

$jq = Redis::JobQueue->new( @redis_params );
isa_ok( $jq, 'Redis::JobQueue' );
is $jq->_server, $redis_addr, $msg;
is $jq->timeout, DEFAULT_TIMEOUT, $msg;
ok ref( $jq->_redis ) =~ /Redis/, $msg;

$jq = Redis::JobQueue->new(
    @redis_params,
    );
isa_ok( $jq, 'Redis::JobQueue' );
is $jq->_server, $redis_addr, $msg;
is $jq->timeout, DEFAULT_TIMEOUT, $msg;
ok ref( $jq->_redis ) =~ /Redis/, $msg;

$jq = Redis::JobQueue->new(
    timeout => $timeout,
    @redis_params,
    );
isa_ok( $jq, 'Redis::JobQueue');
is $jq->_server, $redis_addr, $msg;
is $jq->timeout, $timeout, $msg;
ok ref( $jq->_redis ) =~ /Redis/, $msg;

$jq = Redis::JobQueue->new(
    redis   => $redis_addr,
    timeout => $timeout,
    );
isa_ok( $jq, 'Redis::JobQueue');
is $jq->_server, $redis_addr, $msg;
is $jq->timeout, $timeout, $msg;
ok ref( $jq->_redis ) =~ /Redis/, $msg;

$jq = Redis::JobQueue->new(
    @redis_params,
    );

$next_jq = Redis::JobQueue->new(
    $jq,
    );
isa_ok( $next_jq, 'Redis::JobQueue');
is $jq->_server, $redis_addr, $msg;
is $jq->timeout, DEFAULT_TIMEOUT, $msg;
ok ref( $jq->_redis ) =~ /Redis/, $msg;

$jq = Redis::JobQueue->new(
    $jq,
    timeout => $timeout,
    );
isa_ok( $jq, 'Redis::JobQueue');
is $jq->_server, $redis_addr, $msg;
is $jq->timeout, $timeout, $msg;
ok ref( $jq->_redis ) =~ /Redis/, $msg;

$next_jq = Redis::JobQueue->new(
    $redis,
    timeout => 3,
    );
isa_ok( $next_jq, 'Redis::JobQueue');
#is $next_jq->_redis->{encoding}, $redis->isa( 'Redis' ) ? 'utf8' : undef, $redis->isa( 'Redis' ) ? 'encoding exists' : 'encoding not exists';
is $next_jq->_redis->{encoding}, undef, 'encoding not exists';
is $next_jq->_server, $next_jq->_redis->{server}, $msg;
is $next_jq->timeout, 3, $msg;
ok ref( $jq->_redis ) =~ /Redis/, $msg;

$next_jq = Redis::JobQueue->new(
    $redis,
    timeout => $timeout,
    );
isa_ok( $next_jq, 'Redis::JobQueue');
is $next_jq->_server, $next_jq->_redis->{server}, $msg;
is $next_jq->timeout, $timeout, $msg;
ok ref( $jq->_redis ) =~ /Redis/, $msg;

dies_ok { $jq = Redis::JobQueue->new(
    redis => $timeout,
    ) } "expecting to die";

dies_ok { $jq = Redis::JobQueue->new(
    timeout => $server,
    ) } "expecting to die";

};
