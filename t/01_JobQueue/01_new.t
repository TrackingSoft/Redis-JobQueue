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
# Test::RedisServer does not use timeout = 0
$redis = Test::RedisServer->new( conf => { port => empty_port() }, timeout => 3 );
isa_ok( $redis, 'Test::RedisServer' );

my ( $jq, $next_jq );
my $msg = "attribute is set correctly";

$jq = Redis::JobQueue->new();
isa_ok( $jq, 'Redis::JobQueue' );
is $jq->_server, DEFAULT_SERVER.":".DEFAULT_PORT, $msg;
is $jq->timeout, DEFAULT_TIMEOUT, $msg;
ok ref( $jq->_redis ) =~ /Redis/, $msg;

$jq = Redis::JobQueue->new(
    redis => $server,
    );
isa_ok( $jq, 'Redis::JobQueue' );
is $jq->_server, "$server:".DEFAULT_PORT, $msg;
is $jq->timeout, DEFAULT_TIMEOUT, $msg;
ok ref( $jq->_redis ) =~ /Redis/, $msg;

$jq = Redis::JobQueue->new(
    timeout => $timeout,
    );
isa_ok( $jq, 'Redis::JobQueue');
is $jq->_server, DEFAULT_SERVER.":".DEFAULT_PORT, $msg;
is $jq->timeout, $timeout, $msg;
ok ref( $jq->_redis ) =~ /Redis/, $msg;

$jq = Redis::JobQueue->new(
    redis   => "$server:$port",
    timeout => $timeout,
    );
isa_ok( $jq, 'Redis::JobQueue');
is $jq->_server, "$server:$port", $msg;
is $jq->timeout, $timeout, $msg;
ok ref( $jq->_redis ) =~ /Redis/, $msg;

$jq = Redis::JobQueue->new(
    );

$next_jq = Redis::JobQueue->new(
    $jq,
    );
isa_ok( $next_jq, 'Redis::JobQueue');
is $jq->_server, DEFAULT_SERVER.":".DEFAULT_PORT, $msg;
is $jq->timeout, DEFAULT_TIMEOUT, $msg;
ok ref( $jq->_redis ) =~ /Redis/, $msg;

$jq = Redis::JobQueue->new(
    $jq,
    timeout => $timeout,
    );
isa_ok( $jq, 'Redis::JobQueue');
is $jq->_server, DEFAULT_SERVER.":".DEFAULT_PORT, $msg;
is $jq->timeout, $timeout, $msg;
ok ref( $jq->_redis ) =~ /Redis/, $msg;

$next_jq = Redis::JobQueue->new(
    $redis,
    timeout => 3,
    );
isa_ok( $next_jq, 'Redis::JobQueue');
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
