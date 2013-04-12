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

my $server = DEFAULT_SERVER;
#my $port = 6379;
my $timeout = 1;

my $redis;
my $real_redis;
my $port = Net::EmptyPort::empty_port( 32637 ); # 32637-32766 Unassigned

#eval { $real_redis = Redis->new( server => "$server:$port" ) };
my $exists_real_redis = 1;
eval { $real_redis = Redis->new( server => DEFAULT_SERVER.":".DEFAULT_PORT ) };
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
my $redis_addr = "$server:$redis_port";
my @redis_params = ( $exists_real_redis ? () : ( redis => $redis_addr ) );

my $skip_msg;
$skip_msg = "Redis server is unavailable" unless ( !$@ and $real_redis and $real_redis->ping );

SKIP: {
    diag $skip_msg if $skip_msg;
    skip( "Redis server is unavailable", 1 ) unless ( !$@ and $real_redis and $real_redis->ping );

# For real Redis:
#$redis = $real_redis;
#isa_ok( $redis, 'Redis' );
#$port = DEFAULT_PORT;

# For Test::RedisServer
$real_redis->quit;
# Test::RedisServer does not use timeout = 0
$redis = Test::RedisServer->new( conf => { port => Net::EmptyPort::empty_port( 32637 ) }, timeout => 3 ) unless $redis;
isa_ok( $redis, 'Test::RedisServer' );

my ( $jq, $next_jq );
my $msg = "attribute is set correctly";

$jq = Redis::JobQueue->new( @redis_params );
isa_ok( $jq, 'Redis::JobQueue' );
is $jq->_server, $redis_addr, $msg;
is $jq->timeout, DEFAULT_TIMEOUT, $msg;
ok ref( $jq->_redis ) =~ /Redis/, $msg;

$jq = Redis::JobQueue->new(
    $exists_real_redis ? ( redis => $server ) : ( redis => $redis_addr ),
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
