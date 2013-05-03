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

use List::MoreUtils qw(
    firstidx
    );
use Params::Util qw(
    _NONNEGINT
    _STRING
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

#-- testing

# it's present in JobQueue.pm
my @job_fields = Redis::JobQueue::Job->job_attributes;
splice @job_fields, ( firstidx { $_ eq 'meta_data' } @job_fields ), 1;

# standard fields

my $hash_data = $jq->get_job_data( $job );
foreach my $field ( keys %$hash_data )
{
    if ( $field =~ /^(created|updated)$/ )
    {
        ok defined( _NONNEGINT( $hash_data->{ $field } ) ), "data OK: $field";
    }
    elsif ( $field =~ /^(started|completed|progress)$/ )
    {
        is $hash_data->{ $field }, 0, "data OK: $field";
    }
    elsif ( $field eq 'elapsed' )
    {
        is $hash_data->{ $field }, undef, "data OK: $field";
    }
    elsif ( $field eq 'status' )
    {
        is $hash_data->{ $field }, STATUS_CREATED, "data OK: $field";
    }
    elsif ( $field eq 'message' )
    {
        is $hash_data->{ $field }, '', "data OK: $field";
    }
    elsif ( $field eq 'id' )
    {
        ok defined( _STRING( $hash_data->{ $field } ) ), "data OK: $field";
    }
    elsif ( $field =~ /^(queue|job|expire)$/ )
    {
        is $hash_data->{ $field }, $pre_job->{ $field }, "data OK: $field";
    }
    elsif ( $field =~ /^(workload|result)$/ )
    {
        is ${ $hash_data->{ $field } }, '', "data OK: $field";
    }
}

foreach my $field ( @job_fields )
{
    my ( $data ) = $jq->get_job_data( $job, $field );
    if ( $field =~ /^(created|updated)$/ )
    {
        ok defined( _NONNEGINT( $hash_data->{ $field } ) ), "data OK: $field";
    }
    elsif ( $field =~ /^(started|completed|progress)$/ )
    {
        is $hash_data->{ $field }, 0, "data OK: $field";
    }
    elsif ( $field eq 'elapsed' )
    {
        is $hash_data->{ $field }, undef, "data OK: $field";
    }
    elsif ( $field eq 'status' )
    {
        is $hash_data->{ $field }, STATUS_CREATED, "data OK: $field";
    }
    elsif ( $field eq 'message' )
    {
        is $hash_data->{ $field }, '', "data OK: $field";
    }
    elsif ( $field eq 'id' )
    {
        ok defined( _STRING( $hash_data->{ $field } ) ), "data OK: $field";
    }
    elsif ( $field =~ /^(queue|job|expire)$/ )
    {
        is $hash_data->{ $field }, $pre_job->{ $field }, "data OK: $field";
    }
    elsif ( $field =~ /^(workload|result)$/ )
    {
        is ${ $hash_data->{ $field } }, '', "data OK: $field";
    }
}

my @arr_data = $jq->get_job_data( $job, @job_fields );
foreach my $idx ( 0..$#job_fields )
{
    my $field = $job_fields[ $idx ];
    if ( $field =~ /^(created|updated)$/ )
    {
        ok defined( _NONNEGINT( $arr_data[ $idx ] ) ), "data OK: $field";
    }
    elsif ( $field =~ /^(started|completed|progress)$/ )
    {
        is $arr_data[ $idx ], 0, "data OK: $field";
    }
    elsif ( $field eq 'elapsed' )
    {
        is $arr_data[ $idx ], undef, "data OK: $field";
    }
    elsif ( $field eq 'status' )
    {
        is $arr_data[ $idx ], STATUS_CREATED, "data OK: $field";
    }
    elsif ( $field eq 'message' )
    {
        is $arr_data[ $idx ], '', "data OK: $field";
    }
    elsif ( $field eq 'id' )
    {
        ok defined( _STRING( $arr_data[ $idx ] ) ), "data OK: $field";
    }
    elsif ( $field =~ /^(queue|job|expire)$/ )
    {
        is $arr_data[ $idx ], $pre_job->{ $field }, "data OK: $field";
    }
    elsif ( $field =~ /^(workload|result)$/ )
    {
        is ${ $arr_data[ $idx ] }, '', "data OK: $field";
    }
}
# scalar context
my $data = $jq->get_job_data( $job, @job_fields );
is_deeply $data, $job->{ $job_fields[0] }, 'correct loaded value';

# metadata fields

$job->meta_data( {
        foo     => 12,
        bar     => [ 13, 14, 15 ],
        other   => { a => 'b', c => 'd' },
        s_ref   => \'Hello, Deeply World',
        strble  => Storable::nfreeze( \'Data for Storable' ),
        rstrble => \Storable::nfreeze( \'Data for Storable' ),
        } );
ok $jq->update_job( $job ),                                     'job updated';

$hash_data = $jq->get_job_data( $job );
ok !exists( $hash_data ->{foo} ), 'metadata not present';

( $data ) = $jq->get_job_data( $job, 'foo' );
is $data, $job->meta_data( 'foo' ), "metadata present";

@arr_data = $jq->get_job_data( $job, @job_fields, 'foo' );
is $arr_data[ $#arr_data ], $job->meta_data( 'foo' ),           'metadata present';

is $jq->get_job_data( 'fake' ), undef, 'job not exists';

# internal recalculations

$job->status( STATUS_WORKING );
ok $jq->update_job( $job ),                                     'job updated';

ok( ( $jq->get_job_data( $job, 'started' ) )[0],                'started is set' );
is( ( $jq->get_job_data( $job, 'completed' ) )[0], 0,           'completed not set' );
ok defined( ( $jq->get_job_data( $job, 'elapsed' ) )[0] ),   'elapsed is set';

$job->progress( 0.5 );
$job->message( 'Hello, World!' );
ok $jq->update_job( $job ),                                     'job updated';

is( ( $jq->get_job_data( $job, 'progress' ) )[0], 0.5,          'progress is set' );
is( ( $jq->get_job_data( $job, 'message' ) )[0], 'Hello, World!',   'message is set' );

foreach my $status ( ( STATUS_COMPLETED, STATUS_FAILED ) )
{
    $job = $jq->add_job( $pre_job );
    isa_ok( $job, 'Redis::JobQueue::Job');

    $job->status( $status );
    ok $jq->update_job( $job ),                                 'job updated';
    is( ( $jq->get_job_data( $job->id, 'status' ) )[0], $status,    'status OK' );

    is( ( $jq->get_job_data( $job, 'started' ) )[0], 0,         'started not set' );
    ok( ( $jq->get_job_data( $job, 'completed' ) )[0],          'completed is set' );
    is( ( $jq->get_job_data( $job, 'elapsed' ) )[0], undef,     'elapsed not set' );
}

foreach my $status ( ( STATUS_COMPLETED, STATUS_FAILED ) )
{
    $job = $jq->add_job( $pre_job );
    isa_ok( $job, 'Redis::JobQueue::Job');

    $job->status( STATUS_WORKING );
    $job->status( $status );
    ok $jq->update_job( $job ),                                 'job updated';
    is( ( $jq->get_job_data( $job->id, 'status' ) )[0], $status,    'status OK' );

    ok( ( $jq->get_job_data( $job, 'started' ) )[0],            'started is set' );
    ok( ( $jq->get_job_data( $job, 'completed' ) )[0],          'completed is set' );
    ok( defined( ( $jq->get_job_data( $job, 'elapsed' ) )[0] ), 'elapsed is set' );
}

lives_ok { $jq->get_job_data( $job->id, @job_fields) } "expecting to live - all standard fields";

# good and bad fields
$job = $jq->add_job( $pre_job );
isa_ok( $job, 'Redis::JobQueue::Job');
$job->meta_data( {
        foo     => 12,
        } );
ok $jq->update_job( $job ),                                     'job updated';
foreach my $fields (
    [ 'fake', 'status', 'foo', [ 'bad thing' ] ],
    [ 'status', 'fake', [ 'bad thing' ], 'foo' ],
    [ [ 'bad thing' ], 'status', 'foo', 'fake' ],
    [ 'foo', [ 'bad thing' ], 'status', 'fake' ],
    )
{
    my @result = $jq->get_job_data( $job, @$fields );
    is $result[ firstidx { $_ eq 'status' } @$fields ], $job->status,               'standard field OK';
    is $result[ firstidx { $_ eq 'foo' }    @$fields ], $job->meta_data( 'foo' ),   'meta_data field OK';
    is $result[ firstidx { $_ eq 'fake' }   @$fields ], undef,                      'fake field OK';
    is $result[ firstidx { ref( $_ ) }      @$fields ], undef,                      'bad field OK';
}

# invalid arguments

dies_ok { $jq->get_job_data() } "expecting to die - no arguments";

foreach my $arg ( ( undef, "", \"scalar", [] ) )
{
    dies_ok { $jq->get_job_data( $arg, 'status' ) } "expecting to die: ".( $arg || "" );
}

is $jq->get_job_data( $job->id, "something wrong" ), undef, "job does not exist";

};
