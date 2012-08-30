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
    plan skip_all => "because Test::RedisServer required for testing" if $@;
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
$redis = Test::RedisServer->new( conf => { port => empty_port() } );
isa_ok( $redis, 'Test::RedisServer' );

my ( $jq, $job, @jobs, $idx, @job_names, $to_left, $blocking, $name );
my $pre_job = {
    id           => '4BE19672-C503-11E1-BF34-28791473A258',
    queue        => 'lovely_queue',
    job          => 'strong_job',
    expire       => 30,
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

sleep 1;
foreach my $blocking ( ( 0, 1 ) )
{
    @job_names = ();
    foreach my $name ( qw( yyy zzz ) )
    {
        push @job_names, $name;
        @jobs = ();
        foreach my $job_name ( @job_names )
        {
            $pre_job->{job} = $job_name;
            $job = Redis::JobQueue::Job->new( $pre_job );
            isa_ok( $job, 'Redis::JobQueue::Job');

            push( @jobs, $jq->add_job( $job ) ) for ( 0..2 );
            isa_ok( $jobs[ $_ ], 'Redis::JobQueue::Job' ) for ( 0..2 );
        }

        $idx = 0;
        while ( my $new_job = $jq->get_next_job(
            queue       => $pre_job->{queue},
            job         => scalar( @job_names ) == 1 ? $job_names[0] : \@job_names,
            blocking    => $blocking,
            ) )
        {
            isa_ok( $new_job, 'Redis::JobQueue::Job' );

            foreach my $field ( keys %{$pre_job} )
            {
                if ( $field =~ /workload|result/ )
                {
                    is ${$new_job->$field}, ${$jobs[ $idx ]->$field}, "a valid value (".${$new_job->$field}.")";
                }
                else
                {
                    is $new_job->$field, $jobs[ $idx ]->$field, "a valid value (".$new_job->$field.")";
                }
            }
            ++$idx;
        }
    }
}

$jq->_call_redis( "DEL", $_ ) foreach $jq->_call_redis( "KEYS", "JobQueue:*" );

$to_left = 1;
@jobs = ();
$name = "yyy";
$pre_job->{job} = $name;
$job = Redis::JobQueue::Job->new( $pre_job );
isa_ok( $job, 'Redis::JobQueue::Job');

unshift( @jobs, $jq->add_job( $job, LPUSH => $to_left ) ) for ( 0..2 );
isa_ok( $jobs[ $_ ], 'Redis::JobQueue::Job' ) for ( 0..2 );

$idx = 0;
while ( my $new_job = $jq->get_next_job(
    queue       => $pre_job->{queue},
    job         => $name,
    ) )
{
    isa_ok( $new_job, 'Redis::JobQueue::Job' );

    foreach my $field ( keys %{$pre_job} )
    {
        if ( $field =~ /workload|result/ )
        {
            is ${$new_job->$field}, ${$jobs[ $idx ]->$field}, "a valid value (".${$new_job->$field}.")";
        }
        else
        {
            is $new_job->$field, $jobs[ $idx ]->$field, "a valid value (".$new_job->$field.")";
        }
    }
    ++$idx;
}

foreach my $arg ( ( undef, "", \"scalar", [] ) )
{
    dies_ok { $jq->get_next_job(
        queue       => $arg,
        job         => [ $pre_job->{job} ],
        ) } "expecting to die: ".( $arg || "" );

    dies_ok { $jq->get_next_job(
        queue       => $pre_job->{queue},
        job         => $arg,
        ) } "expecting to die: ".( $arg || "" );

    dies_ok { $jq->get_next_job(
        queue       => $pre_job->{queue},
        job         => [ $arg ],
        ) } "expecting to die: ".( $arg || "" );
}

$blocking = 1;
$pre_job->{job} = 'aaa';
$pre_job->{expire} = $timeout;
$job = Redis::JobQueue::Job->new( $pre_job );
isa_ok( $job, 'Redis::JobQueue::Job');
my $new_job = $jq->add_job( $job );
isa_ok( $new_job, 'Redis::JobQueue::Job' );
$new_job = $jq->get_next_job(
    queue       => $pre_job->{queue},
    job         => $pre_job->{job},
    blocking    => $blocking,
    );
isa_ok( $new_job, 'Redis::JobQueue::Job' );
$new_job = $jq->add_job( $job );
isa_ok( $new_job, 'Redis::JobQueue::Job' );
sleep $timeout * 2;
$new_job = $jq->get_next_job(
    queue       => $pre_job->{queue},
    job         => $pre_job->{job},
    blocking    => $blocking,
    );
is $new_job, undef, "job identifier has already been removed";

$jq->_call_redis( "DEL", $_ ) foreach $jq->_call_redis( "KEYS", "JobQueue:*" );

};
