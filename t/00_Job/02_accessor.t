#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

use lib 'lib';

use Test::More tests => 17;

use Redis::JobQueue::Job;

my $pre_job = {
    id          => '4BE19672-C503-11E1-BF34-28791473A258',
    queue       => 'lovely_queue',
    job         => 'strong_job',
    expire      => 12*60*60,
    status      => 'created',
    workload    => \'Some stuff up to 512MB long',
    result      => \'JOB result comes here, up to 512MB long',
    };

my $job = Redis::JobQueue::Job->new( $pre_job );
isa_ok( $job, 'Redis::JobQueue::Job');

foreach my $field ( keys %{$pre_job} )
{
    if ( $field =~ /workload|result/ )
    {
        is ${$job->$field}, ${$pre_job->{ $field }}, "accessor return a valid value (".${$job->$field}.")";
    }
    else
    {
        is $job->$field, $pre_job->{ $field }, "accessor return a valid value (".$job->$field.")";
    }

    if ( $field =~ /workload|result/ )
    {
        $job->$field( scalar reverse ${$job->$field} );
        is scalar( reverse( ${$job->$field} ) ), ${$pre_job->{ $field }}, "accessor return a valid value (".${$job->$field}.")";
        $job->$field( \( scalar reverse ${$job->$field} ) );
        is ${$job->$field}, ${$pre_job->{ $field }}, "accessor return a valid value (".${$job->$field}.")";
    }
    elsif ( $field =~ /expire/ )
    {
        $job->$field( $job->$field + 1 );
        is $job->$field - 1, $pre_job->{ $field }, "accessor return a valid value (".$job->$field.")";
    }
    else
    {
        $job->$field( scalar reverse $job->$field );
        is scalar( reverse( $job->$field ) ), $pre_job->{ $field }, "accessor return a valid value (".$job->$field.")";
    }
}
