#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

use lib 'lib';

use Test::More tests => 19;

use Redis::JobQueue::Job;

# The names of object attributes
my @job_fields = qw(
    id
    queue
    job
    expire
    status
    meta_data
    workload
    result
    );

my $pre_job = {
    id          => '4BE19672-C503-11E1-BF34-28791473A258',
    queue       => 'lovely_queue',
    job         => 'strong_job',
    expire      => 12*60*60,
    status      => 'created',
    meta_data   => scalar( localtime ),
    workload    => \'Some stuff up to 512MB long',
    result      => \'JOB result comes here, up to 512MB long',
    };

my $job = Redis::JobQueue::Job->new( $pre_job );
isa_ok( $job, 'Redis::JobQueue::Job');

my @modified = sort $job->modified_attributes;
my @all_fields = sort @job_fields;

is "@modified", "@all_fields", "all fields are modified";

my $fields = scalar @job_fields;
foreach my $field ( @job_fields )
{
    $job->clear_variability( $field );
    my @modified = $job->modified_attributes;
    my $modified = scalar @modified;
    is $modified, --$fields, "modified fields: @modified";
}

$fields = 0;
foreach my $field ( @job_fields )
{
    if ( $field =~ /workload|result/ )
    {
        $job->$field( scalar reverse ${$job->$field} );
    }
    elsif ( $field =~ /expire/ )
    {
        $job->$field( $job->$field + 1 );
    }
    else
    {
        $job->$field( scalar reverse $job->$field );
    }

    my @modified = $job->modified_attributes;
    my $modified = scalar @modified;
    is $modified, ++$fields, "modified fields: @modified";
}

$job->clear_variability( @job_fields );
@modified = $job->modified_attributes;
is scalar @modified, 0, "no modified fields";
