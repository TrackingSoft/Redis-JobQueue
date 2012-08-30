#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

use lib 'lib';

use Test::More tests => 20;

BEGIN { use_ok 'Redis::JobQueue::Job' }

my @job_fields = qw(
    id
    queue
    job
    expire
    status
    workload
    result
    );

can_ok( 'Redis::JobQueue::Job', 'new' );
can_ok( 'Redis::JobQueue::Job', 'modified_attributes' );
can_ok( 'Redis::JobQueue::Job', 'clear_variability' );
can_ok( 'Redis::JobQueue::Job', 'job_attributes' );

can_ok( 'Redis::JobQueue::Job', 'id' );
can_ok( 'Redis::JobQueue::Job', 'queue' );
can_ok( 'Redis::JobQueue::Job', 'job' );
can_ok( 'Redis::JobQueue::Job', 'status' );
can_ok( 'Redis::JobQueue::Job', 'expire' );
can_ok( 'Redis::JobQueue::Job', 'workload' );
can_ok( 'Redis::JobQueue::Job', 'result' );

foreach my $field ( @job_fields )
{
    can_ok( 'Redis::JobQueue::Job', $field );
}

my $val;
ok( $val = Redis::JobQueue::Job::MAX_DATASIZE, "import OK: $val" );
