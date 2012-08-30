#!/usr/bin/perl -w

# NAME: JobQueue client demonstration

#-- Common ---------------------------------------------------------------------

use 5.010;
use strict;
use warnings;

use Redis::JobQueue qw(
    DEFAULT_SERVER
    DEFAULT_PORT
    STATUS_CREATED
    STATUS_WORKING
    STATUS_COMPLETED

    ENOERROR
    EMISMATCHARG
    EDATATOOLARGE
    ENETWORK
    EMAXMEMORYLIMIT
    EMAXMEMORYPOLICY
    EJOBDELETED
    EREDIS
    );

my $server = DEFAULT_SERVER.":".DEFAULT_PORT;   # the Redis Server

sub exception {
    my $jq  = shift;
    my $err = shift;

    if ( $jq->last_errorcode == ENOERROR )
    {
        # For example, to ignore
        return unless $err;
    }
    elsif ( $jq->last_errorcode == EMISMATCHARG )
    {
        # Necessary to correct the code
    }
    elsif ( $jq->last_errorcode == EDATATOOLARGE )
    {
        # You must use the control data length
    }
    elsif ( $jq->last_errorcode == ENETWORK )
    {
        # For example, sleep
        #sleep 60;
        # and return code to repeat the operation
        #return "to repeat";
    }
    elsif ( $jq->last_errorcode == EMAXMEMORYLIMIT )
    {
        # For example, return code to restart the server
        #return "to restart the redis server";
    }
    elsif ( $jq->last_errorcode == EMAXMEMORYPOLICY )
    {
        # For example, return code to recreate the job
        my $id = $err =~ /^(\S+)/;
        #return "to recreate $id";
    }
    elsif ( $jq->last_errorcode == EJOBDELETED )
    {
        # For example, return code to ignore
        my $id = $err =~ /^(\S+)/;
        #return "to ignore $id";
    }
    elsif ( $jq->last_errorcode == EREDIS )
    {
        # Independently analyze the $jq->last_error
    }
    else
    {
        # Unknown error code
    }
    die $err if $err;
}

my $jq;

eval {
    $jq = Redis::JobQueue->new(
        redis   => $server,
        timeout => 1,   # DEFAULT_TIMEOUT = 0 for an unlimited timeout
        );
};
exception( $jq, $@ ) if $@;

#-- Producer -------------------------------------------------------------------

#-- Adding new job

my $job;
eval {
    $job = $jq->add_job(
        {
            queue       => 'xxx',
            job         => 'yyy',
            workload    => \'Some stuff up to 512MB long',
            expire      => 12*60*60,
        } );
};
exception( $jq, $@ ) if $@;
print "Added job ", $job->id, "\n" if $job;

#-- Worker ---------------------------------------------------------------------

#-- Run your jobs

sub yyy {
    my $job = shift;

    my $workload = ${$job->workload};
    # do something with workload;
    print "YYY workload: $workload\n";

    $job->result( 'YYY JOB result comes here, up to 512MB long' );
}

sub zzz {
    my $job = shift;

    my $workload = ${$job->workload};
    # do something with workload;
    print "ZZZ workload: $workload\n";

    $job->result( \'ZZZ JOB result comes here, up to 512MB long' );
}

eval {
    while ( my $job = $jq->get_next_job(
        queue       => 'xxx',
        job         => [ 'yyy','zzz' ],
        blocking    => 1
        ) )
    {
        my $id = $job->id;

        my $status = $jq->check_job_status( $id );
        print "Job '", $id, "' was '$status' status\n";

        $job->status( STATUS_WORKING );
        $jq->update_job( $job );

        $status = $jq->check_job_status( $id );
        print "Job '", $id, "' has new '$status' status\n";

        # do my stuff
        if ( $job->job eq 'yyy' )
        {
            yyy( $job );
        }
        elsif ( $job->job eq 'zzz' )
        {
            zzz( $job );
        }

        $job->status( STATUS_COMPLETED );
        $jq->update_job( $job );

        $status = $jq->check_job_status( $id );
        print "Job '", $id, "' has last '$status' status\n";
    }
};
exception( $jq, $@ ) if $@;

#-- Consumer -------------------------------------------------------------------

#-- Check the job status

eval {
    # For example:
    # my $status = $jq->check_job_status( $ARGV[0] );
    # or:
    my @jobs = $jq->get_jobs;

    foreach my $id ( @jobs )
    {
        my $status = $jq->check_job_status( $id );
        print "Job '$id' has '$status' status\n";
    }
};
exception( $jq, $@ ) if $@;

#-- Fetching the result

eval {
    # For example:
    # my $id = $ARGV[0];
    # or:
    my @jobs = $jq->get_jobs;

    foreach my $id ( @jobs )
    {
        my $status = $jq->check_job_status( $id );
        print "Job '$id' has '$status' status\n";

        if ( $status eq STATUS_COMPLETED )
        {
            my $job = $jq->load_job( $id );

            # now safe to compelete it from JobQueue, since it's completed
            $jq->delete_job( $id );

            print "Job result: ", ${$job->result}, "\n";
        }
        else
        {
            print "Job is not complete, has current '$status' status\n";
        }
    }
};
exception( $jq, $@ ) if $@;

#-- Closes and cleans up -------------------------------------------------------

eval { $jq->quit };
exception( $jq, $@ ) if $@;

exit;

__END__