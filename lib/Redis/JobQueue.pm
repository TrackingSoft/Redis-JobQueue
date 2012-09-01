package Redis::JobQueue;
use 5.010;

# Pragmas
use strict;
use warnings;
use bytes;

our $VERSION = '0.06';

use Exporter qw( import );
our @EXPORT_OK  = qw(
    DEFAULT_SERVER
    DEFAULT_PORT
    DEFAULT_TIMEOUT
    NAMESPACE

    STATUS_CREATED
    STATUS_WORKING
    STATUS_COMPLETED
    STATUS_DELETED

    ENOERROR
    EMISMATCHARG
    EDATATOOLARGE
    ENETWORK
    EMAXMEMORYLIMIT
    EMAXMEMORYPOLICY
    EJOBDELETED
    EREDIS
    );

#-- load the modules -----------------------------------------------------------

# Modules
use Mouse;                                      # automatically turns on strict and warnings
use Mouse::Util::TypeConstraints;
use Carp;
use List::Util      qw( min );
use Redis;
use Data::UUID;
use Params::Util    qw( _STRING );
use Redis::JobQueue::Job;

#-- declarations ---------------------------------------------------------------

use constant {
    DEFAULT_SERVER      => 'localhost',
    DEFAULT_PORT        => 6379,
    DEFAULT_TIMEOUT     => 0,                   # 0 for an unlimited timeout

    NAMESPACE           => 'JobQueue',
    EXPIRE_DELETED      => 24*60*60,            # day

    STATUS_CREATED      => '_created_',
    STATUS_WORKING      => 'working',
    STATUS_COMPLETED    => 'completed',
    STATUS_DELETED      => '_deleted_',

    ENOERROR            => 0,
    EMISMATCHARG        => 1,
    EDATATOOLARGE       => 2,
    ENETWORK            => 3,
    EMAXMEMORYLIMIT     => 4,
    EMAXMEMORYPOLICY    => 5,
    EJOBDELETED         => 6,
    EREDIS              => 7,
    };

my %ERROR = (
#    ENOERROR            => 'No error',
    EMISMATCHARG        => 'Mismatch argument',
    EDATATOOLARGE       => 'Data is too large',
#    ENETWORK            => 'Error in connection to Redis server',
#    EMAXMEMORYLIMIT     => "Command not allowed when used memory > 'maxmemory'",
    EMAXMEMORYPOLICY    => 'job was removed by maxmemory-policy',
    EJOBDELETED         => 'job was removed prior to use',
#    EREDIS              => 'Redis error message',
    );

my @job_fields = Redis::JobQueue::Job->job_attributes;
my $uuid = new Data::UUID;

#-- constructor ----------------------------------------------------------------

around BUILDARGS => sub {
    my $orig  = shift;
    my $class = shift;

    if ( eval { $_[0]->isa( 'Redis' ) } )
    {
        my $redis = shift;
        return $class->$orig(
# have to look into the Redis object ...
            redis   => $redis->{server},
# it is impossible to know from Redis now ...
#            timeout => $redis->???,
            _redis  => $redis,
            @_
            );
    }
    elsif ( eval { $_[0]->isa( 'Test::RedisServer' ) } )
    {
# to test only
        my $redis = shift;
        return $class->$orig(
# have to look into the Test::RedisServer object ...
            redis   => '127.0.0.1:'.$redis->conf->{port},
# Test::RedisServer does not use timeout = 0
#            timeout => $redis->timeout,
            @_
            );
    }
    elsif ( eval { $_[0]->isa( __PACKAGE__ ) } )
    {
        my $jq = shift;
        return $class->$orig(
                    redis   => $jq->_server,
                    _redis  => $jq->_redis,
                    timeout => $jq->timeout,
                    @_
                );
    }
    else
    {
        return $class->$orig( @_ );
    }
};

sub BUILD {
    my $self = shift;

    $self->_redis( $self->_redis_constructor )
        unless ( $self->_redis );
    my ( undef, $max_datasize ) = $self->_call_redis( 'CONFIG', 'GET', 'maxmemory' );
    $self->max_datasize( min $max_datasize, $self->max_datasize )
        if $max_datasize;
}

#-- public attributes ----------------------------------------------------------

has 'timeout'           => (
    is          => 'rw',
    isa         => 'Redis::JobQueue::Job::NonNegInt',
    default     => DEFAULT_TIMEOUT,
    );

has 'max_datasize'      => (
    is          => 'rw',
    isa         => 'Redis::JobQueue::Job::NonNegInt',
    default     => Redis::JobQueue::Job::MAX_DATASIZE,
    );

has 'last_errorcode'    => (
    reader      => 'last_errorcode',
    writer      => '_set_last_errorcode',
    isa         => 'Int',
    default     => 0,
    );

#-- private attributes ---------------------------------------------------------

has '_server'           => (
    is          => 'rw',
    init_arg    => 'redis',
    isa         => 'Str',
    default     => DEFAULT_SERVER.':'.DEFAULT_PORT,
    trigger     => sub {
                        my $self = shift;
                        $self->_server( $self->_server.':'.DEFAULT_PORT )
                            unless $self->_server =~ /:/;
                    },
    );

has '_redis'            => (
    is          => 'rw',
# 'Maybe[Test::RedisServer]' to test only
    isa         => 'Maybe[Redis] | Maybe[Test::RedisServer]',
    default     => undef,
    );

has '_transaction'      => (
    is          => 'rw',
    isa         => 'Bool',
    default     => undef,
    );

#-- public methods -------------------------------------------------------------

sub add_job {
    my $self        = shift;

    $self->_set_last_errorcode( ENOERROR );
    ref( $_[0] ) eq 'HASH'
        or eval { $_[0]->isa( 'Redis::JobQueue::Job' ) }
        or ( $self->_set_last_errorcode( EMISMATCHARG ), confess $ERROR{EMISMATCHARG} )
        ;
    my $job = Redis::JobQueue::Job->new( shift );

    my $to_left;
    my %args = ( @_ );
    foreach my $k ( keys %args )
    {
        ++$to_left
            if ( $k =~ /^LPUSH$/i and $args{ $k } );
    }

    my $id;
    do
    {
        $id = $uuid->create_str;
    } while ( $self->_call_redis( 'EXISTS', NAMESPACE.':'.$id ) );
    $job->id( $id );
    $job->status( STATUS_CREATED );

# transaction start
    my $key = NAMESPACE.':'.$id;
    my $expire = $job->expire;
    $self->_call_redis( 'MULTI' );
    foreach my $field ( @job_fields )
    {
        $self->_call_redis( 'HSET', $key, $field, $job->$field // '' )
            if $job->$field ne 'id';
    }
    $self->_call_redis( 'EXPIRE', $key, $expire )
        if $expire;

    $key = NAMESPACE.':queue:'.$job->queue.':'.$job->job;
# Warning: change '$id'
    $id .= ' '.( time + $expire )
        if $expire;
    $self->_call_redis( $to_left ? 'LPUSH' : 'RPUSH', $key, $id );

# transaction end
    $self->_call_redis( 'EXEC' );

    $job->clear_variability( $job->job_attributes );
    return $job;
}

sub check_job_status {
    my $self        = shift;
    my $arg         = shift;

    defined( _STRING( $arg ) )
        or eval { $arg->isa( 'Redis::JobQueue::Job' ) }
        or ( $self->_set_last_errorcode( EMISMATCHARG ), confess $ERROR{EMISMATCHARG} )
        ;

    my $key = NAMESPACE.':'.( ref( $arg )   ? $arg->id
                                            : $arg );
    return $self->_call_redis( 'HGET', $key, 'status' );
}

sub load_job {
    my $self        = shift;
    my $arg         = shift;

    defined( _STRING( $arg ) )
        or eval { $arg->isa( 'Redis::JobQueue::Job' ) }
        or ( $self->_set_last_errorcode( EMISMATCHARG ), confess $ERROR{EMISMATCHARG} )
        ;

    my $id = ref( $arg )    ? $arg->id
                            : $arg;
    my $key = NAMESPACE.':'.$id;
    return
        unless $self->_call_redis( 'EXISTS', $key );

    my $pre_job = { id => $id };
    foreach my $field ( @job_fields )
    {
        next
            if $field eq 'id';
        $pre_job->{ $field } = $field =~ /workload|result/  ? $self->_call_redis( 'Give back references', 'HGET', $key, $field )
                                                            : $self->_call_redis( 'HGET', $key, $field );
    }
    my $new_job = Redis::JobQueue::Job->new( $pre_job );
    $new_job->clear_variability( @job_fields );
    return $new_job;
}

sub get_next_job {
    my $self        = shift;

    !( scalar( @_ ) % 2 )
        or ( $self->_set_last_errorcode( EMISMATCHARG ), confess $ERROR{EMISMATCHARG} )
        ;
    my %args = ( @_ );
    my $queue       = $args{queue};
    my $jobs        = $args{job};
    my $blocking    = $args{blocking};

    $jobs = [ $jobs ]
        if ( !ref( $jobs ) );
    scalar @{$jobs}
        or ( $self->_set_last_errorcode( EMISMATCHARG ), confess $ERROR{EMISMATCHARG} )
        ;

    foreach my $arg ( ( $queue, @{$jobs} ) )
    {
        defined _STRING( $arg )
            or ( $self->_set_last_errorcode( EMISMATCHARG ), confess $ERROR{EMISMATCHARG} )
            ;
    }

    my $base_name = NAMESPACE.':queue:'.$queue;
    my @keys = map { "$base_name:$_" } @$jobs;

    if ( $blocking )
    {
        foreach my $key ( @keys )
        {
# 'BLPOP' waiting time of a given $self->timeout parameter
            my @cmd = ( 'BLPOP', $key, $self->timeout );
            while (1)
            {
                my ( undef, $full_id ) = $self->_call_redis( @cmd );
# if the job is no longer
                last
                    unless $full_id;

                my $job = $self->_get_next_job( $full_id );
                return $job
                    if $job;
            }
        }
    }
    else
    {
# 'LPOP' takes only one queue name at ones
        foreach my $key ( @keys )
        {
            next unless $self->_call_redis( 'EXISTS', $key );
            my @cmd = ( 'LPOP', $key );
            while (1)
            {
                my $full_id = $self->_call_redis( @cmd );
# if the job is no longer
                last
                    unless $full_id;

                my $job = $self->_get_next_job( $full_id );
                return $job
                    if $job;
            }
        }
    }
    return;
}

sub _get_next_job {
    my $self        = shift;
    my $full_id     = shift;

    my ( $id, $expire_time ) = split ' ', $full_id;
    my $key = NAMESPACE.':'.$id;
    if ( $self->_call_redis( 'EXISTS', $key ) )
    {
        my $status = $self->_call_redis( 'HGET', $key, 'status' );
        if ( $status eq STATUS_DELETED )
        {
            $self->_set_last_errorcode( EJOBDELETED );
            confess $id.' '.$ERROR{EJOBDELETED};
        }
        return $self->_reexpire_load_job( $id );
    }
    else
    {
        if ( !$expire_time
            or time < $expire_time
            )
        {
            $self->_set_last_errorcode( EMAXMEMORYPOLICY );
            confess $id.' '.$ERROR{EMAXMEMORYPOLICY};
        }
# If the queue contains the job identifier has already been removed due
# to expiration of the 'expire' time, the cycle will ensure the transition
# to the next job ID selection
        return;
    }
}

sub update_job {
    my $self        = shift;
    my $job         = shift;

    eval { $job->isa( 'Redis::JobQueue::Job' ) }
        or ( $self->_set_last_errorcode( EMISMATCHARG ), confess $ERROR{EMISMATCHARG} )
        ;

    my $id = $job->id;
    my $key = NAMESPACE.':'.$id;
    return
        unless $self->_call_redis( 'EXISTS', $key );

    my $status = $self->_call_redis( 'HGET', $key, 'status' );
    if ( $status eq STATUS_DELETED )
    {
        $self->_set_last_errorcode( EJOBDELETED );
        confess $id.' '.$ERROR{EJOBDELETED};
    }

    if ( my $expire = $job->expire )
    {
        $self->_call_redis( 'EXPIRE', $key, $expire );
    }
    else
    {
        $self->_call_redis( 'PERSIST', $key );
    }

# transaction start
    $self->_call_redis( 'MULTI' );
    my $updated = 0;
    foreach my $field ( $job->modified_attributes )
    {
        if ( $field !~ /expire|id/ )
        {
            $self->_call_redis( 'HSET', $key, $field, $job->$field // '' );
            ++$updated;
        }
    }
# transaction end
    $self->_call_redis( 'EXEC' );
    $job->clear_variability( @job_fields );

    return $updated;
}

sub delete_job {
    my $self    = shift;
    my $arg     = shift;

    defined( _STRING( $arg ) )
        or eval { $arg->isa( 'Redis::JobQueue::Job' ) }
        or ( $self->_set_last_errorcode( EMISMATCHARG ), confess $ERROR{EMISMATCHARG} )
        ;

    my $key = NAMESPACE.':'.( ref( $arg ) ? $arg->id : $arg );
    return
        unless $self->_call_redis( 'EXISTS', $key );

# transaction start
    my $expire = $self->_call_redis( 'HGET', $key, 'expire' );
    $self->_call_redis( 'MULTI' );
    foreach my $field ( grep { $_ !~ /status|id/ } @job_fields )
    {
# release the memory
        $self->_call_redis( 'HDEL', $key, $field );
    }
    $self->_call_redis( 'HSET', $key, 'status', STATUS_DELETED );
    $self->_call_redis( 'EXPIRE', $key, EXPIRE_DELETED )
        unless ( $expire );
# transaction end
    $self->_call_redis( 'EXEC' );

    return 1;
}

sub get_jobs {
    my $self        = shift;

    my $key = NAMESPACE.':*';
#    my $re = '^'.NAMESPACE.':([0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12})$';
    my $re = '^'.NAMESPACE.':([^:]+)$';
    return map { /$re/ } $self->_call_redis( 'KEYS', $key );
}

sub quit {
    my $self        = shift;

    $self->_set_last_errorcode( ENOERROR );
    eval { $self->_redis->quit };
    $self->_redis_exception( $@ )
        if $@;
}

#-- private methods ------------------------------------------------------------

sub _redis_exception {
    my $self    = shift;
    my $error   = shift;

# Use the error messages from Redis.pm
    if (
           $error =~ /^Could not connect to Redis server at /
        or $error =~ /^Can't close socket: /
        or $error =~ /^Not connected to any server/
# Maybe for pub/sub only
        or $error =~ /^Error while reading from Redis server: /
        or $error =~ /^Redis server closed connection/
        )
    {
        $self->_set_last_errorcode( ENETWORK );
    }
    elsif (
           $error =~ /[\S+] ERR command not allowed when used memory > 'maxmemory'/
        or $error =~ /[\S+] OOM command not allowed when used memory > 'maxmemory'/
        )
    {
        $self->_set_last_errorcode( EMAXMEMORYLIMIT );
    }
    else
    {
        $self->_set_last_errorcode( EREDIS );
    }

    if ( $self->_transaction )
    {
        eval { $self->_redis->discard };
        $self->_transaction( 0 );
    }
    die $error;
}

sub _redis_constructor {
    my $self    = shift;

    $self->_set_last_errorcode( ENOERROR );
    my $redis;
    eval { $redis = Redis->new( server => $self->_server ) };
    $self->_redis_exception( $@ )
        if $@;
    return $redis;
}

# Keep in mind the default 'redis.conf' values:
# Close the connection after a client is idle for N seconds (0 to disable)
#    timeout 300

# Send a request to Redis
sub _call_redis {
    my $self        = shift;

# first argument is a little magic
    my $need_ref;
    if ( $_[0] eq 'Give back references' )
    {
        $need_ref = 1;
        shift;
    }

    my $method      = shift;

    if ( $method eq 'HSET'
        and bytes::length( ref( $_[2] ) ? ${$_[2]} : $_[2] ) > $self->max_datasize
        )
    {
        if ( $self->_transaction )
        {
            eval { $self->_redis->discard };
            $self->_transaction( 0 );
        }
        $self->_set_last_errorcode( EDATATOOLARGE );
# 'die' as maybe too long to analyze the data output from the 'confess'
        die $ERROR{EDATATOOLARGE}.': '.$_[1];
    }

    my @return;
    $self->_set_last_errorcode( ENOERROR );
    @return = eval {
                return $self->_redis->$method( map { ref( $_ )  ? $$_
                                                                : $_
                                                    } @_ );
                    };
    $self->_redis_exception( $@ )
        if $@;

    $self->_transaction( 1 )
        if $method eq "MULTI";
    $self->_transaction( 0 )
        if $method eq "EXEC";

    if ( $need_ref )
    {
        return wantarray ? \( @return ) : \$return[0];
    }
    else
    {
        return wantarray ? @return : $return[0];
    }
}

# Jobs data are loaded and for a specified job updates the value of the EXPIRE
sub _reexpire_load_job {
    my $self        = shift;
    my $id          = shift;

    if ( $self->_call_redis( 'EXISTS', NAMESPACE.':'.$id ) )
    {
        my $job = $self->load_job( $id );
        $self->_call_redis( 'EXPIRE', NAMESPACE.':'.$id, $job->expire )
            if $job->expire;
        return $job;
    }
}

#-- Closes and cleans up -------------------------------------------------------

no Mouse::Util::TypeConstraints;
no Mouse;                                       # keywords are removed from the package
__PACKAGE__->meta->make_immutable();

__END__

=head1 NAME

Redis::JobQueue - Object interface for the creation, execution the job queues,
as well as the status and outcome objectives

=head1 VERSION

This documentation refers to C<Redis::JobQueue> version 0.06

=head1 SYNOPSIS

    #-- Common
    use Redis::JobQueue qw( DEFAULT_SERVER DEFAULT_PORT );

    my $connection_string = DEFAULT_SERVER.':'.DEFAULT_PORT;
    my $jq = Redis::JobQueue->new( redis => $connection_string );

    #-- Producer
    my $job = $jq->add_job(
        {
            queue       => 'xxx',
            job         => 'yyy',
            workload    => \'Some stuff up to 512MB long',
            expire      => 12*60*60,            # 12h,
        }
        );

    #-- Worker
    sub yyy {
        my $job = shift;

        my $workload = ${$job->workload};
        # do something with workload;

        $job->result( 'YYY JOB result comes here, up to 512MB long' );
    }

    while ( $job = $jq->get_next_job(
        queue       => 'xxx',
        job         => 'yyy',
        blocking    => 1,
        ) )
    {
        $job->status( 'working' );
        $jq->update_job( $job );

        # do my stuff
        if ( $job->job eq 'yyy' )
        {
            yyy( $job );
        }

        $job->status( 'completed' );
        $jq->update_job( $job );
    }

    #-- Consumer
    my $id = $ARGV[0];
    my $status = $jq->check_job_status( $id );

    if ( $status eq 'completed' )
    {
        # now safe it from JobQueue, since it's completed
        my $job = $jq->load_job( $id );

        $jq->delete_job( $id );
        print "Job result: ", ${$job->result}, "\n";
    }
    else
    {
        print "Job is not complete, has current '$status' status\n";
    }

To see a brief but working code example of the C<Redis::JobQueue>
package usage look at the L</"An Example"> section.

To see a description of the used C<Redis::JobQueue> data
structure (on Redis server) look at the L</"JobQueue data structure"> section.

=head1 ABSTRACT

The C<Redis::JobQueue> package is a set of Perl modules which
provides a simple job queue with Redis server capabilities.

=head1 DESCRIPTION

The user modules in this package provide an object oriented API.
The job queues interface and the jobs are all represented by objects.
This makes a simple and powerful interface to these services.

The main features of the package are:

=over 3

=item *

Contains various reusable components that can be used separately or together.

=item *

Provides an object oriented model of communication.

=item *

Support the work with data structures on the Redis server.

=item *

Supports the automatic creation of job queue, job status monitoring,
updating the job data set, obtaining a consistent job from the queue,
remove job, the classification of possible errors.

=item *

Simple methods for organizing producer, worker and consumer clients.

=back

=head2 CONSTRUCTOR

=head3 C<new( redis =E<gt> $server, timeout =E<gt> $timeout )>

It generates a C<Redis::JobQueue> object to communicate with
the Redis server and can be called as either a class method or an object method.
If invoked with no arguments the constructor C<new> creates and returns
a C<Redis::JobQueue> object that is configured
to work with the default settings.

If invoked with the first argument being an object of C<Redis::JobQueue>
class or L<Redis|Redis> class, then the new object attribute values are taken from
the object of the first argument. It does not create a new connection to
the Redis server.
A created object uses the default value L</DEFAULT_TIMEOUT> when
a L<Redis|Redis> class object is passed to the C<new> constructor,
as L<Redis|Redis> class does not support the timeout attribute while waiting for
a message from the queue.

C<new> optionally takes arguments. These arguments are in key-value pairs.

This example illustrates a C<new()> call with all the valid arguments:

    my $jq = Redis::JobQueue->new(
        redis   => "$server:$port", # Default Redis local server and port
        timeout => $timeout,        # Maximum wait time (in seconds)
                                    # you receive a message from the queue
        );

The following examples illustrate other uses of the C<new> method:

    $jq = Redis::JobQueue->new();
    my $next_jq = Redis::JobQueue->new( $jq );

    my $redis = Redis->new( redis => "$server:$port" );
    $next_jq = Redis::JobQueue->new(
        $redis,
        timeout => $timeout,
        );

An error will cause the program to halt (C<confess>) if an argument is not valid.

=head2 METHODS

An error will cause the program to halt (C<confess>) if an argument is not valid.

ATTENTION: In the L<Redis|Redis> module the synchronous commands throw an
exception on receipt of an error reply, or return a non-error reply directly.

=head3 C<add_job( $pre_job, LPUSH =E<gt> 1 )>

Adds a job to the queue on the Redis server. At the same time creates and
returns a new L<Redis::JobQueue::Job|Redis::JobQueue::Job> object with a new
unique identifier. Job status is set to L</STATUS_CREATED>.

The first argument should be an L<Redis::JobQueue::Job|Redis::JobQueue::Job>
object or a reference to a hash describing L<Redis::JobQueue::Job|Redis::JobQueue::Job>
object attributes.

C<add_job> optionally takes arguments. These arguments are in key-value pairs.

This example illustrates a C<add_job()> call with all the valid arguments:

    my $pre_job = {
        id           => '4BE19672-C503-11E1-BF34-28791473A258',
        queue        => 'lovely_queue',
        job          => 'strong_job',
        expire       => 12*60*60,
        status       => 'created',
        workload     => \'Some stuff up to 512MB long',
        result       => \'JOB result comes here, up to 512MB long',
        };

    my $job = Redis::JobQueue::Job->new(
        id           => $pre_job->{id},
        queue        => $pre_job->{queue},
        job          => $pre_job->{job},
        expire       => $pre_job->{expire},
        status       => $pre_job->{status},
        workload     => $pre_job->{workload},
        result       => $pre_job->{result},
        );

    my $resulting_job = $jq->add_job( $job );
    # or
    $resulting_job = $jq->add_job(
        $pre_job,
        LPUSH       => 1,
        );

If used with a C<LPUSH> optional argument, the job is placed in front of
the queue and not in its end (if the argument is true).

TTL job data sets on the Redis server in accordance with the C<expire>
attribute of the L<Redis::JobQueue::Job|Redis::JobQueue::Job> object.

Method returns the object corresponding to the added job.

=head3 C<check_job_status( $job )>

Status of the job is requested from the Redis server. Job ID is obtained from
the argument. The argument can be either a string or
a L<Redis::JobQueue::Job|Redis::JobQueue::Job> object.

Returns the status string or C<undef> when the job data does not exist.
Returns C<undef> if the job is not on the Redis server.

The following examples illustrate uses of the C<check_job_status> method:

    my $status = $jq->check_job_status( $id );
    # or
    $status = $jq->check_job_status( $job );

=head3 C<load_job( $job )>

Jobs data are loaded from the Redis server. Job ID is obtained from
the argument. The argument can be either a string or
a L<Redis::JobQueue::Job|Redis::JobQueue::Job> object.

Method returns the object corresponding to the loaded job.
Returns C<undef> if the job is not on the Redis server.

The following examples illustrate uses of the C<check_job_status> method:

    $job = $jq->load_job( $id );
    # or
    $job = $jq->load_job( $job );

=head3 C<get_next_job( queue =E<gt> $queue_name, job =E<gt> $job, blocking =E<gt> 1 )>

Selects the first job identifier in the queue for the specified jobs.

C<get_next_job> takes arguments in key-value pairs.
You can specify a job name or a reference to an array of names of jobs.
If the optional C<blocking> argument is true, then the C<get_next_job> method
waits for a maximum period of time specified in the C<timeout> attribute of
the queue object. By default, the result is returned immediately.

Please keep in mind that each job has a separate queue. Job identifiers will be
selected only from the corresponding queue of jobs. The queue that
corresponds to the order of jobs is set in the C<job> argument.

Method returns the job object corresponding to the received job identifier.
Returns the C<undef> if there is no job in the queue.

These examples illustrates a C<get_next_job> call with all the valid arguments:

    $job = $jq->get_next_job(
        queue       => 'xxx',
        job         => 'yyy',
        blocking    => 1,
        );
    # or
    $job = $jq->get_next_job(
        queue       => 'xxx',
        job         => [ 'yyy', 'zzz' ],
        blocking    => 1,
        );

TTL job data for the job resets on the Redis server in accordance with
the C<expire> attribute of the job object.

=head3 C<update_job( $job )>

Saves the changes to the job data to the Redis server. Job ID is obtained from
the argument. The argument can be a L<Redis::JobQueue::Job|Redis::JobQueue::Job>
object.

Returns C<undef> if the job is not on the Redis server and the number of
the attributes that were updated in the opposite case.

Changing the C<expire> attribute is ignored.

The following examples illustrate uses of the C<update_job> method:

    $jq->update_job( $job );

TTL job data for the job resets on the Redis server in accordance with
the C<expire> attribute of the job object.

=head3 C<delete_job( $job )>

Deletes the job data in Redis server. Job ID is obtained from
the argument. The argument can be either a string or
a L<Redis::JobQueue::Job|Redis::JobQueue::Job> object.

Returns C<undef> if the job is not on the Redis server and true in the opposite
case.

The following examples illustrate uses of the C<delete_job> method:

    $jq->delete_job( $job );
    # or
    $jq->delete_job( $id );

Use this method immediately after receiving the results of the job for
the early release of memory on the Redis server.

When the job is deleted, the data set on the Redis server are changed as follows:

=over 3

=item *

All fields are removed (except C<status>).

=item *

C<status> field is set to C<STATUS_DELETED>.

=item *

For a hash of the data set TTL = 24h, if the job was C<expire> = 0.

=item *

Hash of the job data is automatically deleted in accordance with the established
value of TTL (C<expire>).

=back

To see a description of the used C<Redis::JobQueue> data
structure (on Redis server) look at the L</"JobQueue data structure"> section.

=head3 C<get_jobs>

Gets a list of job ids on the Redis server.

The following examples illustrate uses of the C<get_jobs> method:

    @jobs = $jq->get_jobs;

=head3 C<quit>

Ask the Redis server to close the connection.

The following examples illustrate uses of the C<quit> method:

    $jq->quit;

=head3 C<timeout>

The method of access to the C<timeout> attribute.

The method returns the current value of the attribute if called without arguments.

Non-negative integer value can be used to specify a new value of the maximum
waiting time data from the queue (in the L</get_next_job> method).

=head3 C<max_datasize>

The method of access to the C<max_datasize> attribute.

The method returns the current value of the attribute if called without arguments.

Non-negative integer value can be used to specify a new value to the maximum
size of data in the attributes of a
L<Redis::JobQueue::Job|Redis::JobQueue::Job> object.

The C<max_datasize> attribute value is used in the L<constructor|/CONSTRUCTOR>
and operations data entry jobs on the Redis server.

The L<constructor|/CONSTRUCTOR> uses the smaller of the values of 512MB and
C<maxmemory> limit from a C<redis.conf> file.

=head3 C<last_errorcode>

The method of access to the code of the last identified errors.

To see more description of the identified errors look at the L</DIAGNOSTICS>
section.

=head2 EXPORT

None by default.

Additional constants are available for import, which can be used
to define some type of parameters.

These are the defaults:

=over

=item C<DEFAULT_SERVER>

Default Redis local server - C<'localhost'>.

=item C<DEFAULT_PORT>

Default Redis server port - 6379.

=item C<DEFAULT_TIMEOUT>

Maximum wait time (in seconds) you receive a message from the queue -
0 for an unlimited timeout.

=item C<NAMESPACE>

Namespace name used keys on the Redis server - C<'JobQueue'>.

=item C<STATUS_CREATED>

Text of the status of the job after it is created - C<'_created_'>.

=item C<STATUS_WORKING>

Text of the status of the job at run-time - C<'working'>.
Must be set from the worker function.

=item C<STATUS_COMPLETED>

Text of the status of the job at the conclusion - C<'completed'>.
Must be set at the conclusion of the worker function.

=item C<STATUS_DELETED>

Text of the status of the job after removal - C<'_deleted_'>.

=item Error codes are identified

To see more description of the identified errors look at the L</DIAGNOSTICS>
section.

=back

These are the defaults:

=over

=item C<Redis::JobQueue::EXPIRE_DELETED>

TTL (24h) for a hash of the deleted data set, if the job was C<expire> = 0.

=back

=head2 DIAGNOSTICS

The method for the possible error to analyse: L</last_errorcode>.

A L<Redis|Redis> error will cause the program to halt (C<confess>).
In addition to errors in the L<Redis|Redis> module detected errors
L</EMISMATCHARG>, L</EDATATOOLARGE>, L</EMAXMEMORYPOLICY>, L</EJOBDELETED>.
All recognizable errors in C<Redis::JobQueue> lead to
the installation of the corresponding value in the L</last_errorcode> and cause
an exception (C<confess>).
Unidentified errors cause an exception (L</last_errorcode> remains equal to 0).
The initial value of C<$@> is preserved.

The user has the choice:

=over 3

=item *

Use the package methods and independently analyze the situation without the use
of L</last_errorcode>.

=item *

Piece of code wrapped in C<eval {...};> and analyze L</last_errorcode>
(look at the L</"An Example"> section).

=back

In L</last_errortsode> recognizes the following:

=over 3

=item C<ENOERROR>

No error.

=item C<EMISMATCHARG>

This means that you didn't give the right argument to a C<new>
or to other L<method|/METHODS>.

=item C<EDATATOOLARGE>

This means that the data is too large.

=item C<ENETWORK>

This means that an error in connection to Redis server was detected.

=item C<EMAXMEMORYLIMIT>

This means that the command not allowed when used memory > C<maxmemory>.

=item C<EMAXMEMORYPOLICY>

This means that the job was removed by C<maxmemory-policy>.

=item C<EJOBDELETED>

This means that the job was removed prior to use.

=item C<EREDIS>

This means that other Redis error message detected.

=back

=head2 An Example

The example shows a possible treatment for possible errors.

    #-- Common ---------------------------------------------------------------
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

    my $server = DEFAULT_SERVER.':'.DEFAULT_PORT;   # the Redis Server

    # A possible treatment for possible errors
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
            # Independently analyze the $err
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

    #-- Producer -------------------------------------------------------------
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
    print 'Added job ', $job->id, "\n" if $job;

    #-- Worker ---------------------------------------------------------------
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
            blocking    => 1,
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

    #-- Consumer -------------------------------------------------------------
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

                print 'Job result: ', ${$job->result}, "\n";
            }
            else
            {
                print "Job is not complete, has current '$status' status\n";
            }
        }
    };
    exception( $jq, $@ ) if $@;

    #-- Closes and cleans up -------------------------------------------------

    eval { $jq->quit };
    exception( $jq, $@ ) if $@;

=head2 JobQueue data structure

Using the currently selected database (default = 0).

While working on the Redis server creates and uses these data structures:

    #-- To store the job data:
    # HASH    Namespace:id

For example:

    $ redis-cli
    redis 127.0.0.1:6379> KEYS JobQueue:*
    1) "JobQueue:478B9C84-C5B8-11E1-A2C5-D35E0A986783"
    2) "JobQueue:478C81B2-C5B8-11E1-B5B1-16670A986783"
    3) "JobQueue:89116152-C5BD-11E1-931B-0A690A986783"
    #      |                 |
    #   Namespace            |
    #                     Job id (UUID)
    ...
    redis 127.0.0.1:6379> hgetall JobQueue:478B9C84-C5B8-11E1-A2C5-D35E0A986783
    1) "queue"                                  # hash key
    2) "xxx"                                    # the key value
    3) "job"                                    # hash key
    4) "yyy"                                    # the key value
    5) "workload"                               # hash key
    6) "Some stuff up to 512MB long"            # the key value
    7) "expire"                                 # hash key
    8) "43200"                                  # the key value
    9) "status"                                 # hash key
    10) "_created_"                             # the key value

After you create (L</add_job> method) or modify (L</update_job> method)
the data set are within the time specified C<expire> attribute (seconds).
For example:

    redis 127.0.0.1:6379> TTL JobQueue:478B9C84-C5B8-11E1-A2C5-D35E0A986783
    (integer) 42062

Hash of the job data is deleted when you delete the job (L</delete_job> method).

    # -- To store the job queue (the list created but not yet requested jobs):
    # LIST    JobQueue:queue:queue_name:job_name

For example:

    redis 127.0.0.1:6379> KEYS JobQueue:*
    ...
    4) "JobQueue:queue:xxx:yyy"
    5) "JobQueue:queue:xxx:zzz"
    #      |       |    |   |
    #   Namespace  |    |   |
    #    Fixed key word |   |
    #            Queue name |
    #                   Job name
    ...
    redis 127.0.0.1:6379> LRANGE JobQueue:queue:xxx:yyy 0 -1
    1) "478B9C84-C5B8-11E1-A2C5-D35E0A986783 1344070066"
    2) "89116152-C5BD-11E1-931B-0A690A986783 1344070067"
    #                        |                   |
    #                     Job id (UUID)          |
    #                                      Expire time (UTC)
    ...

Please keep in mind that each job has a separate queue.

Job queue will be created automatically when the data arrives to contain.
Job queue will be deleted automatically in the exhaustion of its contents.

=head1 DEPENDENCIES

In order to install and use this package is desirable to use a Perl version
5.010 or later. Some modules within this package depend on other
packages that are distributed separately from Perl. We recommend that
you have the following packages installed before you install C<Redis::JobQueue>
package:

    Data::UUID
    Mouse
    Params::Util
    Redis

C<Redis::JobQueue> package has the following optional dependencies:

    Test::Distribution
    Test::Exception
    Test::Kwalitee
    Test::Perl::Critic
    Test::Pod
    Test::Pod::Coverage
    Test::RedisServer
    Test::TCP

If the optional modules are missing, some "prereq" tests are skipped.

=head1 BUGS AND LIMITATIONS

The use of C<maxmemory-police all*> in the C<redis.conf> file could lead to
a serious (but hard to detect) problem as Redis server may delete
the job queue lists.

We strongly recommend using the option C<maxmemory> in the C<redis.conf> file if
the data set may be large.

The C<Redis::JobQueue> package was written, tested, and found working on recent
Linux distributions.

There are no known bugs in this package.

Please report problems to the L</"AUTHOR">.

Patches are welcome.

=head1 MORE DOCUMENTATION

All modules contain detailed information on the interfaces they provide.

=head1 SEE ALSO

The basic operation of the C<Redis::JobQueue> package modules:

L<Redis::JobQueue|Redis::JobQueue> - Object interface for creating,
executing jobs queues, as well as monitoring the status and results of jobs.

L<Redis::JobQueue::Job|Redis::JobQueue::Job> - Object interface for jobs
creating and manipulating.

L<Redis|Redis> - Perl binding for Redis database.

=head1 AUTHOR

Sergey Gladkov, E<lt>sgladkov@trackingsoft.comE<gt>

=head1 CONTRIBUTORS

Alexander Solovey

Jeremy Jordan

Vlad Marchenko

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2012 by TrackingSoft LLC.
All rights reserved.

This package is free software; you can redistribute it and/or modify it under
the same terms as Perl itself. See I<perlartistic> at
L<http://dev.perl.org/licenses/artistic.html>.

This program is
distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE.

=cut