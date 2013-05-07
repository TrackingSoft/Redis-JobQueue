package Redis::JobQueue::Job;
use 5.010;

# Pragmas
use strict;
use warnings;

our $VERSION = '1.00';

use Exporter qw( import );
our @EXPORT_OK  = qw(
    STATUS_CREATED
    STATUS_WORKING
    STATUS_COMPLETED
    STATUS_FAILED
    );

#-- load the modules -----------------------------------------------------------

# Modules
use Carp;
use List::MoreUtils qw(
    firstidx
    );
use List::Util qw(
    min
    );
use Mouse;                                      # automatically turns on strict and warnings
use Mouse::Util::TypeConstraints;
use Params::Util qw(
    _HASH0
    );

#-- declarations ---------------------------------------------------------------

use constant {
    STATUS_CREATED      => '__created__',
    STATUS_WORKING      => '__working__',
    STATUS_COMPLETED    => '__completed__',
    STATUS_FAILED       => '__failed__',
    };

my $meta = __PACKAGE__->meta;

subtype __PACKAGE__.'::NonNegInt',
    as 'Int',
    where { $_ >= 0 },
    message { ( $_ || '' ).' is not a non-negative integer!' }
    ;

subtype __PACKAGE__.'::Progress',
    as 'Num',
    where { $_ >= 0 and $_ <= 1 },
    message { ( $_ || '' ).' is not a progress number!' }
    ;

subtype __PACKAGE__.'::WOSpStr',
    as 'Str',
    where { $_ !~ / / },
    message { ( $_ || '' ).' contains spaces!' }
    ;

subtype __PACKAGE__.'::DataRef',
    as 'ScalarRef';

coerce __PACKAGE__.'::DataRef',
    from 'Str',
    via { \$_ }
    ;

#-- constructor ----------------------------------------------------------------

around BUILDARGS => sub {
    my $orig  = shift;
    my $class = shift;

    if ( eval { $_[0]->isa( __PACKAGE__ ) } )
    {
        my $job = shift;
        return $class->$orig( ( map { ( $_, $job->$_ ) } $job->job_attributes ), @_ );
    }
    else
    {
        return $class->$orig( @_ );
    }
};

#-- public attributes ----------------------------------------------------------

has 'id'            => (
    is          => 'rw',
    isa         => __PACKAGE__.'::WOSpStr',
    default     => '',
    trigger     => sub { $_[0]->_variability_set( 'id' ) },
    );

has 'queue'         => (
    is          => 'rw',
    isa         => 'Maybe[Str]',
    required    => 1,
    trigger     => sub { $_[0]->_variability_set( 'queue' ) },
    );

has 'job'           => (
    is          => 'rw',
    isa         => 'Maybe[Str]',
    default     => '',
    trigger     => sub { $_[0]->_variability_set( 'job' ) },
    );

has 'status'        => (
    is          => 'rw',
    isa         => 'Str',
    default     => STATUS_CREATED,
    trigger     => sub { $_[0]->_variability_set( 'status', $_[1] ) },
    );

has '_meta_data'     => (
    is          => 'rw',
    isa         => 'HashRef',
    init_arg    => 'meta_data',
    default     => sub { {} },
    trigger     => sub { $_[0]->_variability_set( 'meta_data' ) },
    );

has 'expire'        => (
    is          => 'rw',
    isa         => 'Maybe['.__PACKAGE__.'::NonNegInt]',
    required    => 1,
    trigger     => sub { $_[0]->_variability_set( 'expire' ) },
    );

for my $name ( qw( workload result ) )
{
    has $name           => (
        is          => 'rw',
        # A reference because attribute can contain a large amount of data
        isa         => __PACKAGE__.'::DataRef | HashRef | ArrayRef | ScalarRef | Object',
        coerce      => 1,
        builder     => '_build_data',           # will throw an error if you pass a bare non-subroutine reference as the default
        trigger     => sub { $_[0]->_variability_set( $name ) },
    );
}

has 'progress'      => (
    is          => 'rw',
    isa         => __PACKAGE__.'::Progress',
    default     => 0,
    trigger     => sub { $_[0]->_variability_set( 'progress' ) },
    );

has 'message'       => (
    is          => 'rw',
    isa         => 'Maybe[Str]',
    default     => '',
    trigger     => sub { $_[0]->_variability_set( 'message' ) },
    );

for my $name ( qw( created updated ) )
{
    has $name           => (
        is          => 'rw',
        isa         => __PACKAGE__.'::NonNegInt',
        default     => sub { time },
        trigger     => sub { $_[0]->_variability_set( $name ) },
        );
}

for my $name ( qw( started completed ) )
{
    has $name           => (
        is          => 'rw',
        isa         => __PACKAGE__.'::NonNegInt',
        default     => 0,
        trigger     => sub { $_[0]->_variability_set( $name ) },
        );
}

#-- private attributes ---------------------------------------------------------

has '_variability'   => (
    is          => 'ro',
    isa         => 'HashRef[Int]',
    lazy        => 1,
    init_arg    => undef,                       # we make it impossible to set this attribute when creating a new object
    builder     => '_build_variability',
    );

#-- public methods -------------------------------------------------------------

sub clear_variability {
    my $self    = shift;

    my @fields = @_;
    @fields = $self->job_attributes unless @fields;
    foreach my $field ( @fields )
    {
        $self->_variability->{ $field } = 0
            if exists $self->_variability->{ $field };
    }
}

sub modified_attributes {
    my $self        = shift;

    return grep { $self->_variability->{ $_ } } $self->job_attributes;
}

sub job_attributes {
    return( sort map { $_->name eq '_meta_data' ? 'meta_data' : $_->name } grep { $_->name ne '_variability' } $meta->get_all_attributes );
}

sub elapsed {
    my $self        = shift;

    if ( my $started = $self->started )
    {
        return( ( $self->completed || time ) - $started );
    }
    else
    {
        return( undef );
    }
}

sub meta_data {
    my $self    = shift;
    my $key     = shift;
    my $val     = shift;

    return $self->_meta_data if !defined $key;

    # metadata can be set with an external hash
    if ( _HASH0( $key ) )
    {
        my @attributes = $self->job_attributes;
        foreach my $field ( keys %$key )
        {
            confess 'The name of the metadata field the same as standart job field name'
                if ( firstidx { $_ eq $field } @attributes ) != -1;
        }
        $self->_meta_data( $key );
    }

    # getter
    return $self->_meta_data->{ $key } if !defined $val;

    # setter
    confess 'The name of the metadata field the same as standart job field name'
        if ( firstidx { $_ eq $key } $self->job_attributes ) != -1;
    $self->_meta_data->{ $key } = $val;

    # job data change
    $self->updated( time );
    ++$self->_variability->{ 'updated' };
    ++$self->_variability->{ 'meta_data' };

    return;
}

#-- private methods ------------------------------------------------------------

sub _build_data {
    return \'';
}

sub _build_variability {
    my $self    = shift;

    my %variability = ();
    map { $variability{ $_ } = 1 } $self->job_attributes;
    return \%variability;
}

sub _variability_set {
    my $self    = shift;
    my $field   = shift;

    if ( $field =~ /^(status|meta_data|workload|result|progress|message|started|completed)$/ )
    {
        $self->updated( time );
        ++$self->_variability->{ 'updated' };
    }

    if ( $field eq 'status' )
    {
        my $new_status = shift;
        if      ( $new_status eq STATUS_CREATED )   { $self->created( time ) }
        elsif   ( $new_status eq STATUS_WORKING )   { $self->started( time ) unless $self->started }
        elsif   ( $new_status eq STATUS_COMPLETED ) { $self->completed( time ) }
        elsif   ( $new_status eq STATUS_FAILED )    { $self->completed( time ) }
    }

    ++$self->_variability->{ $field };
}

#-- Closes and cleans up -------------------------------------------------------

no Mouse::Util::TypeConstraints;
no Mouse;                                       # keywords are removed from the package
__PACKAGE__->meta->make_immutable();

__END__

=head1 NAME

Redis::JobQueue::Job - Object interface for creating and manipulating jobs

=head1 VERSION

This documentation refers to C<Redis::JobQueue::Job> version 1.00

=head1 SYNOPSIS

There are several ways to create a C<Redis::JobQueue::Job>
object:

    my $pre_job = {
        id           => '4BE19672-C503-11E1-BF34-28791473A258',
        queue        => 'lovely_queue',
        job          => 'strong_job',
        expire       => 12*60*60,               # 12h
        status       => STATUS_CREATED,
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

    $job = Redis::JobQueue::Job->new( $pre_job );

    my $next_job = Redis::JobQueue::Job->new( $job );

Access methods to read and assign the relevant attributes of the object.
For example:

    $job->$workload( \'New workload' );
    # or
    $job->$workload( 'New workload' );

    my $id = $job->id;
    # 'workload' and 'result' return a reference to the data
    my $result = ${$job->result};

Returns a list of names of the modified object fields:

    my @modified = $job->modified_attributes;

Resets the sign of changing an attribute. For example:

    $job->clear_variability( qw( status ) );

=head1 DESCRIPTION

Job API is implemented by C<Redis::JobQueue::Job> class.

The main features of the C<Redis::JobQueue::Job> class are:

=over 3

=item *

Provides an object oriented model of communication.

=item *

Supports data representing various aspects of the job.

=item *

Supports the creation of the job object, an automatic allowance for the change
attributes and the ability to cleanse the signs of change attributes.

=back

=head2 CONSTRUCTOR

An error will cause the program to halt if the argument is not valid.

=head3 C<new( id =E<gt> $uuid, ... )>

It generates a Job object and can be called as either a class method or
an object method.

If invoked with the first argument being an object of C<Redis::JobQueue::Job> class
or a reference to a hash, then the new object attribute values are taken from
the hash of the first argument.

C<new> optionally takes arguments. These arguments are in key-value pairs.

This example illustrates a C<new()> call with all the valid arguments:

    $job = Redis::JobQueue::Job->new(
        id          => '4BE19672-C503-11E1-BF34-28791473A258',
                # UUID string, using conventional UUID string format.
                # Do not use it because filled in automatically when
                # you create a job.
        queue       => 'lovely_queue',  # The name of the job queue.
                                        # (required)
        job         => 'strong_job',    # The name of the job.
                                        # (optional attribute)
        expire      => 12*60*60,        # Job's time to live in seconds.
                                        # 0 for no expire time.
                                        # (required)
        status      => STATUS_CREATED,  # Current status of the job.
                # Do not use it because value should be set by the worker.
        workload    => \'Some stuff up to 512MB long',
                # Baseline data for the function of the worker
                # (the function name specified in the 'job').
                # Can be a scalar, an object or a reference to a scalar, hash, or array
        result      => \'JOB result comes here, up to 512MB long',
                # The result of the function of the worker
                # (the function name specified in the 'job').
                # Do not use it because value should be set by the worker.
        );

Returns the object itself, we can chain settings.

The attributes C<workload> and C<result> may contain a large amount of data,
therefore, it is desirable that they be passed as references to the actual
data to improve performance.

Do not use spaces in an C<id> attribute value.

Each element in the struct data has an accessor method, which is
used to assign and fetch the element's value.

=head2 METHODS

An error will cause the program to halt if the argument is not valid.

=head3 C<id>

=head3 C<queue>

=head3 C<job>

=head3 C<expire>

=head3 C<status>

=head3 C<workload>

=head3 C<result>

The family of methods for a multitude of accessor methods for your data with
the appropriate names. These methods are able to read and assign the relevant
attributes of the object.

As attributes C<workload> and C<result> may contain a large amount of data
(scalars, references to arrays and hashes, objects):

=over 3

=item *

A read method returns a reference to the data.

=item *

A write method can receive both data or a reference to the data.

=back

=head3 C<created>

Returns time (UTC) of job creation.
Set to the current time (C<time>) when job is created.

If necessary, alternative value can be set as:

    $job->created( time );

=head3 C<started>

Returns the time (UTC) that the job started processing.
Set to the current time (C<time>) when the L</status> of the job is set to L</STATUS_WORKING>.

If necessary, you can set your own value, for example:

    $job->started( time );

=head3 C<updated>

Returns the time (UTC) of the most recent modification of the job.

Set to the current time (C<time>) when value(s) of any of the following data changes:
L</status>, L</workload>, L</result>, L</progress>, L</message>, L</completed>.

Can be updated manually:

    $job->updated( time );

=head3 C<completed>

Returns the time (UTC) of the task completion.

It is set to 0 when task is created.

Set to C<time> when L</status> is changed to L</STATUS_COMPLETED> or to L</STATUS_FAILED>.

Can be modified manually:

    $job->completed( time );

=head3 C<elapsed>

Returns the time (in seconds) since the job started processing (see L</started>)
to the current time.
Returns C<undef> if the start processing time was set to 0.

=head3 C<meta_data>

With no arguments, returns a reference to a hash of metadata (additional information related to the job).
For example:

    my $md = $job->meta_data;

Hash value of an individual item metadata is available by specifying the name of the hash key.
For example:

    my $foo = $job->meta_data( 'foo' );

Separate metadata value can be set as follows:

    my $foo = $job->meta_data( next => 16 );

Group metadata can be specified by reference to a hash.
Metadata may contain scalars, references to arrays and hashes, objects.
For example:

    $job->meta_data( {
        'foo'   => 12,
        'bar'   => [ 13, 14, 15 ],
        'other' => { a => 'b', c => 'd' },
        } );

The name of the metadata fields should not match the standard names returned by
L</job_attributes>.
An invalid name causes die (C<confess>).

=head3 C<clear_variability( @fields )>

Resets the sign of any specified attributes that have been changed.
If no attribute names are specified, the signs are reset for all attributes.

=head3 C<modified_attributes>

Returns a list of names of the object attributes that have been modified.

=head3 C<job_attributes>

Returns a sorted list of the names of object attributes.

=head1 EXPORT

None by default.

Additional constants are available for import, which can be used
to define some type of parameters.

These are the defaults:

=over

=item C<STATUS_CREATED>

Initial status of the job, showing that it was created.

=item C<STATUS_WORKING>

Jobs is being executed. Set by the worker function.

=item C<STATUS_COMPLETED>

Job is completed. Set by the worker function.

=item C<STATUS_FAILED>

Job has failed. Set by the worker function.

=back

User himself should specify the status L</ STATUS_WORKING>, L</ STATUS_COMPLETED>, L</ STATUS_FAILED>
or own status when processing the job.

=head2 DIAGNOSTICS

An error will cause the program to halt (C<confess>) if an argument
is not valid. Use C<$@> for the analysis of the specific reasons.

=head1 SEE ALSO

The basic operation of the L<Redis::JobQueue|Redis::JobQueue> package modules:

L<Redis::JobQueue|Redis::JobQueue> - Object interface for creating and
executing jobs queues, as well as monitoring the status and results of jobs.

L<Redis::JobQueue::Job|Redis::JobQueue::Job> - Object interface for creating
and manipulating jobs.

L<Redis|Redis> - Perl binding for Redis database.

=head1 SOURCE CODE

Redis::JobQueue is hosted on GitHub:
L<https://github.com/TrackingSoft/Redis-JobQueue>

=head1 AUTHOR

Sergey Gladkov, E<lt>sgladkov@trackingsoft.comE<gt>

=head1 CONTRIBUTORS

Alexander Solovey

Jeremy Jordan

Vlad Marchenko

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2012-2013 by TrackingSoft LLC.

This package is free software; you can redistribute it and/or modify it under
the same terms as Perl itself. See I<perlartistic> at
L<http://dev.perl.org/licenses/artistic.html>.

This program is
distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE.

=cut
