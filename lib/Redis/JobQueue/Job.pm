package Redis::JobQueue::Job;
use 5.010;

# Pragmas
use bytes;
use strict;
use warnings;

our $VERSION = '0.16';

#-- load the modules -----------------------------------------------------------

# Modules
use Mouse;                                      # automatically turns on strict and warnings
use Mouse::Util::TypeConstraints;

#-- declarations ---------------------------------------------------------------

use constant {
    MAX_DATASIZE        => 512*1024*1024,       # A String value can be at max 512 Megabytes in length.
    };

my $meta = __PACKAGE__->meta;

subtype __PACKAGE__.'::NonNegInt',
    as 'Int',
    where { $_ >= 0 },
    message { ( $_ || '' ).' is not a non-negative integer!' }
    ;

subtype __PACKAGE__.'::WOSpStr',
    as 'Str',
    where { $_ !~ / / },
    message { ( $_ || '' ).' is not a without-space string!' }
    ;

subtype __PACKAGE__.'::DataStr',
    as 'Str',
    where { bytes::length( $_ ) <= MAX_DATASIZE },
    message { "'".( $_ || '' )."' is not a valid data string!" }
    ;

subtype __PACKAGE__.'::DataStrRef',
    as 'ScalarRef',
    where { !defined( ${$_} ) or bytes::length( ${$_} ) <= MAX_DATASIZE },
    message { "a reference to '".substr( ${$_}, 0, 10 )."...' is not a reference to a valid data string!" }
    ;

subtype __PACKAGE__.'::DataRef',
    as __PACKAGE__.'::DataStrRef';

coerce __PACKAGE__.'::DataRef',
    from __PACKAGE__.'::DataStr',
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
    default     => '',
    trigger     => sub { $_[0]->_variability_set( 'status' ) },
    );

has 'meta_data'     => (
    is          => 'rw',
    isa         => 'Maybe[Str]',
    default     => '',
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
    has $name      => (
        is          => 'rw',
        # A reference because attribute can contain a large amount of data
        isa         => __PACKAGE__.'::DataStrRef | '.__PACKAGE__.'::DataRef',
        coerce      => 1,
        builder     => '_build_data',           # will throw an error if you pass a bare non-subroutine reference as the default
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

    foreach my $field ( @_ )
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
    return map { $_->name } grep { $_->name !~ /variability/ } $meta->get_all_attributes;
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

    ++$self->_variability->{ $_[0] };
}

#-- Closes and cleans up -------------------------------------------------------

no Mouse::Util::TypeConstraints;
no Mouse;                                       # keywords are removed from the package
__PACKAGE__->meta->make_immutable();

__END__

=head1 NAME

Redis::JobQueue::Job - Object interface for jobs creating and manipulating

=head1 VERSION

This documentation refers to C<Redis::JobQueue::Job> version 0.16

=head1 SYNOPSIS

There are several ways to create a C<Redis::JobQueue::Job>
object:

    my $pre_job = {
        id           => '4BE19672-C503-11E1-BF34-28791473A258',
        queue        => 'lovely_queue',
        job          => 'strong_job',
        expire       => 12*60*60,               # 12h
        status       => 'created',
        meta_data    => scalar( localtime ),
        workload     => \'Some stuff up to 512MB long',
        result       => \'JOB result comes here, up to 512MB long',
        };

    my $job = Redis::JobQueue::Job->new(
        id           => $pre_job->{id},
        queue        => $pre_job->{queue},
        job          => $pre_job->{job},
        expire       => $pre_job->{expire},
        status       => $pre_job->{status},
        meta_data    => $pre_job->{meta_data},
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

    Redis::JobQueue::Job->new(
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
        status      => '_created_',     # Current status of the job.
                # Do not use it because value should be set by the worker.
        meta_data   => scalar( localtime ), # Job meta-data, such as custom
                                        # attributes etc. (optional attribute)
        workload    => \'Some stuff up to 512MB long',
                # Baseline data for the function of the worker
                # (the function name specified in the 'job').
                # For example, can be prepared by a function
                # 'Storable::freeze'.
        result      => \'JOB result comes here, up to 512MB long',
                # The result of the function of the worker
                # (the function name specified in the 'job').
                # Do not use it because value should be set by the worker.
        );

Returns the object itself, we can chain settings.

As attributes C<workload>, C<result> may contain a large amount of data,
therefore, to improve performance, it is desirable that they be passed
as references to the actual data.

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

=head3 C<meta_data>

=head3 C<workload>

=head3 C<result>

A family of methods for a multitude of accessor methods for your data with
the appropriate names. These methods to read and assign the relevant attributes
of the object.

As attributes C<workload>, C<result> may contain a large amount of data,
for them:

=over 3

=item *

A read method returns a reference to the data.

=item *

A write method can receive both data and a reference to the data.

=back

=head3 C<clear_variability( @fields )>

Resets the sign of changing attributes.

=head3 C<modified_attributes>

Returns a list of names of the modified object fields.

=head3 C<job_attributes>

Returns a list of the names of object attributes.

=head1 EXPORT

None by default.

Additional constants are available for import, which can be used
to define some type of parameters.

These are the defaults:

=over

=item C<Redis::JobQueue::Job::MAX_DATASIZE>

Maximum size of the data provided by data on the references C<workload>,
C<result>: 512MB.

=back

=head2 DIAGNOSTICS

An error will cause the program to halt (C<confess>) if the argument
is not valid. Use C<$@> for the analysis of the specific reasons.

=head1 SEE ALSO

The basic operation of the L<Redis::JobQueue|Redis::JobQueue> package modules:

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

Copyright (C) 2012-2013 by TrackingSoft LLC.
All rights reserved.

This package is free software; you can redistribute it and/or modify it under
the same terms as Perl itself. See I<perlartistic> at
L<http://dev.perl.org/licenses/artistic.html>.

This program is
distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE.

=cut
