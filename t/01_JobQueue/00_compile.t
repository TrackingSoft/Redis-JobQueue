#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

use lib 'lib';

use Test::More tests => 28;

BEGIN { use_ok 'Redis::JobQueue', qw(
    DEFAULT_SERVER
    DEFAULT_PORT
    DEFAULT_TIMEOUT

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
    ) }

can_ok( 'Redis::JobQueue', 'new' );
can_ok( 'Redis::JobQueue', 'add_job' );
can_ok( 'Redis::JobQueue', 'check_job_status' );
can_ok( 'Redis::JobQueue', 'load_job' );
can_ok( 'Redis::JobQueue', 'get_next_job' );
can_ok( 'Redis::JobQueue', 'update_job' );
can_ok( 'Redis::JobQueue', 'delete_job' );
can_ok( 'Redis::JobQueue', 'get_jobs' );
can_ok( 'Redis::JobQueue', 'quit' );

can_ok( 'Redis::JobQueue', 'timeout' );
can_ok( 'Redis::JobQueue', 'max_datasize' );
can_ok( 'Redis::JobQueue', 'last_errorcode' );

my $val;
ok( $val = DEFAULT_SERVER,      "import OK: $val" );
ok( $val = DEFAULT_PORT,        "import OK: $val" );
$val = undef;
ok( defined ( $val = DEFAULT_TIMEOUT ),     "import OK: $val" );
ok( $val = STATUS_CREATED,      "import OK: $val" );
ok( $val = STATUS_WORKING,      "import OK: $val" );
ok( $val = STATUS_COMPLETED,    "import OK: $val" );
ok( $val = STATUS_DELETED,      "import OK: $val" );

ok( ( $val = ENOERROR ) == 0,   "import OK: $val" );
ok( $val = EMISMATCHARG,        "import OK: $val" );
ok( $val = EDATATOOLARGE,       "import OK: $val" );
ok( $val = ENETWORK,            "import OK: $val" );
ok( $val = EMAXMEMORYLIMIT,     "import OK: $val" );
ok( $val = EMAXMEMORYPOLICY,    "import OK: $val" );
ok( $val = EJOBDELETED,         "import OK: $val" );
ok( $val = EREDIS,              "import OK: $val" );
