#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

use lib 'lib';

use Test::More tests => 26;
use Test::NoWarnings;

BEGIN { use_ok 'Redis::JobQueue', qw(
    DEFAULT_SERVER
    DEFAULT_PORT
    DEFAULT_TIMEOUT

    ENOERROR
    EMISMATCHARG
    EDATATOOLARGE
    ENETWORK
    EMAXMEMORYLIMIT
    EJOBDELETED
    EREDIS
    ) }

can_ok( 'Redis::JobQueue', 'new' );
can_ok( 'Redis::JobQueue', 'add_job' );
can_ok( 'Redis::JobQueue', 'get_job_status' );
can_ok( 'Redis::JobQueue', 'load_job' );
can_ok( 'Redis::JobQueue', 'get_next_job' );
can_ok( 'Redis::JobQueue', 'update_job' );
can_ok( 'Redis::JobQueue', 'delete_job' );
can_ok( 'Redis::JobQueue', 'get_job_ids' );
can_ok( 'Redis::JobQueue', 'ping' );
can_ok( 'Redis::JobQueue', 'quit' );

can_ok( 'Redis::JobQueue', 'timeout' );
can_ok( 'Redis::JobQueue', 'max_datasize' );
can_ok( 'Redis::JobQueue', 'last_errorcode' );

my $val;
ok( $val = DEFAULT_SERVER,      "import OK: $val" );
ok( $val = DEFAULT_PORT,        "import OK: $val" );
$val = undef;
ok( defined ( $val = DEFAULT_TIMEOUT ),     "import OK: $val" );

ok( ( $val = ENOERROR ) == 0,   "import OK: $val" );
ok( $val = EMISMATCHARG,        "import OK: $val" );
ok( $val = EDATATOOLARGE,       "import OK: $val" );
ok( $val = ENETWORK,            "import OK: $val" );
ok( $val = EMAXMEMORYLIMIT,     "import OK: $val" );
ok( $val = EJOBDELETED,         "import OK: $val" );
ok( $val = EREDIS,              "import OK: $val" );

ok( $val = Redis::JobQueue::MAX_DATASIZE, "import OK: $val" );
