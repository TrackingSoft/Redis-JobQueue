#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

use lib 'lib';

use Test::More tests => 10;
use Test::NoWarnings;

BEGIN { use_ok 'Redis::JobQueue', qw(
    DEFAULT_SERVER
    DEFAULT_PORT
    DEFAULT_TIMEOUT
    ) }

BEGIN { use_ok 'Redis::JobQueue::Job', qw(
    STATUS_CREATED
    STATUS_WORKING
    STATUS_COMPLETED
    STATUS_FAILED
    ) }

my $val;
ok( defined( $val = DEFAULT_SERVER() ),             "import OK: $val" );
ok( defined( $val = DEFAULT_PORT() ),               "import OK: $val" );
ok( defined( $val = DEFAULT_TIMEOUT() ),            "import OK: $val" );

ok( defined( $val = STATUS_CREATED() ),             "import OK: $val" );
ok( defined( $val = STATUS_WORKING() ),             "import OK: $val" );
ok( defined( $val = STATUS_COMPLETED() ),           "import OK: $val" );
ok( defined( $val = STATUS_FAILED() ),              "import OK: $val" );
