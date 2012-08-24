#!/usr/bin/perl -w

use 5.014002;
use strict;
use warnings;

use lib 'lib';

use Test::More tests => 7;

BEGIN { use_ok 'Redis::JobQueue', qw(
    DEFAULT_SERVER
    DEFAULT_PORT
    DEFAULT_TIMEOUT
    STATUS_CREATED
    STATUS_WORKING
    STATUS_COMPLETED
    ) }

my $val;
ok( defined( $val = DEFAULT_SERVER ),               "import OK: $val" );
ok( defined( $val = DEFAULT_PORT ),                 "import OK: $val" );
ok( defined( $val = DEFAULT_TIMEOUT ),              "import OK: $val" );
ok( defined( $val = STATUS_CREATED ),               "import OK: $val" );
ok( defined( $val = STATUS_WORKING ),               "import OK: $val" );
ok( defined( $val = STATUS_COMPLETED ),             "import OK: $val" );
