#!/usr/bin/perl -w

use 5.014002;
use strict;
use warnings;

use Test::More;
use lib 'lib';

#eval 'use Test::Distribution not => "prereq"';
eval 'use Test::Distribution not => [ qw/prereq podcover/ ]';
plan( skip_all => 'Test::Distribution not installed' ) if $@;
Test::Distribution->import(  );
