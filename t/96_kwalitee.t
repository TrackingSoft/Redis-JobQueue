#!/usr/bin/perl -w

use 5.014002;
use strict;
use warnings;

use Test::More;

eval { require Test::Kwalitee };
plan skip_all => "Test::Kwalitee required for testing Kwalitee" if $@;
Test::Kwalitee->import(  );
