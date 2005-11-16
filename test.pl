# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl test.pl'

#########################

# change 'tests => 1' to 'tests => last_test_to_print';

use Test;
BEGIN { plan tests => 11 };
use Teradata::SQL qw(:all);
ok(1); # If we made it this far, we're ok.

#########################

$TDLOGON = $ENV{'TDLOGON'};
die ">>>> Please specify TDLOGON\n" unless $TDLOGON;

$VERBOSE = $ENV{'TEST_VERBOSE'};

print "connect......................";
$dbh = Teradata::SQL::connect($TDLOGON, 'UTF8')
  or die "Could not connect";
is_ok(check_ec(), 2);

print "open,fetch,close.............";
$sth = $dbh->open("select * from dbc.Tables
 sample 10  order by 1,2")   or die "Could not open";
print "Activity count: $activcount\n" if $VERBOSE;
while ( @z = $sth->fetchrow_list()) {
   print " data: @z\n" if $VERBOSE;
}
$sth->close();
is_ok(check_ec(), 3);

print "prepare......................";
$sth = $dbh->prepare("select * from dbc.Columns
 where DatabaseName = ? and TableName = ?")
  or die "Could not prepare";
$sth->openp('DBC', 'Acctg')  or die "Could not open";
print "Activity count: $activcount\n" if $VERBOSE;
while ( @z = $sth->fetchrow_list()) {
   print " data: @z\n" if $VERBOSE;
}
$sth->close();
is_ok(check_ec(), 4);

print "fetchrow_hash................";
$sth = $dbh->open("select 2+5 wise_men")
  or die "Could not prepare";
while ( %z = $sth->fetchrow_hash ) {
   if ($VERBOSE) {
      foreach $k (keys %z) { print " key <$k> value <$z{$k}>\n"; }
   }
   is_ok($z{'wise_men'} == 7, 5);
}
$sth->close();

print "execute......................";
if (defined $ENV{'TDDB'}) {
   my $DB = $ENV{'TDDB'};
   $all_ok = 1;

   $dbh->execute("create table $DB.notbloodylikely
(ident    integer,
 float02  float,
 ch03     char(10),
 vch04    varchar(30) )
unique primary index(ident)");
   $all_ok &&= check_ec();
   $sth = $dbh->prepare("insert into $DB.notbloodylikely (?,?,?,?)");
   $sth->executep(1, 3.14159265, 'pi', "transcendental");
   $all_ok &&= check_ec();
   $sth->executep(2, 2.71828183, 'Homer', "Iliad");
   $sth->executep(3, undef, undef, undef);
   $all_ok &&= check_ec();
   $dbh->execute("drop table $DB.notbloodylikely");
   $all_ok &&= check_ec();

   is_ok($all_ok, 6);
} else {
   print "skipping test\n";
}

print "data types...................";
if (defined $ENV{'TDDB'}) {
   my $DB = $ENV{'TDDB'};
   $all_ok = 1;

   $dbh->execute("create table $DB.ZQ_decimaltests
(ident    integer  not null,
 dec01    decimal(2,0),
 dec02    decimal(4,1),
 dec03    decimal(9,3),
 dec04    decimal(18,0),
 dec05    decimal(18,6) )
unique primary index(ident)");
   $all_ok &&= check_ec();
   $sth = $dbh->prepare("insert into $DB.ZQ_decimaltests (?,?,?,?,?,?)");
   $sth->executep(1, 11, 987.6, 773355.118, 9081726354666.0, 3.141592);
   $sth->executep(2, -99, -999.9, -2.718, -1029384756777.0, -987.654);
   $sth->executep(3, undef, undef, undef, undef, undef);
   $sth->executep(4, undef, 123.4, undef, 17, undef);

   @exp = ('1 11 987.6 773355.118 9081726354666 3.141592',
     '2 -99 -999.9 -2.718 -1029384756777 -987.654000',
     '3     ',
     '4  123.4  17 ');
   $i = 0;
   $sth = $dbh->open("select * from $DB.ZQ_decimaltests
    order by 1");
   while (@r = $sth->fetchrow_list) {
      $all_ok &&= ("@r" eq $exp[$i]);
      if ($VERBOSE) {
         print "expected: <$exp[$i]>\n";
         print "  actual: <@r>\n";
      }
      $i++;
   }
   $sth->close;
   $dbh->execute("drop table $DB.ZQ_decimaltests");

   is_ok($all_ok, 7);
} else {
   print "skipping test\n";
}

print "UTF-8........................";
$sth = $dbh->open("select _unicode '002104304E8C'xc");
$utf8 = $sth->fetchrow_list;
print " UTF-8 string: ", unpack('H*', $utf8), "\n" if $VERBOSE;
$sth->close;
is_ok($utf8 eq "\x21\xD0\xB0\xE4\xBA\x8C", 8);

print "ANSI date....................";
$dbh->execute("set session dateform=ansidate")
   or warn "Error in set session";
$sth = $dbh->open("select 1010911 (date)");
$ansidate = $sth->fetchrow_list;
print " date: $ansidate\n" if $VERBOSE;
$sth->close;
is_ok($ansidate eq '2001-09-11', 9);

print "quiet errors.................";
$Teradata::SQL::msglevel = 0;
$sth = $dbh->open("select * from vanity.of_vanities")
  && $sth->close;
is_ok($errorcode == 3802, 10);

print "disconnect...................";
$Teradata::SQL::msglevel = 1;
$dbh->disconnect;
is_ok(check_ec(), 11);


#--- Was it okay?
sub is_ok {
 my ($cond, $n) = @_;

 if ($cond) { print "ok $n\n"; }
 else { print "not ok $n\n"; }
}

#--- Check the Teradata error code.
sub check_ec {
 return (Teradata::SQL::errorcode == 0);
}
