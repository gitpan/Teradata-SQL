package Teradata::SQL;

use 5.006;
use strict;
use warnings;
use Carp;

require Exporter;
require DynaLoader;
use AutoLoader;

our @ISA = qw(Exporter DynaLoader);

# Items to export into caller's namespace by default.
our %EXPORT_TAGS = ( 'all' => [ qw($activcount
  $errorcode $errormsg) ] );

our @EXPORT_OK = ( @{ $EXPORT_TAGS{'all'} } );

our @EXPORT = qw();
our $VERSION = '0.01';

#sub AUTOLOAD {
#    # This AUTOLOAD is used to 'autoload' constants from the constant()
#    # XS function.  If a constant is not found then control is passed
#    # to the AUTOLOAD in AutoLoader.
#    # We have no constants, so we can just use AutoLoader's.
#
#    goto &AutoLoader::AUTOLOAD;
#}

#-------------------------
#--- PACKAGE VARIABLES
#-------------------------

$Teradata::SQL::msglevel = 1;
$Teradata::SQL::activcount = 0.0;
$Teradata::SQL::errorcode = 0;
$Teradata::SQL::errormsg = '';

bootstrap Teradata::SQL $VERSION;

#-------------------------
#--- METHODS
#-------------------------

#--- Connect. Returns a connection handle or undef.
sub connect {
 my ($logonstring, $ccs, $trx_mode) = @_;

 $ccs ||= "ASCII";
 $ccs = uc($ccs);

 $trx_mode ||= "BTET";
 die "Transaction mode must be BTET or ANSI"
   unless $trx_mode =~ /^(BTET|ANSI)$/i;
 $trx_mode = uc($trx_mode);

 my $self = {
    htype => 'conn',  # I am a connection handle.
# We don't want the logonstring sticking around in memory, so we
# don't save it in the hash.
    ccs => $ccs,
    trx_mode => $trx_mode,
 };

 my $sess_id = Xconnect($logonstring, $ccs, $trx_mode);
 $logonstring = 'x' x 40;

 if ( $sess_id != 0 ) {
    $self->{'sess_id'} = $sess_id;
    bless $self, 'Teradata::SQL';
    return $self;
 } else {
    return undef;
 }
}

#--- Disconnect.
sub disconnect {
 my $ch = shift;
 if ($ch->{htype} ne 'conn') {
    Carp::carp "Invalid handle passed to disconnect";
    return 0;
 }

 return Xdisconnect($ch->{'sess_id'});
}

#--- Execute a request (no data returned).
sub execute {
 my ($ch, $sql) = @_;
 if ($ch->{htype} ne 'conn') {
    Carp::carp "Invalid handle passed to execute";
    return 0;
 }
 $sql =~ tr/\n\r/  /;  # Teradata doesn't like newlines.

 return Xexecute($ch->{sess_id}, $sql);
}

#--- Open a request. No input variables.
sub open {
 my ($ch, $sql) = @_;
 if ($ch->{htype} ne 'conn') {
    Carp::carp "Invalid handle passed to open";
    return 0;
 }
 $sql =~ tr/\n\r/  /;  # Teradata doesn't like newlines.

 my $self = {
    htype => 'req',  # I am a request handle.
    sess_id => $ch->{'sess_id'},
    sql => $sql,
 };

 my $req_id = Xopen($ch->{sess_id}, $sql);

 if ( $req_id != 0 ) {
    $self->{'req_id'} = $req_id;
    bless $self, 'Teradata::SQL';
    return $self;
 } else {
    return undef;
 }
}

#--- Prepare a request. Returns a request handle.
#--- The actual SQL calls can be deferred until openp or executep.
sub prepare {
 my ($ch, $sql) = @_;
 if ($ch->{htype} ne 'conn') {
    Carp::carp "Invalid handle passed to prepare";
    return 0;
 }
 $sql =~ tr/\n\r/  /;  # Teradata doesn't like newlines.

 my $self = {
    htype => 'preq',  # I am a Prepared request handle.
    sess_id => $ch->{'sess_id'},
    sql => $sql,
 };

 bless $self, 'Teradata::SQL';
 return $self;
}

#--- Execute a prepared request (no data returned).  May have arguments.
sub executep {
 my ($rh, @hvars) = @_;
 if ($rh->{htype} ne 'preq') {
    Carp::carp "Invalid handle passed to executep";
    return 0;
 }

# Apparently Teradata doesn't save the prepared request, so we have
# to prepare it again.
 return Xexecutep($rh->{sess_id}, $rh->{sql}, @hvars);
}

#--- Open a prepared request. May have input arguments.
sub openp {
 my ($rh, @hvars) = @_;
 if ($rh->{htype} ne 'preq') {
    Carp::carp "Invalid handle passed to openp";
    return 0;
 }

# Apparently Teradata doesn't save the prepared request, so we have
# to prepare it again.
 my $req_id = Xopenp($rh->{sess_id}, $rh->{sql}, @hvars);

 if ( $req_id != 0 ) {
    $rh->{'req_id'} = $req_id;
    return 1;
 } else {
    return 0;
 }
}

#--- Fetch a row from an open cursor.
sub fetchrow_list {
 my $rh = shift;
 if ($rh->{htype} !~ /p?req/) {
    Carp::carp "Invalid handle passed to fetchrow_list";
    return 0;
 }

 return Xfetch($rh->{req_id}, 0);
}

#--- Fetch a row from an open cursor into a hash.
sub fetchrow_hash {
 my $rh = shift;
 if ($rh->{htype} !~ /p?req/) {
    Carp::carp "Invalid handle passed to fetchrow_hash";
    return 0;
 }

 return Xfetch($rh->{req_id}, 1);
}

#--- Close a cursor.
sub close {
 my $rh = shift;
 if ($rh->{htype} !~ /p?req/) {
    Carp::carp "Invalid handle passed to close";
    return 0;
 }

 return Xclose($rh->{req_id});
}

#--- Abort. This is an asynchronous abort request, not a ROLLBACK.
sub abort {
 my $ch = shift;
 if ($ch->{htype} ne 'conn') {
    Carp::carp "Invalid handle passed to abort";
    return 0;
 }

 return Xabort($ch->{sess_id});
}

1;

__END__

=head1 Name

Teradata::SQL - Perl interface to Teradata SQL

=head1 Synopsis

  use Teradata::SQL;
  use Teradata::SQL qw(:all);  # Exports variables
  $dbh = Teradata::SQL::connect(logonstring [,tranmode]);
  $dbh->execute($request);
  $rh = $dbh->open($request);
  $rh->fetchrow_list();
  $rh->close();
  $dbh->disconnect;
  # And others. See below.

=head1 Description

Teradata::SQL is a Perl interface to Teradata SQL. It does not attempt
to be a complete interface to Teradata -- for instance, it does not
allow asynchronous requests or PM/API connections -- but it should
be sufficient for many applications.

=head1 Methods

This is an object-oriented module; no methods are exported by default.
The connect method must be called with its full name; other methods
are called with object handles.

Most methods return a true value when they succeed and FALSE upon
failure. The fetch methods, however, return the data to be fetched.
If there is no row to be fetched, they return an empty list.

=over 4

=item B<Teradata::SQL::connect> LOGONSTRING [CHARSET] [TRANMODE]

Connects to Teradata. The first argument is a standard Teradata logon
string in the form "[server/]user,password[,'account']".
The second argument (optional) is the client character set for the
session, 'ASCII' by default. The most common character sets besides
ASCII are 'UTF8' and 'UTF16'.
The third argument (optional) is the session transaction mode, either
'BTET' (the default) or 'ANSI'.

This method returns a connection handle that must be used for future
requests. If the connection fails, undef will be returned. Many
connections (sessions) can be active at a time.

=item B<disconnect>

Connection method. Disconnects from Teradata. This method must be
applied to an active connection handle.

=item B<execute> REQUEST

Connection method. Executes a single request without input variables.
The argument is the SQL request to be run. It can be a
multi-statement request, i.e. contain multiple statements
separated by semicolons.

This method should be used only when the request does not return
data. If data is to be returned, use B<open> instead.

=item B<open> REQUEST

Connection method. Opens a request for execution. The
argument is the SQL request to be prepared. It can be a
multi-statement request, i.e. contain multiple statements
separated by semicolons. The WITH clause (to add subtotals and
totals) is not supported.

You can have as many requests open at a time as you wish, but be
aware that each one allocates additional memory.

The request cannot include parameter markers ('?' in the
place of variables or literals). If you need parameter markers,
use B<prepare> instead.

open returns a request handle or, if the open fails, undef.

After fetching all the rows, be sure to close() the cursor.

=item B<prepare> REQUEST

Connection method. Opens a request for execution. The arguments
are the same as for B<open>, and prepare also returns a request
handle or, if the prepare fails, undef. The difference is that
a prepared request can include parameter markers
('?' in the place of variables or literals).

=item B<executep> [ARGS]

Request method. Executes the prepared request. If the request
includes parameter markers, arguments can be supplied to take the
place of the markers. For more information, see L<"Data Types">.

This method should be used only when the request does not return
data. If data is to be returned, use B<openp> instead.

=item B<openp> [ARGS]

Request method. Executes the prepared request and opens a cursor
to contain the results. If the request
includes parameter markers, arguments can be supplied to take the
place of the markers.

After fetching all the rows, be sure to close() the cursor.

=item B<fetchrow_list>

Request method. Returns the next row from the open cursor in list
form, or an empty list if no more rows are available; e.g.:

   @row = $rh->fetchrow_list();

This works with cursors opened by open() or by openp().

=item B<fetchrow_hash>

Request method. Returns the next row from the open cursor in hash
form, or an empty hash if no more rows are available; e.g.:

   %row = $rh->fetchrow_hash();

This works with cursors opened by open() or by openp().
The hash entries are those specified by ColumnName, not ColumnTitle.
See the SQLv2 Reference, s.v. "PrepInfo Parcel".

=item B<close>

Request method. Closes the cursor. This should always be called
after opening and fetching the results.

=item B<abort>

Connection method. Aborts the currently active request for the session.
Note that this is an asynchronous ABORT (like the .ABORT command in
BTEQ), not a ROLLBACK. Ordinarily it would have to be called from
a signal handler; for example:

   sub abort_req {
    $dbh->abort;
    print "Request has been aborted.\n";
    $dbh->disconnect;
    exit;
   }
   $SIG{'INT'} = \&abort_req;

=back

=head1 Example

  # Connect and get a database handle.
  $dbh = Teradata::SQL::connect("dbc/user,password")
    or die "Could not connect";
  # Prepare a request; read the results.
  $rh = $dbh->open("sel * from edw.employees");
  while (@emp_row = $rh->fetchrow_list) {
     print "employee data: @emp_row\n";
  }
  $rh->close;
  #
  # Prepare, then insert some rows.
  $rh = $dbh->prepare("insert into edw.departments (?,?,?,?)");
  while (<DATA>) {
     chomp;  @incoming = split;
     $rh->executep(@incoming);
  }
  $rh->finish;  # To clean up storage used by executep.
  # All finished.
  $dbh->disconnect;  # Note: $dbh, not $rh.

For more examples, see test.pl.

=head1 Variables

=over 4

=item B<$Teradata::SQL::activcount>

Activity count, i.e. the number of rows affected by the last
SQL operation. This variable can be exported to your namespace.

=item B<$Teradata::SQL::errorcode>

The Teradata error code from the last SQL operation.
This variable can be exported.

=item B<$Teradata::SQL::errormsg>

The Teradata error message from the last SQL operation. This
variable can be exported.

These three variables can be exported to your namespace all
at once by this means:

   use Teradata::SQL qw(:all);

=item B<$Teradata::SQL::msglevel>

By default, Teradata::SQL will display error codes and messages
from Teradata on stderr. Setting this variable to 0 will suppress
these messages. The default value is 1. The module will honor
changes to the value of this variable at any point during your
program.

=back

=head1 Data Types

Perl uses only three data types: integers, double-precision
floating point, and byte strings.
The data returned from Teradata will be converted to one of
these types and will look like ordinary Perl values.

Dates are returned in either integer form (e.g., 1020815 for
15 August 2002) or in ANSI character form (e.g., '2002-08-15'),
depending on the default for your system, the session
characteristics, and whether you have issued a SET
SESSION DATEFORM request. If you want dates returned in some
other form, you must explicitly cast them, e.g. like this:

   cast(cast(sale_dt as format 'MM/DD/YYYY') as char(10))

By default, times and timestamps are returned as character
strings in their default formats. Again, you can cast them
as you wish in your select request.

A word of caution is in order about decimal fields.
Decimal fields with a precision of 9 or lower will be
converted to doubles (numeric) and will behave more or less
as expected, with the usual caveats about floating-point
arithmetic. Decimal fields with a higher precision (10-18 digits)
will be converted to character strings. This has the advantage
of preserving their full precision, but it means that Perl
will not treat them as numeric. To convert them to numeric
fields, you can add 0 to them, but values with 16 or more
significant digits will lose precision. You have been warned!

Arguments passed to Teradata via B<openp> and B<executep> will
be passed in Perl internal form (integer, double, or byte
string). You can pass undefs to become nulls in the database, but
there are limitations. Since all undefs look the same to the module,
it coerces them all to "integers". This works for most data types,
but Teradata will not allow integer nulls to be placed in BYTE,
TIME, or TIMESTAMP fields. At present, the only workaround for this
situation would be to code a request without parameter
markers and hard-code the nulls to be of the type you want.
In other words, instead of this:

   $rh = $dbh->prepare("insert into funkytown values (?,?,?)");
   $rh->executep(1, "James Brown", undef);

you would code this:

   $rh = $dbh->prepare("insert into funkytown values
      (1, 'James Brown', cast(null as timestamp(0)) )");
   $rh->executep();

=head1 Limitations

The maximum length of a request to be prepared is 64 Kbytes.
The maximum length of data to be returned is 65400 bytes.
These limits cannot be relaxed without rewriting the module.

The maximum number of fields selected or returned by any request
is 500. Likewise, you can pass no more than 500 arguments to
B<openp> or B<executep>.  If these limitations are too strict,
you can ask your Perl administrator to change the value of
MAX_FIELDS in the module's header file and recompile the module.

Multiple sessions are supported. This feature would be most useful
when connecting to multiple servers; multiple sessions on a single
server are of little use without support for asynchronous requests.

Using SQL, it is possible to use a different client character set
for each request, but this module sets it only at the session level.

The following Teradata features are not supported:

   Partitions other than DBC/SQL (e.g. MONITOR or MLOAD)
   Asynchronous requests
   WITH clause
   LOB data types
   CHECKPOINT
   DESCRIBE
   ECHO
   POSITION
   REWIND

If you would like some features added, write to the author at
the address shown below. No guarantees!

=head1 Reference

Teradata Call-Level Interface Version 2 Reference for
Network-Attached Systems, B035-2418-093A (Sep. 2003).

=head1 Author

Geoffrey Rommel, GROMMEL [at] cpan [dot] org.

=cut
