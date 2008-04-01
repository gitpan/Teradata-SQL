use ExtUtils::MakeMaker;

# If CLI is not installed, go quietly.
unless (-f "/usr/include/coptypes.h") {
   print "CLIv2 installation not found.\n";
   exit 0;
}

WriteMakefile(
    'NAME'		=> 'Teradata::SQL',
    'VERSION_FROM'	=> 'SQL.pm', # finds $VERSION
    'PREREQ_PM'		=> {}, # e.g., Module::Name => 1.1
    ($] >= 5.005 ?    ## Add these new keywords supported since 5.005
      ( AUTHOR     => 'Geoffrey Rommel <GROMMEL@cpan.org>') : ()),
    'LIBS'		=> [''], # e.g., '-lm'
    'DEFINE'		=> '', # e.g., '-DHAVE_SOMETHING'
    'C'			=> ['SQL.c', 'tdsql.c'] ,
    'INC'		=> '-I.', # e.g., '-I/usr/include/other'
    'OBJECT'		=> '$(O_FILES) tdsql.o',
    'LDFROM'		=> '$(O_FILES)',
    'LIBS'		=> ["-ltdusr -lcliv2 -lnsl -lsocket"],
);
