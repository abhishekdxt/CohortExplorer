package CohortExplorer::Command::Find;

use strict;
use warnings;

our $VERSION = 0.13;

use base qw(CLI::Framework::Command);
use CLI::Framework::Exceptions qw( :all );
use Exception::Class::TryCatch;

#-------

sub usage_text {

	      q{
           find [--fuzzy|f] [--ignore-case|i] [--and|a] [keyword] : find variables using keywords

         
           By default, the command returns variables that contain either of the keywords in at least one of their attributes.
           The user can override this setting by specifying 'and' option.

           EXAMPLES
             
             find --fuzzy --ignore-case cancer diabetes mmhg  (fuzzy and case insensitive search)

             find Demographics  (exact search)

             find -fia mmse total (using AND operation with bundling and aliases)
        };
}

sub option_spec {

	(
		[],
		[ 'ignore-case|i' => 'ignore case' ],
		[ 'fuzzy|f'       => 'fuzzy search' ],
		[], [ 'and|a' => 'Join keywords using AND (default OR)' ], 
		[]
	);
}

sub validate {

	my ( $self, $opts, @args ) = @_;

	if ( @args == 0 ) {
		throw_cmd_validation_exception(
			error => "At least one argument is required" );
	}
}

sub run {

	my ( $self, $opts, @args ) = @_;
	my ( $ds, $verbose ) =
	  @{ $self->cache->get('cache') }{qw/datasource verbose/};
	my $oper = $opts->{ignore_case} ? -like : 'like binary';

	# Convert all arguments to upper case for case insensitive match
	@args = map { uc $_ } @args if ( $opts->{ignore_case} );

	# Add wildcard characters to each argument for fuzzy matching
	@args = map { '%' . $_ . '%' } @args if ( $opts->{fuzzy} );

	# May or may not be preloaded
	eval 'require ' . ref $ds;

	# Get variable structure
	my $struct = $ds->variable_structure;

	# Get column names-SQL pairs
	my $map = $ds->variable_columns;

	# Set the condition key to be used
	my $condition_key = $struct->{-group_by} ? -having : -where;

	# Get the existing 'OR' within $struct (if any)
	my $OR_condition = $struct->{$condition_key}{-or};

 # By default assume the user is interested in finding variables which contain at least one of the
 # supplied keywords in at least one of the variable attributes
 # If there is already a 'OR' within $struct->{$condition_key} then merge two conditions
 $struct->{ $struct->{-group_by} ? -having : -where }{-or} = [
		map {
			{
				$opts->{ignore_case} ? "UPPER($_)" : $_ => [
					( $opts->{'and'} ? '-and' : '-or' ) => map {
						{ $oper => $_ }
					  } @args
				  ]
			}

		  } $struct->{-group_by}
		? map { "`$_`" } keys %$map
		: values %$map
	];

	# Do a merger of old and new 'OR'
	if ($OR_condition) {
		$struct->{$condition_key}{-or} =
		  $struct->{$condition_key}
		  { [ -or => $struct->{$condition_key}{-or}, $OR_condition ] };
	}

	# Set table and variable as first two columns
	push my @column, qw/variable table/;

	for ( keys %$map ) {
		if ( $_ ne 'variable' && $_ ne 'table' ) {
			push @column, $_;
		}
	}

	# Format column name-SQL pairs along the lines of columns in
	# SQL::Abstract::More
	$struct->{-columns} = [ map { "$map->{$_}|`$_`" } @column ];

	##----- SQL TO FETCH VARIABLE DATA -----##

	my ( $stmt, @bind, $sth, $row );

	eval { ( $stmt, @bind ) = $ds->sqla->select(%$struct); };

	if ( catch my $e ) {
		throw_cmd_run_exception( error => $e );
	}

 # Modify query to allow case insensitve search for tables that store metadata in EAV format (e.g. Opal)
	my $order_by = $1 if ( $stmt =~ /(ORDER BY\s+.+)$/ );
	$stmt =~ s/ORDER BY .+$//;

	my $where = $1 if ( $stmt =~ /HAVING(\s+.+)$/ );
	$stmt =~ s/HAVING .+$//;

	$stmt .= $order_by;
	$stmt =
	    'SELECT '
	  . join( ', ', map { "`$_`" } @column )
	  . " FROM ( $stmt ) AS custom ";
	$stmt .= "WHERE $where " if ($where);

	eval {
		$row = $ds->dbh->selectall_arrayref( $stmt, undef, @bind );

	};

	if ( catch my $e ) {
		throw_cmd_run_exception( error => $e );
	}

	if (@$row) {

		# Add column names
		unshift @$row, [@column];

		print STDERR
		  "\nFound $#$row variable(s) matching the find query criteria ...\n\n"
		  . "Rendering variable description ...\n\n"
		  if ($verbose);

		return {
			headingText => 'variable description',
			rows        => $row
		};
	}

	else {
		print STDERR
		  "\nFound 0 variable(s) matching the find query criteria ...\n\n"
		  if ($verbose);

		return undef;
	}

}

#-------
1;

__END__

=pod

=head1 NAME

CohortExplorer::Command::Find - CohortExplorer class to find variables using keywords

=head1 SYNOPSIS

B<find [OPTIONS] [KEYWORD]>

B<f [OPTIONS] [KEYWORD]>

=head1 DESCRIPTION

This class is inherited from L<CLI::Framework::Command> and overrides the following methods:

=head2 usage_text()

This method returns the usage information for the command.

=head2 option_spec() 

   ( 
     [ 'ignore-case|i' => 'ignore case'                  ], 
     [ 'fuzzy|f' => 'fuzzy search'                       ],
     [ 'and|a' => 'Join keywords using AND (default OR)' ] 
   )

=head2 validate( $opts, @args )

Validates the command options and arguments and throws exception if validation fails.

=head2 run( $opts, @args )

This method enables the user to find variables using keywords. The command looks for the presence of keywords in columns specified in L<variable_structure|CohortExplorer::Datasource/variable_structure()> method of the sub class. The command attempts to print the variable dictionary (i.e. meta data) of variables meeting the search criteria. The variable dictionary can include the following variable attributes:

=over

=item 1

variable name (mandatory)

=item 2

table name (mandatory)

=item 3

type (i.e. integer, decimal, date, datetime etc.)

=item 4

unit

=item 5

categories (if any) separated by newlines 

=item 6

label

=back

=head1 OPTIONS

=over

=item B<-f>, B<--fuzzy>

Fuzzy search

=item B<-i>, B<--ignore-case>

Ignore case

=item B<-a>, B<--and>

Join keywords using AND (default OR)

=back

=head1 DIAGNOSTICS

This command throws the following exceptions imported from L<CLI::Framework::Exceptions>:

=over

=item 1

C<throw_cmd_run_exception>: This exception is thrown if one of the following conditions are met:

=over

=item *

C<select> method in L<SQL::Abstract::More> fails to construct the SQL query from the supplied hash ref.

=item *

C<execute> method in L<DBI> fails to execute the SQL query.

=back

=item 2

C<throw_cmd_validation_exception>: This exception is thrown if the user has not supplied any arguments/keywords to the command. The command expects at least one keyword.

=back

=head1 DEPENDENCIES

L<CLI::Framework::Command>

L<CLI::Framework::Exceptions>

L<DBI>

L<Exception::Class::TryCatch>

L<SQL::Abstract::More>


=head1 EXAMPLES

 find --fuzzy --ignore-case cancer diabetes mmhg (fuzzy and case insensitive search)

 find Demographics (exact search)

 find -fia mmse total (using AND operation with bundling and aliases)


=head1 SEE ALSO

L<CohortExplorer>

L<CohortExplorer::Datasource>

L<CohortExplorer::Command::Describe>

L<CohortExplorer::Command::History>

L<CohortExplorer::Command::Query::Search>

L<CohortExplorer::Command::Query::Compare>


=head1 LICENSE AND COPYRIGHT

Copyright (c) 2013-2014 Abhishek Dixit (adixit@cpan.org). All rights reserved.

This program is free software: you can redistribute it and/or modify it under the terms of either:

=over

=item *
the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or 
(at your option) any later version, or

=item *
the "Artistic Licence".

=back

=head1 AUTHOR

Abhishek Dixit

=cut
