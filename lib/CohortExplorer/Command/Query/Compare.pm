package CohortExplorer::Command::Query::Compare;

use strict;
use warnings;

our $VERSION = 0.13;

use base qw(CohortExplorer::Command::Query);
use CLI::Framework::Exceptions qw( :all );

#-------

# Command is only available to longitudinal datasources
sub usage_text {

	q\
                compare [--out|o=<directory>] [--export|e=<table>] [--export-all|a] [--save-command|s] [--stats|S] [--cond|c=<cond>]
                [variable] : compare entities across visits with/without conditions on variables


                NOTES
                   The variables entity_id and visit (if applicable) must not be provided as arguments as they are already part of the
                   query-set. However, the user can impose conditions on both variables.

                   Other variables in arguments/cond (option) must be referenced as <table>.<variable> or <visit>.<table>.<variable> where
                   visit can be vAny, vLast, v1, v2, v3 ... vMax. Here vMax is the maximum visit number for which data is available.

                   Conditions can be imposed using the operators: =, !=, >, <, >=, <=, between, not_between, like, not_like, in, not_in, 
                   regexp and not_regexp. The keyword undef can be used to specify null.

                   When condition is imposed on variable with no prefix such as vAny, vLast, v1, v2 and v3 the command assumes the
                   condition applies to all visits of the variable.

                   The directory specified in 'out' option must have RWX enabled (i.e. chmod 777) for CohortExplorer.


                EXAMPLES
                   compare --out=/home/user/exports --stats --save-command --cond=v1.CER.Score='>, 20' v1.SC.Date

                   compare --out=/home/user/exports --export=CER --cond=SD.Sex='=, Male' v1.CER.Score v3.DIS.Status
 
                   compare --out=/home/user/exports --export=CER --cond=v2.CER.Score'!=, undef' vLast.DIS.Status

                   compare -o/home/user/exports -Ssa -c vLast.CER.Score='in, 25, 30, 40' DIS.Status 

                   compare -o/home/user/exports -eCER -eSD -c vLast.CER.Score='between, 25, 30' DIS.Status

             \;
}

sub get_valid_variables {

	my ($self) = @_;

	my $ds = $self->cache->get('cache')->{datasource};

	return [ 'entity_id', keys %{ $ds->variables }, @{ $ds->visit_variables } ];

}

sub create_query_params {

	my ( $self, $opts, @args ) = @_;
	my $ds           = $self->cache->get('cache')->{datasource};
	my $variables    = $ds->variables;
	my @visit        = 1 .. $ds->visit_max;
	my @static_table = @{ $ds->static_tables || [] };
	my $dbh          = $ds->dbh;
	my $csv          = $self->cache->get('cache')->{csv};
	my $struct       = $ds->entity_structure;
	$struct->{-columns} = $ds->entity_columns;
	my %param;

	# Extract all variables from args/cond (option) except
	# entity_id and visit as they are dealt separately
	my @var = grep( !/^(entity_id|visit)$/,
		keys %{
			{
				map { $_ => 1 } map { s/^v(Any|Last|[0-9]+)\.//; $_ } @args,
				keys %{ $opts->{cond} }
			}
		  } );

	for my $v (@var) {
		$v =~ /^([^\.]+)\.(.+)$/;

		# Extract tables and variable names
		# Build a hash with keys: static and dynamic
		# Each key contains its own sql parameters
		my $table_type =
		  grep ( $_ eq $1, @static_table ) ? 'static' : 'dynamic';

		push
		  @{ $param{$table_type}{-where}{ $struct->{-columns}{table} }{-in} },
		  $1;
		push @{ $param{$table_type}{-where}{ $struct->{-columns}{variable} }
			  {-in} }, $2;

		if ( $table_type eq 'dynamic' ) {

			# Each column corresponds to one visit
			for (@visit) {
				push @{ $param{$table_type}{-columns} },
                    " CAST( GROUP_CONCAT( IF( CONCAT( $struct->{-columns}{table}, '.', $struct->{-columns}{variable} ) = '$v'"
				  . " AND $struct->{-columns}{visit} = $_, $struct->{-columns}{value}, NULL)) AS "
				  . ( uc $variables->{$v}{type} )
				  . " ) AS `v$_.$v`";
			}
		}
		else {
			push @{ $param{$table_type}{-columns} },
			    " CAST( GROUP_CONCAT( DISTINCT "
			  . " IF( CONCAT( $struct->{-columns}{table}, '.', $struct->{-columns}{variable} ) = '$v', $struct->{-columns}{value}, NULL)) AS "
			  . ( uc $variables->{$v}{type} )
			  . " ) AS `$v`";
		}

		if ( $table_type eq 'static' ) {
			if (   $opts->{cond}
				&& $opts->{cond}{$v}
				&& $csv->parse( $opts->{cond}{$v} ) )
			{

			# Set condition on static variable (i.e. variable from static table)
				my @cond = grep ( s/^\s*|\s*$//g, $csv->fields );
				$param{$table_type}{-having}{"`$v`"} =
				  { $cond[0] => [ @cond[ 1 .. $#cond ] ] };
			}
		}

		else {

	  # Build conditions for visit variables e.g. v1.var, vLast.var, vAny.var etc.
	  # Values inside array references are joined as 'OR' and hashes as 'AND'
			my @visit_var =
			  grep( /^(v(Any|Last|[0-9]+)\.$v|$v)$/, keys %{ $opts->{cond} } );

			for my $vv ( sort @visit_var ) {

				# Parse conditions
				my @cond = grep ( s/^\s*|\s*$//g, $csv->fields )
				  if ( $csv->parse( $opts->{cond}{$vv} ) );

    # Last visits (i.e. vLast) for entities are not known in advance so practically any
    # visit can be the last visit for any entity
				if ( $vv =~ /^(vLast\.$v)$/ ) {
					if ( defined $param{$table_type}{-having}{-or} ) {
						map {
							${ $param{$table_type}{-having}{-or} }[$_]
							  ->{ "`v" . ( $_ + 1 ) . ".$v`" } =
							  { $cond[0] => [ @cond[ 1 .. $#cond ] ] }
						} 0 .. $#{ $param{$table_type}{-having}{-or} };
					}
					else {
						$param{$table_type}{-having}{-or} = [
							map {
								{
									'vLast' => { -ident => $_ },
									"`v$_.$v`" =>
									  { $cond[0] => [ @cond[ 1 .. $#cond ] ] }
								}
							  } @visit
						];
					}
				}

				# vAny includes all visit variables joined as 'OR'
				elsif ( $vv =~ /^(vAny\.$v)$/ ) {
					if ( defined $param{$table_type}{-having}{-and} ) {
						push @{ $param{$table_type}{-having}{-and} }, [
							map {
								{ "`v$_.$v`" =>
									  { $cond[0] => [ @cond[ 1 .. $#cond ] ] } }
							  } @visit
						];
					}

					else {
						$param{$table_type}{-having}{-and} = [
							{
								-or => [
									map {
										{
											"`v$_.$v`" => {
												$cond[0] =>
												  [ @cond[ 1 .. $#cond ] ]
											}
										}
									  } @visit
								]
							}
						];
					}
				}

				# Individual visits (v1.var, v2.var, v3.var etc.)
				elsif ( $vv =~ /^v[0-9]{1,2}\.$v$/ ) {
					my @var_cond = grep ( s/^\s*|\s*$//g, $csv->fields )
					  if ( $csv->parse( $opts->{cond}{$v} ) );
					if ( $param{$table_type}{-having}{"`$vv`"} ) {
						$param{$table_type}{-having}{"`$vv`"} = [
							-and => { $cond[0] => [ @cond[ 1 .. $#cond ] ] },
							[
								-or => {
									$var_cond[0] =>
									  [ @var_cond[ 1 .. $#var_cond ] ]
								},
								{ '=', undef }
							]
						];
					}
					else {
						$param{$table_type}{-having}{"`$vv`"} =
						  { $cond[0] => [ @cond[ 1 .. $#cond ] ] };
					}
				}

    # When condition is imposed on a variable (with no prefix v1, v2, vLast, vAny)
    # assume condition applies to all visits of the variable (i.e. 'AND' case)
				else {
					map {
						$param{$table_type}{-having}{"`v$_.$v`"} = [
							{ $cond[0] => [ @cond[ 1 .. $#cond ] ] },
							{ '=', undef }
						  ]
					} @visit;

				}

			}
		}

	}

	for ( keys %param ) {
		if ( $_ eq 'static' ) {
			unshift @{ $param{$_}{-columns} },
			  $struct->{-columns}{entity_id} . ' AS `entity_id`';
		}

		else {

		 # entity_id and visit are added to the list of SQL cols in dynamic param
			unshift @{ $param{$_}{-columns} },
			  (
				$struct->{-columns}{entity_id} . ' AS `entity_id`',
				'MIN( ' . $struct->{-columns}{visit} . ' + 0 ) AS `vFirst`',
				'MAX( ' . $struct->{-columns}{visit} . ' + 0 ) AS `vLast`',
				'GROUP_CONCAT( DISTINCT '
				  . $struct->{-columns}{visit}
				  . ') AS `visit`'
			  );

			if (   $opts->{cond}
				&& $opts->{cond}{visit}
				&& $csv->parse( $opts->{cond}{visit} ) )
			{

				# Set condition on visit
				my @cond = grep ( s/^\s*|\s*$//g, $csv->fields );
				$param{$_}{-having}{visit} =
				  { $cond[0] => [ @cond[ 1 .. $#cond ] ] };
			}
		}

		$param{$_}{-from} = $struct->{-from};
		$param{$_}{-where} =
		  $struct->{-where}
		  ? { %{ $param{$_}{-where} }, %{ $struct->{-where} } }
		  : $param{$_}{-where};

		$param{$_}{-group_by} = 'entity_id';
		$param{$_}{-order_by} = 'entity_id + 0';

		if (   $opts->{cond}
			&& $opts->{cond}{entity_id}
			&& $csv->parse( $opts->{cond}{entity_id} ) )
		{

			# Set condition on entity_id
			my @cond = grep ( s/^\s*|\s*$//g, $csv->fields );
			$param{$_}{-having}{entity_id} =
			  { $cond[0] => [ @cond[ 1 .. $#cond ] ] };
		}

		# Make sure condition on 'tables' has no duplicate placeholders
		$param{$_}{-where}{ $struct->{-columns}{table} }{-in} = [
			keys %{
				{
					map { $_ => 1 }
					  @{ $param{$_}{-where}{ $struct->{-columns}{table} }{-in} }
				}
			  }
		];

	}

	return \%param;
}

sub process_result {

	my ( $self, $opts, $rs, $dir, @args ) = @_;

    # Header of the csv must pay attention to args and variables on which the condition is imposed
    # Extract visit specific variables from the result-set based on the variables provided as args/cond (option).
    # Say, variables in args/cond variables are v1.var and vLast.var but as the result-set contains all visits of
    # the variable 'var' so discard v2.var and v3.var and select v1.var and the equivalent vLast.var
	my $index = $rs->[0][3] && $rs->[0][3] eq 'visit' ? 3 : 0;

	# Compiling regex to extract variables specified as args/cond (option)
	my $regex = join '|', map { s/^vAny\.//; $_ } @args,
	  keys %{ $opts->{cond} };

	$regex = qr/$regex/;

	my @index_to_use = sort { $a <=> $b } keys %{
		{
			map { $_ => 1 } (
				0 .. $index,
				grep( $rs->[0][$_] =~ $regex, 0 .. $#{ $rs->[0] } )
			)
		}
	  };

	my @var = @{ $rs->[0] }[@index_to_use];

    # Extract last visit specific variables (i.e. vLast.var) in args/cond (option)
	my @last_visit_var = keys %{
		{
			map { $_ => 1 }
			  grep( /^vLast\./, ( @args, keys %{ $opts->{cond} } ) )
		}
	  };

	# Entities from the query are stored within a list
	my @rs_entity;

	my $file = File::Spec->catfile( $dir, "QueryOutput.csv" );

	my $fh = FileHandle->new("> $file")
	  or throw_cmd_run_exception( error => "Failed to open file: $!" );

	my @column = ( @var, @last_visit_var );

	my $csv = $self->cache->get('cache')->{csv};

	$csv->print( $fh, \@column )
	  or throw_cmd_run_exception( error => $csv->error_diag );

	for my $row ( 1 .. $#$rs ) {
		push @rs_entity, $rs->[$row][0];

		# Sort visits in the visit column
		if ( $index == 3 ) {
			$rs->[$row][3] =
			  join( ', ', ( sort { $a <=> $b } split ',', $rs->[$row][3] ) );
		}

		my @last_visit_column =
		  map { s/^vLast\.//; "v$rs->[$row][2].$_" } @last_visit_var;

		my @last_visit_val;

		for my $col (@last_visit_column) {
			my ($index) = grep { $rs->[0][$_] eq $col } 0 .. $#{ $rs->[0] };
			push @last_visit_val, $rs->[$row][$index];
		}

		my @val =
		  ( ( map { $rs->[$row][$_] } @index_to_use ), @last_visit_val );

		$csv->print( $fh, \@val )
		  or throw_cmd_run_exception( error => $csv->error_diag );
	}

	$fh->close;
	return \@rs_entity;
}

sub process_table {

	my ( $self, $table, $ts, $dir, $rs_entity ) = @_;

	my ( $ds, $csv ) = @{ $self->cache->get('cache') }{qw/datasource csv/};

	my @static_table = @{ $ds->static_tables || [] };

	my $table_type =
	  $ds->type eq 'standard'
	  ? 'static'
	  : ( grep ( $_ eq $table, @static_table ) ? 'static' : 'dynamic' );

	# Extract the variables appertaining to the table from the variable list
	my @var = map { /^$table\.(.+)$/ ? $1 : () } keys %{ $ds->variables };

	my %data;

	# Get variables for static/dynamic tables
	my @header =
	    $table_type eq 'static'
	  ? @var
	  : map {
		my $visit = $_;
		map { "v$visit.$_" } @var
	  } 1 .. $ds->visit_max;

	for (@$ts) {

		# For static tables in longitudinal datasources table data comprise of
		# entity_id and values of all table variables
		# and in dynamic tables (longitudinal datasources only) it contains
		# an additional column visit
		if ( $table_type eq 'static' ) {
			$data{ $_->[0] }{ $_->[1] } = $_->[2];
		}

		else {
			$data{ $_->[0] }{ 'v' . $_->[3] . '.' . $_->[1] } = $_->[2];
		}
	}

	# Write table data
	my $file = File::Spec->catfile( $dir, "$table.csv" );

	my $untainted = $1 if ( $file =~ /^(.+)$/ );

	my $fh = FileHandle->new("> $untainted")
	  or throw_cmd_run_exception( error => "Failed to open file: $!" );

	my @column = ( qw(entity_id), @header );

	$csv->print( $fh, \@column )
	  or throw_cmd_run_exception( error => $csv->error_diag );

	# Write data for entities present in the result set
	for my $entity (@$rs_entity) {

		my @val = ( $entity, map { $data{$entity}{$_} } @header );

		$csv->print( $fh, \@val )
		  or throw_cmd_run_exception( error => $csv->error_diag );
	}

	$fh->close;
}

sub create_dataset {

	my ( $self, $rs ) = @_;
	my $index = $rs->[0][3] && $rs->[0][3] eq 'visit' ? 3 : 0;
	my %data;

	# Remove visit suffix vAny, vLast, v1, v2 etc. from the variables
	# in the result-set (i.e. args/cond (option))
	my @var = keys %{
		{
			map { s/^v(Any|Last|[0-9]+)\.//; $_ => 1 }
			  @{ $rs->[0] }[ $index + 1 .. $#{ $rs->[0] } ]
		}
	  };

	# Generate dataset for calculating summary statistics from the result-set
	for my $r ( 1 .. $#$rs ) {
		for my $v (@var) {

			$data{ $rs->[$r][0] }{$v} =
			  [ map { $rs->[0][$_] =~ /$v$/ ? $rs->[$r][$_] || () : () }
				  $index + 1 .. $#{ $rs->[0] } ];

			if ( $index != 0 ) {
				$data{ $rs->[$r][0] }{'visit'} =
				  [ split ',', $rs->[$r][$index] ];
			}

		}
	}

	return ( \%data, 1, ( $index == 0 ? qw/entity_id/ : qw/entity_id visit/ ),
		@var );
}

#-------
1;

__END__

=pod

=head1 NAME

CohortExplorer::Command::Query::Compare - CohortExplorer class to compare entities across visits

=head1 SYNOPSIS

B<compare [OPTIONS] [VARIABLE]>

B<c [OPTIONS] [VARIABLE]>

=head1 DESCRIPTION

The compare command enables the user to compare entities across visits. The user can also impose conditions on variables. Moreover, the command also enables the user to view summary statistics and export data in csv format. The command is only available to longitudinal datasources with data on at least 2 visits.

This class is inherited from L<CohortExplorer::Command::Query> and overrides the following methods:

=head2 usage_text()

This method returns the usage information for the command.

=head2 get_valid_variables()

This method returns a ref to the list containing all variables (including visit variables) for validating arguments and condition option(s).

=head2 create_query_params( $opts, @args )

This method returns a hash ref with keys, C<static>, C<dynamic> or both depending on the variables supplied as arguments and conditions. The value of each key is a hash containing SQL parameters such as C<-columns>, C<-from>, C<-where>, C<-group_by> and C<-having>.

=head2 process_result( $opts, $rs, $dir, @args )
     
This method writes result set into a csv file and returns a ref to the list containing entity_ids.
        
=head2 process_table( $table, $ts, $dir, $rs_entity )
        
This method writes the table data into a csv file. The data includes C<entity_id> of all entities present in the result set followed by values of all visit variables.

=head2 create_dataset( $rs )

This method returns a hash ref with C<entity_id> as keys and variable name-value pairs as values. The statistics in this command are calculated with respect to C<entity_id> and the number of observations for each variable is the number of times (or visits) each variable was recorded for each entity during the course of the study.
 
=head1 OPTIONS

=over

=item B<-o> I<DIR>, B<--out>=I<DIR>

Provide directory to export data

=item B<-e> I<TABLE>, B<--export>=I<TABLE>

Export table by name

=item B<-a>, B<--export-all>

Export all tables

=item B<-s>, B<--save--command>

Save command

=item B<-S>, B<--stats>

Show summary statistics

=item B<-c> I<COND>, B<--cond>=I<COND>
            
Impose conditions using the operators: C<=>, C<!=>, C<E<gt>>, C<E<lt>>, C<E<gt>=>, C<E<lt>=>, C<between>, C<not_between>, C<like>, C<not_like>, C<in>, C<not_in>, C<regexp> and C<not_regexp>.

The keyword C<undef> can be used to specify null.

=back

=head1 NOTES

The variables C<entity_id> and C<visit> (if applicable) must not be provided as arguments as they are already part of the query-set.
However, the user can impose conditions on both variables. Other variables in arguments and conditions must be referenced as C<table.variable> or C<visit.table.variable> where visit = C<vAny>, C<vLast>, C<v1>, C<v2>, C<v3> ... C<vMax>. Here vMax is the maximum visit number for which data is available. When a condition is imposed on a variable with no prefix such as C<vAny>, C<vLast>, C<v1>, C<v2> and C<v3> the command assumes the condition applies to all visits of the variable.

The directory specified in C<out> option must have RWX enabled for CohortExplorer.

=head1 EXAMPLES

 compare --out=/home/user/exports --stats --save-command --cond=v1.CER.Score='>, 20' v1.SC.Date

 compare --out=/home/user/exports --export=CER --cond=SD.Sex='=, Male' v1.CER.Score v3.DIS.Status
 
 compare --out=/home/user/exports --export=CER --cond=v2.CER.Score'!=, undef' vLast.DIS.Status

 compare -o/home/user/exports -Ssa -c vLast.CER.Score='in, 25, 30, 40' DIS.Status 

 compare -o/home/user/exports -eCER -eSD -c vLast.CER.Score='between, 25, 30' DIS.Status

=head1 DIAGNOSTICS

This class throws C<throw_cmd_run_exception> exception imported from L<CLI::Framework::Exceptions> if L<Text::CSV_XS> fails to construct a csv string from the list containing variable values.

=head1 SEE ALSO

L<CohortExplorer>

L<CohortExplorer::Datasource>

L<CohortExplorer::Command::Describe>

L<CohortExplorer::Command::Find>

L<CohortExplorer::Command::History>

L<CohortExplorer::Command::Query::Search>

L<CohortExplorer::Command::Query::Compare>

=head1 LICENSE AND COPYRIGHT

Copyright (c) 2013-2014 Abhishek Dixit (adixit@cpan.org). All rights reserved.

This program is free software: you can redistribute it and/or modify it under the terms of either:

=over

=item *
the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version, or

=item *
the "Artistic Licence".

=back

=head1 AUTHOR

Abhishek Dixit

=cut
