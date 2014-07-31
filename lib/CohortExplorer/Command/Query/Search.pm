package CohortExplorer::Command::Query::Search;

use strict;
use warnings;

our $VERSION = 0.13;

use base qw(CohortExplorer::Command::Query);
use CLI::Framework::Exceptions qw( :all );

#-------

# Command is available to both standard and longitudinal datasources
sub usage_text {

	q\
              search [--out|o=<directory>] [--export|e=<table>] [--export-all|a] [--save-command|s] [--stats|S] [--cond|c=<cond>] 
              [variable] : search entities with/without conditions on variables
              
              
              NOTES
                 The variables entity_id and visit (if applicable) must not be provided as arguments as they are already part of
                 the query-set. However, the user can impose conditions on both variables.

                 Other variables in arguments/cond (option) must be referenced as <table>.<variable>.

                 The conditions can be imposed using the operators such as =, !=, >, <, >=, <=, between, not_between, like, not_like, 
                 in, not_in, regexp and not_regexp. The keyword undef can be used to search for null values.

                 The directory specified in 'out' option must have RWX enabled (i.e. chmod 777) for CohortExplorer.


              EXAMPLES
                 search --out=/home/user/exports --stats --save-command --cond=DS.Status='=, CTL, MCI' GDS.Score
                 
                 search --out=/home/user/exports --stats --save-command --cond=CER.Score='<=, 30' GDS.Score

                 search --out=/home/user/exports --export-all --cond=SD.Sex='=, Male' CER.Score DIS.Status

                 search -o/home/user/exports -eDS -eSD -c entity_id='like, DCR%' DIS.Status

                 search -o/home/user/exports -Ssa -c visit='in, 1, 3, 5' DIS.Status 

                 search -o/home/user/exports -c CER.Score='between, 25, 30' DIS.Status
 \;
}

sub get_valid_variables {

	my ($self) = @_;

	my $ds = $self->cache->get('cache')->{datasource};

	my @var = keys %{ $ds->variables };

	return $ds->type eq 'standard'
	  ? [ qw/entity_id/, @var ]
	  : [ qw/entity_id visit/, @var ];

}

sub create_query_params {

	my ( $self, $opts, @args ) = @_;
	my $ds           = $self->cache->get('cache')->{datasource};
	my $ds_type      = $ds->type;
	my $variables    = $ds->variables;
	my @static_table = @{ $ds->static_tables || [] };
	my $dbh          = $ds->dbh;
	my $csv          = $self->cache->get('cache')->{csv};
	my $struct       = $ds->entity_structure;
	$struct->{-columns} = $ds->entity_columns;

	my @condition_var =
	  grep ( !/^(entity_id|visit)$/, keys %{ $opts->{cond} } );

	my %param;

	require Tie::IxHash;

	tie my %args, 'Tie::IxHash', map { $_ => 1 } @args;

	my @var = ( keys %args, grep { !$args{$_} } @condition_var );

	for (@var) {
		/^([^\.]+)\.(.+)$/;

     # Extract tables and variable names, a variable is referenced as <table>.<variable>
		my $table_type =
		  $ds_type eq 'standard'
		  ? 'static'
		  : ( grep ( $_ eq $1, @static_table ) ? 'static' : 'dynamic' );

  # Build a hash with keys 'static' and 'dynamic'.
  # Each key contains its own SQL parameters
  # In static tables rows are grouped on entity_id where as in dynamic tables
  # (i.e. longitudinal datasources) the rows are grouped on entity_id and visit
		push
		  @{ $param{$table_type}{-where}{ $struct->{-columns}{table} }{-in} },
		  $1;
		push @{ $param{$table_type}{-where}{ $struct->{-columns}{variable} }
			  {-in} }, $2;

		push @{ $param{$table_type}{-columns} },
		    " CAST( GROUP_CONCAT( "
		  . ( $table_type eq 'static' ? 'DISTINCT' : '' )
		  . (
                      " IF( CONCAT( $struct->{-columns}{table}, '.', $struct->{-columns}{variable} ) = '$_', $struct->{-columns}{value}, NULL ) ) AS "
		  )
		  . ( uc $variables->{$_}{type} )
		  . " ) AS `$_`";

		if (   $opts->{cond}
			&& $opts->{cond}{$_}
			&& $csv->parse( $opts->{cond}{$_} ) )
		{

			# Split the condition on comma
			# First element is operator followed by value
			my @cond = grep ( s/^\s*|\s*$//g, $csv->fields );
			$param{$table_type}{-having}{"`$_`"} =
			  { $cond[0] => [ @cond[ 1 .. $#cond ] ] };
		}
	}

	for ( keys %param ) {

		if ( $_ eq 'static' ) {
			unshift @{ $param{$_}{-columns} },
			  $struct->{-columns}{entity_id} . ' AS `entity_id`';
			$param{$_}{-group_by} = 'entity_id';
		}

		else {

		 # entity_id and visit are added to the list of SQL cols in dynamic param
			unshift @{ $param{$_}{-columns} },
			  (
				$struct->{-columns}{entity_id} . ' AS `entity_id`',
				' (' . $struct->{-columns}{visit} . ' + 0 ) AS `visit`'
			  );

			$param{$_}{-group_by} = [qw/entity_id visit/];

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

		if ( $opts->{cond} && $opts->{cond}{entity_id} ) {

			# Set condition on entity_id
			my @cond = split /\s*,\s*/, $opts->{cond}{entity_id};
			$param{$_}{-having}{entity_id} =
			  { $cond[0] => [ @cond[ 1 .. $#cond ] ] };
		}

		$param{$_}{-order_by} = 'entity_id + 0';

		# Make sure condition clause in 'tables' has no duplicate placeholders
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

	my %rs_entity;

	# Write result set
	my $file = File::Spec->catfile( $dir, "QueryOutput.csv" );

	my $fh = FileHandle->new("> $file")
	  or throw_cmd_run_exception( error => "Failed to open file: $!" );

	my $csv = $self->cache->get('cache')->{csv};

	# Returns a ref to hash with key as entity_id and value either:
	# list of visit numbers if the result-set contains visit column
	# (i.e. dynamic tables- Longitudinal datasources) or,
	# empty list (i.e. static tables)
	for ( 0 .. $#$rs ) {
		if ( $_ > 0 ) {
			push @{ $rs_entity{ $rs->[$_][0] } },
			  $rs->[0][1] eq 'visit' ? $rs->[$_][1] : ();
		}

		$csv->print( $fh, $rs->[$_] )
		  or throw_cmd_run_exception( error => $csv->error_diag );
	}

	$fh->close;

	return \%rs_entity;

}

sub process_table {

	my ( $self, $table, $ts, $dir, $rs_entity ) = @_;

	my ( $ds, $csv ) = @{ $self->cache->get('cache') }{qw/datasource csv/};

	my @static_table = @{ $ds->static_tables || [] };

	my $table_type =
	  $ds->type eq 'standard'
	  ? 'static'
	  : ( grep ( $_ eq $table, @static_table ) ? 'static' : 'dynamic' );

	# Get table header
	my @var = map { /^$table\.(.+)$/ ? $1 : () } keys %{ $ds->variables };

	my %data;

	for (@$ts) {

		if ( $table_type eq 'static' ) {
			$data{ $_->[0] }{ $_->[1] } = $_->[2];
		}
		else {
			$data{ $_->[0] }{ $_->[3] }{ $_->[1] } = $_->[2];
		}
	}

	# Add visit column to the header if the table is dynamic
	my $file = File::Spec->catfile( $dir, "$table.csv" );

	my $untainted = $1 if ( $file =~ /^(.+)$/ );

	my $fh = FileHandle->new("> $untainted")
	  or throw_cmd_run_exception( error => "Failed to open file: $!" );

	my @column =
	  $table_type eq 'static'
	  ? ( qw(entity_id), @var )
	  : ( qw(entity_id visit), @var );

	$csv->print( $fh, \@column )
	  or throw_cmd_run_exception( error => $csv->error_diag );

	my @sorted_entity =
	  ( keys %$rs_entity )[-1] =~ /^[0-9]+$/
	  ? sort { $a <=> $b } keys %$rs_entity
	  : sort { $a cmp $b } keys %$rs_entity;

	# Write data for entities present in the result set
	for my $entity (@sorted_entity) {
		if ( $table_type eq 'static' ) {
			my @val = ( $entity, map { $data{$entity}{$_} } @var );
			$csv->print( $fh, \@val )
			  or throw_cmd_run_exception( error => $csv->error_diag );
		}
		else {    # dynamic tables
			for my $visit (
				  @{ $rs_entity->{$entity} }
				? @{ $rs_entity->{$entity} }
				: keys %{ $data{$entity} }
			  )
			{
				my @val =
				  ( $entity, $visit, map { $data{$entity}{$visit}{$_} } @var );

				$csv->print( $fh, \@val )
				  or throw_cmd_run_exception( error => $csv->error_diag );
			}
		}
	}

	$fh->close;
}

sub create_dataset {

	my ( $self, $rs ) = @_;

	# If the result set contains visit column group data
	# by visit (i.e. dynamic tables/longitudinal datasources)
	my $index = $rs->[0][1] eq 'visit' ? 1 : 0;
	my %data;

	for my $r ( 1 .. $#$rs ) {
		my $key = $index == 0 ? 1 : $rs->[$r][$index];
		for ( $index + 1 .. $#{ $rs->[0] } ) {
			push @{ $data{$key}{ $rs->[0][$_] } }, $rs->[$r][$_] || ();
		}
	}

	return ( \%data, $index, splice @{ $rs->[0] }, 1 );
}

#-------
1;

__END__

=pod

=head1 NAME

CohortExplorer::Command::Query::Search - CohortExplorer class to search entities

=head1 SYNOPSIS

B<search [OPTIONS] [VARIABLE]>

B<s [OPTIONS] [VARIABLE]>

=head1 DESCRIPTION

The search command enables the user to search entities using the variables of interest. The user can also impose conditions on the variables. Moreover, the command also enables the user to view summary statistics and export data in csv format. The command is available to both standard/cross-sectional and longitudinal datasources.

This class is inherited from L<CohortExplorer::Command::Query> and overrides the following methods:

=head2 usage_text()

This method returns the usage information for the command.

=head2 get_valid_variables()

This method returns a ref to the list of variables for validating arguments and condition option(s).

=head2 create_query_params( $opts, @args )

This method returns a hash ref with keys, C<static>, C<dynamic> or both depending on the datasource type and variables supplied as arguments and conditions. The value of each key is a hash containing SQL parameters such as C<-columns>, C<-from>, C<-where>, C<-group_by> and C<-having>.

=head2 process_result( $opts, $rs, $dir, @args ) 
        
This method returns a hash ref with keys as C<entity_id> and values can be a list of visit numbers provided the result-set contains visit column (dynamic tables), or empty list (static tables).

=head2 process_table( $table, $ts, $dir, $rs_entity ) 
        
This method writes the table set (C<$ts>) into a csv file. The data includes C<entity_id> of all entities present in the result set followed by values of all variables. In case of dynamic tables the csv also contains C<visit> column.

=head2 create_dataset( $rs )
        
This method returns a hash ref with C<visit> as keys and variable-value hash as its value provided the query set contains at least one dynamic variable. For all other cases it simply returns a hash ref with variable name-value pairs.
  
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

=back

=head1 NOTES

The variables C<entity_id> and C<visit> (if applicable) must not be provided as arguments as they are already part of the query-set.  However, the user can impose conditions on both variables.

The directory specified in C<out> option must have RWX enabled for CohortExplorer.

=head1 EXAMPLES

 search --out=/home/user/exports --stats --save-command --cond=DS.Status='=, CTL, MCI' GDS.Score
                 
 search --out=/home/user/exports --stats --save-command --cond=CER.Score='<=, 30' GDS.Score

 search --out=/home/user/exports --export-all --cond=SD.Sex='=, Male' CER.Score DIS.Status

 search -o/home/user/exports -eDS -eSD -c entity_id='like, DCR%' DIS.Status

 search -o/home/user/exports -Ssa -c visit='in, 1, 3, 5' DIS.Status 

 search -o/home/user/exports -c CER.Score='between, 25, 30' DIS.Status

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
