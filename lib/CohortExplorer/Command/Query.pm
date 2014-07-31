package CohortExplorer::Command::Query;

use strict;
use warnings;

our $VERSION = 0.13;
our ( $COMMAND_HISTORY_FILE, $COMMAND_HISTORY_CONFIG, $COMMAND_HISTORY );
our @EXPORT_OK = qw($COMMAND_HISTORY);
my $ARG_MAX = 500;

#-------

BEGIN {

	use base qw(CLI::Framework::Command Exporter);
	use CLI::Framework::Exceptions qw( :all );
	use CohortExplorer::Datasource;
	use Exception::Class::TryCatch;
	use FileHandle;
	use File::HomeDir;
	use File::Spec;
	use Config::General;

	# Untaint and set command history file
	$COMMAND_HISTORY_FILE = $1
	  if (
		File::Spec->catfile(
			File::HomeDir->my_home, ".CohortExplorer_History"
		) =~ /^(.+)$/
	  );

	my $fh = FileHandle->new(">> $COMMAND_HISTORY_FILE");

	# Throw exception if command history file does not exist or
	# is not readable and writable
	if ( !$fh ) {
		throw_cmd_run_exception( error =>
        "'$COMMAND_HISTORY_FILE' must exist with RW enabled (i.e. chmod 766) for CohortExplorer"
		);
	}
	$fh->close;

	# Read command history file
	eval {
		$COMMAND_HISTORY_CONFIG = Config::General->new(
			-ConfigFile            => $COMMAND_HISTORY_FILE,
			-MergeDuplicateOptions => "false",
			-StoreDelimiter        => "=",
			-SaveSorted            => 1
		);
	};

	if ( catch my $e ) {
		throw_cmd_run_exception( error => $e );
	}

	$COMMAND_HISTORY = { $COMMAND_HISTORY_CONFIG->getall };

}

sub option_spec {

	(
		[],
		[ 'cond|c=s%'      => 'impose conditions'        ],
		[ 'out|o=s'        => 'provide output directory' ],
		[ 'save-command|s' => 'save command'             ],
		[ 'stats|S'        => 'show summary statistics'  ],
		[ 'export|e=s@'    => 'export tables by name'    ],
		[ 'export-all|a'   => 'export all tables'        ],
		[]
	);
}

sub validate {

	my ( $self, $opts, @args ) = @_;

	my $cache = $self->cache->get('cache');

	eval 'require ' . ref $cache->{datasource};    # May or may not be preloaded

	print STDERR "\nValidating command options/arguments ...\n\n"
	  if ( $cache->{verbose} );

	##----- VALIDATE ARG LENGTH, EXPORT AND OUT OPTIONS -----##

	if ( !$opts->{out} || !-d $opts->{out} || !-w $opts->{out} ) {
		throw_cmd_validation_exception( error =>
                 "Option 'out' is required. The directory specified in 'out' option must exist with RWX enabled (i.e. chmod 777) for CohortExplorer"
		);
	}

	if ( $opts->{export} && $opts->{export_all} ) {
		throw_cmd_validation_exception( error =>
                 "Mutually exclusive options (export and export-all) specified together"
		);
	}

	if ( @args == 0 || @args > $ARG_MAX ) {
		throw_cmd_validation_exception(
			error => "At least 1-$ARG_MAX variable(s) are required" );
	}

	my $ds = $cache->{datasource};

	# Match table names supplied in the export option to
	# datasource tables and throw exception if they don't match
	if ( $opts->{export} ) {
		my $tables = $ds->tables;
		my @invalid_tables = grep { !$tables->{$_} } @{ $opts->export };

		if (@invalid_tables) {
			throw_cmd_validation_exception(
				error => 'Invalid table(s) ' . join ', ',
				@invalid_tables . ' in export'
			);
		}
	}

	# Set export to all tables
	if ( $opts->{export_all} ) {
		$opts->{export} = [ keys %{ $ds->tables } ];
	}

	# --- VALIDATE CONDITION OPTION AND ARGS ---

	# Get valid variables for validation
	my @var = @{ $self->get_valid_variables };

	for my $v (@args) {

		# Throw exception if entity_id/visit are supplied as an argument
		if ( $v =~ /^(entity_id|visit)$/ ) {
			throw_cmd_validation_exception( error =>
                         "entity_id and visit (if applicable) are already part of the query set"
			);
		}

		# Throw exception if some invalid variable is supplied as an argument
		if ( !grep( $_ eq $v, @var ) ) {
			throw_cmd_validation_exception(
				error => "Invalid variable '$v' in arguments" );
		}
	}

	my $csv = $cache->{csv};

	# Condition can be imposed on all variables including
	# entity_id and visit (if applicable)
	for my $v ( keys %{ $opts->{cond} } ) {

		# Throw exception if some invalid variable is supplied as argument
		if ( !grep( $_ eq $v, @var ) ) {
			throw_cmd_validation_exception(
				error => "Invalid variable '$v' in condition option" );
		}

		# Regexp to validate condition option
		if ( $opts->{cond}{$v} =~
                     /^\s*(=|\!=|>|<|>=|<=|between|not_between|like|not_like|in|not_in|regexp|not_regexp)\s*,\s*([^\`]+)\s*$/
		  )
		{
			my ( $opr, $val ) = ( $1, $2 );

			# Remove space (if any) following the operator
			$opts->{cond}{$v} =~ s/$opr,\s+/$opr,/;

			# Validating SQL conditions
			if ( $opr && $val && $csv->parse($val) ) {
				my @val = grep ( s/^\s*|\s*$//g, $csv->fields );

    # Operators between and not_between require array but for others it is optional
				if ( $opr =~ /(between)/ && scalar @val != 2 ) {
					throw_cmd_validation_exception( error =>
              "Expecting min and max for '$opr' in '$v' (i.e. 'between, min, max' )"
					);
				}
			}

			else {
				throw_cmd_validation_exception(
					error => "Invalid condition on variable '$v'" );
			}
		}

		else {
			throw_cmd_validation_exception(
				error => "Invalid condition on variable '$v'" );
		}
	}
}

sub run {

	# Overall running of the command
	my ( $self, $opts, @args ) = @_;

	my $rs = $self->process( $opts, @args );

	if ( $opts->{save_command} ) {
		$self->save_command( $opts, @args );
	}

	# If result-set is not empty
	if (@$rs) {

		my $dir;

		if ( $opts->{out} =~ /^(.+)$/ ) {
			$dir = File::Spec->catdir( $1, 'CohortExplorer-' . time . $$ );
		}

		# Create dir to export data
		eval { mkdir $dir };

		if ( catch my $e ) {
			warn $e . "\n";
			$dir = $1;
		}

		else {

			eval { chmod 0777, $dir };

			if ( catch my $e ) {
				warn $e . "\n";
				$dir = $1;
			}
		}

		$self->export( $opts, $rs, $dir, @args );

		return {
			headingText => 'summary statistics',
			rows        => $self->summary_stats( $opts, $rs, $dir )
		  }
		  if ( $opts->{stats} );
	}

	return undef;
}

sub process {

	my ( $self, $opts, @args ) = @_;

	my $cache = $self->cache->get('cache');
	my $ds    = $cache->{datasource};

	##----- PREPARE QUERY PARAMETERS FROM CONDITION OPTION AND ARGS -----##

 # Query parameters can be static, dynamic or both
 # Static type is applicable to 'standard' datasource but it may also be applicable
 # to 'longitudinal' datasource provided the datasource contains tables which
 # are independent of visits (i.e. static tables). Dynamic type only applies to
 # longitudinal datasources
	my $params = $self->create_query_params( $opts, @args );

	my ( $stmt, $var, $sth, @rows );

	# Get comamnd name from ref
	( my $command = lc ref $self ) =~ s/^.+:://;

	# Construct sql query for static/dynamic or both params (if applicable)
	for my $p ( keys %$params ) {
		eval {
			( $params->{$p}{stmt}, @{ $params->{$p}{bind} } ) =
			  $ds->sqla->select( %{ $params->{$p} } );
		};

		if ( catch my $e ) {
			throw_cmd_run_exception( error => $e );
		}

		# Store all variables from sql in hash
		tie my %vars, 'Tie::IxHash',
		  map { $_ => 1 }
		  ( '`entity_id`', $params->{$p}{stmt} =~ /AS\s+(\`[^\`]+\`),?/g );

		# Back quote all bind parameters except 'visit'. 'Visit' is not
		# treated as a variable, only to avoid clash between the
		# variable and table name, what if some table is called visit ??
		my @backquoted_bind = map { s/\'//g; "`$_`" }
		  grep ( $_ ne 'visit', @{ $params->{$p}{bind} } );

		# Get all indices in @bind where variable names or undef is present
		my @var_placeholder =
		  grep ( defined $vars{ $backquoted_bind[$_] }
			  || $backquoted_bind[$_] eq '`undef`',
			0 .. $#backquoted_bind );

		# Remove variable names from placeholders as they need to be hard coded
		if (@var_placeholder) {
			for ( 0 .. $#var_placeholder ) {
				my $count = 0;
				$params->{$p}{stmt} =~
                s/(\?)/$count++ == $var_placeholder[$_]-$_ ? $backquoted_bind[$var_placeholder[$_]] : $1/ge;
				delete( $params->{$p}{bind}->[ $var_placeholder[$_] ] );
			}

			# Update @bind after removal of variable names
			@{ $params->{$p}{bind} } =
			  grep( defined($_), @{ $params->{$p}{bind} } );
		}

		# undef needs to be hard coded as 'is null' or 'is not null'
		# depending on the operator (i.e. = or !=)
		$params->{$p}{stmt} =~ s/\s+=\s+\`undef\`\s+/ IS NULL /g;
		$params->{$p}{stmt} =~ s/\s+!=\s+\`undef\`\s+/ IS NOT NULL /g;

		delete $vars{'`entity_id`'};

		$var->{$p} = [ keys %vars ];
	}

	# Either static or dynamic parameter
	if ( keys %$params == 1 ) {
		$stmt = $params->{ ( keys %$params )[0] }{stmt};
	}

	# Both static and dynamic parameters are present
	else {

  # Give priority to visit dependent tables (i.e. dynamic tables) therefore do left join
  # Inner join is done when conditions are imposed on static tables alone
		$stmt =
		    'SELECT dynamic.entity_id, '
		  . join( ', ', map { @{ $var->{$_} } } keys %$var )
		  . ' FROM '
		  . join(
			(
				(
					(
						!$params->{static}{-having}{entity_id}
						  && keys %{ $params->{static}{-having} } == 1
					)
					  || keys %{ $params->{static}{-having} } > 1
				) ? ' INNER JOIN ' : ' LEFT OUTER JOIN '
			),
			map { "( " . $params->{$_}{stmt} . " ) AS $_" } keys %$params
		  ) . ' ON dynamic.entity_id = static.entity_id';
	}

	my @bind = map { @{ $params->{$_}{bind} } } keys %$params;

	print STDERR "Running the query with "
	  . scalar @bind
	  . " bind variables ...\n\n"
	  if ( $cache->{verbose} );

	require Time::HiRes;

	my $timeStart = Time::HiRes::time();

	eval {
		$sth = $ds->dbh->prepare_cached($stmt);
		$sth->execute(@bind);
	};

	if ( catch my $e ) {
		throw_cmd_run_exception( error => $e );
	}

	my $timeEnd = Time::HiRes::time();

	printf(
		"Found %d rows in %.2f sec matching the %s query criteria ...\n\n",
		( $sth->rows || 0 ),
		( $timeEnd - $timeStart ), $command
	) if ( $cache->{verbose} );

	push @rows, ( $sth->{NAME}, @{ $sth->fetchall_arrayref( [] ) } )
	  if ( $sth->rows );

	$sth->finish;
	return \@rows;
}

sub save_command {

	my ( $self, $opts, @args ) = @_;
	my $cache = $self->cache->get('cache');
	my $alias = $cache->{datasource}->alias;
	my $count = scalar keys %{ $COMMAND_HISTORY->{datasource}{$alias} };
	( my $command = lc ref $self ) =~ s/^.+:://;

	print STDERR "Saving command ...\n\n" if ( $cache->{verbose} );

	require POSIX;

	# Remove the save-command option
	delete $opts->{save_command};

	# Construct the command run by the user and store it in $COMMAND_HISTORY
	for my $opt ( keys %$opts ) {
		if ( ref $opts->{$opt} eq 'ARRAY' ) {
			$command .= " --$opt=" . join( " --$opt=", @{ $opts->{$opt} } );
		}
		elsif ( ref $opts->{$opt} eq 'HASH' ) {
			$command .= join( ' ',
				map ( "--$opt=$_='$opts->{$opt}{$_}' ",
					keys %{ $opts->{$opt} } ) );
		}
		else {
			( $_ = $opt ) =~ s/_/-/g;
			$command .= " --$_=$opts->{$opt} ";
			$command =~ s/($_)=1/$1/
			  if ( $opts->{export_all} || $opts->{stats} );
		}
	}

	$command .= ' ' . join( ' ', @args );
	$command =~ s/\-\-export=[^\s]+\s*/ /g if ( $opts->{export_all} );
	$command =~ s/\s+/ /g;

	for ( keys %{ $COMMAND_HISTORY->{datasource} } ) {
		$COMMAND_HISTORY->{datasource}{$_}{ ++$count } = {
			datetime => POSIX::strftime( '%d/%m/%Y %T', localtime ),
			command  => $command
		  }
		  if ( $_ eq $alias );

	}
}

sub export {

	my ( $self, $opts, $rs, $dir, @args ) = @_;

	my $cache = $self->cache->get('cache');

	##---- WRITE QUERY PARAMETERS FILE -----##

	my $file = File::Spec->catfile( $dir, "QueryParameters" );

	my $fh = FileHandle->new("> $file")
	  or throw_cmd_run_exception( error => "Failed to open file: $!" );

	print $fh "Query Parameters" . "\n\n";
	print $fh "Arguments supplied: " . join( ', ', @args ) . "\n\n";
	print $fh "Conditions imposed: "
	  . scalar( keys %{ $opts->{cond} } ) . "\n\n";

	my @var = keys %{ $opts->{cond} };

	# Write all imposed conditions
	for ( 0 .. $#var ) {
		$opts->{cond}{ $var[$_] } =~ /^\s*([^,]+),\s*(.+)\s*$/;
		print $fh ( $_ + 1 ) . ") $var[$_]: '$1' => $2" . "\n";
	}

	# Write all tables to be exported
	print $fh "\n"
	  . "Tables exported: "
	  . ( $opts->{export} ? join ', ', @{ $opts->{export} } : 'None' ) . "\n";

	$fh->close;

	print STDERR "Exporting query results in $dir ...\n\n"
	  if ( $cache->{verbose} );

	# Process result set and get entities in the result set
	my $rs_entity = $self->process_result( $opts, $rs, $dir, @args );

	if ( $opts->{export} ) {
		my $ds = $cache->{datasource};
		my ( $stmt, @bind, $sth );
		my $struct = $ds->entity_structure;

		# Get column names-SQL pairs
		$struct->{-columns} = $ds->entity_columns;

  # Construct sql query with a placeholder for table name
  # Columns follow the order: entity_id, variable, value and visit (if applicable)
		eval {
			( $stmt, @bind ) = $ds->sqla->select(
				-columns => [
					map { $struct->{-columns}{$_} || 'NULL' }
					  qw/entity_id variable value visit/
				],
				-from  => $struct->{-from},
				-where => {
					%{ $struct->{-where} },
					$struct->{-columns}{table} => { '=' => '?' }
				}
			);
		};

		if ( catch my $e ) {
			throw_cmd_run_exception( error => $e );
		}

		$sth = $ds->dbh->prepare_cached($stmt);

  # The user might have supplied multiple conditions in the where clause
  # of entity_structure() method so split the $stmt by '?' and get the index of
  # 'table' placeholder
		my @chunk = split /\?/, $stmt;
		my ($placeholder) =
		  grep ( $chunk[$_] =~ /\s+$struct->{-columns}{table}\s+=\s+/,
			0 .. $#chunk );

		for my $table ( @{ $opts->{export} } ) {

   # Ensure the user has access to at least one variable in the table to be exported
			if ( grep ( /^$table\..+$/, keys %{ $ds->variables } ) ) {

				# Bind table name
				$bind[$placeholder] = $table;

				eval { $sth->execute(@bind); };

				if ( catch my $e ) {
					throw_cmd_run_exception( error => $e );
				}

				my $rows = $sth->fetchall_arrayref( [] );
				$sth->finish;

				if (@$rows) {

					print STDERR "Exporting $table ...\n\n"
					  if ( $cache->{verbose} );

					# Process table set
					$self->process_table( $table, $rows, $dir, $rs_entity );
				}
				else {
					print STDERR "Omitting $table (no entities) ...\n\n"
					  if ( $cache->{verbose} );
				}

			}
			else {
				print STDERR "Omitting $table (no variables) ...\n\n"
				  if ( $cache->{verbose} );
			}
		}

	}
}

sub summary_stats {

	my ( $self, $opts, $rs, $dir ) = @_;

	my $cache = $self->cache->get('cache');

	print STDERR "Preparing dataset summary statistics ...\n\n"

	  if ( $cache->{verbose} );

	# Create data for calculating summary statistics from the result set
	my ( $data, $key_index, @column ) = $self->create_dataset($rs);

	my $vars = $cache->{datasource}->variables;

	# Open a file for writing descriotive statistics
	my $file = File::Spec->catfile( $dir, "SummaryStatistics.csv" );

	my $fh = FileHandle->new("> $file")
	  or throw_cmd_run_exception( error => "Failed to open file: $!" );

	my $csv = $cache->{csv};

	$csv->print( $fh, \@column )
	  or throw_cmd_run_exception( error => $csv->error_diag );

	push my @summary_stats, [@column];

	# Sort keys (i.e. visit/entity_id) according to their type (text/number)
	my @keys =
	  ( keys %$data )[-1] =~ /^[0-9]+$/
	  ? sort { $a <=> $b } keys %$data
	  : sort keys %$data;

	@column = $key_index == 0 ? @column : splice @column, 1;

	print STDERR "calculating summary statistics for "
	  . ( $#column + 1 )
	  . " query variable(s): "
	  . join( ', ', @column )
	  . " ... \n\n"
	  if ( $cache->{verbose} );

  # Key can be entity_id, visit or none depending on the command (i.e. search/compare) run.
  # For longitudinal datasources the search command calculates statistics with respect to visit,
  # hence the key is visit. Standard datasources are not visit based so no key is used.
  # Compare command uses entity_id as the key when calculating statistics for longitudinal datasources.
	 require Statistics::Descriptive;

	for my $key (@keys) {
		push my @row, ( $key_index == 0 ? () : $key );
		for my $c (@column) {
			my $sdf = Statistics::Descriptive::Full->new;

			# Calculate statistics for integer/decimal variables
			if (   $vars->{$c}
				&& ( $vars->{$c}{type} =~ /(signed|decimal)/i )
				&& scalar @{ $data->{$key}{$c} } > 0 )
			{

				# Remove single/double quotes (if any) from the numeric array
				$sdf->add_data( map { s/[\'\"]+//; $_ }
					  @{ $data->{$key}{$c} } );

				eval {
					push @row,
					  sprintf(
                                                "N: %3s\nMean: %.2f\nMedian: %.2f\nSD: %.2f\nMax: %.2f\nMin: %.2f",
						$sdf->count, $sdf->mean, $sdf->median,
						$sdf->standard_deviation, $sdf->max, $sdf->min );
				};

				if ( catch my $e ) {
					throw_cmd_run_exception($e);
				}
			}

  # Calculate statistics for categorical variables with type 'text' and boolean variables only
			elsif ($vars->{$c}
				&& $vars->{$c}{type} =~ /^char/i
				&& $vars->{$c}{category} )
			{
				my $N = @{ $data->{$key}{$c} } || 1;

				tie my %category, 'Tie::IxHash',
				  map { /^([^,]+),\s*(.+)$/, $1 => $2 } split /\s*\n\s*/,
				  $vars->{$c}{category};

				# Order of categories should remain the same
				tie my %count, 'Tie::IxHash', map { $_ => 0 } keys %category;

				# Get break-down by each category
				for ( @{ $data->{$key}{$c} } ) {
					$count{$_}++;
				}

				push @row,
				  sprintf( "N: %1s\n", scalar @{ $data->{$key}{$c} } )
				  . join "\n", map {
					sprintf( ( $category{$_} || $_ ) . "\: %d (%1.2f%s",
						$count{$_}, $count{$_} * 100 / $N, '%)' )
				  } keys %count;
			}

   # For all other variable types (e.g. date, datetime) get no. of observations alone
			else {
				push @row, sprintf( "N: %3s\n", scalar @{ $data->{$key}{$c} } );
			}
		}

		$csv->print( $fh, \@row )
		  or throw_cmd_run_exception( error => $csv->error_diag );

		push @summary_stats, [@row];
	}

	$fh->close;

	return \@summary_stats;

}

#------------- SUBCLASSES HOOKS -------------#

sub usage_text { }

sub get_valid_variables { }

sub create_query_params { }

sub process_result { }

sub process_table { }

sub create_dataset { }

END {

	# Write saved commands to command history file
	eval {
		$COMMAND_HISTORY_CONFIG->save_file( $COMMAND_HISTORY_FILE,
			$COMMAND_HISTORY );
	};

	if ( catch my $e ) {
		throw_cmd_run_exception( error => $e );
	}

}

#-------
1;

__END__

=pod

=head1 NAME

CohortExplorer::Command::Query - CohortExplorer base class to search and compare command classes

=head1 DESCRIPTION

This class serves as the base class to search and compare command classes. The class is inherited from L<CLI::Framework::Command> and overrides the following methods:

=head2 option_spec()

Returns application option specifications as expected by L<Getopt::Long::Descriptive>

       ( 
         [ 'cond|c=s%'      => 'impose conditions'                            ],
         [ 'out|o=s'        => 'provide output directory'                     ],
         [ 'save-command|s' => 'save command'                                 ],
         [ 'stats|S'        => 'show summary statistics'                      ],
         [ 'export|e=s@'    => 'export tables by name'                        ],
         [ 'export-all|a'   => 'export all tables'                            ] 
       )

=head2 validate( $opts, @args )

This method validates the command options and arguments and throws exceptions when validation fails.

=head2 run( $opts, @args )

This method is responsible for the overall functioning of the command. The method calls option specific methods for option specific processing.


=head1 OPTION SPECIFIC PROCESSING

=head2 process( $opts, @args )

The method attempts to construct the SQL query from the hash ref returned by L<create_query_params|/create_query_params( $opts, @args )>. Upon successful execution of the SQL query the method returns the result set (C<$rs>) which is a ref to array of arrays where each array corresponds to data on one entity or entity-visit combination (if applicable).

=head2 save_command( $opts, @args)

This method is only called if the user has specified the save command option (C<--save-command>). The method first constructs the command from command options and arguments (C<$opts> and C<@args>) and adds the command to C<$COMMAND_HISTORY> hash along with the datetime information. C<$COMMAND_HISTORY> contains all commands previously saved by the user. 

=head2 export( $opts, $rs, $dir, @args )

This method creates a export directory in the directory specified by C<--out> option and calls L<process_result|/process_result( $opts, $rs, $dir, @args )> method in the subclass. Further processing by the method depends on the presence of C<--export> option(s). If the user has specified C<--export> option, the method first constructs the SQL query from the hash ref returned by L<entity_structure|CohortExplorer::Datasource/entity_structure()> with a placeholder for table name. The method executes the same SQL query with a different bind value (table name) depending on the number of tables to be exported. The output obtained from successful execution of SQL is passed to L<process_table|/process_table( $table, $td, $dir, $rs_entity )> for further processing.

=head2 summary_stats( $opts, $rs, $dir )

This method is only called if the user has specified summary statistics option (C<--stats>). The method attempts to calculate statistics from the data frame returned by L<create_dataset|/create_dataset( $rs )>.


=head1 SUBCLASS HOOKS

=head2 usage_text()

This method should return the usage information for the command.

=head2 get_valid_variables()

This method should return a ref to the list of variables for validating arguments and condition option(s).

=head2 create_query_params( $opts, @args )

This method should return a hash ref with keys, C<static>, C<dynamic>, or both depending on the datasource type and variables supplied as arguments and conditions. As standard datasource only contains static tables so the hash ref must contain only one key, C<static> where as a longitudinal datasource may contain both keys, C<static> and C<dynamic> provided the datasource has static tables. The value of each key comprises of SQL parameters such as C<-from>, C<-where>, C<-group_by> and C<-order_by>. The parameters to this method are as follows:

C<$opts> an options hash with the user-provided command options as keys and their values as hash values.

C<@args> arguments to the command.

=head2 process_result( $opts, $rs, $dir, @args )

This method should process the result set obtained after running the SQL query and write the results into a csv file. If the variables provided as arguments and conditions belong to static tables, the method should return a ref to list of entities present in the result set otherwise return a hash ref with C<entity_id> as keys and their visit numbers as values.

In this method, 

C<$opts> an options hash with the user-provided options as keys and their values as hash values.

C<$rs> is the result set obtained upon executing the SQL query. 

C<$dir> is the export directory.

C<@args> arguments to the command.

=head2 process_table( $table, $ts, $dir, $rs_entity )

This method should process the table data obtained from running the export SQL query. The method should write the table data for all entities present in the result set into a csv file.

The parameters to the method are:

C<$table> is the name of the table to be exported.

C<$ts> is the table set obtained from executing the table export SQL query.

C<$dir> is the export directory.

C<$rs_entity> is a ref to a list/hash containing entities present in the result set.

=head2 create_dataset( $rs )

This method should create a data frame for calculating statistics. The method should return a hash ref where key is the parameter, the statistics are calculated with respect to and value is the variable name-value pairs.

=head1 DIAGNOSTICS

CohortExplorer::Command::Query throws following exceptions imported from L<CLI::Framework::Exceptions>:

=over

=item 1

C<throw_cmd_run_exception>: This exception is thrown if one of the following conditions are met:

=over

=item *

The command history file fails to load. For the save command option to work it is expected that the file C<$HOME/.CohortExplorer_History> exists with RWX enabled for CohortExplorer.

=item *

C<select> method in L<SQL::Abstract::More> fails to construct the SQL from the supplied hash ref.

=item *

C<execute> method in L<DBI> fails to execute the SQL query.

=item *

The full methods in package L<Statistics::Descriptive> fail to calculate statistics.

=back

=item 2

C<throw_cmd_validation_exception>: This exception is thrown whenever the command options/arguments fail to validate.

=back

=head1 DEPENDENCIES

L<CLI::Framework::Command>

L<CLI::Framework::Exceptions>

L<Config::General>

L<DBI>

L<Exception::Class::TryCatch>

L<FileHandle>

L<File::HomeDir>

L<File::Spec>

L<SQL::Abstract::More>

L<Statistics::Descriptive>

L<Text::CSV_XS>

L<Tie::IxHash>

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
