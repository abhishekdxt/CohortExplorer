package CohortExplorer::Datasource;

use strict;
use warnings;

our $VERSION = 0.13;

use Carp;
use Config::General;
use CLI::Framework::Exceptions qw ( :all);
use DBI;
use Exception::Class::TryCatch;
use SQL::Abstract::More;
use Tie::Autotie 'Tie::IxHash';

#-------

sub initialize {

	my ( $class, $opts, $config_file ) = @_;

	my $config;

	# Get supplied datasource's configuration from the config file
	eval {
		$config = {
			Config::General->new(
				-ConfigFile            => $config_file,
				-LowerCaseNames        => 1,
				-MergeDuplicateBlocks  => 1,
				-MergeDuplicateOptions => 1
			  )->getall
		}->{datasource}{ $opts->{datasource} };
	};

	if ( catch my $e ) {
		throw_app_init_exception( error => $e );
	}

	if ( !$config ) {
		throw_app_init_exception(
		 error => "Invalid datasource '$opts->{datasource}'" );
	}

	if ( !$config->{namespace} ) {
		throw_app_init_exception( error =>
                 "Mandatory parameter 'namespace' is missing from '$opts->{datasource}'"
		);
	}

	# Untaint and load the datasource package
	if ( $config->{namespace} =~ /^(.+)$/g ) {
		  eval "require $1";
	}

	# Add name and alias to config
	$config->{name} ||= $opts->{datasource};
	$config->{alias} = $opts->{datasource};

	# Connect to the database
	eval {
		$config->{dbh} = DBI->connect(
			@{$config}{qw/dsn username password/},
			{ PrintError => 0, RaiseError => 1 }
		);
	};

	if ( catch my $e ) {
		throw_app_init_exception( error => $e );
	}

	# Add sqla object
	$config->{sqla} = SQL::Abstract::More->new(
		sql_dialect => $config->{dialect} ||= 'MySQL_old',
		max_members_IN => 100
	);

	# Instantiate datasource
	my $obj = $1->new($config)
	  or croak "Failed to instantiate datasource package '$1' via new(): $!";

	$obj->_process($opts);
	return $obj;
}

sub _process {

	my ( $ds, $opts ) = @_;

	my $class = ref $ds;

	if ( $opts->{verbose} ) {
		print STDERR
		"Authenticating $opts->{username}\@$opts->{datasource} ...\n";
	}

	my $response = $ds->authenticate($opts);

	# Successful authentication returns a defined response
	if ( !$response ) {
		throw_app_init_exception( error =>
                 "Either the username/password combination is incorrect or you do not seem to have the correct permission to query the datasource"
		);
	}

	if ( $opts->{verbose} ) {
		print STDERR
                 "Initializing application for $opts->{username}\@$opts->{datasource} ...\n";
	}

 # Response is passed to additional_params as it may contain
 # some data which the subclass hook can use to fetch other paramaters
	my $additional_params = $ds->additional_params( $opts, $response );

	if ( ref $additional_params ne 'HASH' ) {
		throw_app_hook_exception( error =>
                 "List returned by method 'additional_params' in class '$class' is not hash-worthy"
		);
	}

	# Add return by additional_params to datasource
	@$ds{ keys %$additional_params } =
	  @$additional_params{ keys %$additional_params };

	if ( !$ds->type || $ds->type !~ /^(standard|longitudinal)$/ ) {
		throw_app_hook_exception( error =>
                   "Datasource type (i.e. standard/longitudinal) is not specified for datasource '$opts->{datasource}'"
		);
	}

	# Check all subclass hooks for mandatory SQL parameters
	for my $p (qw/entity table variable/) {
		my $method = $p . '_structure';
		my $struct = $ds->$method;

		for (qw/-columns -from -where/) {
			if ( !$struct->{$_} ) {
				throw_app_hook_exception( error =>
					  "'$_' missing in method '$method' in class '$class'" );
			}
		}

		# -columns field is expected to be hash-worthy
		if ( scalar @{ $struct->{-columns} } % 2 != 0 ) {
			throw_app_hook_exception( error =>
                         "'-columns' in method '$method' in class '$class' is not hash-worthy"
			);
		}

		# Transform -columns to hashref
		my $map = $ds->_list_to_hashref( $method, @{ $struct->{-columns} } );

		# Set parameters specific to subclass hook's name
		$method = 'set_' . $p . '_params';

		$ds->$method( $struct, $map );
	}

	if ( $ds->type eq 'longitudinal' ) {

	 # Visit variables are only set if the datasource is longitudinal and contains
	 # data on at least 2 visits; visit variables are used in compare command
		if ( $ds->visit_max && $ds->visit_max > 1 ) {
			 $ds->set_visit_variables;
		}

  # If visit max is either undefined or <= 1 set datasource type to standard
  # At the start of some study there may not be any data in any dynamic table or
  # data may only be available for the first visit
		if ( ( $ds->visit_max && $ds->visit_max < 2 ) || !$ds->visit_max ) {
			   $ds->{type} = 'standard';

			if ( $opts->{verbose} ) {
				print STDERR "Datasource is set to standard because visit max is either undefined or less than 2 ...\n";
			}
		}
	}

}

# Convert a list to a HASH ref if list is hash-worthy
sub _list_to_hashref {

	my ( $ds, $method, @map ) = @_;

	my $class = ref $ds;

	# Preserve column order
	tie my %h, 'Tie::IxHash';

	for my $i ( 0 .. $#map - 1 ) {
		if ( $i % 2 == 0 ) {
			my ( $k, $v ) = ( lc $map[$i], $map[ $i + 1 ] );

			# Throw exception if list contains duplicate column names
			if ( exists $h{$k} ) {
				throw_app_hook_exception( error =>
                                 "List returned by '$method' in class '$class' is not hash-worthy (duplicate keys for $i)"
				);
			}
			$h{$k} = $v;
		}
	}

	return \%h;
}

sub set_entity_params {

	my ( $self, $struct, $map ) = @_;
	my $class = ref $self;

	# Check all mandatory columns are present
	for my $c (
		$self->type eq 'standard'
		? qw/entity_id table variable value/
		: qw/entity_id table variable value visit/
	  )
	{
		if ( !$map->{$c} ) {
			throw_app_hook_exception( error =>
                         "Column '$c' in method 'entity_structure' in class '$class' is not defined"
			);
		}
	}

	# Add column name-SQL pair to datasource for use
	# within search/compare commands
	$self->{entity_columns} = $map;

	##----- SQL TO FETCH ENTITY_COUNT & VISIT MAX -----##

	# If visit max is not defined by the sub-class,
	# create visit_max using 'visit' column
	$struct->{-columns} = [
		" COUNT( DISTINCT $map->{entity_id} ) ",
		       $self->visit_max
		  || ( $map->{visit} ? " MAX( DISTINCT $map->{visit} + 0 ) " : 'NULL' )
	];

	my ( $stmt, @bind );

	eval { ( $stmt, @bind ) = $self->sqla->select(%$struct); };

	if ( catch my $e ) {
		throw_app_hook_exception( error => $e );
	}

	eval {
		@{$self}{qw/entity_count visit_max/} =
		  $self->dbh->selectrow_array( $stmt, undef, @bind );
	};

	if ( catch my $e ) {
		throw_app_hook_exception( error => $e );
	}

	# Validate entity_count
	if ( $self->{entity_count} == 0 ) {
		throw_app_init_exception(
	         error => "No entities are found in datasource '" . $self->name . "'" );
	}
}

sub set_table_params {

	my ( $self, $struct, $map ) = @_;
	my $class   = ref $self;
	my $ds_name = $self->name;

	# Throw exception if column 'table' is not defined
	if ( !$map->{table} ) {
		throw_app_hook_exception( error =>
                 "Column 'table' in method 'table_structure' in class '$class' is not defined"
		);
	}

	##----- SQL TO FETCH TABLE DATA -----##

	my ( $stmt, @bind, $table );

	# Format column name-SQL pairs along the lines of columns in
	# SQL::Abstract::More (add names as aliases really)
	$struct->{-columns} = [ map { "$map->{$_}|`$_`" } keys %$map ];

	eval { ( $stmt, @bind ) = $self->sqla->select(%$struct); };

	if ( catch my $e ) {
		throw_app_hook_exception( error => $e );
	}

	eval {
		$table =
		  $self->dbh->selectall_arrayref( $stmt, { Slice => {} }, @bind );
	};

	if ( catch my $e ) {
		throw_app_hook_exception( error => $e );
	}

	if ( @$table == 0 ) {
		throw_app_init_exception( error =>
		 "No accessible tables are found in datasource '$ds_name'" );
	}

	# Preserve order of tables
	tie %{ $self->{tables} }, 'Tie::IxHash';

	for my $t (@$table) {
		for my $c ( keys %$map ) {
			$self->{tables}{ $t->{table} }{$c} = $t->{$c};
		}
	}
}

sub set_variable_params {

	my ( $self, $struct, $map ) = @_;
	my $class   = ref $self;
	my $ds_name = $self->name;

	# Throw exception if column 'table' or 'variable' is not defined
	if ( !$map->{table} || !$map->{variable} ) {
		 throw_app_hook_exception( error =>
                  "No mapping found for column 'table' or 'variable' in method 'variable_structure' in class '$class'"
		);
	}

	# Add column name-SQL pair to datasource for use
	# within find command
	$self->{variable_columns} = $map;

	##----- SQL TO FETCH TABLE DATA -----##

	my ( $stmt, @bind, $variable );

	# Format column name-SQL pairs along the lines of columns in
	# SQL::Abstract::More
	$struct->{-columns} = [ map { "$map->{$_}|`$_`" } keys %$map ];

	eval { ( $stmt, @bind ) = $self->sqla->select(%$struct); };

	if ( catch my $e ) {
		throw_app_hook_exception( error => $e );
	}

	eval {
		$variable =
		  $self->dbh->selectall_arrayref( $stmt, { Slice => {} }, @bind );
	};

	if ( catch my $e ) {
		throw_app_hook_exception( error => $e );
	}

	if ( @$variable == 0 ) {
		throw_app_init_exception( error =>
		 "No accessible variables are found in datasource '$ds_name'" );
	}

	# Get the variable data type to sql data type mapping
	my $datatype_map = $self->datatype_map;

	if ( ref $datatype_map ne 'HASH' ) {
		throw_app_init_exception( error =>
                 "Return by method 'datatype_map' in class '$class' is not hash-worthy"
		);
	}

	# Preserve order of variables
	tie %{ $self->{variables} }, 'Tie::IxHash';

	for my $v (@$variable) {

		if ( !$v->{table} || !$v->{variable} ) {
		      throw_app_init_exception( error =>
		      "Undefined table/variable found in datasource '$ds_name'" );
		}

		# Add table-variable combination as key because
		# variables across tables may have same names
		my $k = $v->{table} . '.' . $v->{variable};

		for my $c ( keys %$map ) {
   # Add all variable attributes
			$self->{variables}{$k}{$c} = $v->{$c};
		}

		# Set 'category' and 'type' attributes if not set
		$self->{variables}{$k}{category} = $v->{'category'} || undef,
		$self->{variables}{$k}{type} =
		  $datatype_map->{ $v->{type} } || 'CHAR(255)';
	}
}

sub new {

	return bless $_[1], $_[0];
}

sub set_visit_variables {

	my ($self) = @_;

	my @static_table = @{ $self->static_tables || [] };
	my $vars         = $self->variables;
	my $visit_max    = $self->visit_max;

	# Add suffix (vAny, vLast, v1, v2 etc.) to variables
	# belonging to dynamic tables
	for my $v ( keys %$vars ) {
		if ( !grep( $_ eq $vars->{$v}{table}, @static_table ) ) {
			for my $i ( qw(Any Last), 1 .. $self->visit_max ) {
				push @{ $self->{visit_variables} }, "v$i.$v";
			}
		}
	}
}

sub DESTROY {

	my ($ds) = @_;

	$ds->dbh->disconnect if ( $ds->dbh );

}

sub AUTOLOAD {

	my ($ds) = @_;

	our $AUTOLOAD;

	( my $param = lc $AUTOLOAD ) =~ s/.*:://;

	return $ds->{$param} || undef;
}

#--------- SUBCLASSES HOOKS --------#

sub authenticate { 1 }

sub additional_params { {} }

sub entity_structure { }

sub table_structure { }

sub variable_structure { }

sub datatype_map { {} }

#-------
1;

__END__

=pod

=head1 NAME

CohortExplorer::Datasource - CohortExplorer datasource superclass

=head1 SYNOPSIS

    # The code below shows methods your datasource class overrides

    package CohortExplorer::Application::My::Datasource;
    use base qw( CohortExplorer::Datasource );

    sub authenticate { 
        
        my ($self, $opts) = @_;
                
          # authentication code...

          # Successful authentication returns a scalar response (e.g. project_id)
          return $response
        
    }

    sub additional_params {
        
         my ($self, $opts, $response) = @_;
          
         my %params;

         # Get database handle (i.e. $self->dbh) and run some SQL queries to get additional parameters
         # to be used in entity/variable/table structure hooks
          
         return \%params;
    }
    
    sub entity_structure {
         
         my ($self) = @_;
         
         my %struct = (
                      -columns =>  {
                                     entity_id => 'd.record',
                                     variable => 'd.field_name',
                                     value => 'd.value',
                                     table => 'm.form_name'
                       },
                       -from =>  [ -join => qw/data|d <=>{project_id=project_id} metadata|m/ ],
                       -where =>  { 
                                       'd.project_id' => $self->project_id
                        }
          );
          
          return \%struct;
     }
     
         
    sub table_structure {
         
         my ($self) = @_;
         
         return {
                 
                  -columns => {
                                 table => 'GROUP_CONCAT( DISTINCT form_name )', 
                                 variable_count => 'COUNT( field_name )',
                                 label => 'element_label'
                  },
                 -from  => 'metadata'',
                 -where => {
                             project_id => $self->project_id
                  },
                 -order_by => 'field_order',
                 -group_by => 'form_name'
        };
     }
     
     sub variable_structure {
         
         my ($self) = @_;
         
         return {
                 -columns => {
                               variable => 'field_name',
                               table => 'form_name',
                               label => 'element_label',
                               type => "IF( element_validation_type IS NULL, 'text', element_validation_type)",
                               category => "IF( element_enum like '%, %', REPLACE( element_enum, '\\\\n', '\n'), '')"
                 },
                -from => 'metadata',
                -where => { 
                             project_id => $self->project_id
                 },
                -order_by => 'field_order'
        };
     }
     
     sub datatype_map {
        
       return {
                  int         => 'signed',
                 float        => 'decimal',
                 date_dmy     => 'date',
                 date_mdy     => 'date',
                 date_ymd     => 'date',
                 datetime_dmy => 'datetime'
       };
    }
    
=head1 DESCRIPTION

CohortExplorer::Datasource is the base class for all datasources. When connecting CohortExplorer to EAV repositories other than L<Opal (OBiBa)|http://obiba.org/node/63/> and L<REDCap|http://project-redcap.org/> the user is expected to create a class which inherits from CohortExplorer::Datasource. The datasources stored in Opal and REDCap can be queried using the in-built L<Opal|CohortExplorer::Application::Opal::Datasource> and L<REDCap|CohortExplorer::Application::REDCap::Datasource> API (see L<here|http://www.youtube.com/watch?v=Tba9An9cWDY>).


=head1 OBJECT CONSTRUCTION

=head2 initialize( $opts, $config_file )

CohortExplorer::Datasource is an abstract factory; C<initialize()> is the factory method that constructs and returns an object of the datasource supplied as an application option. This class reads the datasource configuration from the config file C<datasource-config.properties> to instantiate the datasource object. A sample config file is shown below:

        <datasource datasourceA> 
         namespace=CohortExplorer::Application::Opal::Datasource
         url=http://example.com
         entity_type=Participant
         dsn=DBI:mysql:database=opal;host=hostname;port=3306
         username=database_username
         password=database_password
       </datasource> 

       <datasource datasourceB> 
         namespace=CohortExplorer::Application::Opal::Datasource
         url=http://example.com
         entity_type=Instrument
         dsn=DBI:mysql:database=opal;host=hostname;port=3306
         username=database_username
         password=database_password
         name=datasourceA
       </datasource> 

       <datasource datasourceC> 
         namespace=CohortExplorer::Application::REDCap::Datasource
         dsn=DBI:mysql:database=opal;host=myhost;port=3306
         username=database_username
         password=database_password
       </datasource>

Each block holds a unique datasource configuration. In addition to reserve parameters C<namespace>, C<name>, C<dsn>, C<username>, C<password>, C<static_tables> and C<visit_max> it is up to the user to decide what other parameters they want to include in the configuration file. If the block name is an alias the user can specify the actual name of the datasource using C<name> parameter. If C<name> parameter is not found the block name is assumed to be the actual name of the datasource. In the example above, both datasourceA and datasourceB connect to the same datasource (i.e. datasourceA) but with different configuration, datasourceA is configured to query the participant data where as, datasourceB can be used to query the instrument data. Once the class has instantiated the datasource object, the user can access the parameters by simply calling the accessors which have the same name as the parameters. For example, the database handle can be retrieved by C<$self-E<gt>dbh> and entity_type by C<$self-E<gt>entity_type>. 

The namespace is the full package name of the in-built API the application will use to consult the parent EAV schema. The parameters present in the configuration file can be used by the subclass hooks to provide user or project specific functionality.

=head2 new()

    $object = $ds_pkg->new();

Basic constructor.

=head1 PROCESSING

After instantiating the datasource object, the class first calls L<authenticate|/authenticate( $opts )> to perform the user authentication. If the authentication is successful (i.e. returns a defined C<$response>), it sets some additional parameters, if any ( via L<additional_params|/additional_params( $opts, $response )>). The subsequent steps include calling methods; L<entity_structure|/entity_structure()>, L<table_structure|/table_structure()>, L<variable_structure|/variable_structure()>, L<datatype_map|/datatype_map()> and validating the return by each method. Upon successful validation the class attempts to set entity, table and variable specific parameters by invoking the methods below:

=head2 set_entity_params( $struct )

This method attempts to retrieve the entity parameters such as C<entity_count> and C<visit_max> (if applicable) from the database. The method accepts the input from L<entity_structure|/entity_structure()> method.

=head2 set_table_params( $struct )

This method attempts to retrieve data on table and table attributes from the database. The method accepts the input from L<table_structure|/table_structure()> method. 

=head2 set_variable_params( $struct )

This method attempts to retrieve data on variable and variable attributes from the database. The method accepts the input from L<variable_structure|/variable_structure()> method. 

=head2 set_visit_variables()

This method attempts to set the visit variables. The method is called only if the datasource is longitudinal with data on at least 2 visits. The visit variables are valid to dynamic tables and represent the visit transformation of variables (e.g. vAny.var, vLast.var, v1.var ... vMax). The prefix C<vAny> implies any visit, C<vLast> last visit, C<v1> first visit and C<vMax> the maximum visit for which data is available in the database. The L<compare|CohortExplorer::Command::Query::Compare> command allows the use of visit variables when searching for entities of interest.

=head1 SUBCLASS HOOKS

The subclasses override the following hooks:

=head2 authenticate( $opts )

This method should return a scalar response upon successful authentication otherwise return C<undef>. The method is called with one parameter, C<$opts> which is a hash with application options as keys and their user-provided values as hash values. B<Note> the methods below are only called if the authentication is successful.

=head2 additional_params( $opts, $response )

This method should return a hash ref containing parameter name-value pairs. Not all parameter values are known in advance so they can not be specified in the datasource configuration file. Sometimes the value of some parameter first needs to be retrieved from the database (e.g. variables and records a given user has access to). This hook can be used specifically for this purpose. The user can run some SQL queries to retrieve values of the parameters they want to add to the datasource object. The parameters used in calling this method are:
   
C<$opts> a hash with application options as keys and their user-provided values as hash values.

C<$response> a scalar received upon successful authentication. The user may want to use the scalar response to fetch other parameters (if any).

=head2 entity_structure()

The method should return a hash ref defining the entity structure in the database. The hash ref must have the following keys:

=over

=item B<-columns> 

C<entity_id>
 
C<variable> 

C<value>

C<table> 

C<visit> (valid to longitudinal datasources)

=item B<-from>

table specifications (see L<SQL::Abstract::More|SQL::Abstract::More/Table_specifications>)

=item B<-where> 

where clauses (see L<SQL::Abstract|SQL::Abstract/WHERE_CLAUSES>)

=back

=head2 table_structure()

The method should return a hash ref defining the table structure in the database. C<table> in this context implies questionnaires or forms. For example,

      {
          -columns => [
                        table => 'GROUP_CONCAT( DISTINCT form_name )', 
                        variable_count => 'COUNT( field_name )',
                        label => 'element_label'
          ],
         -from  => 'metadata',
         -where => {
                     project_id => $self->project_id
         },
        -order_by => 'field_order',
        -group_by => 'form_name'

      }

the user should make sure the SQL query constructed from hash ref is able to produce the output like the one below:

       +-------------------+-----------------+------------------+
       | table             | variable_count  | label            |
       +-------------------+-----------------+------------------+
       | demographics      |              26 | Demographics     |
       | baseline_data     |              19 | Baseline Data    |
       | month_1_data      |              20 | Month 1 Data     |
       | month_2_data      |              20 | Month 2 Data     |
       | month_3_data      |              28 | Month 3 Data     |
       | completion_data   |               6 | Completion Data  |
       +-------------------+-----------------+------------------+

B<Note> C<-columns> hash must contain C<table> definition. It is up to the user to decide what table attributes they think are suitable for the description of tables.

=head2 variable_structure()

This method should return a hash ref defining the variable structure in the database. For example,

         {
             -columns => [
                            variable => 'field_name',
                            table => 'form_name',
                            label => 'element_label',
                            category => "IF( element_enum like '%, %', REPLACE( element_enum, '\\\\n', '\n'), '')",
                            type => "IF( element_validation_type IS NULL, 'text', element_validation_type)"
             ],
            -from => 'metadata',
            -where => { 
                        project_id => $self->project_id
             },
             -order_by => 'field_order'
         }

the user should make sure the SQL query constructed from the hash ref is able to produce the output like the one below:

       +---------------------------+---------------+-------------------------+---------------+----------+
       | variable                  | table         |label                    | category      | type     |
       +---------------------------+---------------+-------------------------+---------------------------
       | kt_v_b                    | baseline_data | Kt/V                    |               | float    |
       | plasma1_b                 | baseline_data | Collected Plasma 1?     | 0, No         | text     |
       |                           |               |                         | 1, Yes        |          |
       | date_visit_1              | month_1_data  | Date of Month 1 visit   |               | date_ymd |
       | alb_1                     | month_1_data  | Serum Albumin (g/dL)    |               | float    |
       | prealb_1                  | month_1_data  | Serum Prealbumin (mg/dL)|               | float    |
       | creat_1                   | month_1_data  | Creatinine (mg/dL)      |               | float    |
       +---------------------------+---------------+-----------+-------------------------------+--------+

B<Note> C<-columns> hash must define C<variable> and C<table> columns. Again it is up to the user to decide what variable attributes they think define the variables in the datasource. The categories within C<category> column must be separated by newline.          
          
=head2 datatype_map()

This method should return a hash ref with value types as keys and equivalent SQL types (i.e. castable) as hash values. For example, 

      {
          'int'        => 'signed',
          'float'      => 'decimal',
          'number_1dp' => 'decimal(10,1)',
          'datetime'   => 'datetime'
      }


By default, the value type is assumed to be varchar(255).

=head1 DIAGNOSTICS

=over

=item *

L<Config::General> fails to parse the datasource configuration file.

=item *

Failed to instantiate datasource package '<datasource pkg>' via new().

=item *

Return by methods C<additional_params>, C<entity_structure>, C<table_structure>, C<variable_structure> and C<datatype_map> is either not hash-worthy or contains missing columns.

=item *

C<select> method in L<SQL::Abstract::More> fails to construct the SQL query from the supplied hash ref.

=item *

C<execute> method in L<DBI> fails to execute the SQL query.

=back

=head1 DEPENDENCIES

L<Carp>

L<CLI::Framework::Exceptions>

L<Config::General>

L<DBI>

L<Exception::Class::TryCatch>

L<SQL::Abstract::More>

L<Tie::IxHash>

=head1 SEE ALSO

L<CohortExplorer>

L<CohortExplorer::Application::Opal::Datasource>

L<CohortExplorer::Application::REDCap::Datasource>

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
the " Artistic Licence ".

=back

=head1 AUTHOR

Abhishek Dixit

=cut
