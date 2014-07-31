package CohortExplorer::Application::REDCap::Datasource;

use strict;
use warnings;

our $VERSION = 0.13;

use base qw(CohortExplorer::Datasource);

#-------

sub authenticate {

	my ( $self, $opts ) = @_;

	my $legacy_hash;

	# Get the value of legacy_hash if it exists
	if (
		$self->dbh->selectrow_arrayref(
			"SHOW COLUMNS FROM redcap_auth like 'legacy_hash'")
	  )
	{
		($legacy_hash) = $self->dbh->selectrow_array(
			"SELECT legacy_hash FROM redcap_auth WHERE username = ?",
			undef, $opts->{username} );
	}

	my $stmt =
      "SELECT rp.project_id, rur.data_export_tool, rur.group_id FROM redcap_auth AS ra INNER JOIN redcap_user_rights AS rur ON ra.username = rur.username INNER JOIN redcap_projects AS rp ON rur.project_id=rp.project_id WHERE rp.project_name = ? AND rur.data_export_tool != 0 AND ra.username = ? AND ra.password = "
	  . (
		defined $legacy_hash
		? (
			$legacy_hash == 0
			? "SHA2(CONCAT(?, ra.password_salt), 512)"
			: "MD5(CONCAT(?, ra.password_salt))"
		  )
		: '?'
	  )
	  . " AND ( rp.project_id NOT IN ( SELECT project_id FROM redcap_external_links_exclude_projects ) AND ( rur.expiration <= CURDATE() OR rur.expiration IS NULL))";

	return $self->dbh->selectrow_hashref( $stmt, undef, $self->name,
		@{$opts}{qw/username password/} );
}

sub additional_params {

	my ( $self, $opts, $response ) = @_;

 # Get static tables and dynamic_event_ids (i.e. comma separated event_ids of all repeating forms)
	my $row = $self->dbh->selectall_arrayref(
        "SELECT GROUP_CONCAT( DISTINCT IF (form_count = 1, form, NULL)) AS static_tables,  GROUP_CONCAT(DISTINCT IF (form_count > 1, dynamic_event_id, NULL)) AS dynamic_event_ids FROM ( SELECT GROUP_CONCAT( DISTINCT ref.form_name ) AS form, MIN(ref.event_id) AS dynamic_event_id, COUNT(ref.event_id) AS form_count, GROUP_CONCAT( DISTINCT rea.arm_id ORDER BY rea.arm_id ) AS arm FROM redcap_events_forms AS ref INNER JOIN redcap_events_metadata AS rem ON ref.event_id=rem.event_id INNER JOIN redcap_events_arms AS rea ON rea.arm_id = rem.arm_id WHERE rea.project_id = ? GROUP BY ref.form_name, rea.arm_id ORDER BY ref.event_id) AS x GROUP BY arm ORDER BY arm",
		undef, $response->{project_id}
	);

	die "Currently querying multiple arms is not supported\n" if ( @$row > 1 );

	if (@$row) {

		# Split and sort dynamic event_ids
		my @dynamic_event_id = sort split /,/, $row->[0][1];
		$response->{type} = 'longitudinal';

		# init_event_id is the event_id of the first repeating form
		$response->{init_event_id} = $dynamic_event_id[0];
		$response->{arms}          = @$row;
		$response->{static_tables} = [ split /,/, $row->[0][0] ]
		  if ( $row->[0][0] );
	}

	else {
		$response->{type} = 'standard';
	}

 # Get a list of records/entities the user has access to
	if ( $response->{group_id} ) {
		 $response->{allowed_records} = $self->dbh->selectcol_arrayref(
           "SELECT record FROM redcap_data WHERE project_id = ? AND field_name = '__GROUPID__' AND value = ?",
			undef, @{$response}{qw/project_id group_id/}
		);
	}

	return $response;
}

sub entity_structure {

	my ($self) = @_;

	my %struct = (
		-columns => [
			entity_id => 'rd.record',
			variable  => 'rd.field_name',
			value     => 'rd.value',
			table     => 'form_name'
		],
		-from => [
			-join => (
				$self->type eq 'standard'
				? qw/redcap_data|rd <=>{project_id=project_id} redcap_metadata|rm/
				: qw/redcap_data|rd <=>{event_id=event_id} redcap_events_forms|ref/
			  )

		],
		-where => $self->allowed_records
		? {
			'rd.project_id' => $self->project_id,
			'rd.record'     => { -in, $self->allowed_records },
		  }
		: { 'rd.project_id' => $self->project_id }
	);

	# Add visit column if the datasource is longitudinal
	# Visit number is determined using the init_event_id
	if ( $self->type eq 'longitudinal' ) {

		if ( $self->static_tables ) {
			push @{ $struct{-columns} },
			  (
				'visit',
				'IF (form_name IN ('
				  . join( ',', map { "'$_'" } @{ $self->static_tables } )
				  . '), NULL, rd.event_id - '
				  . $self->init_event_id
				  . ' + 1 )'
			  )

		}
		else {
			push @{ $struct{-columns} },
			  ( 'visit', 'rd.event_id - ' . $self->init_event_id . ' + 1 ' );
		}

	}

	return \%struct;
}

sub table_structure {

	my ($self) = @_;

	my @column = (
		arm            => "GROUP_CONCAT( DISTINCT rea.arm_name)",
		table          => 'GROUP_CONCAT( DISTINCT rm.form_name)',
		label          => 'GROUP_CONCAT( DISTINCT rm.form_menu_description)',
		variable_count => 'COUNT( DISTINCT rm.field_name)',
		event_count    => 'COUNT( DISTINCT rem.day_offset)',
		event_description =>
        "GROUP_CONCAT(DISTINCT CONCAT( rem.descrip, '(', rem.day_offset,')' ) ORDER BY rem.day_offset SEPARATOR '\n ')"
	);

	if ( $self->type eq 'longitudinal' ) {

		return {

			-columns => \@column,
			-from    => [
				-join =>
				  qw/redcap_metadata|rm <=>{form_name=form_name} redcap_events_forms|ref <=>{event_id=event_id} redcap_events_metadata|rem <=>{arm_id=arm_id} redcap_events_arms|rea/
			],
			-order_by => $self->arms
			? [qw/rea.arm_id rm.form_name rem.day_offset/]
			: 'rm.form_name',
			-group_by => $self->arms ? [qw/rea.arm_id rm.form_name/]
			: 'rem.day_offset',
			-having => { 'variable_count' => { '>', 0 } },
			-where  => $self->data_export_tool == 1
			? {
				'rm.project_id'  => $self->project_id,
				'rea.project_id' => { -ident => 'rm.project_id' }
			  }
			: {
				'rm.project_id'  => $self->project_id,
				'rm.field_phi'   => { '=', undef },
				'rea.project_id' => { -ident => 'rm.project_id' }
			},
		};
	}

	else {

		return {

			-columns  => [ splice( @column, 2, 6 ) ],
			-from     => 'redcap_metadata AS rm',
			-order_by => 'rm.field_order',
			-group_by => 'rm.form_name',
			-having => { 'variable_count' => { '>', 0 } },
			-where  => $self->data_export_tool == 1
			? { 'rm.project_id' => $self->project_id }
			: {
				'rm.project_id' => $self->project_id,
				'rm.field_phi'  => { '=', undef },
			}
		  }

	}
}

sub variable_structure {

	my ($self) = @_;

	# If data_export_tool is != 1 remove variables tagged as identifiers
	return {
		-columns => [
			variable => 'field_name',
			table    => 'form_name',
			type =>
            "IF( element_validation_type IS NULL, 'text', element_validation_type)",
			unit => 'field_units',
			category =>
            "IF( element_enum like '%, %', REPLACE( element_enum, '\\\\n', '\n'), '')",
			label => 'element_label'
		],
		-from     => 'redcap_metadata',
		-order_by => 'field_order',
		-where    => $self->data_export_tool == 1
		? { 'project_id' => $self->project_id }
		: {
			'project_id' => $self->project_id,
			'field_phi'  => { '=', undef },
		},
	};

}

sub datatype_map {

	return {
		'int'                  => 'signed',
		'float'                => 'decimal',
		'date_dmy'             => 'date',
		'date_mdy'             => 'date',
		'date_ymd'             => 'date',
		'datetime_dmy'         => 'datetime',
		'datetime_mdy'         => 'datetime',
		'datetime_ymd'         => 'datetime',
		'datetime_seconds_dmy' => 'datetime',
		'datetime_seconds_mdy' => 'datetime',
		'datetime_seconds_ymd' => 'datetime',
		'number'               => 'decimal',
		'number_1dp'           => 'decimal(10,1)',
		'number_2dp'           => 'decimal(10,2)',
		'number_3dp'           => 'decimal(10,3)',
		'number_4dp'           => 'decimal(10,4)',
		'time'                 => 'time',
		'time_mm_sec'          => 'time'
	};
}

#-------
1;

__END__

=pod

=head1 NAME

CohortExplorer::Application::REDCap::Datasource - CohortExplorer class to initialize datasource stored under L<REDCap|http://project-redcap.org/> framework

=head1 SYNOPSIS

The class is inherited from L<CohortExplorer::Datasource> and overrides the following methods:

=head2 authenticate( $opts )

This method authenticates the user by running the authentication query against the REDCap database. The successful authentication returns hash ref containing C<project_id>, C<data_export_tool> and C<group_id>. In order to use CohortExplorer with REDCap the user must have the permission to export data in REDCap (C<data_export_tool != 0>). At present the application only supports the standard REDCap table authentication.

=head2 additional_params( $opts, $response )

This method adds the authentication response to the datasource object. The method also runs a SQL query to determine the datasource type (i.e. standard/cross-sectional or longitudinal). For longitudinal datasources the method attempts to set C<static_tables> and C<init_event_id> (i.e. event_id of the first repeating form). At present the application does not support datasources with multiple arms.

=head2 entity_structure()

This method returns a hash ref defining the entity structure. The method uses C<init_event_id> parameter set in C<additional_params> to define C<visit> column for the longitudinal datasources. The hash ref contains the condition for the inclusion and exclusion of records.

=head2 table_structure() 

This method returns a hash ref defining the table structure. C<-columns> key within the table structure depends on the datasource type. For standard datasources C<-columns> key includes table attributes such as C<table>, C<label> and C<variable_count> where as for longitudinal datasources it comprises of C<table>, C<arm>, C<variable_count>, C<label>, C<event_count> and C<event_description>.

=head2 variable_structure()

This method returns a hash ref defining the variable structure. The hash ref uses C<data_export_tool> parameter set in C<additional_params> to specify condition for the inclusion and exclusion of variables tagged as identifiers. The variable attributes include columns such as C<table>, C<unit>, C<type>, C<category> and C<label>.

=head2 datatype_map()

This method returns variable type to SQL type mapping.

=head1 SEE ALSO

L<CohortExplorer>

L<CohortExplorer::Datasource>

L<CohortExplorer::Command::Describe>

L<CohortExplorer::Command::Find>

L<CohortExplorer::Command::History>

L<CohortExplorer::Command::Query::Search>

L<CohortExplorer::Command::Query::Compare>

=head1 LICENSE AND COPYRIGHT

Copyright (c) 2013 Abhishek Dixit (adixit@cpan.org). All rights reserved.

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
