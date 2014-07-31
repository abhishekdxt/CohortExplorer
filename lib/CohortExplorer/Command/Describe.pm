package CohortExplorer::Command::Describe;

use strict;
use warnings;

our $VERSION = 0.13;

use base qw(CLI::Framework::Command);
use CLI::Framework::Exceptions qw( :all );

#-------

sub usage_text {

	        q{
            describe : show datasource description including the entity count 
          };

}

sub validate {

	my ( $self, $opts, @args ) = @_;

	if (@args) {
		throw_cmd_validation_exception(
			error => 'Specified arguments when none required' );
	}

}

sub run {

	my ( $self, $opts, @args ) = @_;

	my $cache = $self->cache->get('cache');

	# May or may not be preloaded
	eval 'require ' . ref $cache->{datasource};

	my $tables = $cache->{datasource}->tables;

	# Get all table attributes using the last key in $tables
	# The first column is always table name
	push my @column, 'table';

	for ( keys %{ $tables->{ ( keys %$tables )[-1] } } ) {
		if ( $_ ne 'table' ) {
			push @column, $_;
		}
	}

	push my @rows, \@column;

	for my $t ( keys %$tables ) {
		push @rows,
		  [ map { $tables->{$t}{ $rows[0]->[$_] } } 0 .. $#{ $rows[0] } ];
	}

	print STDERR "\nRendering datasource description ...\n\n"
	  if ( $cache->{verbose} );

	return {
		headingText => $cache->{datasource}->name
		  . ' datasource description ('
		  . $cache->{datasource}->entity_count
		  . ' entities)',
		rows => \@rows
	};
}

#-------
1;

__END__

=pod

=head1 NAME

CohortExplorer::Command::Describe - CohortExplorer class to print datasource description

=head1 SYNOPSIS

B<describe>

B<d>

=head1 DESCRIPTION

The class is inherited from L<CLI::Framework::Command> and overrides the following methods:

=head2 usage_text()

This method returns the usage information for the command.

=head2 validate( $opts, @args )

This method throws C<throw_cmd_validation_exception> exception imported from L<CLI::Framework::Exceptions> if the user has supplied arguments to this command.

=head2 run( $opts, @args )

This method attempts to retrieve the table information and the entity count from the datasource class and returns them to L<CohortExplorer::Application>.

=head1 DEPENDENCIES

L<CLI::Framework::Command>

L<CLI::Framework::Exceptions>

=head1 SEE ALSO

L<CohortExplorer>

L<CohortExplorer::Datasource>

L<CohortExplorer::Command::Find>

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
