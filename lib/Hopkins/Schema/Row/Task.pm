package Hopkins::Schema::Row::Task;

use strict;

=head1 NAME

Hopkins::Schema::Row::Task - ORM relational class defining a point-in-time execution

=head1 DESCRIPTION

=cut

use base 'DBIx::Class';

__PACKAGE__->load_components(qw/PK::Auto Core/);

__PACKAGE__->table('tasks');
__PACKAGE__->add_columns(qw/id name date_queued date_started date_completed/);
__PACKAGE__->set_primary_key('id');

=back

=head1 AUTHOR

Mike Eldridge <meldridge@magazines.com>

=cut

1;
