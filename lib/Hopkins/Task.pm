package Hopkins::Task;

use strict;

=head1 NAME

Hopkins::Task - local task object

=head1 DESCRIPTION

Hopkins::Task represents a task as executing in the local
hopkins environment.  it is eventually associated with a
database object by the store session, but can and will live
on its own without database connectivity.

=cut

use Class::Accessor::Fast;

use base 'Class::Accessor::Fast';

__PACKAGE__->mk_accessors(qw(id name class cmd queue date_enqueued date_to_execute date_started date_completed row));

=back

=head1 AUTHOR

Mike Eldridge <diz@cpan.org>

=head1 LICENSE

=cut

1;

