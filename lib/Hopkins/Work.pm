package Hopkins::Work;

use strict;

=head1 NAME

Hopkins::Work - task queued as a unit of work

=head1 DESCRIPTION

Hopkins::Work represents an instance of a task.  in other
words, it is a task that has been queued as a unit of work
ready for execution.  it is eventually associated with a
database object by the store session, but can and will live
on its own without database connectivity.

=cut

use Class::Accessor::Fast;

use base 'Class::Accessor::Fast';

__PACKAGE__->mk_accessors(qw(id task options queue date_enqueued date_to_execute date_started date_completed row));

=back

=head1 AUTHOR

Mike Eldridge <diz@cpan.org>

=head1 LICENSE

=cut

1;

