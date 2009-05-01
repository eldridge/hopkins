package Hopkins::Constants;

use strict;
use warnings;

=head1 NAME

Hopkins::Constants - hopkins constants

=head1 DESCRIPTION

Hopkins::Constants takes care of exporting subroutines to
the caller's namespace for any of the particular constants
used throughout the system.

=cut

require Exporter;

our @ISA = qw(Exporter);

our @EXPORT =
qw/
	HOPKINS_ENQUEUE_OK
	HOPKINS_ENQUEUE_TASK_NOT_FOUND
	HOPKINS_ENQUEUE_QUEUE_UNAVAILABLE
	HOPKINS_QUEUE_ALREADY_RUNNING
	HOPKINS_QUEUE_STARTED
	HOPKINS_QUEUE_NOT_FOUND
/;

use constant HOPKINS_ENQUEUE_OK					=> 1;
use constant HOPKINS_ENQUEUE_TASK_NOT_FOUND		=> 2;
use constant HOPKINS_ENQUEUE_QUEUE_UNAVAILABLE	=> 3;

use constant HOPKINS_QUEUE_ALREADY_RUNNING		=> 1;
use constant HOPKINS_QUEUE_STARTED				=> 2;
use constant HOPKINS_QUEUE_NOT_FOUND			=> 3;

=back

=head1 AUTHOR

Mike Eldridge <diz@cpan.org>

=head1 LICENSE

=cut

1;
