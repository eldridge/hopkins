package Hopkins::State;

use strict;

=head1 NAME

Hopkins::State - local task state

=head1 DESCRIPTION

Hopkins::State encapsulates all of the busywork associated
with keeping a list of all running tasks in each individual
queue.  i'd prefer not to have to do this, but it appears
that POE::Component::JobQueue provides no means by which i
may access this information.

=cut

use Class::Accessor::Fast;

use base 'Class::Accessor::Fast';

__PACKAGE__->mk_accessors(qw(tasks));

sub new
{
	my $self = shift->SUPER::new(@_);

	$self->tasks({});

	return $self;
}

sub task_insert
{
	my $self = shift;
	my $task = shift;

	my $queue	= $task->queue;
	my $id		= $task->id;
	my $href	= { status => 'queued', task => $task };

	$self->tasks->{$queue}->{$id} = $href;
}

sub task_update
{
	my $self	= shift;
	my $task	= shift;
	my $key		= shift;
	my $val		= shift;

	my $queue	= $task->queue;
	my $id		= $task->id;

	$self->tasks->{$queue}->{$id}->{$key} = $val;
}

sub task_remove
{
	my $self = shift;
	my $task = shift;

	my $queue	= shift;
	my $id		= shift;

	delete $self->tasks->{$queue}->{$id};
}

=back

=head1 AUTHOR

Mike Eldridge <diz@cpan.org>

=head1 LICENSE

=cut

1;

