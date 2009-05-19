package Hopkins::State;

use strict;
use warnings;

=head1 NAME

Hopkins::State - local task state

=head1 DESCRIPTION

Hopkins::State encapsulates all of the busywork associated
with keeping a list of all running tasks in each individual
queue.  i'd prefer not to have to do this, but it appears
that POE::Component::JobQueue provides no means by which i
may access this information.

=cut

use POE;

use Class::Accessor::Fast;
use Data::UUID;

use base 'Class::Accessor::Fast';

__PACKAGE__->mk_accessors(qw(tasks));

my $ug = new Data::UUID;

sub new
{
	my $self = shift->SUPER::new(@_);

	$self->tasks({});

	POE::Session->create
	(
		object_states =>
		[
			$self =>
			{
				_start			=> 'start',
				_stop			=> 'stop',

				task_enqueued	=> 'task_enqueued',
				task_stored		=> 'task_stored',
				task_completed	=> 'task_completed'
			}
		]
	);

	return $self;
}

sub start
{
	my $self	= $_[OBJECT];
	my $kernel	= $_[KERNEL];

	$kernel->alias_set('state');
}

sub stop
{
	my $self = $_[OBJECT];

	Hopkins->log_debug('state exiting');
}

sub task_enqueued
{
	my $self	= $_[OBJECT];
	my $task	= $_[ARG0];

	$task->id($ug->create_str);

	$self->tasks->{$task->id} = $task;

	return $task->id;
}

sub task_stored
{
	my $self	= $_[OBJECT];
	my $id		= $_[ARG0];
	my $row		= $_[ARG1];

	$self->state->tasks->{$id}->row($row);
}

sub task_completed
{
}

=back

=head1 AUTHOR

Mike Eldridge <diz@cpan.org>

=head1 LICENSE

=cut

1;

