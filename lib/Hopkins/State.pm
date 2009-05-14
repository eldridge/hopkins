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
		inline_states =>
		{
			_start  => \&start,
			_stop   => \&stop,

			task_enqueued	=> \&task_enqueued,
			task_stored		=> \&task_stored,
			task_completed	=> \&task_completed
		},

		args => [ $self ]
	);

	return $self;
}

sub start
{
	my $kernel	= $_[KERNEL];
	my $heap	= $_[HEAP];
	my $state	= $_[ARG0];

	$kernel->alias_set('state');

	$heap->{state} = $state;
}

sub stop
{
	my $kernel	= $_[KERNEL];
	my $heap	= $_[HEAP];
	my $state	= $_[ARG0];

	#Hopkins->log_debug('state exiting');
}

sub task_enqueued
{
	my $kernel	= $_[KERNEL];
	my $heap	= $_[HEAP];
	my $task	= $_[ARG0];
	my $state	= $heap->{state};

	$task->id($ug->create_str);

	$state->tasks->{$task->id} = $task;

	return $task->id;
}

sub task_stored
{
	my $kernel	= $_[KERNEL];
	my $heap	= $_[HEAP];
	my $id		= $_[ARG0];
	my $row		= $_[ARG1];
	my $state	= $heap->{state};

	$state->tasks->{$id}->row($row);
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

