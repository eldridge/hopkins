package Hopkins::Queue;

use strict;

=head1 NAME

Hopkins::Queue - hopkins queue states and methods

=head1 DESCRIPTION

Hopkins::Queue contains all of the POE event handlers and
supporting methods for the initialization and management of
each configured hopkins queue.

=cut

use POE;
use Tie::IxHash;
use Class::Accessor::Fast;

use Hopkins::Constants;
use Hopkins::Worker;

use base 'Class::Accessor::Fast';

__PACKAGE__->mk_accessors(qw(kernel config name alias onerror onfatal concurrency tasks halted frozen error));

=head1 STATES

=over 4

=item new

=cut

sub new
{
	my $self = shift->SUPER::new(@_);

	Hopkins->log_debug('creating queue ' . $self->name);

	$self->alias('queue.' . $self->name);
	$self->tasks(new Tie::IxHash);

	$self->onerror(undef)
		unless grep { $self->onerror eq $_ }
		qw(halt freeze shutdown flush);

	return $self;
}

sub spawn
{
	my $self = shift;

	Hopkins->log_debug('spawning queue ' . $self->name);

	POE::Component::JobQueue->spawn
	(
		Alias		=> $self->alias,
		WorkerLimit	=> $self->concurrency,
		Worker		=> sub { $self->spawn_worker(@_) },
		Passive		=> { Prioritizer => \&Hopkins::Queue::prioritize },
	);

	foreach my $work ($self->tasks->Keys) {
		$self->kernel->post($self->alias => enqueue => dequeue => $work);
	}

	# this passive queue will act as an on-demand task
	# execution queue, waiting for enqueue events to be
	# posted to the kernel.

	#POE::Component::JobQueue->spawn
	#(
	#	Alias		=> 'worker',
	#	WorkerLimit	=> 16,
	#	Worker		=> \&queue_worker,
	#	Passive		=> { },
	#);

	# this active queue will act as a scheduler, checking
	# the time and polling the list of loaded task configs
	# for new tasks to spawn

	#POE::Component::JobQueue->spawn
	#(
	#	Alias		=> 'scheduler',
	#	WorkerLimit	=> 16,
	#	Worker		=> \&queue_scheduler,
	#	Active		=>
	#	{
	#		PollInterval	=> $global->{poll},
	#		AckAlias		=> 'scheduler',
	#		AckState		=> \&job_completed
	#	}
	#);
}

=item stop

stops the queue, shutting down the PoCo::JobQueue session
if running by sending a stop event to it.

=cut

sub stop
{
	my $self = shift;

	$self->kernel->post($self->alias => 'stop');
}

=item spawn_worker

=cut

sub spawn_worker
{
	my $self = shift;

	my $args =
	{
		postback	=> shift,
		work		=> shift,
		queue		=> $self
	};

	new Hopkins::Worker $args;
}

=item prioritize

=cut

sub prioritize
{
	my $a = shift;
	my $b = shift;

	my $aopts = $a->[5] || {};
	my $bopts = $b->[4] || {};

	my $apri = $aopts->{priority} || 5;
	my $bpri = $bopts->{priority} || 5;

	$apri = 1 if $apri < 1;
	$apri = 9 if $apri > 9;
	$bpri = 1 if $bpri < 1;
	$bpri = 9 if $bpri > 9;

	return $apri <=> $bpri;
}

=item is_running

=cut

sub is_running
{
	my $self = shift;
	my $name = shift;

	return Hopkins->is_session_active($self->alias);
}

=item status

=cut

sub status
{
	my $self = shift;

	return HOPKINS_QUEUE_STATUS_HALTED	if $self->halted;
	return HOPKINS_QUEUE_STATUS_RUNNING	if $self->tasks->Length > 0;

	return HOPKINS_QUEUE_STATUS_IDLE;
}

=item status_string

=cut

sub status_string
{
	my $self = shift;

	for ($self->status) {
		$_ == HOPKINS_QUEUE_STATUS_IDLE && return 'idle';

		$_ == HOPKINS_QUEUE_STATUS_RUNNING && $self->frozen
			&& return 'running (frozen)';

		$_ == HOPKINS_QUEUE_STATUS_HALTED && $self->frozen
			&& return 'halted (frozen)';

		$_ == HOPKINS_QUEUE_STATUS_RUNNING		&& return 'running';
		$_ == HOPKINS_QUEUE_STATUS_HALTED		&& return 'halted';
	}
}

=item start

=cut

sub start
{
	my $self = shift;

	$self->error(undef);
	$self->halted(0);

	$self->spawn;
}

=item halt

halts the queue.  no tasks will be executed, although tasks
may still be enqueued.

=cut

sub halt
{
	my $self = shift;

	$self->stop;
	$self->halted(1);
}

=item continue

reverses the action of halt by starting the queue back up.
the existing state of the frozen flag will be preserved.

=cut

sub continue
{
	my $self = shift;

	$self->start;
	$self->halted(0);
}

=item freeze

freezes the queue.  no more tasks will be enqueued, but
currently queued tasks will be allowed to execute.

=cut

sub freeze
{
	my $self = shift;

	$self->frozen(1);
}

=item thaw

reverses the action of freeze by unsetting the frozen flag.
tasks will not be queable.  the existing halt state will be
preserved.

=cut

sub thaw
{
	my $self = shift;

	$self->frozen(0);
}

=item shutdown

shuts the queue down.  this is basically a shortcut for the
freeze and halt actions.  no more tasks will be executed and
no further tasks may be enqueud.

=cut

sub shutdown
{
	my $self = shift;

	$self->freeze;
	$self->halt;
}

=item flush

flush the queue of any tasks waiting to execute.  stops the
PoCo::JobQueue session (if running) and clears the internal
list of tasks.  if the queue was running prior to the flush,
the PoCo::JobQueue session is spun back up.

=cut

sub flush
{
	my $self = shift;

	$self->stop;
	$self->tasks->Delete($self->tasks->Keys);
	$self->start if not $self->halted;
}

=item DESTROY

=cut

sub DESTROY { shift->shutdown }

=back

=head1 AUTHOR

Mike Eldridge <diz@cpan.org>

=head1 LICENSE

This program is free software; you may redistribute it
and/or modify it under the same terms as Perl itself.

=cut

1;
