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

sub start
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

	$self->error(undef);
	$self->halted(0);
	$self->frozen(0);

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

	return HOPKINS_QUEUE_STATUS_RUNNING
		if $self->is_running;

	return HOPKINS_QUEUE_STATUS_HALTED
		if $self->halted && !$self->frozen;

	return HOPKINS_QUEUE_STATUS_FROZEN
		if $self->frozen;

	return HOPKINS_QUEUE_STATUS_SHUTDOWN;
}

=item status_string

=cut

sub status_string
{
	my $self = shift;

	for ($self->status) {
		$_ == HOPKINS_QUEUE_STATUS_RUNNING		&& return 'running';
		$_ == HOPKINS_QUEUE_STATUS_HALTED		&& return 'halted';
		$_ == HOPKINS_QUEUE_STATUS_FROZEN		&& return 'frozen';
		$_ == HOPKINS_QUEUE_STATUS_SHUTDOWN		&& return 'shutdown';
	}
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

=item flush

=cut

sub flush
{
	my $self = shift;

	$self->stop;
	$self->tasks->Delete($self->tasks->Keys);
	$self->start;
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
	$self->frozen(0);
}

=item freeze

freezes the queue.  no tasks will be executed and no more
tasks will be enqueued.

=cut

sub freeze
{
	my $self = shift;

	$self->halt;
	$self->frozen(1);
}

=item shutdown

shuts the queue down, flushing any queued tasks.  no more
tasks will be executed and no further tasks may be enqueud.

=cut

sub shutdown
{
	my $self = shift;

	$self->flush;
	$self->freeze;
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
