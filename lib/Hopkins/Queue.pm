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
use Class::Accessor::Fast;

use Cache::FileCache;
use DateTime::Format::ISO8601;
use Tie::IxHash;

use Hopkins::Constants;
use Hopkins::Worker;
use Hopkins::Work;

use base 'Class::Accessor::Fast';

__PACKAGE__->mk_accessors(qw(kernel config cache name alias onerror onfatal concurrency tasks halted frozen error));

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
		unless $self->onerror
		and grep { $self->onerror eq $_ } qw(halt freeze shutdown flush);

	$self->cache(new Cache::FileCache {
		cache_root		=> $self->config->fetch('state/root')->stringify,
		namespace		=> 'queue/' . $self->name,
		directory_umask	=> 0077
	});

	$self->read_state;
	$self->write_state;

	return $self;
}

sub read_state
{
	my $self = shift;

	$self->frozen($self->cache->get('frozen') ? 1 : 0);
	$self->halted($self->cache->get('halted') ? 1 : 0);
	$self->error($self->cache->get('error'));

	my $aref = $self->cache->get('tasks');

	return if not ref $aref eq 'ARRAY';

	foreach my $href (@$aref) {
		my $work = new Hopkins::Work;
		my $date = undef;

		$work->queue($self);
		$work->options($href->{options});

		if (my $id = $href->{id}) {
			$work->id($id);
		} else {
			Hopkins->log_error('unable to determine task ID when reading state');
			next;
		}

		if (my $date = Hopkins->parse_datetime($href->{date_enqueued})) {
			$work->date_enqueued($date);
		} else {
			Hopkins->log_error('unable to parse date/time information when reading state');
		}

		# FIXME: date_to_execute not yet implemented

		if (my $date = Hopkins->parse_datetime($href->{date_to_execute})) {
			$work->date_to_execute($date);
		} else {
			#Hopkins->log_error('unable to parse date/time information when reading state');
			$work->date_to_execute(DateTime->now(time_zone => 'local'));
		}

		if (my $val = $href->{date_started}) {
			if (my $date = Hopkins->parse_datetime($href->{date_started})) {
				$work->date_started($date);
			} else {
				Hopkins->log_error('unable to parse date/time information when reading state');
			}
		}

		# attempt to locate the referenced task.  if the
		# configuration has changed and we can't locate the
		# referenced task, we'll want to halt the queue
		# until an operator can take a look at it.

		if (my $task = $self->config->get_task_info($href->{task})) {
			$work->task($task);
		} else {
			Hopkins->log_error("unable to locate task '$href->{task}' when reading state");

			$self->kernel->post(store => notify => task_aborted => $work->serialize);
		}

		# if the task was already started or has an invalid
		# vconfiguration, mark it as orphaned.  otherwise,
		# go ahead and queue it up for execution.

		if ($work->date_started or not defined $work->task) {
			$self->kernel->post(store => notify => task_orphaned => $work->serialize);
		} else {
			$self->tasks->Push($work->id => $work);
		}
	}
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

	foreach my $work ($self->tasks->Values) {
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

=item write_state

write the queue's state to disk.

=cut

sub write_state
{
	my $self = shift;

	$self->cache->set(frozen => $self->frozen);
	$self->cache->set(halted => $self->halted);
	$self->cache->set(error => $self->error);
	$self->cache->set(tasks => [ map { $_->serialize } $self->tasks->Values ]);
}

=item stop

stops the queue, shutting down the PoCo::JobQueue session
if running by sending a stop event to it.

=cut

sub stop
{
	my $self = shift;

	$self->kernel->post($self->alias => 'stop') if $self->kernel;
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

=item num_queued

=cut

sub num_queued
{
	my $self = shift;
	my $task = shift;

	return $self->tasks->Length if not defined $task;

	if (not ref $task eq 'Hopkins::Task') {
		Hopkins->log_warn('Hopkins::Queue->num_queued called with argument that is not a Hopkins::Task object');
		return 0;
	}

	return scalar grep { $_->task->name eq $task->name } $self->tasks->Values
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
