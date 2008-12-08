package Hopkins::Manager;

use strict;

=head1 NAME

Hopkins::Manager - hopkins manager session states

=head1 DESCRIPTION

Hopkins::Manager contains all of the event handlers for the
core of hopkins.  aside from the RPC session, the manager
session *IS* hopkins.

=cut

use POE;
use File::Monitor;

=head1 STATES

=over 4

=item start

=cut

sub start
{
	my $kernel	= $_[KERNEL];
	my $heap	= $_[HEAP];
	my $opts	= $_[ARG0];

	# save our options for future use
	$heap->{opts} = $opts;

	# set the alias for the current session
	$kernel->alias_set('manager');

	# create a new state object for keeping up with ourself
	$heap->{state} = new Hopkins::State;

	# create a File::Monitor object, a callback, and an
	# associated POE alarm to watch the configuration file
	# for subsequent changes

	my $callback = sub {
		Hopkins->log_info('configuration file changed');
		$kernel->post(manager => 'confload')
	};

	$heap->{confmon} = new File::Monitor;
	$heap->{confmon}->watch($opts->{conf}, $callback);

	$kernel->alarm(confscan => time + $opts->{scan});

	# post events for initial setup
	$kernel->post(manager => 'confload');	# configuration file
	$kernel->post(manager => 'storeinit');	# database schema
	$kernel->post(manager => 'queueinit');	# scheduler and worker
	$kernel->post(manager => 'rpcinit');	# RPC session

	$kernel->alarm(scheduler => time + $opts->{poll});
}

=item stop

=cut

sub stop
{
	Hopkins->log_debug('manager exiting');
}

=item manager_shutdown

=cut

sub shutdown
{
	my $kernel	= $_[KERNEL];
	my $heap	= $_[HEAP];

	Hopkins->log_info('received shutdown request');

	foreach my $name (Hopkins::Config->get_queue_names) {
		Hopkins->log_debug("posting stop event for $name queue");
		$kernel->post($name => 'stop');
	}
}

=item scheduler

=cut

sub scheduler
{
	my $kernel	= $_[KERNEL];
	my $heap	= $_[HEAP];

	Hopkins->log_debug('checking queue for tasks to post');

	foreach my $name (Hopkins::Config->get_task_names) {
		my $now		= DateTime->now;
		my $task	= Hopkins::Config->get_task_info($name);
		my $set		= $task->{schedules};

		next if not defined $set;

		my $opts	= $task->{option};
		my $serial	= $task->{run} eq 'serial' ? 1 : 0;
		my $last	= $set->previous($now);
		my $active	= lc $task->{active} eq 'no' ? 0 : 1;

		Hopkins->log_debug("checking if $name is marked inactive");
		next if not $active;

		#Hopkins->log_debug("checking if $name has been executed since $last");
		#next if $rsTask->task_executed_since($name, $last);

		#Hopkins->log_debug("checking if $name is currently executing");
		#next if $rsTask->task_executing_now($name) and $serial;

		my $state = $kernel->call(manager => enqueue => $name => $opts);

		Hopkins->log_error("failure in scheduler while attempting to enqueue $name")
			if not $state;
	}

	$kernel->alarm(scheduler => time + $heap->{opts}->{poll});
}

=item enqueue

queue a task by posting to POE::Component::JobQueue session.
if the destination queue is not running, no event will be
posted and a 0 will be returned to the caller.

this state can be posted to by any session, but is primarily
utilized by the manager session's scheduler event.  the RPC
session exposes an enqueue method via SOAP that also posts
to this event.

=cut

sub enqueue
{
	my $kernel	= $_[KERNEL];
	my $heap	= $_[HEAP];
	my $name	= $_[ARG0];
	my $opts	= $_[ARG1];

	my $now		= DateTime->now;
	my $state	= $heap->{state};
	my $schema	= Hopkins::Store->schema;
	my $rsTask	= $schema->resultset('Task');

	my $task	= Hopkins::Config->get_task_info($name);
	my $class	= $task->{class};
	my $queue	= $task->{queue};
	my $cmd		= $task->{cmd};

	if (not Hopkins::Queue->is_running($queue)) {
		Hopkins->log_warn("unable to enqueue $name; queue not running");
		return 0;
	}

	# create an associated Task row for tracking this task
	# throughout its lifetime.  insert that task object into
	# the local state tracker.

	my $task	= $rsTask->create({ name => $name, queue => $queue, date_queued => $now });
	my $id		= $task->id;

	$state->task_insert($task);

	# post one of two enqueue events, depending on whether
	# the task has an associated class name or an explicit
	# command line specification.

	Hopkins->log_debug("posting enqueue event for $name ($id)");

	my @args	= ("queue.$queue", 'enqueue', 'dequeue', $task->id, $name);
	my $res		= undef;

	$kernel->post(@args, 'perl', $class, $opts)	if $class;
	$kernel->post(@args, 'exec', $cmd)			if $cmd;

	return 1;
}

sub dequeue
{
	my $kernel	= $_[KERNEL];
	my $heap	= $_[HEAP];
	my $params	= $_[ARG0];
	my $id		= $params->[0];
	my $name	= $params->[1];

	Hopkins->log_debug("received dequeue event for $name ($id)");

	my $now		= DateTime->now;
	my $state	= $heap->{state};
	my $schema	= Hopkins::Store->schema;
	my $rsTask	= $schema->resultset('Task');
	my $task	= $rsTask->find($id);

	$state->task_remove($task);
	$task->date_completed($now);
	$task->update;

	#my $task	= Hopkins::Config->get_task_info($task->name);
}

sub taskstart
{
	my $kernel	= $_[KERNEL];
	my $heap	= $_[HEAP];
	my $id		= $_[ARG0];

	my $state	= $heap->{state};
	my $schema	= Hopkins::Store->schema;
	my $rsTask	= $schema->resultset('Task');
	my $task	= $rsTask->find($id);

	$state->task_update($task, status => 'running');
}

=back

=head1 AUTHOR

Mike Eldridge <diz@cpan.org>

=head1 LICENSE

=cut

1;
