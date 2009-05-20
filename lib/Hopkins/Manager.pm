package Hopkins::Manager;

use strict;

=head1 NAME

Hopkins::Manager - hopkins manager session states

=head1 DESCRIPTION

Hopkins::Manager encapsulates the manager session, which is
responsible for configuration parsing and change scanning,
queue and plugin management, and last, but certainly not
least, task scheduling.

=cut

use POE;
use Class::Accessor::Fast;

use Hopkins::Store;
use Hopkins::Config;
use Hopkins::Queue;
use Hopkins::State;
use Hopkins::Task;
use Hopkins::Work;

use Hopkins::Constants;

use base 'Class::Accessor::Fast';

__PACKAGE__->mk_accessors(qw(hopkins config plugins queues));

=head1 STATES

=over 4

=item new

=cut

sub new
{
	my $self = shift->SUPER::new(@_);

	$self->plugins({});
	$self->queues({});

	# create the POE Session that will be the bread and butter
	# of the job daemon's normal function.  the manager session
	# will read the configuration upon execution and will begin
	# the rest of the startup process in the following order:
	#
	#	- storage initialization via DBIx::Class
	#	- queue creation via POE::Component::JobQueue
	#	- RPC session creation via POE::Component::Server::SOAP

	# create manager session
	POE::Session->create
	(
		object_states =>
		[
			$self =>
			{
				_start			=> 'start',
				_stop			=> 'stop',

				config_scan		=> 'config_scan',
				config_load		=> 'config_load',

				init_config		=> 'init_config',
				init_queues		=> 'init_queues',
				init_plugins	=> 'init_plugins',
				init_store		=> 'init_store',
				init_state		=> 'init_state',

				queue_check_all	=> 'queue_check_all',
				queue_check		=> 'queue_check',
				queue_failure	=> 'queue_failure',
				queue_start		=> 'queue_start',
				queue_flush		=> 'queue_flush',
				queue_halt		=> 'queue_halt',
				queue_freeze	=> 'queue_freeze',
				queue_shutdown	=> 'queue_shutdown',

				scheduler		=> 'scheduler',
				enqueue			=> 'enqueue',
				taskstart		=> 'taskstart',
				dequeue			=> 'dequeue',

				shutdown		=> 'shutdown'
			}
		]
	);

	return $self;
}

=item start

=cut

sub start
{
	my $self	= $_[OBJECT];
	my $kernel	= $_[KERNEL];

	# set the alias for the current session
	$kernel->alias_set('manager');

	# post events for initial setup
	$kernel->call(manager => 'init_config');	# configuration file
	$kernel->post(manager => 'init_state');		# machine state session
	$kernel->post(manager => 'init_store');		# database storage sessage
	$kernel->post(manager => 'init_queues');	# worker queue sessions

	# go ahead and kick off the scheduler too
	$kernel->alarm(scheduler => time + $self->hopkins->poll);
}

=item stop

=cut

sub stop
{
	my $self = $_[OBJECT];

	Hopkins->log_debug('manager exiting');
}

=item init_queues

=cut

sub init_queues
{
	my $self	= $_[OBJECT];
	my $kernel	= $_[KERNEL];

	# create a passive queue for each configured queue.  we
	# use POE::Component::JobQueue and leave the scheduling
	# up to the manager session.

	foreach my $name ($self->config->get_queue_names) {
		my $opts = $self->config->get_queue_info($name);

		$self->queues->{$name} = new Hopkins::Queue { kernel => $kernel, %$opts };

		$kernel->post(manager => queue_start => $name);
	}
}

sub queue_start
{
	my $self	= $_[OBJECT];
	my $name	= $_[ARG0];

	my $queue = $self->queue($name);

	return HOPKINS_QUEUE_NOT_FOUND			if not defined $queue;
	return HOPKINS_QUEUE_ALREADY_RUNNING	if $queue->is_running;

	$queue->start;

	return HOPKINS_QUEUE_STARTED;
}

sub queue_halt
{
	my $self	= $_[OBJECT];
	my $name	= $_[ARG0];
	my $queue	= $self->queue($name);

	$queue->halt if $queue;
}

sub queue_freeze
{
	my $self	= $_[OBJECT];
	my $name	= $_[ARG0];
	my $queue	= $self->queue($name);

	$queue->freeze if $queue;
}

sub queue_shutdown
{
	my $self	= $_[OBJECT];
	my $name	= $_[ARG0];
	my $queue	= $self->queue($name);

	$queue->shutdown if $queue;
}

sub queue_check
{
	my $self	= $_[OBJECT];
	my $name	= $_[ARG0];

	return $self->queues->{$name};
}

sub queue_check_all
{
	my $self = $_[OBJECT];

	return values %{ $self->queues };
}

sub queue_flush
{
	my $self	= $_[OBJECT];
	my $name	= $_[ARG0];
	my $queue	= $self->queue($name);

	$queue->flush if $queue;
}

=item queue_failure

=cut

sub queue_failure
{
	my $self	= $_[OBJECT];
	my $kernel	= $_[KERNEL];
	my $queue	= $_[ARG0];
	my $error	= $_[ARG1];

	my $msg = 'task failure in ' . $queue->name . ' queue';

	if (my $action = $queue->onerror) {
		$queue->$action($error);
	}

	Hopkins->log_error($msg);
}

=item init_store

=cut

sub init_store
{
	new Hopkins::Store;
}

=item init_state

=cut

sub init_state
{
	new Hopkins::State;
}

=item init_config

=cut

sub init_config
{
	my $self	= $_[OBJECT];
	my $kernel	= $_[KERNEL];

	$self->config(new Hopkins::Config { file => $self->hopkins->conf });

	$kernel->call(manager => 'config_load');
	$kernel->alarm(confscan => time + $self->hopkins->scan);
}

=item init_plugins

=cut

sub init_plugins
{
	my $self	= $_[OBJECT];
	my $kernel	= $_[KERNEL];

	Hopkins->log_debug('initializing plugins');

	my $config	= $self->config;
	my $plugins	= $self->plugins;

	delete $plugins->{$_}
		foreach grep { not $config->has_plugin($_) } keys %$plugins;

	foreach my $name ($config->get_plugin_names) {
		if (not exists $plugins->{$name}) {
			my $options	= $config->get_plugin_info($name);
			my $package = $name =~ /^\+/ ? $name : "Hopkins::Plugin::$name";
			my $path	= $package;

			$path =~ s{::}{/}g;

			require "$path.pm";

			$plugins->{$name} = $package->new({ manager => $self, config => $options });
		}
	}
}

=item config_load

=cut

sub config_load
{
	my $self	= $_[OBJECT];
	my $kernel	= $_[KERNEL];

	my $status	= $self->config->load;

	$kernel->post(manager => 'shutdown') unless $status->ok;

	if ($status->failed) {
		my $err = $status->parsed
			? 'errors in configuration, discarding new version'
			: 'unable to load configuration file: ' . $status->errmsg;

		Hopkins->log_error($err);
	}

	return unless $status->updated;

	if ($status->store_modified) {
		Hopkins->log_debug('database information changed');
		$kernel->post(manager => 'init_store');
	}

	$kernel->post(manager => 'init_plugins');
}

=item config_scan

=cut

sub config_scan
{
	my $self	= $_[OBJECT];
	my $kernel	= $_[KERNEL];

	print "WHAT THE FUCK MAN\n";

	if ($self->config->scan) {
		Hopkins->log_info('configuration file changed');
		$kernel->post(manager => 'confload')
	}

	$kernel->alarm(confscan => time + $self->hopkins->scan);
}

=item shutdown

=cut

sub shutdown
{
	my $self	= $_[OBJECT];
	my $kernel	= $_[KERNEL];

	Hopkins->log_info('received shutdown request');

	$kernel->alarm('scheduler');
	$kernel->alarm('confscan');

	foreach my $name ($self->config->get_queue_names) {
		Hopkins->log_debug("posting stop event for $name queue");
		$kernel->post($name => 'stop');
	}
}

=item scheduler

=cut

sub scheduler
{
	my $self	= $_[OBJECT];
	my $kernel	= $_[KERNEL];

	Hopkins->log_debug('checking for tasks to enqueue');

	foreach my $name ($self->config->get_task_names) {
		my $now		= DateTime->now;
		my $task	= $self->config->get_task_info($name);

		next if not defined $task->schedule;

		my $opts	= $task->options;
		my $serial	= $task->run eq 'serial' ? 1 : 0;
		my $last	= $task->schedule->previous($now);

		Hopkins->log_debug("checking if $name is marked inactive");
		next if not $task->enabled;

		#Hopkins->log_debug("checking if $name has been executed since $last");
		#next if $rsTask->task_executed_since($name, $last);

		#Hopkins->log_debug("checking if $name is currently executing");
		#next if $rsTask->task_executing_now($name) and $serial;

		my $state = $kernel->call(manager => enqueue => $name => $opts);

		Hopkins->log_error("failure in scheduler while attempting to enqueue $name")
			if not $state;
	}

	$kernel->alarm(scheduler => time + $self->hopkins->poll);
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
	my $self	= $_[OBJECT];
	my $kernel	= $_[KERNEL];
	my $name	= $_[ARG0];
	my $opts	= $_[ARG1];

	my $task = $self->config->get_task_info($name);

	if (not defined $task) {
		Hopkins->log_warn("unable to enqueue $name; task not found");
		return HOPKINS_ENQUEUE_TASK_NOT_FOUND;
	}

	my $queue = $self->queue($task->queue);

	if (not defined $queue) {
		Hopkins->log_warn("unable to enqueue $name; queue " . $task->queue . ' not found');
		return HOPKINS_ENQUEUE_QUEUE_UNAVAILABLE;
	}

	if ($queue->frozen) {
		Hopkins->log_warn("unable to enqueue $name; queue " . $task->queue . ' frozen');
		return HOPKINS_ENQUEUE_QUEUE_FROZEN;
	}

	# notify the state tracker that we're going to enqueue
	# a task.  the state tracker will assign an identifier
	# that will be used to identify it throughout its life.

	my $work = new Hopkins::Work { task => $task, queue => $queue };

	$queue->tasks->Push($work);

	$kernel->call(state => task_enqueued => $work);

	# post one of two enqueue events, depending on whether
	# the task has an associated class name or an explicit
	# command line specification.

	Hopkins->log_debug("enqueuing task $name (" . $work->id . ')');

	$kernel->post($queue->alias => enqueue => dequeue => $work);

	return HOPKINS_ENQUEUE_OK;
}

sub dequeue
{
	my $self	= $_[OBJECT];
	my $kernel	= $_[KERNEL];
	my $params	= $_[ARG0];
	my $work	= $params->[0];

	Hopkins->log_debug('dequeued task ' . $work->task->name . ' (' . $work->id . ')');

	$work->queue->tasks->Delete($work);

	#my $now		= DateTime->now;
	#my $state	= $heap->{state};
	#my $schema	= Hopkins::Store->schema;
	#my $rsTask	= $schema->resultset('Task');
	#my $task	= $rsTask->find($id);

	$kernel->call(state => task_completed => $work);

	#$task->date_completed($now);
	#$task->update;

	#my $task	= Hopkins::Config->get_task_info($task->name);
}

sub taskstart
{
	my $self	= $_[OBJECT];
	my $kernel	= $_[KERNEL];
	my $heap	= $_[HEAP];
	my $id		= $_[ARG0];

	#$kernel->post(store => 'notify', 'task_update', $id, status => 'running');

	print STDERR "HOLY ASSCOW\n";
}

sub queue
{
	my $self = shift;
	my $name = shift;

	return $self->queues->{$name};
}

=back

=head1 AUTHOR

Mike Eldridge <diz@cpan.org>

=head1 LICENSE

=cut

1;
