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
use Class::Accessor::Fast;
use File::Monitor;

use Hopkins::Store;
use Hopkins::Config;
use Hopkins::Queue;
use Hopkins::State;
use Hopkins::Task;
use Hopkins::Work;

use Hopkins::Constants;

use base 'Class::Accessor::Fast';

__PACKAGE__->mk_accessors(qw(conf l4pconf scan poll));

=head1 STATES

=over 4

=item new

=cut

sub new
{
	my $self	= shift->SUPER::new(@_);
	my $hopkins	= shift;

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
		inline_states => {
			_start			=> \&start,
			_stop			=> \&stop,

			config_scan		=> \&config_scan,
			config_load		=> \&config_load,

			init_config		=> \&init_config,
			init_queues		=> \&init_queues,
			init_plugins	=> \&init_plugins,
			init_store		=> \&init_store,
			init_state		=> \&init_state,

			queue_check		=> \&queue_check,
			queue_start		=> \&queue_start,
			queue_failure	=> \&queue_failure,

			scheduler		=> \&scheduler,
			enqueue			=> \&enqueue,
			taskstart		=> \&taskstart,
			dequeue			=> \&dequeue,

			shutdown		=> \&shutdown
		},

		args => [ $hopkins ]
	);

	return $self;
}

=item start

=cut

sub start
{
	my $kernel	= $_[KERNEL];
	my $heap	= $_[HEAP];
	my $hopkins	= $_[ARG0];

	# a little initialization:
	#
	# save the Hopkins object for future use (mostly for
	# accessing the values of any command-line switches).
	# also initialize the list of plugins and queues.  oh,
	# yeah, get our Ps and Qs in order...

	$heap->{hopkins} = $hopkins;
	$heap->{plugins} = {};

	# set the alias for the current session
	$kernel->alias_set('manager');

	# create a File::Monitor object, a callback, and an
	# associated POE alarm to watch the configuration file
	# for subsequent changes

	# post events for initial setup
	$kernel->call(manager => 'init_config');	# configuration file
	$kernel->post(manager => 'init_state');		# machine state session
	$kernel->post(manager => 'init_store');		# database storage sessage
	$kernel->post(manager => 'init_queues');	# worker queue sessions

	$kernel->alarm(scheduler => time + $hopkins->poll);
}

=item stop

=cut

sub stop
{
	Hopkins->log_debug('exiting');
}

=item init_queues

=cut

sub init_queues
{
	my $kernel	= $_[KERNEL];
	my $heap	= $_[HEAP];
	my $config	= $heap->{config};

	# create a passive queue for each configured queue.  we
	# use POE::Component::JobQueue and leave the scheduling
	# up to the manager session.

	foreach my $name ($config->get_queue_names) {
		$kernel->post(manager => queue_start => $name);
	}
}

sub queue_start
{
	my $kernel	= $_[KERNEL];
	my $heap	= $_[HEAP];
	my $name	= $_[ARG0];
	my $config	= $heap->{config};

	return HOPKINS_QUEUE_ALREADY_RUNNING if exists $heap->{queues}->{$name};

	if (my $opts = $config->get_queue_info($name)) {
		$heap->{queues}->{$name} = new Hopkins::Queue $opts;
	} else {
		return HOPKINS_QUEUE_NOT_FOUND;
	}

	return HOPKINS_QUEUE_STARTED;
}

sub queue_check
{
	my $kernel	= $_[KERNEL];
	my $heap	= $_[HEAP];
	my $name	= $_[ARG0];
	my $config	= $heap->{config};

	return $heap->{queues}->{$name};
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
	my $kernel	= $_[KERNEL];
	my $heap	= $_[HEAP];

	$heap->{config} = new Hopkins::Config { file => $heap->{hopkins}->conf };

	$kernel->call(manager => 'config_load');
	$kernel->alarm(confscan => time + $heap->{hopkins}->scan);
}

=item init_plugins

=cut

sub init_plugins
{
	my $kernel	= $_[KERNEL];
	my $heap	= $_[HEAP];

	Hopkins->log_debug('initializing plugins');

	my $config	= $heap->{config};
	my $plugins	= $heap->{plugins};

	delete $plugins->{$_}
		foreach grep { not $config->has_plugin($_) } keys %$plugins;

	foreach my $name ($config->get_plugin_names) {
		if (not exists $plugins->{$name}) {
			my $package = $name =~ /^\+/ ? $name : "Hopkins::Plugin::$name";
			my $path	= $package;

			$path =~ s{::}{/}g;

			require "$path.pm";

			$plugins->{$name} = $package->new($config->get_plugin_info($name));
		}
	}
}

=item config_load

=cut

sub config_load
{
	my $kernel	= $_[KERNEL];
	my $heap	= $_[HEAP];

	my $config	= $heap->{config};
	my $status	= $config->load;

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
	my $kernel	= $_[KERNEL];
	my $heap	= $_[HEAP];

	print "WHAT THE FUCK MAN\n";

	if ($heap->{config}->scan) {
		Hopkins->log_info('configuration file changed');
		$kernel->post(manager => 'confload')
	}

	$kernel->alarm(confscan => time + $heap->{hopkins}->scan);
}

=item shutdown

=cut

sub shutdown
{
	my $kernel	= $_[KERNEL];
	my $heap	= $_[HEAP];
	my $config	= $heap->{config};

	Hopkins->log_info('received shutdown request');

	$kernel->alarm('scheduler');
	$kernel->alarm('confscan');

	foreach my $name ($config->get_queue_names) {
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
	my $config	= $heap->{config};
	my $hopkins	= $heap->{hopkins};

	Hopkins->log_debug('checking queue for tasks to post');

	foreach my $name ($config->get_task_names) {
		my $now		= DateTime->now;
		my $task	= $config->get_task_info($name);

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

	$kernel->alarm(scheduler => time + $hopkins->poll);
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
	my $config	= $heap->{config};

	my $task = $config->get_task_info($name);

	if (not defined $task) {
		Hopkins->log_warn("unable to enqueue $name; task not found");
		return HOPKINS_ENQUEUE_TASK_NOT_FOUND;
	}

	my $queue = $kernel->call(manager => queue_check => $task->queue);

	if (not defined $queue) {
		Hopkins->log_warn("unable to enqueue $name; queue " . $task->queue . ' not running');
		return HOPKINS_ENQUEUE_QUEUE_UNAVAILABLE;
	}

	# notify the state tracker that we're going to enqueue
	# a task.  the state tracker will assign an identifier
	# that will be used to identify it throughout its life.

	my $work = new Hopkins::Work { task => $task, queue => $queue };

	$kernel->call(state => task_enqueued => $work);

	# post one of two enqueue events, depending on whether
	# the task has an associated class name or an explicit
	# command line specification.

	Hopkins->log_debug("enqueuing task $name (" . $work->id . ')');

	$kernel->post($queue->alias => enqueue => dequeue => $work);

	return HOPKINS_ENQUEUE_OK;
}

=item queue_failure

=cut

sub queue_failure
{
	my $kernel	= $_[KERNEL];
	my $heap	= $_[HEAP];
	my $queue	= $_[ARG0];

	my $msg = 'task failure in ' . $queue->name . ' queue';

	if ($queue->onerror eq 'halt') {
		$msg .= '; halting queue';
		$kernel->post($queue->alias => 'stop');
	}

	Hopkins->log_error($msg);
}

sub dequeue
{
	my $kernel	= $_[KERNEL];
	my $heap	= $_[HEAP];
	my $params	= $_[ARG0];
	my $work	= $params->[0];

	Hopkins->log_debug('dequeued task ' . $work->task->name . ' (' . $work->id . ')');

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
	my $kernel	= $_[KERNEL];
	my $heap	= $_[HEAP];
	my $id		= $_[ARG0];

	$kernel->post(store => 'notify', 'task_update', $id, status => 'running');
}

=back

=head1 AUTHOR

Mike Eldridge <diz@cpan.org>

=head1 LICENSE

=cut

1;
