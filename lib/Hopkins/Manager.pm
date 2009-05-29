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

use Data::UUID;
use Path::Class::Dir;

use Hopkins::Store;
use Hopkins::Config;
use Hopkins::Queue;
use Hopkins::Task;
use Hopkins::Work;

use Hopkins::Constants;

use base 'Class::Accessor::Fast';

__PACKAGE__->mk_accessors(qw(hopkins config plugins queues));

my $ug = new Data::UUID;

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

				queue_check_all	=> 'queue_check_all',
				queue_check		=> 'queue_check',
				queue_failure	=> 'queue_failure',
				queue_start		=> 'queue_start',
				queue_halt		=> 'queue_halt',
				queue_continue	=> 'queue_continue',
				queue_freeze	=> 'queue_freeze',
				queue_thaw		=> 'queue_thaw',
				queue_shutdown	=> 'queue_shutdown',
				queue_flush		=> 'queue_flush',

				scheduler		=> 'scheduler',
				enqueue			=> 'enqueue',
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
		my $opts	= $self->config->get_queue_info($name);
		my $queue	= new Hopkins::Queue { kernel => $kernel, config => $self->config, %$opts };

		$self->queues->{$name} = $queue;

		$kernel->post(manager => queue_start => $name) unless $queue->halted;
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

sub queue_continue
{
	my $self	= $_[OBJECT];
	my $name	= $_[ARG0];
	my $queue	= $self->queue($name);

	$queue->continue if $queue;
}

sub queue_freeze
{
	my $self	= $_[OBJECT];
	my $name	= $_[ARG0];
	my $queue	= $self->queue($name);

	$queue->freeze if $queue;
}

sub queue_thaw
{
	my $self	= $_[OBJECT];
	my $name	= $_[ARG0];
	my $queue	= $self->queue($name);

	$queue->thaw if $queue;
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
	my $self = shift;

	new Hopkins::Store { config => $self->config };
}

=item init_config

=cut

sub init_config
{
	my $self	= $_[OBJECT];
	my $kernel	= $_[KERNEL];

	my $class = 'Hopkins::Config::' . $self->hopkins->conf->[0];

	eval "use $class";

	$self->config($class->new($self->hopkins->conf->[1]));

	$kernel->call(manager => 'config_load');
	$kernel->alarm(config_scan => time + $self->hopkins->scan);
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
			: 'unable to load configuration: ' . $status->errmsg;

		Hopkins->log_error($err);
	}

	$kernel->post(manager => 'shutdown') if not $self->config->loaded;

	return unless $status->updated;

	if ($status->store_modified) {
		Hopkins->log_debug('database information changed');
		$kernel->post(store => 'init');
	}

	$kernel->post(manager => 'init_plugins');
}

=item config_scan

=cut

sub config_scan
{
	my $self	= $_[OBJECT];
	my $kernel	= $_[KERNEL];

	if ($self->config->scan) {
		Hopkins->log_info('configuration file changed');
		$kernel->post(manager => 'config_load')
	}

	$kernel->alarm(config_scan => time + $self->hopkins->scan);
}

=item shutdown

=cut

sub shutdown
{
	my $self	= $_[OBJECT];
	my $kernel	= $_[KERNEL];

	Hopkins->log_info('received shutdown request');

	$kernel->alarm('scheduler');
	$kernel->alarm('config_scan');

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

	# create new work for the queue.  assign a unique ID via
	# Data::UUID, add it to the queue, and flush the queue's
	# state to disk.

	my $now		= DateTime->now;
	my $work	= new Hopkins::Work;

	$work->id($ug->create_str);
	$work->task($task);
	$work->queue($queue);
	$work->options($opts);
	$work->date_enqueued($now);

	$queue->tasks->Push($work->id => $work);
	$queue->write_state;

	Hopkins->log_debug("enqueued task $name (" . $work->id . ')');

	# notify the Store that we've enqueued a task

	$kernel->post(store => notify => task_enqueued => $work->serialize);

	# post an enqueue event to the PoCo::JobQueue session.
	# if the session is not running, this event will have
	# no effect, though the task will still be enqueued
	# in hopkins.

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

	$work->date_completed(DateTime->now);

	$work->queue->tasks->Delete($work->id);
	$work->queue->write_state;

	$kernel->post(store => notify => task_completed => $work->serialize);
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
