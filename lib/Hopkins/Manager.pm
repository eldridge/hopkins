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

	#$kernel->post($_ => 'stop') foreach keys %{ $heap->{config}->{queue} };

	foreach my $name (Hopkins::Config->get_queue_names) {
		Hopkins->log_debug("posting stop event for $name queue");
		$kernel->post($name => 'stop');
	}
}

=item postback

=cut

sub postback
{
	Hopkins->log_debug('postback');
}

=item scheduler

=cut

sub scheduler
{
	my $kernel	= $_[KERNEL];
	my $heap	= $_[HEAP];

	Hopkins->log_debug('checking queue for tasks to post');

	my $now		= DateTime->now;
	my $schema	= Hopkins::Store->schema;
	my $rsTask	= $schema->resultset('Task');

	foreach my $name (Hopkins::Config->get_task_names) {
		my $task	= Hopkins::Config->get_task_info($name);
		my $set		= $task->{schedules};
		my $queue	= $task->{queue};
		my $class	= $task->{class};
		my $opts	= $task->{option};
		my $cmd		= $task->{cmd};
		my $serial	= $task->{run} eq 'serial' ? 1 : 0;
		my $active	= lc $task->{active} eq 'no' ? 0 : 1;
		my $last	= $set->previous($now);

		Hopkins->log_debug("checking if $name is marked inactive");
		next if not $active;

		#Hopkins->log_debug("checking if $name has been executed since $last");
		#next if $rsTask->task_executed_since($name, $last);

		#Hopkins->log_debug("checking if $name is currently executing");
		#next if $rsTask->task_executing_now($name) and $serial;

		# post one of two enqueue events, depending on the type of job
		Hopkins->log_debug("posting enqueue event for $name");
		$kernel->post($queue => enqueue => undef => perl => $class => $opts) if $class;
		$kernel->post($queue => enqueue => undef => exec => $cmd) if $cmd;

		# record the fact that we've queued this job
		$rsTask->create({ name => $name, date_queued => $now });
	}

	$kernel->alarm(scheduler => time + $heap->{opts}->{poll});
}

=back

=head1 AUTHOR

Mike Eldridge <diz@cpan.org>

=head1 LICENSE

=cut

1;
