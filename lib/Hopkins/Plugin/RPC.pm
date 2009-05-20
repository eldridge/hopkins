package Hopkins::Plugin::RPC;

use strict;

=head1 NAME

Hopkins::Plugin::RPC - hopkins RPC session

=head1 DESCRIPTION

Hopkins::Plugin::RPC encapsulates the RPC (remote procedure
call) POE session created by the manager session.  this
session uses the Server::SOAP component to provide a SOAP
interface to the job server.

=cut

use POE;

use Class::Accessor::Fast;

use Hopkins::Constants;

use base 'Class::Accessor::Fast';

use constant HOPKINS_QUEUE_STATUS_WAIT_TIME		=> 1;
use constant HOPKINS_QUEUE_STATUS_WAIT_ITER_MAX	=> 5;

__PACKAGE__->mk_accessors(qw(kernel manager soap config));

my @procedures =
qw/
	enqueue
	status
	queue_start
	queue_start_waitchk
	queue_halt
	queue_stop_waitchk
	queue_freeze
	queue_shutdown
	queue_flush
/;

=head1 METHODS

=over 4

=item new

=cut

sub new
{
	my $self = shift->SUPER::new(@_);

	$self->config({}) if not ref $self->config eq 'HASH';

	$self->config->{address}	||= 0;
	$self->config->{port}		||= 8080;

	# create RPC session
	POE::Session->create
	(
		object_states =>
		[
			$self =>
			{
				_start	=> 'start',
				_stop	=> 'stop',

				map { $_ => $_ } @procedures
			}
		]
	);

	return $self;
}

sub DESTROY
{
	my $self = shift;

	$self->kernel->post('rpc.soap'	=> 'SHUTDOWN');
	$self->kernel->post(rpc			=> 'shutdown');
}

=item start

=cut

sub start
{
	my $self	= $_[OBJECT];
	my $kernel	= $_[KERNEL];

	my $args =
	{
		ALIAS	=> 'rpc.soap',
		ADDRESS	=> $self->config->{address},
		PORT	=> $self->config->{port}
	};

	$self->soap(new POE::Component::Server::SOAP %$args);

	$kernel->alias_set('rpc');
	$kernel->post('rpc.soap' => ADDMETHOD => rpc => $_) foreach @procedures;
}

=item stop

=cut

sub stop
{
	my $kernel = $_[KERNEL];

	$kernel->post('rpc.soap' => DELMETHOD => rpc => $_) foreach @procedures;
}

=item enqueue

=cut

sub enqueue
{
	my $kernel	= $_[KERNEL];
	my $res		= $_[ARG0];

	# pull out the first two parameters we received, grab
	# the client's IP address, and attempt to locate the
	# requested task in the local configuration

	my $params			= $res->soapbody;
	my ($name, $opts)	= map { $params->{$_} } sort keys %$params;
	my $client			= $res->connection->remote_ip;

	Hopkins->log_debug("enqueue request received from $client for $name");

	my $rv = $kernel->call(manager => enqueue => $name => $opts);

	for ($rv) {
		# success!  the task has been queued!

		$rv == HOPKINS_ENQUEUE_OK
		and do {
			$res->content({ success => 1 });
			last;
		};

		# if the task configuration was unable to be found,
		# tell the client so.

		$rv == HOPKINS_ENQUEUE_TASK_NOT_FOUND
		and do {
			$res->content({ success => 0, err => "unable to locate task $name" });
			last;
		};

		# the queue has been frozen, so we can't enqueue
		# anything to it.  tell the client to piss off.

		$rv == HOPKINS_ENQUEUE_QUEUE_FROZEN
		and do {
			Hopkins->log_error("unable to enqueue $name: queue is frozen");
			$res->content({ success => 0, err => "unable to enqueue $name: queue is frozen" });
			last;
		};

		# the queue isn't even in the same dimension as we
		# are, so we can't enqueue anything to it.  tell the
		# client to piss off.

		$rv == HOPKINS_ENQUEUE_QUEUE_UNAVAILABLE
		and do {
			Hopkins->log_error("unable to enqueue $name: queue is unavailable");
			$res->content({ success => 0, err => "unable to enqueue $name: queue is unavailable" });
			last;
		};

		# something else failed during the enqueing process,
		# but we don't know what it was.  report back with a
		# generic error message.

		Hopkins->log_error("failure in scheduler while attempting to enqueue $name");
		$res->content({ success => 0, err => "failure in scheduler while attempting to enqueue $name" });
	}

	# post a DONE event to the soap session; this will cause
	# a SOAP response to be sent back to the client.

	$kernel->post('rpc.soap' => DONE => $res);
}

sub status
{
	my $kernel	= $_[KERNEL];
	my $res		= $_[ARG0];

	my $status = {};

	foreach my $queue ($kernel->call(manager => 'queue_check_all')) {
		$status->{queues}->{$queue->name} =
		{
			concurrency	=> $queue->concurrency,
			queued		=> $queue->tasks->Length,
			status		=> $queue->status_string
		};

		$status->{queues}->{$queue->name}->{error} = $queue->error
			if $queue->error;
	}

	$status->{sessions} = [ Hopkins->get_running_sessions ];

	$res->content($status);

	$kernel->post('rpc.soap' => DONE => $res);
}

sub queue_start
{
	my $kernel	= $_[KERNEL];
	my $res		= $_[ARG0];

	# grab the client, the SOAP parameters, and the name of
	# the queue that we've been requested to start up.

	my $client	= $res->connection->remote_ip;
	my $params	= $res->soapbody;
	my ($name)	= map { $params->{$_} } sort keys %$params;

	Hopkins->log_debug("queue_start request received from $client for $name");

	my $rv = $kernel->call(manager => queue_start => $name);

	for ($rv) {
		# the queue was spawned; the only thing to do
		# now is wait until that session shows itself.

		$rv == HOPKINS_QUEUE_STARTED and do
		{
			$kernel->alarm(queue_start_waitchk => time + HOPKINS_QUEUE_STATUS_WAIT_TIME, $res, $name, 0);
			last;
		};

		# the queue was already running; go ahead and post a
		# DONE event to the soap session.  this will cause a
		# SOAP response to be sent back to the client.

		$rv == HOPKINS_QUEUE_ALREADY_RUNNING and do
		{
			$res->content({ success => 1 });
			$kernel->post('rpc.soap' => DONE => $res);
			last;
		};

		# the queue wasn't found in the configuration; go
		# ahead and post a DONE event to the soap session.
		# this will cause a SOAP response to be sent back to
		# the client.

		$rv == HOPKINS_QUEUE_NOT_FOUND and do
		{
			$res->content({ success => 0, err => "invalid queue $name" });
			$kernel->post('rpc.soap' => DONE => $res);
			last;
		};

		# something else failed during the request, but we
		# don't know what it was.  report back with a
		# generic error message.

		Hopkins->log_error("failure in scheduler while attempting to start queue $name");
		$res->content({ success => 0, err => "failure in scheduler while attempting to start queue $name" });
	}
}

sub queue_start_waitchk
{
	my $self	= $_[OBJECT];
	my $kernel	= $_[KERNEL];
	my $res		= $_[ARG0];
	my $name	= $_[ARG1];
	my $iter	= $_[ARG2];

	Hopkins->log_debug("queue_start_waitchk: checking status of queue $name");

	my $queue = $self->manager->queue($name);

	if ($queue && $queue->status == HOPKINS_QUEUE_STATUS_RUNNING) {
		# the session was located; the queue is now running.
		#
		# post a DONE event to the soap session; this will
		# cause a SOAP response to be sent back to the
		# client.

		$res->content({ success => 1 });
		$kernel->post('rpc.soap' => DONE => $res);
	} else {
		# if the session wasn't found, we'll try to wait a
		# bit for it to show up.  if we exceed the maximum
		# number of wait iterations, we'll return an error
		# to the client.

		if ($iter > HOPKINS_QUEUE_STATUS_WAIT_ITER_MAX) {
			# exceeded maximum wait iterations; return an
			# error to the client.

			$res->content({ success => 0, err => "unable to start queue $name" });
			$kernel->post('rpc.soap' => DONE => $res);
		} else {
			# else we'll go another round.  set a kernel
			# alarm for the appropriate time.

			$kernel->alarm(queue_start_waitchk => time + HOPKINS_QUEUE_STATUS_WAIT_TIME, $res, $name, ++$iter);
		}
	}
}

sub queue_halt
{
	my $kernel	= $_[KERNEL];
	my $res		= $_[ARG0];

	# grab the client, the SOAP parameters, the name of
	# the queue that we've been requested to shutdown, and
	# an instance of the POE instrospection API

	my $client	= $res->connection->remote_ip;
	my $params	= $res->soapbody;
	my ($name)	= map { $params->{$_} } sort keys %$params;

	Hopkins->log_debug("queue halt request received from $client for $name");

	$kernel->post(manager => queue_halt => $name);
	$kernel->alarm(queue_stop_waitchk => time + HOPKINS_QUEUE_STATUS_WAIT_TIME, $res, $name, 0);
}

sub queue_stop_waitchk
{
	my $self	= $_[OBJECT];
	my $kernel	= $_[KERNEL];
	my $res		= $_[ARG0];
	my $name	= $_[ARG1];
	my $iter	= $_[ARG2];

	Hopkins->log_debug("queue_stop_waitchk: checking status of queue $name");

	my $queue = $self->manager->queue($name);

	if (not $queue or not $queue->is_running) {
		# the session is gone; the queue is now stopped.
		#
		# post a DONE event to the soap session; this will
		# cause a SOAP response to be sent back to the
		# client.

		$res->content({ success => 1 });
		$kernel->post('rpc.soap' => DONE => $res);
	} else {
		# if the session was found, we'll try to wait a bit
		# for it to be stopped.  if we exceed the maximum
		# number of wait iterations, we'll return an error
		# to the client.

		if ($iter > HOPKINS_QUEUE_STATUS_WAIT_ITER_MAX) {
			# exceeded maximum wait iterations; return an
			# error to the client.

			$res->content({ success => 0, err => "unable to stop $name" });
			$kernel->post('rpc.soap' => DONE => $res);
		} else {
			# else we'll go another round.  set a kernel
			# alarm for the appropriate time.

			$kernel->alarm(queue_stop_waitchk => time + HOPKINS_QUEUE_STATUS_WAIT_TIME, $res, $name, ++$iter);
		}
	}
}

sub queue_freeze
{
	my $kernel	= $_[KERNEL];
	my $res		= $_[ARG0];

	# grab the client, the SOAP parameters, and the name of
	# the queue that we've been requested to start up.

	my $client	= $res->connection->remote_ip;
	my $params	= $res->soapbody;
	my ($name)	= map { $params->{$_} } sort keys %$params;

	Hopkins->log_debug("queue freeze request received from $client for $name");

	$kernel->post(manager => queue_freeze => $name);
	$kernel->alarm(queue_stop_waitchk => time + HOPKINS_QUEUE_STATUS_WAIT_TIME, $res, $name, 0);
}

sub queue_shutdown
{
	my $kernel	= $_[KERNEL];
	my $res		= $_[ARG0];

	# grab the client, the SOAP parameters, and the name of
	# the queue that we've been requested to start up.

	my $client	= $res->connection->remote_ip;
	my $params	= $res->soapbody;
	my ($name)	= map { $params->{$_} } sort keys %$params;

	Hopkins->log_debug("queue shutdown request received from $client for $name");

	$kernel->post(manager => queue_shutdown => $name);
	$kernel->alarm(queue_stop_waitchk => time + HOPKINS_QUEUE_STATUS_WAIT_TIME, $res, $name, 0);
}

sub queue_flush
{
	my $kernel	= $_[KERNEL];
	my $res		= $_[ARG0];

	# grab the client, the SOAP parameters, and the name of
	# the queue that we've been requested to start up.

	my $client	= $res->connection->remote_ip;
	my $params	= $res->soapbody;
	my ($name)	= map { $params->{$_} } sort keys %$params;

	Hopkins->log_debug("queue_flush request received from $client for $name");

	$kernel->post(manager => queue_flush => $name);

	$res->content({ success => 1 });
	$kernel->post('rpc.soap' => DONE => $res);
}

=back

=head1 AUTHOR

Mike Eldridge <diz@cpan.org>

=head1 LICENSE

This program is free software; you may redistribute it
and/or modify it under the same terms as Perl itself.

=cut

1;
