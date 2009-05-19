package Hopkins::RPC;

use strict;

=head1 NAME

Hopkins::RPC - hopkins RPC session

=head1 DESCRIPTION

Hopkins::RPC encapsulates the RPC (remote procedure call)
POE session created by the manager session.  this session
uses the Server::SOAP component to provide a SOAP interface
to the job server.

=cut

use POE;
use POE::API::Peek;
use Class::Accessor::Fast;

use base 'Class::Accessor::Fast';

__PACKAGE__->mk_accessors(qw(soap config));

use constant HOPKINS_QUEUE_STATUS_WAIT_TIME		=> 1;
use constant HOPKINS_QUEUE_STATUS_WAIT_ITER_MAX	=> 5;

my @procedures =
qw/
	enqueue
	status
	queue_start
	queue_start_waitchk
	queue_stop
	queue_stop_waitchk
/;

=head1 STATES

=over 4

=item init

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
}

=item start

=cut

sub start
{
	my $self	= $_[OBJECT];
	my $kernel	= $_[KERNEL];

	my $args =
	{
		ALIAS	=> 'soap',
		ADDRESS	=> $self->config->{address},
		PORT	=> $self->config->{port}
	};

	$self->soap(new POE::Component::Server::SOAP %$args);

	$kernel->alias_set('rpc');
	$kernel->post(soap => ADDMETHOD => rpc => $_) foreach @procedures;
}

=item stop

=cut

sub stop
{
	my $kernel = $_[KERNEL];

	$kernel->post(soap => DELMETHOD => rpc => $_) foreach @procedures;
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
	my $task			= Hopkins::Config->get_task_info($name);

	Hopkins->log_debug("enqueue request received from $client for $name");

	if ($task) {
		# if the task configuration was located, attempt to
		# enqueue it by sending a synchronous event to the
		# manager session.

		my $queued = $kernel->call(manager => enqueue => $name => $opts);

		if ($queued) {
			# success!  the task has been queued!
			$res->content({ success => 1 });
		} else {
			# something failed during the enqueing process.
			# report back with a generic error message.

			Hopkins->log_error("failure in scheduler while attempting to enqueue $name");
			$res->content({ success => 0, err => "failure in scheduler while attempting to enqueue $name" });
		}
	} else {
		# if the task configuration was unable to be found,
		# tell the client so.

		$res->content({ success => 0, err => "unable to locate task $name" });
	}

	# post a DONE event to the soap session; this will cause
	# a SOAP response to be sent back to the client.

	$kernel->post(soap => DONE => $res);
}

sub status
{
	my $kernel	= $_[KERNEL];
	my $res		= $_[ARG0];
	#my $api		= new POE::API::Peek;

	#my @sessions = map { $kernel->alias($_) } $api->session_list;

	#my $now		= DateTime->now;
	#my $schema	= Hopkins::Store->schema;
	#my $rsTask	= $schema->resultset('Task');

	#$rsTask->search({ date_completed => undef });

	#$res->content({ sessions => \@sessions });

	my @queues = $kernel->call(manager => 'queue_check_all');

	my $status =
	{
		queues =>
		{
			map {
				$_->name => 
				{
					concurrency	=> $queue->concurrency,
					tasks		=> $queue->tasks->count,
					status		=> 'running',
				}
			} @queues
		}
	};

	$res->content($status);

	$kernel->post(soap => DONE => $res);
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

	my $res = $kernel->call(manager => queue_start => $name);

	for ($res) {
		$_ == HOPKINS_QUEUE_STARTED && do
		{
			# the manager session successfully created the
			# queue.  the only thing to do now is wait until
			# that session shows itself.

			$kernel->alarm(queue_start_waitchk => time + HOPKINS_QUEUE_STATUS_WAIT_TIME, $res, $name, 0);
			last;
		};

		$_ == HOPKINS_QUEUE_ALREADY_RUNNING && do
		{
			# the queue was already running.  post a DONE
			# event to the soap session indicating success,
			# but note that the queue was already running.

			$res->content({ success => 1, err => "queue $name already running" });
			$kernel->post(soap => DONE => $res);

			last;
		};

		$_ == HOPKINS_QUEUE_NOT_FOUND and do
		{
			# the specified queue was unable to be found in
			# the configuration.  post a DONE event to the
			# soap session; this will cause a SOAP response
			# to be sent back to the client.

			$res->content({ success => 0, err => "invalid queue $name" });
			$kernel->post(soap => DONE => $res);
			last;
		};
	}
}

sub queue_start_waitchk
{
	my $kernel	= $_[KERNEL];
	my $res		= $_[ARG0];
	my $name	= $_[ARG1];
	my $iter	= $_[ARG2];

	Hopkins->log_debug("queue_start_waitchk: checking status of queue $name");

	my $queue	= $kernel->call(manager => queue_check => $name);
	my $running	= $queue ? $queue->is_running : 0;

	if ($running) {
		# the session was located; the queue is now running.
		#
		# post a DONE event to the soap session; this will
		# cause a SOAP response to be sent back to the
		# client.

		$res->content({ success => 1 });
		$kernel->post(soap => DONE => $res);
	} else {
		# if the session wasn't found, we'll try to wait a
		# bit for it to show up.  if we exceed the maximum
		# number of wait iterations, we'll return an error
		# to the client.

		if ($iter > HOPKINS_QUEUE_STATUS_WAIT_ITER_MAX) {
			# exceeded maximum wait iterations; return an
			# error to the client.

			$res->content({ success => 0, err => "unable to start queue $name" });
			$kernel->post(soap => DONE => $res);
		} else {
			# else we'll go another round.  set a kernel
			# alarm for the appropriate time.

			$kernel->alarm(queue_start_waitchk => time + HOPKINS_QUEUE_STATUS_WAIT_TIME, $res, $name, ++$iter);
		}
	}
}

sub queue_stop
{
	my $kernel	= $_[KERNEL];
	my $res		= $_[ARG0];

	# grab the client, the SOAP parameters, the name of
	# the queue that we've been requested to shutdown, and
	# an instance of the POE instrospection API

	my $client	= $res->connection->remote_ip;
	my $params	= $res->soapbody;
	my ($name)	= map { $params->{$_} } sort keys %$params;
	my $queue	= $kernel->call(manager => queue_check => $name);

	Hopkins->log_debug("queue_stop request received from $client for $name");

	$kernel->post(manager => queue_stop => $name);
	$kernel->alarm(queue_stop_waitchk => time + HOPKINS_QUEUE_STATUS_WAIT_TIME, $res, $queue, 0);
}

sub queue_stop_waitchk
{
	my $kernel	= $_[KERNEL];
	my $res		= $_[ARG0];
	my $queue	= $_[ARG1];
	my $iter	= $_[ARG2];

	Hopkins->log_debug('queue_stop_waitchk: checking status of queue ' . $queue->name);

	if ($queue->is_running) {
		# if the session was found, we'll try to wait a bit
		# for it to be stopped.  if we exceed the maximum
		# number of wait iterations, we'll return an error
		# to the client.

		if ($iter > HOPKINS_QUEUE_STATUS_WAIT_ITER_MAX) {
			# exceeded maximum wait iterations; return an
			# error to the client.

			$res->content({ success => 0, err => 'unable to stop ' . $queue->name });
		} else {
			# else we'll go another round.  set a kernel
			# alarm for the appropriate time.

			$kernel->alarm(queue_stop_waitchk => time + HOPKINS_QUEUE_STATUS_WAIT_TIME, $res, $queue, ++$iter);
		}
	} else {
		# the session is gone; the queue is now stopped.
		#
		# post a DONE event to the soap session; this will
		# cause a SOAP response to be sent back to the
		# client.

		$res->content({ success => 1 });
		$kernel->post(soap => DONE => $res);
	}
}

=back

=head1 AUTHOR

Mike Eldridge <diz@cpan.org>

=head1 LICENSE

This program is free software; you may redistribute it
and/or modify it under the same terms as Perl itself.

=cut

1;
