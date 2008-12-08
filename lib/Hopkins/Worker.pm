package Hopkins::Worker;

use strict;

=head1 NAME

Hopkins::Worker - hopkins worker session

=head1 DESCRIPTION

Hopkins::Worker encapsulates a POE session created by
Hopkins::Queue->spawn_worker via POE::Component::JobQueue.

=cut

use POE;

=head1 STATES

=over 4

=item spawn

=cut

sub spawn
{
	my $postback	= shift;
	my $id			= shift;
	my $name		= shift;
	my $method		= shift;
	my $source		= shift;
	my $params		= shift;
	my $queue		= shift;

	Hopkins->log_debug("spawning worker: type=$method; source=$source");

	my $now		= DateTime->now;
	my $schema	= Hopkins::Store->schema;
	my $rsTask	= $schema->resultset('Task');

	$rsTask->find($id)->update({ date_started => $now });

	POE::Session->create
	(
		inline_states => {
			_start		=> \&start,
			_stop		=> \&stop,

			stdout		=> \&log_stdout,
			stderr		=> \&log_stderr,
			shutdown	=> \&shutdown,
			done		=> \&done
		},

		args => [ $postback, $id, $name, $method, $source, $params, $queue ]
	);
}

=item closure

=cut

sub inline
{
	my $self	= shift;
	my $source	= shift;
	my $params	= shift;

	return sub
	{
		Hopkins::Store->schema->storage->dbh->{InactiveDestroy} = 1;

		eval "require $source";
		die $@ if $@;

		my $obj = new $source '', $params;

		return $obj->run;
	}
}

=item start

=cut

sub start
{
	my $kernel		= $_[KERNEL];
	my $heap		= $_[HEAP];
	my $postback	= $_[ARG0];
	my $id			= $_[ARG1];
	my $name		= $_[ARG2];
	my $method		= $_[ARG3];
	my $source		= $_[ARG4];
	my $params		= $_[ARG5];
	my $queue		= $_[ARG6];

	$kernel->post('manager' => 'taskstart', $id);

	# set the name of this session's alias based on the queue and session ID
	my $session = $kernel->get_active_session;
	$kernel->alias_set(join '.', $queue, 'worker', $session->ID);

	Hopkins->log_debug('worker session created');

	# determine the Program argument based upon what "method" we're using
	my $program = $method eq 'perl'
		? Hopkins::Worker->inline($source, $params)
		: $source;

	# construct the arguments neccessary for POE::Wheel::Run
	my %args =
	(
		Program		=> $program,
		StdoutEvent	=> 'stdout',
		StderrEvent	=> 'stderr'
	);

	$kernel->sig(CHLD => 'done');

	# set a few session-specific things in the heap and then spawn the child
	$heap->{postback}	= $postback;
	$heap->{queue}		= $queue;
	$heap->{name}		= $name;
	$heap->{child}		= new POE::Wheel::Run %args;
}

sub stop
{
	my $kernel	= $_[KERNEL];
	my $heap	= $_[HEAP];

	Hopkins->log_debug('session destroyed');
}

sub done
{
	my $kernel	= $_[KERNEL];
	my $heap	= $_[HEAP];
	my $signal	= $_[ARG0];
	my $pid		= $_[ARG1];
	my $status	= $_[ARG2];

	return if $pid != $heap->{child}->PID;

	Hopkins->log_debug("child process $pid exited with status $status");

	if ($status == 0) {
		Hopkins->log_info("worker successfully completed $heap->{name}");
	} else {
		Hopkins->log_error("worker exited abnormally ($status) while executing $heap->{name}");
		$kernel->call(manager => queuefail => $heap->{queue});
	}

	$heap->{postback}->($pid, $status);
	$kernel->yield('shutdown');
}

sub shutdown
{
	my $kernel	= $_[KERNEL];
	my $heap	= $_[HEAP];

	# we have to remove the session alias from the kernel,
	# else POE will never destroy the session.

	my $session	= $kernel->get_active_session;
	my $alias	= join '.', $heap->{queue}, 'worker', $session->ID;

	$kernel->alias_remove($alias);
}

sub log_stdout { Hopkins->log_worker_stdout($_[HEAP]->{name}, $_[ARG0]) }
sub log_stderr { Hopkins->log_worker_stderr($_[HEAP]->{name}, $_[ARG0]) }

=back

=head1 AUTHOR

Mike Eldridge <diz@cpan.org>

=head1 LICENSE

This program is free software; you may redistribute it
and/or modify it under the same terms as Perl itself.

=cut

1;
