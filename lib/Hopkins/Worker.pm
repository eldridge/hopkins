package Hopkins::Worker;

use strict;

=head1 NAME

Hopkins::Worker - hopkins worker session

=head1 DESCRIPTION

Hopkins::Worker encapsulates a POE session created by
Hopkins::Queue->spawn_worker via POE::Component::JobQueue.

=cut

use POE;

use Class::Accessor::Fast;

use base 'Class::Accessor::Fast';

__PACKAGE__->mk_accessors(qw(alias postback task params child));

=head1 STATES

=over 4

=item spawn

=cut

sub new
{
	my $self = shift->SUPER::new(@_);

	my $method = $self->task->class ? 'perl' : 'exec';
	my $source = $self->task->class || $self->task->cmd;

	Hopkins->log_debug("spawning worker: type=$method; source=$source");

	#my $now		= DateTime->now;
	#my $schema	= Hopkins::Store->schema;
	#my $rsTask	= $schema->resultset('Task');

	#$rsTask->find($id)->update({ date_started => $now });

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

		args => [ $self ]
	);
}

=item closure

=cut

sub inline
{
	my $self	= shift;
	my $class	= shift;
	my $params	= shift || {};

	return sub
	{
		#Hopkins::Store->schema->storage->dbh->{InactiveDestroy} = 1;

		eval "require $class";
		die $@ if $@;

		my $obj = $class->new({ options => $params });

		return $obj->run;
	}
}

=item start

=cut

sub start
{
	my $kernel		= $_[KERNEL];
	my $heap		= $_[HEAP];
	my $worker		= $_[ARG0];
	my $task		= $worker->task;

	$kernel->post(store => 'notify', 'task_update', $task->id, status => 'running');
	#$kernel->post('manager' => 'taskstart', $worker->id);

	# set the name of this session's alias based on the queue and session ID
	my $session = $kernel->get_active_session;

	$worker->alias(join '.', $task->queue->name, 'worker', $session->ID);
	$kernel->alias_set($worker->alias);

	Hopkins->log_debug('worker session created');

	# determine the Program argument based upon what method
	# we're using.  POE::Wheel::Run will execute both native
	# perl code as well as external binaries depending upon
	# whether the argument is a coderef or a simple scalar.

	my $program = $task->class
		? $worker->inline($task->class, $worker->params)
		: $task->cmd;

	# construct the arguments neccessary for POE::Wheel::Run

	my %args =
	(
		Program		=> $program,
		StdoutEvent	=> 'stdout',
		StderrEvent	=> 'stderr'
	);

	# store the Worker object in the HEAP, make sure we're
	# setup to handle any signals, then spawn the child in
	# a separate process via POE::Wheel::Run.

	$heap->{worker}	= $worker;

	$kernel->sig(CHLD => 'done');
	$worker->child(new POE::Wheel::Run %args);
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
	my $worker	= $heap->{worker};
	my $task	= $worker->task;

	return if $pid != $worker->child->PID;

	Hopkins->log_debug("child process $pid exited with status $status");

	if ($status == 0) {
		Hopkins->log_info('worker successfully completed ' . $task->name);
	} else {
		Hopkins->log_error("worker exited abnormally ($status) while executing " . $task->name);
		$kernel->call(manager => queuefail => $task->queue->name);
	}

	$worker->postback->($pid, $status);
	$kernel->yield('shutdown');
}

sub shutdown
{
	my $kernel	= $_[KERNEL];
	my $heap	= $_[HEAP];
	my $worker	= $heap->{worker};

	# we have to remove the session alias from the kernel,
	# else POE will never destroy the session.

	$kernel->alias_remove($worker->alias);
}

sub log_stdout { Hopkins->log_worker_stdout($_[HEAP]->{worker}->task->name, $_[ARG0]) }
sub log_stderr { Hopkins->log_worker_stderr($_[HEAP]->{worker}->task->name, $_[ARG0]) }

=back

=head1 AUTHOR

Mike Eldridge <diz@cpan.org>

=head1 LICENSE

This program is free software; you may redistribute it
and/or modify it under the same terms as Perl itself.

=cut

1;
