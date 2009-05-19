package Hopkins::Worker;

use strict;

=head1 NAME

Hopkins::Worker - hopkins worker session

=head1 DESCRIPTION

Hopkins::Worker encapsulates a POE session created by
Hopkins::Queue->spawn_worker via POE::Component::JobQueue.

=cut

use POE;
use POE::Filter::Reference;
use YAML;

use Class::Accessor::Fast;

use base 'Class::Accessor::Fast';

__PACKAGE__->mk_accessors(qw(alias postback work params child status));

=head1 STATES

=over 4

=item spawn

=cut

sub new
{
	my $self = shift->SUPER::new(@_);

	my $method = $self->work->task->class ? 'perl' : 'exec';
	my $source = $self->work->task->class || $self->work->task->cmd;

	Hopkins->log_debug("spawning worker: type=$method; source=$source");

	#my $now		= DateTime->now;
	#my $schema	= Hopkins::Store->schema;
	#my $rsTask	= $schema->resultset('Task');

	#$rsTask->find($id)->update({ date_started => $now });

	POE::Session->create
	(
		object_states =>
		[
			$self =>
			{
				_start		=> 'start',
				_stop		=> 'stop',

				stdout		=> 'stdout',
				stderr		=> 'stderr',
				done		=> 'done',
				shutdown	=> 'shutdown'
			}
		]
	);
}

=item closure

=cut

sub inline
{
	my $self	= $_[OBJECT];
	my $class	= shift;
	my $params	= shift || {};

	return sub
	{
		my $status = {};
		my $filter = new POE::Filter::Reference 'YAML';

		# redirect STDOUT to STDERR so that we can use
		# the original STDOUT pipe to report status
		# information back to hopkins via YAML

		open STATUS, '>&STDOUT';
		open STDOUT, '>&STDERR';

		eval { require $class; $class->new({ options => $params })->run };

		if (my $err = $@) {
			$status->{error} = $err;
			Hopkins->log_worker_stderr($self->work->task->name, $err);
		}

		# make sure to close the handle so that hopkins will
		# receive the information before the child exits.

		print STATUS $filter->put([ $status ])->[0];
		close STATUS;
	}
}

=item start

=cut

sub start
{
	my $self		= $_[OBJECT];
	my $kernel		= $_[KERNEL];
	my $heap		= $_[HEAP];

	$kernel->post(store => 'notify', 'task_update', $self->work->id, status => 'running');
	#$kernel->post('manager' => 'taskstart', $worker->id);

	# set the name of this session's alias based on the queue and session ID
	my $session = $kernel->get_active_session;

	$self->alias(join '.', $self->work->queue->alias, 'worker', $session->ID);
	$kernel->alias_set($self->alias);

	Hopkins->log_debug('worker session created');

	# determine the Program argument based upon what method
	# we're using.  POE::Wheel::Run will execute both native
	# perl code as well as external binaries depending upon
	# whether the argument is a coderef or a simple scalar.

	my $program = $self->work->task->class
		? $self->inline($self->work->task->class, $self->params)
		: $self->work->task->cmd;

	# construct the arguments neccessary for POE::Wheel::Run

	my %args =
	(
		Program			=> $program,
		StdoutEvent		=> 'stdout',
		StderrEvent		=> 'stderr',
		StdoutFilter	=> new POE::Filter::Reference 'YAML'
	);

	# after making sure to setup appropriate signal handlers
	# beforehand, spawn the actual worker in a child process
	# via POE::Wheel::Run.  this protects us from code that
	# may potentially block POE for a very long time.  it
	# also isolates hopkins from code that would otherwise
	# be able to alter the running environment.

	$kernel->sig(CHLD => 'done');
	$self->child(new POE::Wheel::Run %args);
}

sub stop
{
	my $self	= $_[OBJECT];
	my $kernel	= $_[KERNEL];
	my $heap	= $_[HEAP];

	Hopkins->log_debug('session destroyed');
}

sub done
{
	my $self	= $_[OBJECT];
	my $kernel	= $_[KERNEL];
	my $signal	= $_[ARG0];
	my $pid		= $_[ARG1];
	my $status	= $_[ARG2];

	return if $pid != $self->child->PID;

	Hopkins->log_debug("child process $pid exited with status $status");

	if ($self->status->{error}) {
		Hopkins->log_error('worker failure executing ' . $self->work->task->name);
		$kernel->call(manager => queue_failure => $self->work->queue);
	} else {
		Hopkins->log_info('worker successfully executed ' . $self->work->task->name);
	}

	$self->postback->($pid, $status);
	$kernel->yield('shutdown');
}

sub shutdown
{
	my $self	= $_[OBJECT];
	my $kernel	= $_[KERNEL];

	# we have to remove the session alias from the kernel,
	# else POE will never destroy the session.

	$kernel->alias_remove($self->alias);
}

sub stdout
{
	my $self = $_[OBJECT];

	$self->status($_[ARG0]);
}

sub stderr
{
	my $self = $_[OBJECT];

	Hopkins->log_worker_stderr($self->work->task->name, $_[ARG0]);
}

=back

=head1 AUTHOR

Mike Eldridge <diz@cpan.org>

=head1 LICENSE

This program is free software; you may redistribute it
and/or modify it under the same terms as Perl itself.

=cut

1;
