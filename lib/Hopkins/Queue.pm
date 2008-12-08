package Hopkins::Queue;

use strict;

=head1 NAME

Hopkins::Queue - hopkins queue states and methods

=head1 DESCRIPTION

Hopkins::Queue contains all of the POE event handlers and
supporting methods for the initialization and management of
each configured hopkins queue.

=cut

use POE;

use Hopkins::Worker;

=head1 STATES

=over 4

=item init

=cut

sub init
{
	my $heap = $_[HEAP];

	# create a passive queue for each configured queue.  we
	# use POE::Component::JobQueue and leave the scheduling
	# up to the manager session.

	foreach my $name (Hopkins::Config->get_queue_names) {
		Hopkins::Queue->spawn($name);
	}
}

sub spawn
{
	my $self	= shift;
	my $name	= shift;

	my $queue = Hopkins::Config->get_queue_info($name);

	return 0 if not defined $queue;

	my $alias = "queue.$name";

	Hopkins->log_debug("spawning queue $name");

	POE::Component::JobQueue->spawn
	(
		Alias		=> $alias,
		WorkerLimit	=> $queue->{concurrency},
		Worker		=> sub { Hopkins::Worker::spawn @_, $alias },
		Passive		=> { Prioritizer => \&Hopkins::Queue::prioritize },
	);

	# this passive queue will act as an on-demand task
	# execution queue, waiting for enqueue events to be
	# posted to the kernel.

	#POE::Component::JobQueue->spawn
	#(
	#	Alias		=> 'worker',
	#	WorkerLimit	=> 16,
	#	Worker		=> \&queue_worker,
	#	Passive		=> { },
	#);

	# this active queue will act as a scheduler, checking
	# the time and polling the list of loaded task configs
	# for new tasks to spawn

	#POE::Component::JobQueue->spawn
	#(
	#	Alias		=> 'scheduler',
	#	WorkerLimit	=> 16,
	#	Worker		=> \&queue_scheduler,
	#	Active		=>
	#	{
	#		PollInterval	=> $global->{poll},
	#		AckAlias		=> 'scheduler',
	#		AckState		=> \&job_completed
	#	}
	#);

	return 1;
}

=item prioritize

=cut

sub prioritize
{
	my $a = shift;
	my $b = shift;

	my $aopts = $a->[5] || {};
	my $bopts = $b->[4] || {};

	my $apri = $aopts->{priority} || 5;
	my $bpri = $bopts->{priority} || 5;

	$apri = 1 if $apri < 1;
	$apri = 9 if $apri > 9;
	$bpri = 1 if $bpri < 1;
	$bpri = 9 if $bpri > 9;

	return $apri <=> $bpri;
}

=item fail

=cut

sub fail
{
	my $kernel	= $_[KERNEL];
	my $heap	= $_[HEAP];
	my $alias	= $_[ARG0];

	my ($name)	= ($alias =~ /^queue\.(.+)?/);
	my $queue	= Hopkins::Config->get_queue_info($name);
	my $msg		= "failure in $name queue";

	if ($queue->{onerror} eq 'suspend') {
		$msg .= '; stopping queue';
		$kernel->post("queue.$name" => 'stop');
	}

	Hopkins->log_error($msg);
}

=item is_running

=cut

sub is_running
{
	my $self = shift;
	my $name = shift;

	my $api			= new POE::API::Peek;
	my @sessions	= map { POE::Kernel->alias($_) } $api->session_list;

	return grep { "queue.$name" eq $_ } @sessions;
}

=back

=head1 AUTHOR

Mike Eldridge <diz@cpan.org>

=head1 LICENSE

This program is free software; you may redistribute it
and/or modify it under the same terms as Perl itself.

=cut

1;
