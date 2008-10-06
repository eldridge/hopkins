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
		my $queue = $heap->{config}->{queue}->{$name};

		Hopkins->log_debug("creating queue with alias $name");

		POE::Component::JobQueue->spawn
		(
			Alias		=> $name,
			WorkerLimit	=> $queue->{concurrency},
			Worker		=> \&spawn_worker,
			Passive		=> { },
		);
	}

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
}

=item spawn_worker

=cut

sub spawn_worker
{
	my $postback	= shift;
	my $method		= shift;
	my $source		= shift;
	my $params		= shift;

	Hopkins->log_debug("received worker task enqueue notice");
	Hopkins->log_debug("task is of type $method");
	Hopkins->log_debug("source of work is $source");

	my $queue = ($poe_kernel->alias_list())[0];

	Hopkins::Worker->spawn($postback, $method, $source, $params, $queue);
}

=back

=head1 AUTHOR

Mike Eldridge <diz@cpan.org>

=head1 LICENSE

Copyright (c) 2008 Mike Eldridge.  All rights reserved.

This program is free software; you may redistribute it
and/or modify it under the same terms as Perl itself.

=cut

1;
