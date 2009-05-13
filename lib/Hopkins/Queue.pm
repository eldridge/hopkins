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

use Class::Accessor::Fast;

use Hopkins::Worker;

use base 'Class::Accessor::Fast';

__PACKAGE__->mk_accessors(qw(name alias onerror onfatal concurrency));

=head1 STATES

=over 4

=item new

=cut

sub new
{
	my $self = shift->SUPER::new(@_);

use YAML;
print Dump($self);

	Hopkins->log_debug('spawning queue ' . $self->name);

	$self->alias('queue.' . $self->name);

	POE::Component::JobQueue->spawn
	(
		Alias		=> $self->alias,
		WorkerLimit	=> $self->concurrency,
		Worker		=> sub { $self->spawn_worker(@_) },
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

	return $self;
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

=item is_running

=cut

sub is_running
{
	my $self = shift;
	my $name = shift;

	return Hopkins->is_session_active("queue.$name");
}

=item spawn_worker

=cut

sub spawn_worker
{
	my $self = shift;

	my $args =
	{
		postback	=> shift,
		task		=> shift,
		queue		=> $self
	};

	new Hopkins::Worker $args;
}

=back

=head1 AUTHOR

Mike Eldridge <diz@cpan.org>

=head1 LICENSE

This program is free software; you may redistribute it
and/or modify it under the same terms as Perl itself.

=cut

1;
