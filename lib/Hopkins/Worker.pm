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
	my $self = shift;

	POE::Session->create
	(
		inline_states => {
			_start		=> \&start,
			_stop		=> \&stop,

			stdout		=> \&output,
			stderr		=> \&output,
			error		=> \&error,
		},

		args => [ @_ ]
	);
}

=item start

=cut

sub start
{
	my $kernel		= $_[KERNEL];
	my $heap		= $_[HEAP];
	my $postback	= $_[ARG0];
	my $method		= $_[ARG1];
	my $source		= $_[ARG2];
	my $params		= $_[ARG3];
	my $queue		= $_[ARG4];

	Hopkins->log_debug("worker session created");

	# set the name of this session's alias based on the queue and session ID
	my $session = $poe_kernel->get_active_session;
	$kernel->alias_set(join '.', 'worker', $queue, $session->ID);

	# determine the Program argument based upon what "method" we're using
	my $program = $method eq 'perl'
		? sub { require $source; my $obj = new $source '', $params; $obj->run }
		: $source;
	
	# construct the arguments neccessary for POE::Wheel::Run
	my %args =
	(
		Program		=> $program,
		StderrEvent	=> 'stderr',
		ErrorEvent	=> 'error',
		StdoutEvent	=> 'stdout'
	);

	# set a few session-specific things in the heap and then spawn the child
	$heap->{postback}	= $postback;
	$heap->{queue}		= $queue;
	$heap->{child}		= new POE::Wheel::Run %args;
}

sub stop
{
	my $heap = $_[HEAP];

	Hopkins->log_debug('child exited');

	$heap->{postback}->();
}

sub output { Hopkins->log_debug($_[ARG0]) }

sub error
{
	my $heap	= $_[HEAP];
	my $op		= $_[ARG0];
	my $err		= $_[ARG2];

	Hopkins->log_warn('OH GOD!');

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
