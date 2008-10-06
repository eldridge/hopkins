package Hopkins::RPC;

use strict;

=head1 NAME

Hopkins::RPC - hopkins RPC session

=head1 DESCRIPTION

Hopkins::Worker encapsulates the RPC POE session created by
the manager session.

=cut

use POE;

=head1 STATES

=over 4

=item init

=cut

sub init
{
	# create RPC session
	POE::Session->create
	(
		inline_states => {
			_start  => \&start,
			_stop   => \&stop,
			enqueue => \&enqueue
		}
	);
}

=item start

=cut

sub start
{
	my $kernel = $_[KERNEL];

	$kernel->alias_set('rpc');
	$kernel->post(soap => ADDMETHOD => rpc => 'enqueue');
}

=item stop

=cut

sub stop
{
	my $kernel = $_[KERNEL];

	$kernel->post(soap => DELMETHOD => rpc => 'enqueue');
}

=item enqueue

=cut

sub enqueue
{
	my $kernel	= $_[KERNEL];
	my $res		= $_[ARG0];
	my $params	= $res->soapbody;

	my ($class, $options) = map { $params->{$_} } sort keys %$params;

	#print "enqueue message received for '$class'\n";
	#print "options are: \n";
	#print "\t$_: $options->{$_}\n" foreach keys %$options;

	$kernel->post(worker => enqueue => postback => $class, $options);

	$res->content(1);
	$kernel->post(soap => DONE => $res);
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
