package Hopkins::Plugin::HMI;

use strict;
use warnings;

=head1 NAME

Hopkins::Plugin::HMI - hopkins HMI session (using HTTP)

=head1 DESCRIPTION

Hopkins::Plugin::HMI encapsulates the HMI (human machine
interface) POE session created by the manager session.  this
session uses the Server::HTTP component to provide a web
interface to the job server using Catalyst.

=cut

BEGIN { $ENV{CATALYST_ENGINE} = 'Embeddable' }

use POE;
use POE::Component::Server::HTTP;

use Class::Accessor::Fast;

use Hopkins::Plugin::HMI::Catalyst;

use base 'Class::Accessor::Fast';

__PACKAGE__->mk_accessors(qw(app));

=head1 STATES

=over 4

=item new

=cut

sub new
{
	my $proto	= shift;
	my $opts	= shift;

	$opts->{port} ||= 8088;

	my $self = $proto->SUPER::new($opts);

	$self->app(new Hopkins::Plugin::HMI::Catalyst);

	my %args =
	(
		Port			=> $opts->{port},
		ContentHandler	=> { '/' => sub { $self->handler(@_) } },
		Headers			=> { Server => "hopkins/$Hopkins::VERSION" }
	);

	new POE::Component::Server::HTTP %args;
}

sub handler
{
	my $self	= shift;
	my $req		= shift;
	my $res		= shift;
	my $app		= $self->app;

	my $obj;

	$app->handle_request($req, \$obj);

	if (ref($obj->content) eq 'IO::File') {
		my $content;

		while (not eof $obj->content) {
			read $obj->content, my ($buf), 64 * 1024;
			$content .= $buf;
		}

		$obj->content($content);
	}

	$res->code($obj->code);
	$res->content($obj->content);
	$res->message('');

	return RC_OK;
}

=back

=head1 AUTHOR

Mike Eldridge <diz@cpan.org>

=head1 LICENSE

This program is free software; you may redistribute it
and/or modify it under the same terms as Perl itself.

=cut

1;
