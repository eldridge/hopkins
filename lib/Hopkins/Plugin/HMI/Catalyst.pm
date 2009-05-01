package Hopkins::Plugin::HMI::Catalyst;

use strict;
use warnings;

=head1 NAME


=head1 DESCRIPTION

=cut

use Catalyst;

use Sys::Hostname::FQDN 'fqdn';

#qw/-Debug Browser StackTrace ConfigLoader ConfigLoader::Environment
#				FormValidator Session Session::Store::DBIC/;

use base 'Catalyst::Controller::REST';

#use Catalyst::Log::Log4perl;
#use Greenspan::Config;
#use Greenspan::Exception;

#__PACKAGE__->config

#__PACKAGE__->config('Plugin::ConfigLoader' => { file => '/etc/greenspan/apps/subnet.yml' });
#__PACKAGE__->log(new Catalyst::Log::Log4perl '/etc/greenspan/log4perl.conf');

__PACKAGE__->setup(qw/Static::Simple/);

=head1 METHODS

=cut

=over 4

=item new

=cut

sub auto : Private
{
	my $self	= shift;
	my $c		= shift;

	$c->stash->{host} = fqdn;
}

sub default : Private
{
	my $self	= shift;
	my $c		= shift;

}

sub login : Local : ActionClass('REST') { }

sub login_GET
{
	my $self	= shift;
	my $c		= shift;

	print "FUCK ME MAN\n";

	$c->stash->{template} = 'login.tt';
}

sub end : ActionClass('RenderView') { }

=back

=head1 AUTHOR

Mike Eldridge <diz@cpan.org>

=head1 LICENSE

This program is free software; you may redistribute it
and/or modify it under the same terms as Perl itself.

=cut

1;
