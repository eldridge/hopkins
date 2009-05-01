package Hopkins::Plugin::HMI::Catalyst::View::TT;

use strict;

=head1 NAME


=head1 DESCRIPTION


=cut

use base 'Catalyst::View::TT';

__PACKAGE__->config({
	WRAPPER				=> 'wrapper.tt',
	TEMPLATE_EXTENSION	=> '.tt',
	TIMER				=> 0,
	static_root			=> '/static',
	static_build		=> 0
});


=back

=head1 AUTHOR

Mike Eldridge <diz@cpan.org>

=head1 LICENSE

This program is free software; you may redistribute it
and/or modify it under the same terms as Perl itself.

=cut

1;
