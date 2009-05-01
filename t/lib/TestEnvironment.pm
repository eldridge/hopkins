package TestEnvironment;

use strict;
use warnings;

=head1 NAME

TestEnvironment

=head1 DESCRIPTION

TestEnvironment for hopkins

=cut

use Class::Accessor::Fast;
use Directory::Scratch;
use FindBin;
use Template;

use base 'Class::Accessor::Fast';

__PACKAGE__->mk_accessors(qw(conf config scratch));

=head1 METHODS

=over 4

=item new

=cut

sub new
{
	my $self = shift->SUPER::new(@_);

	my $scratch		= new Directory::Scratch TEMPLATE => 'hopkins-test-XXXXX';
	my $template	= new Template { INCLUDE_PATH => $FindBin::Bin };
	my ($fh, $path)	= $scratch->openfile('hopkins.xml');

	$self->conf('hopkins.xml.tt') if not $self->conf;
	$template->process($self->conf, { scratch => $scratch }, $fh);

	$self->config($path);
	$self->scratch($scratch);

	$fh->sync;
	$fh->close;

	return $self;
}

=head1 SEE ALSO

L<TestManager>

=head1 AUTHOR

Mike Eldridge <diz@cpan.org>

=head1 LICENSE

=cut

1;

