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

__PACKAGE__->mk_accessors(qw(conf source scratch template));

=head1 METHODS

=over 4

=item new

=cut

sub new
{
	my $self = shift->SUPER::new(@_);

	$self->scratch(new Directory::Scratch TEMPLATE => 'hopkins-test-XXXXX');

	$self->process if $self->source;

	#$self->conf('hopkins.xml.tt') if not $self->conf;
	return $self;
}

sub source
{
	my $self = shift;

	my $rv;

	if (scalar @_) {
		$rv = $self->set('source', @_);
		$self->process;
	} else {
		$rv = $self->get('source');
	}
}

sub process
{
	my $self = shift;

	my ($fh, $path)	= $self->scratch->openfile('hopkins.xml');

	my $template = new Template { INCLUDE_PATH => $FindBin::Bin };

	$template->process($self->source, $self, $fh);

	$fh->sync;
	$fh->close;

	$self->conf($path->stringify);
}

=head1 SEE ALSO

L<TestManager>

=head1 AUTHOR

Mike Eldridge <diz@cpan.org>

=head1 LICENSE

=cut

1;

