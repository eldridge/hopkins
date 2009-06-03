package TestEnvironment;

use strict;
use warnings;

=head1 NAME

TestEnvironment

=head1 DESCRIPTION

TestEnvironment for hopkins

=cut

use POE;
use Class::Accessor::Fast;
use Directory::Scratch;
use FindBin;
use Template;
use Test::MockObject;
use UNIVERSAL::can;

use base 'Class::Accessor::Fast';

__PACKAGE__->mk_accessors(qw(conf source scratch template fake_task fake_work));

no warnings 'redefine';

sub UNIVERSAL::can::_report_warning { }

use warnings 'redefine';

=head1 METHODS

=over 4

=item new

=cut

sub new
{
	my $self = shift->SUPER::new(@_);

	$self->scratch(new Directory::Scratch TEMPLATE => 'hopkins-test-XXXXX');

	$self->fake_task(new Test::MockObject);
	$self->fake_task->set_always('class', 'Hopkins::Test::Count');
	$self->fake_task->set_always('name', 'counter');

	$self->fake_work(new Test::MockObject);
	$self->fake_work->set_always('task', $self->fake_task);
	$self->fake_work->set_always('date_started', undef);
	$self->fake_work->set_always('serialize',
		{
			id				=> 'DEADBEEF',
			task			=> 'Count',
			options			=> { fruit => 'apple' },
			succeeded		=> 0,
			output			=> undef,
			date_enqueued	=> '2009-06-01T20:24:42',
			date_started	=> undef,
			date_completed	=> undef
		}
	);

	no warnings 'redefine';

	*Hopkins::log_debug	= \&log;
	*Hopkins::log_info	= \&log;
	*Hopkins::log_warn	= \&log;
	*Hopkins::log_error	= \&log;

	*POE::Kernel::DESTROY = sub {};

	use warnings 'redefine';

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

sub log
{
	my $self = shift;
	my $text = shift;

	print STDERR "$text" if $ENV{HOPKINS_DEBUG};
}

=head1 SEE ALSO

L<TestManager>

=head1 AUTHOR

Mike Eldridge <diz@cpan.org>

=head1 LICENSE

=cut

1;

