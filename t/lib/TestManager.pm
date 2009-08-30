package TestManager;

use strict;

=head1 NAME

TestManager

=head1 DESCRIPTION

TestManager is an interface that, among other things, makes
using Test::More with fork()ing tests operate as expected.
in addition, TestManager provides a try() method that will
wrap code inside of an eval, calling diag() if the tried
code fails.

=cut

use Test::More;
use IO::Handle;

my $tests = 0;
my $pplan = 0;

=head1 METHODS

=over 4

=item initialize

starts the process manager by mingling with Test::More and
Test::Builder's innards.  the initialize method will setup
a pipe for Test::Builder output to be collated through and
subsequently fork, assuming collator duties in the parent
and returning control to the caller in the child.

=cut

sub initialize
{
	my $self = shift;
	my $opts = { @_ };

	$tests = $opts->{tests} if $opts->{tests};
	if ($opts->{postplan}) {
		Test::More->builder->no_header(1);
		Test::More->builder->plan(tests => 999999);
		Test::More->builder->no_header(0);
		$pplan = 1;
	}

	pipe TR, TW;

	TW->autoflush;

	if (fork) {
		close TW;

		my $i = 1;

		Test::More->builder->no_ending(0);

		while (my $line = <TR>) {
			chomp $line;

			if ($line =~ /^([0-9]+) ((?:not )?ok) [0-9]+ ([-#]) (.*)$/) {
				print "$2 $i $3 [$1] $4\n";

				Test::More->builder->current_test($i);

				Test::More->builder->{Test_Results}[$i-1] =
				{
					'ok'		=> ($2 eq 'ok' ? 1 : 0),
					'actual_ok'	=> ($2 eq 'ok' ? 1 : 0),
					($3 eq '-')
						? ('name' => $4, type => '')
						: ('name' => '', type => 'skip', reason => $4)
				};

				$i++;
			} elsif ($line =~ /^([0-9]+) # (.*)$/) {
				print "# [$1] $2\n";
			} elsif ($line =~ /^[0-9]+\.\.([0-9]+)$/) {
				Test::More->builder->expected_tests($1);
			} else {
				print "$line\n";
			}
		}

		exit 0;
	}

	# tell Test::Builder to send its output down our collator pipe
	Test::More->builder->output(\*TW);
	Test::More->builder->failure_output(\*TW);

	# move the original _print and _print_diag routines somewhere else
	*Test::Builder::__print			= \&Test::Builder::_print;
	*Test::Builder::__print_diag	= \&Test::Builder::_print_diag;

	no warnings 'redefine';

	# redefine _print and _print_diag to include the PID of the process
	*Test::Builder::_print		= sub { Test::Builder::__print(shift, map { /^[0-9]+\.\.[0-9]+$/ ? $_ : "$$ $_" } @_) };
	*Test::Builder::_print_diag	= sub { Test::Builder::__print_diag(shift, map { /^[0-9]+\.\.[0-9]+$/ ? $_ : "$$ $_" } @_) };

	use warnings 'redefine';
}

=item try

=cut

sub try
{
	my $self	= shift;
	my $coderef	= shift;

	eval { $coderef->(@_) };

	my $err = $@;
	diag($err) if $err;

	Test::More->builder->expected_tests($err ? 999999 : $tests) if $pplan;
}

=item expect_more_tests

=cut

sub expect_more_tests
{
	my $self = shift;

	$tests += shift;
}

=back

=head1 SEE ALSO

L<TestTemplate>
L<JobHarness>

=head1 AUTHOR

Mike Eldridge <diz@cpan.org>

=head1 LICENSE

(c) 2007 Magazines.com

For Internal Use Only

=cut

1;
