package Hopkins::Worker::Native;

use strict;

=head1 NAME

Hopkins::Worker::Native - perl worker

=head1 DESCRIPTION

Hopkins::Worker::Native represents the special case of a
worker that will be executing a perl task.  it wraps the
execution in order to return results to hopkins.  it also
does pre-execution cleansing of the log4perl environment.

=cut

use base 'Class::Accessor::Fast';

__PACKAGE__->mk_accessors(qw(work));

=head1 METHODS

=over 4

=item new

=cut

sub new
{
	my $self = shift->SUPER::new(@_);

	return sub { $self->execute };
}

=item execute

=cut

sub execute
{
	my $self = shift;

	my $class	= $self->work->task->class;
	my $file	= "$class.pm";

	$file =~ s{::}{/}g;

	# immediately undefine the log4perl config watcher.
	# the logic in Log::Log4perl->init_and_watch will
	# use the existing configuration if it is called at
	# a later time.  this will cause problems if any of
	# the perl workers use init_and_watch.
	#
	# this should probably be considered a bug.  it's
	# not init()ing and watching.  just more watching.

	$Log::Log4perl::Config::WATCHER = undef;

	# create a status hashref and a POE filter by which
	# status information will be reported back to the
	# controlling POE::Component::JobQueue worker.

	my $status = {};
	my $filter = new POE::Filter::Reference 'YAML';

	# redirect STDOUT to STDERR so that we can use
	# the original STDOUT pipe to report status
	# information back to hopkins via YAML

	open STATUS, '>&STDOUT';
	open STDOUT, '>&STDERR';

	eval { require $file; $class->new({ options => $self->work->options })->run };

	if (my $err = $@) {
		print STDERR $err;
		$status->{error} = $err;
		Hopkins->log_worker_stderr($self->work->task->name, $err);
	}

	# make sure to close the handle so that hopkins will
	# receive the information before the child exits.

	print STATUS $filter->put([ $status ])->[0];
	close STATUS;
}

=back

=head1 AUTHOR

Mike Eldridge <diz@cpan.org>

=head1 LICENSE

This program is free software; you may redistribute it
and/or modify it under the same terms as Perl itself.

=cut

1;
