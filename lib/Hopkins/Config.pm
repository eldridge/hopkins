package Hopkins::Config;

use strict;

=head1 NAME

Hopkins::Config - interface to XML configuration

=head1 DESCRIPTION

Hopkins::Config encapsulates all of the busywork associated
in the reading and post-processing of the XML configuration
in addition to providing a simple interface to accessing
values when required.

=cut

use POE;

use DateTime;
use DateTime::Event::MultiCron;
use DateTime::Set;
use XML::Simple;

my $config;
my $err;

=head1 STATES

=over 4

=item load

=cut

sub load
{
	my $kernel	= $_[KERNEL];
	my $heap	= $_[HEAP];

	Hopkins->log_debug('loading XML configuration file');

	my $ref	= Hopkins::Config->parse($heap->{opts}->{conf});
	my $ok	= 1;

	if (not defined $ref) {
		Hopkins->log_error("error loading configuration file: $err");
		return $config || exit;
	}

	# process any cron-like schedules
	foreach my $name (keys %{ $ref->{task} }) {
		my $task = $ref->{task}->{$name};
		my $node = $task->{schedule};

		next if not defined $node;

		if (!($task->{class} || $task->{cmd})) {
			Hopkins->log_error("task $name lacks a class or command line");
			$ok = 0;
		}

		my @a	= ref($node) eq 'ARRAY' ? @$node : ($node);
		my $set	= DateTime::Event::MultiCron->from_multicron(@a);

		$ref->{task}->{$name}->{schedules} = $set;
	}

	# if we already have a schema object, check to see if
	# the new configuration sports a modified database
	# configuration.  if it does, we want to reinitialize
	# the backend

	if ($heap->{schema}) {
		my @a = @{ $heap->{schema}->storage->connect_info };
		my @b = map { $ref->{database}->{$_} } qw(dsn user pass options);

		# replace the options hashref (very last element in
		# the array) with a flattened representation

		splice @a, -1, 1, keys %{ $a[-1] }, values %{ $a[-1] };
		splice @b, -1, 1, keys %{ $b[-1] }, values %{ $b[-1] };

		# temporarily change the list separator character
		# (default 0x20, a space) to the subscript separator
		# character (default 0x1C) for a precise comparison
		# of the two configurations

		local $" = $;;

		if ("@a" ne "@b") {
			Hopkins->log_debug('database information changed');
			$kernel->post(manager => 'storeinit') if $ok;
		}
	}

	if ($ok) {
		$config = $ref;
	} else {
		Hopkins->log_error('errors in configuration, discarding new version');
	}

	return $config;
}

sub parse
{
	my $self = shift;
	my $file = shift;

	my %xmlsopts =
	(
		KeyAttr			=> { option => 'name', queue => 'name', task => 'name' },
		ValueAttr		=> [ 'value' ],
		GroupTags		=> { options => 'option' },
		SuppressEmpty	=> ''
	);

	my $xs	= new XML::Simple %xmlsopts;
	my $ref	= eval { $xs->XMLin($file) };

	return undef if $err = $@;

	# flatten options attributes
	if (my $href = $ref->{database}->{options}) {
		$href->{$_} = $href->{$_}->{value} foreach keys %$href;
	}

	return $ref;
}

sub scan
{
	my $kernel	= $_[KERNEL];
	my $heap	= $_[HEAP];

	#Hopkins->log_debug('scanning configuration file');

	$heap->{confmon}->scan;
	$kernel->alarm(confscan => time + $heap->{opts}->{scan});
}

sub get_queue_names
{
	return keys %{ $config->{queue} };
}

sub get_task_names
{
	return keys %{ $config->{task} };
}

sub get_task_info
{
	my $self = shift;
	my $task = shift;

	return $config->{task}->{$task};
}

sub get_queue_info
{
	my $self = shift;
	my $name = shift;

	return $config->{queue}->{$name};
}

sub fetch
{
	my $self = shift;
	my $path = shift;

	$path =~ s/^\/+//;

	my $ref = $config;

	foreach my $spec (split '/', $path) {
		for (ref($ref)) {
			/ARRAY/	and do { $ref = $ref->[$spec] }, next;
			/HASH/	and do { $ref = $ref->{$spec} }, next;

			$ref = undef;
		}
	}

	return $ref;
}

=back

=head1 AUTHOR

Mike Eldridge <diz@cpan.org>

=head1 LICENSE

=cut

1;
