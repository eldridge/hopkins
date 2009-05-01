package Hopkins::Store;

use strict;
use warnings;

=head1 NAME

Hopkins::Store - interface to backend storage

=head1 DESCRIPTION

Hopkins::Store encapsulates all of the busywork associated
with the DBIx::Class schema creation.

=cut

use POE;

use Hopkins::Schema;

use constant HOPKINS_STORE_CONNECTION_CHECK_INTERVAL	=> 60;
use constant HOPKINS_STORE_EVENT_PROC_INTERVAL			=> 60;
use constant HOPKINS_STORE_EVENT_PROC_MAX_TIME			=> 20;

my $schema;

=head1 METHODS

=over 4

=item init

=cut

sub new
{
	POE::Session->create
	(
		inline_states =>
		{
			_start	=> \&start,
			_stop	=> \&stop
		}
	);
}

=back

=head1 STATES

=over 4

=item start

=cut

sub start
{
	my $kernel	= $_[KERNEL];
	my $heap	= $_[HEAP];

	$heap->{events} = [];

	$kernel->alias_set('store');
	$kernel->post(store => 'connchk');
}

=item stop

=cut

sub stop
{
}

=item connchk

=cut

sub connchk
{
	my $kernel	= $_[KERNEL];
	my $heap	= $_[HEAP];

	#eval { $schema && $schema->ensure_connected }
	if ($schema) {
		if (!$schema->ensure_connected) {
			Hopkins->log_error('lost connection to database');
			$kernel->alarm('evtproc');
			$kernel->post(store => 'connect');
		}
	} else {
		Hopkins->log_info('initiating database connection');
		$kernel->post(store => 'connect');
	}

	$kernel->alarm(connchk	=> time + HOPKINS_STORE_CONNECTION_CHECK_INTERVAL);
}

sub connect
{
	my $kernel	= $_[KERNEL];
	my $heap	= $_[HEAP];

	my $config	= Hopkins::Config->fetch('database');
	my $dsn		= $config->{dsn};
	my $user	= $config->{user};
	my $pass	= $config->{pass};
	my $opts	= $config->{options};

	if (not defined $dsn) {
		Hopkins->log_error('database/dsn not specified');
		Hopkins->log_debug('trying again in ' . HOPKINS_STORE_CONNECTION_CHECK_INTERVAL . ' seconds');
		return;
	}

	Hopkins->log_debug("attempting to connect to $dsn as $user");

	# attempt to connect to the schema.  gracefully handle
	# any exceptions that may occur.

	my $schema2;

	eval {
		# DBIx::Class is lazy.  it will wait until the last
		# possible moment to connect to the database.  this
		# prevents unnecessary database connections, but we
		# but we want to immediately and gracefully handle
		# any errors, so we force the connection now with
		# the storage object's ensure_connected method.

		$schema2 = Hopkins::Schema->connect($dsn, $user, $pass, $opts);
		$schema2->storage->ensure_connected;
	};

	# if the connection was successful, replace our existing
	# schema object with the new schema object.

	if (my $err = $@) {
		Hopkins->log_error("failed to connect to schema: $err");
		Hopkins->log_debug('trying again in ' . HOPKINS_STORE_CONNECTION_CHECK_INTERVAL . ' seconds');
	} else {
		Hopkins->log_debug('successfully connected to schema');
		$schema = $schema2;
	}
}

sub evtproc
{
	my $kernel	= $_[KERNEL];
	my $heap	= $_[HEAP];

	my $start = time;

	while (scalar @{ $heap->{events} }) {
		return if $start + HOPKINS_STORE_EVENT_PROC_MAX_TIME > time;

		my $aref	= $heap->{events}->[0];
		my $res		= $kernel->call($aref->[0], $aref->[1..$#{$aref}]);
	}

	$kernel->alarm(evtproc => time + HOPKINS_STORE_EVENT_PROC_INTERVAL);
}

sub evtflush
{
	my $kernel	= $_[KERNEL];
	my $heap	= $_[HEAP];

	while (scalar @{ $heap->{events} }) {
		my $aref	= $heap->{events}->[0];
		my $res		= $kernel->call($aref->[0], $aref->[1..$#{$aref}]);
	}
}

sub notify
{
	my $kernel	= $_[KERNEL];
	my $heap	= $_[HEAP];

	push @{ $heap->{events} }, [ @_ ];
}

sub task_update
{
	my $kernel	= $_[KERNEL];
	my $heap	= $_[HEAP];
	my $id		= $_[ARG0];
	my $args	= { @_[ARG0..$#_] };

	my $rsTask	= $schema->resultset('Task');
	my $task	= $rsTask->find($id);

	my $coderef = sub
	{
		foreach my $key (keys %$args) {
			$kernel->call("$_[STATE]_$key", $args->{$key});
		}
	};

	return $schema->txn_do($coderef);
}

=back

=head1 AUTHOR

Mike Eldridge <diz@cpan.org>

=head1 LICENSE

=cut

1;

