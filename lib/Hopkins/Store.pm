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
use List::Object;
use Class::Accessor::Fast;

use Hopkins::Schema;

use base 'Class::Accessor::Fast';

__PACKAGE__->mk_accessors(qw(schema events));

use constant HOPKINS_STORE_CONNECTION_CHECK_INTERVAL	=> 60;
use constant HOPKINS_STORE_EVENT_PROC_INTERVAL			=> 60;
use constant HOPKINS_STORE_EVENT_PROC_MAX_TIME			=> 20;

=head1 METHODS

=over 4

=item init

=cut

sub new
{
	my $self = shift->SUPER::new(@_);

	$self->events(new List::Object);

	POE::Session->create
	(
		object_states =>
		[
			$self =>
			{
				_start	=> 'start',
				_stop	=> 'stop'
			}
		]
	);
}

=back

=head1 STATES

=over 4

=item start

=cut

sub start
{
	my $kernel = $_[KERNEL];

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
	my $self	= $_[OBJECT];
	my $kernel	= $_[KERNEL];

	#eval { $schema && $schema->ensure_connected }
	if ($self->schema) {
		if (!$self->schema->ensure_connected) {
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
	my $self	= $_[OBJECT];
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

	my $schema;

	eval {
		# DBIx::Class is lazy.  it will wait until the last
		# possible moment to connect to the database.  this
		# prevents unnecessary database connections, but we
		# but we want to immediately and gracefully handle
		# any errors, so we force the connection now with
		# the storage object's ensure_connected method.

		$schema = Hopkins::Schema->connect($dsn, $user, $pass, $opts);
		$schema->storage->ensure_connected;
	};

	# if the connection was successful, replace our existing
	# schema object with the new schema object.

	if (my $err = $@) {
		Hopkins->log_error("failed to connect to schema: $err");
		Hopkins->log_debug('trying again in ' . HOPKINS_STORE_CONNECTION_CHECK_INTERVAL . ' seconds');
	} else {
		Hopkins->log_debug('successfully connected to schema');
		$self->schema($schema);
	}
}

sub evtproc
{
	my $self	= $_[OBJECT];
	my $kernel	= $_[KERNEL];

	my $start = time;

	while ($self->events->count) {
		last if $start + HOPKINS_STORE_EVENT_PROC_MAX_TIME > time;

		my $aref	= $self->events->shift;
		my $res		= $kernel->call(store => $aref->[0] => $aref->[1..$#{$aref}]);
	}

	$kernel->alarm(evtproc => time + HOPKINS_STORE_EVENT_PROC_INTERVAL);
}

sub evtflush
{
	my $self	= $_[OBJECT];
	my $kernel	= $_[KERNEL];
	my $heap	= $_[HEAP];

	while ($self->events->count) {
		my $aref	= $self->events->shift;
		my $res		= $kernel->call(store => $aref->[0] => $aref->[1..$#{$aref}]);
	}
}

sub notify
{
	my $self	= $_[OBJECT];
	my $kernel	= $_[KERNEL];

	$self->events->add([ @_ ]);
}

sub task_update
{
	my $self	= $_[OBJECT];
	my $kernel	= $_[KERNEL];
	my $id		= $_[ARG0];
	my $args	= { @_[ARG0..$#_] };

	my $rsTask	= $self->schema->resultset('Task');
	my $task	= $rsTask->find($id);

	my $coderef = sub
	{
		# XXX: no idea what i'm trying to do here :/

		#foreach my $key (keys %$args) {
		#	$kernel->call(store => "$_[STATE]_$key", $args->{$key});
		#}
	};

	return $self->schema->txn_do($coderef);
}

=back

=head1 AUTHOR

Mike Eldridge <diz@cpan.org>

=head1 LICENSE

=cut

1;

