package Hopkins::Store::Backend;

use strict;
use warnings;

=head1 NAME

Hopkins::Store::Backend - synchronous database services

=head1 DESCRIPTION

Hopkins::Store::Backend encapsulates database functionality
in a simple event loop.  no POE services are utilized in the
event processing -- the loop is spawned off in a separate
process via POE::Child::Run in order to provide asynchronous
operation.  hence, database queries may block in the backend
without affecting the rest of hopkins.

Store::Backend communicates with the store session in the
parent process via message passing on stdin/stdout.  these
messages are YAML encoded via POE::Filter::Reference.

=cut

use Class::Accessor::Fast;
use POE::Filter::Reference;

use Hopkins::Store::Schema;

use base 'Class::Accessor::Fast';

__PACKAGE__->mk_accessors(qw(config filter schema));

=head1 METHODS

=over 4

=item init

=cut

sub new
{
	my $self = shift->SUPER::new(@_);

	open STATUS, '>&STDOUT';
	open STDOUT, '>&STDERR';

	$self->filter(new POE::Filter::Reference 'YAML');

	$self->connect and $self->loop;
}

sub loop
{
	my $self = shift;

	while (my $line = <STDIN>) {
		my $aref = $self->filter->get([ $line ]);

		foreach my $href (@$aref) {
			$self->process($href) or return;
		}
	}
}

sub process
{
	my $self = shift;
	my $href = shift;

	$self->connected or return 0;

	use YAML;

	print Dump($href);

	return 1;
}

sub connected
{
	my $self = shift;

	my $ok = $self->schema->storage->ensure_connected ? 1 : 0;

	Hopkins->log_error('lost connection to database') if not $ok;

	return $ok;
}

sub connect
{
	my $self = shift;

	$self->schema(undef);

	my $config	= $self->config->fetch('database');
	my $dsn		= $config->{dsn};
	my $user	= $config->{user};
	my $pass	= $config->{pass};
	my $opts	= $config->{options};

	if (not defined $dsn) {
		Hopkins->log_error('database/dsn not specified');
		return undef;
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

	# if the connection was successful, supply the schema
	# object to the Store::Backend object.

	if (my $err = $@) {
		Hopkins->log_error("failed to connect to schema: $err");
	} else {
		Hopkins->log_debug('successfully connected to schema');
		$self->schema($schema);
	}

	return $self->schema;
}

=back

=head1 AUTHOR

Mike Eldridge <diz@cpan.org>

=head1 LICENSE

=cut

1;

