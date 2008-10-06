package Hopkins::Store;

use strict;

=head1 NAME

Hopkins::Store - interface to backend storage

=head1 DESCRIPTION

Hopkins::Store encapsulates all of the busywork associated
with the DBIx::Class schema creation.

=cut

use POE;

use Hopkins::Schema;

my $schema;

=head1 STATES

=over 4

=item init

=cut

sub init
{
	my $kernel	= $_[KERNEL];
	my $heap	= $_[HEAP];

	my $config	= Hopkins::Config->fetch('database');
	my $dsn		= $config->{dsn};
	my $user	= $config->{user};
	my $pass	= $config->{pass};
	my $opts	= $config->{options};

	if (not defined $dsn) {
		Hopkins->log_error('database/dsn not specified, trying again in 5 minutes');
		$kernel->alarm(storeinit => time + 300);
	}

	Hopkins->log_debug("attempting to connect to $dsn as $user");

	# connect to the schema.  DBIx::Class does many things
	# lazily.  i don't mean sans effort; DBIx::Class waits
	# until the last possible moment to perform many tasks
	# such as queries.  database connection is no exception,
	# so make sure that we've made a connection attempt!

	my $schema2;

	eval {
		$schema2 = Hopkins::Schema->connect($dsn, $user, $pass, $opts);
		$schema2->storage->ensure_connected;
	};

	if (my $err = $@) {
		Hopkins->log_error("failed to connect to schema: $err");
		Hopkins->log_error("trying again in 60 seconds");
		$kernel->alarm(storeinit => time + 60);
	} else {
		Hopkins->log_debug('successfully connected to schema');
		$schema = $schema2;
	}
}

sub schema { return $schema }

=back

=head1 AUTHOR

Mike Eldridge <diz@cpan.org>

=head1 LICENSE

=cut

1;
