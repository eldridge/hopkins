package Hopkins::State::Schema::Row::Work;

use strict;

=head1 NAME

Hopkins::State::Schema::Row::Work

=head1 DESCRIPTION

=cut

use base 'DBIx::Class';

__PACKAGE__->load_components(qw/PK::Auto Core/);

__PACKAGE__->table('work');
__PACKAGE__->add_columns(
	id => {
		data_type			=> 'varchar',
		size				=> 36,
		is_nullable			=> 0,
		default_value		=> undef,
		is_auto_increment	=> 0,
		is_foreign_key		=> 0
	},
	id_queue => {
		data_type			=> 'bigint',
		size				=> 20,
		is_nullable			=> 0,
		default_value		=> undef,
		is_auto_increment	=> 0,
		is_foreign_key		=> 1
	},
	name => {
		data_type			=> 'varchar',
		size				=> 255,
		is_nullable			=> 0,
		default_value		=> undef,
		is_auto_increment	=> 0,
		is_foreign_key		=> 0
	},
	store_id => {
		data_type			=> 'bigint',
		size				=> 20,
		is_nullable			=> 1,
		default_value		=> undef,
		is_auto_increment	=> 0,
		is_foreign_key		=> 0
	},
	options => {
		data_type			=> 'text',
		size				=> 65535,
		is_nullable			=> 0,
		default_value		=> undef,
		is_auto_increment	=> 0,
		is_foreign_key		=> 0
	},
	date_enqueued => {
		data_type			=> 'datetime',
		size				=> 0,
		is_nullable			=> 0,
		default_value		=> undef,
		is_auto_increment	=> 0,
		is_foreign_key		=> 0
	},
	date_to_execute => {
		data_type			=> 'datetime',
		size				=> 0,
		is_nullable			=> 0,
		default_value		=> undef,
		is_auto_increment	=> 0,
		is_foreign_key		=> 0
	},
	date_started => {
		data_type			=> 'datetime',
		size				=> 0,
		is_nullable			=> 1,
		default_value		=> 'NULL',
		is_auto_increment	=> 0,
		is_foreign_key		=> 0
	},
	date_completed => {
		data_type			=> 'datetime',
		size				=> 0,
		is_nullable			=> 1,
		default_value		=> 'NULL',
		is_auto_increment	=> 0,
		is_foreign_key		=> 0
	}
);
__PACKAGE__->set_primary_key('id');

__PACKAGE__->add_relationship('queue', 'Hopkins::State::Schema::Row::Queue',
	{ 'foreign.id'	=> 'self.id_queue'	},
	{ 'accessor'	=> 'single'			}
);

__PACKAGE__->inflate_column('date_enqueued', {
    inflate => sub { DateTime::Format::ISO8601->parse_datetime(shift) },
    deflate => sub { shift->iso8601 }
});
__PACKAGE__->inflate_column('date_to_execute', {
    inflate => sub { DateTime::Format::ISO8601->parse_datetime(shift) },
    deflate => sub { shift->iso8601 }
});
__PACKAGE__->inflate_column('date_started', {
    inflate => sub { DateTime::Format::ISO8601->parse_datetime(shift) },
    deflate => sub { shift->iso8601 }
});
__PACKAGE__->inflate_column('date_completed', {
    inflate => sub { DateTime::Format::ISO8601->parse_datetime(shift) },
    deflate => sub { shift->iso8601 }
});

=back

=head1 AUTHOR

Mike Eldridge <diz@cpan.org>

=cut

1;
