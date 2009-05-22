package Hopkins::State::Schema::Row::Queue;

use strict;

=head1 NAME

Hopkins::State::Schema::Row::Queue

=head1 DESCRIPTION

=cut

use base 'DBIx::Class';

__PACKAGE__->load_components(qw/PK::Auto Core/);

__PACKAGE__->table('queue');
__PACKAGE__->add_columns(
	id => {
		data_type			=> 'bigint',
		size				=> 20,
		is_nullable			=> 0,
		default_value		=> undef,
		is_auto_increment	=> 1,
		is_foreign_key		=> 0,
		extra				=> { unsigned => 1 }
	},
	name => {
		data_type			=> 'varchar',
		size				=> 255,
		is_nullable			=> 0,
		default_value		=> undef,
		is_auto_increment	=> 0,
		is_foreign_key		=> 0
	},
	halted => {
		data_type			=> 'integer',
		size				=> 1,
		is_nullable			=> 0,
		default_value		=> 0,
		is_auto_increment	=> 0,
		is_foreign_key		=> 0
	},
	frozen => {
		data_type			=> 'integer',
		size				=> 1,
		is_nullable			=> 0,
		default_value		=> 0,
		is_auto_increment	=> 0,
		is_foreign_key		=> 0
	},
	error => {
		data_type			=> 'text',
		size				=> 65535,
		is_nullable			=> 1,
		default_value		=> undef,
		is_auto_increment	=> 0,
		is_foreign_key		=> 0
	}
);
__PACKAGE__->set_primary_key('id');

=back

=head1 AUTHOR

Mike Eldridge <diz@cpan.org>

=cut

1;
