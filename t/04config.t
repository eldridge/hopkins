use Test::More tests => 13;

use strict;
use warnings;

use lib 't/lib';

use TestEnvironment;
use Test::MockObject;
use Test::MockObject::Extends;

sub Hopkins::Config::KERNEL	{ 1 }
sub Hopkins::Config::HEAP	{ 2 }

sub Hopkins::log_debug		{ }
sub Hopkins::log_info		{ }
sub Hopkins::log_warn		{ }
sub Hopkins::log_error		{ }

my $env1	= new TestEnvironment;
my $env2	= new TestEnvironment { conf => 'hopkins.xml2.tt' };
my $kernel	= new Test::MockObject;
my $schema	= new Test::MockObject;
my $storage	= new Test::MockObject;
my $heap	= {};
my $events	= {};

$kernel->fake_module('POE');
$kernel->mock('post', sub { $events->{$_[1]}->{$_[2]} = 1 });

$heap->{opts}->{conf} = sprintf '%s', $env1->config;

use_ok('Hopkins::Config');

my $config = Hopkins::Config->load($kernel, $heap);

isa_ok($config, 'HASH', 'hopkins config');

my $href =
{
	database =>
	{
		dsn		=> 'dbi:SQLite:dbname=' . $env1->scratch . '/hopkins.db',
		user	=> 'root',
		pass	=> '',
		options	=>
		{
			AutoCommit	=> 1,
			RaiseError	=> 1,
			name_sep	=> '.',
			quote_char	=> ''
		},
	},

	task =>
	{
		Count =>
		{
			class		=> 'Hopkins::Test::Count',
			queue		=> 'serial',
			schedule	=> '* * * * *',
			schedules	=> DateTime::Event::MultiCron->from_multicron('* * * * *')
		},
		Wrench =>
		{
			class => 'Hopkins::Test::Die'
		}
	},

	queue =>
	{
		serial		=> { concurrency => 1 },
		parallel	=> { concurrency => 4 }
	}
};

is_deeply($config, $href, 'hopkins config tree');
is_deeply($events, {}, 'no kernel events');

$schema->set_always('storage', $storage);
$storage->set_always('connect_info', [ @{$href->{database}}{qw(dsn user pass options)} ]);

$heap->{schema} = $schema;

# reload the configuration without making any changes.  the
# configuration data structure should still match and no POE
# kernel events shhould have been posted.

$config = Hopkins::Config->load($kernel, $heap);

is_deeply($config, $href, 'no changes in config tree');
is_deeply($events, {}, 'no changes in kernel events');

# reload the configuration after making a small change to
# the running database configuration.  the config data
# structure should now differ under the database node and a
# POE storeinit event should have been posted to the manager
# session.

$href->{database}->{user}	= 'toor';
$href->{database}->{pass}	= 'secret';
$href->{database}->{dsn}	= 'dbi:SQLite:dbname=' . $env2->scratch . '/hopkins.db',
$heap->{opts}->{conf}		= sprintf '%s', $env2->config;

$config = Hopkins::Config->load($kernel, $heap);

is_deeply($config, $href, 'change in config tree');
is_deeply($events, { manager => { storeinit => 1 } }, 'storeinit posted to manager session');

# test out all of the helper methods of Hopkins::Config

is_deeply([ sort Hopkins::Config->get_queue_names ],	[ qw(parallel serial) ],	'Hopkins::Config->get_queue_names');
is_deeply([ sort Hopkins::Config->get_task_names ],		[ qw(Count Wrench) ],		'Hopkins::Config->get_task_names');
is_deeply(Hopkins::Config->get_queue_info('serial'),	$href->{queue}->{serial},	'Hopkins::Config->get_queue_info');
is_deeply(Hopkins::Config->get_task_info('Count'),		$href->{task}->{Count},		'Hopkins::Config->get_task_info');

is(Hopkins::Config->fetch('database/options/name_sep'), '.', 'Hopkins::Config->fetch');

