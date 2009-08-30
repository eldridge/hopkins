use Test::More tests => 37;

use strict;
use warnings;

use lib 't/lib';

use TestEnvironment;
use TestHelper;
use POE;

use_ok('Hopkins');

# create a test environment.  this entails processing
# hopkins and log4perl configuration files using templates
# and also instantiating a Hopkins object.

my $env = new TestEnvironment { source => 'hopkins.xml.tt' };
isa_ok($env, 'TestEnvironment', 'hopkins test environment');

my $envl4p = new TestEnvironment { source => 'log4perl.conf.tt' };
isa_ok($env, 'TestEnvironment', 'hopkins test environment');

my $hopkins = new Hopkins { conf => [ XML => { file => $env->conf } ], l4pconf => $envl4p->conf, scan => 30, poll => 30 };
isa_ok($hopkins, 'Hopkins', 'hopkins object');

# instantiate a new TestHelper object, which is simply a
# subclass of POE::API::Peek with a few convenience methods
# added for our testing harness.

my $helper = new TestHelper;
isa_ok($helper, 'TestHelper', 'test helper');
isa_ok($helper, 'POE::API::Peek', 'POE API');

# check hopkins state: 0 timeslice

ok($helper->is_kernel_running);

isa_ok($hopkins->manager->config, 'Hopkins::Config', 'hopkins->manager->config');
ok($hopkins->manager->config->loaded, 'config loaded');

cmp_ok($helper->session_count, '==', 2, 'session count');
ok($helper->resolve_alias('manager'), 'session running: manager');

ok($helper->events_waiting({ manager => [ qw(init_plugins init_store init_queues config_scan) ] }), 'queued events');

# check hopkins state: 1 timeslice

ok(POE::Kernel->run_one_timeslice, 'run one timeslice');

cmp_ok($helper->session_count, '==', 3, 'session count');
ok($helper->resolve_alias('manager'),	'session running: manager');
ok($helper->resolve_alias('store'),		'session running: store');

ok($helper->events_waiting({ manager => [ qw(queue_start scheduler) ] }), 'queued events');

# check hopkins state: 2 timeslice

ok(POE::Kernel->run_one_timeslice, 'run one timeslice');

cmp_ok($helper->session_count, '==', 5, 'session count');

ok($helper->resolve_alias('manager'),			'session running: manager');
ok($helper->resolve_alias('store'),				'session running: store');
ok($helper->resolve_alias('queue.parallel'),	'session running: queue.parallel');
ok($helper->resolve_alias('queue.serial'),		'session running: queue.serial');

cmp_ok($hopkins->manager->queue('serial')->num_queued, '==', 0, 'queue length: serial');
cmp_ok($hopkins->manager->queue('parallel')->num_queued, '==', 0, 'queue length: parallel');

# invoke the scheduler synchronously and check state again

POE::Kernel->call(manager => 'scheduler');

cmp_ok($hopkins->manager->queue('serial')->num_queued, '==', 0, 'queue length: serial');
cmp_ok($hopkins->manager->queue('parallel')->num_queued, '==', 2, 'queue length: parallel');

ok($helper->events_waiting({ store => [ 'notify' ], 'queue.parallel' => [ 'enqueue' ] }), 'queued events');

# check hopkins state: 3 timeslice

ok(POE::Kernel->run_one_timeslice, 'run one timeslice');

ok($helper->events_waiting({ 'queue.parallel' => [ 'dequeue' ] }), 'queued events');

# check hopkins state: 3 timeslice

ok(POE::Kernel->run_one_timeslice, 'run one timeslice');

ok($helper->resolve_alias('manager'),			'session running: manager');
ok($helper->resolve_alias('store'),				'session running: store');
ok($helper->resolve_alias('queue.parallel'),	'session running: queue.parallel');
ok($helper->resolve_alias('queue.serial'),		'session running: queue.serial');
ok($helper->resolve_alias('queue.parallel.worker.6'),	'session running: queue.parallel.worker.6');
ok($helper->resolve_alias('queue.parallel.worker.7'),	'session running: queue.parallel.worker.7');

#use Data::Dumper;
#$Data::Dumper::Indent = 1;

#diag Dumper [ $helper->event_queue_dump ];
#diag Dumper [ map { $helper->session_alias_list($_) } $helper->session_list ];

exit;

POE::Session->create
(
	inline_states =>
	{
		_start					=> \&test_start,
		test_01startup_sessions	=> \&test_01startup_sessions,
		test_02queue_task		=> \&test_02queue_task,
	}
);

$hopkins->run;

sub test_start
{
	my $kernel = $_[KERNEL];

	ok(1, 'test sesssion running');

	$kernel->alias_set('test_05scheduler');

	$kernel->alarm(test_01startup_sessions	=> time + 1);
	$kernel->alarm(test_02queue_task		=> time + 2);
}

sub test_01startup_sessions
{
	my $kernel = $_[KERNEL];

	ok(1, 'test_01startup_sessions');

	my @sessions	= $helper->session_list;
	my @aliases		= sort map { POE::Kernel->alias($_) } @sessions;

	cmp_ok(scalar(@aliases), '==', 6, '6 running sessions');

	is($aliases[0], 'manager',			'session: manager');
	is($aliases[1], 'queue.parallel',	'session: queue.parallel');
	is($aliases[2], 'queue.serial',		'session: queue.serial');
	is($aliases[3], 'state',			'session: state');
	is($aliases[4], 'store',			'session: store');
	is($aliases[5], 'test_05scheduler',	'session: test_05scheduler');
}

sub test_02queue_task
{
	my $kernel = $_[KERNEL];

	ok(1, 'test_02queue_task');
}

