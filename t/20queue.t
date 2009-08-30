use Test::More tests => 34;

use strict;
use warnings;

use lib 't/lib';

use TestEnvironment;
use TestHelper;

use_ok('Hopkins::Queue');

# create a test environment.  this entails processing
# hopkins and log4perl configuration files using templates
# and also instantiating a Hopkins object.

my $env = new TestEnvironment;
isa_ok($env, 'TestEnvironment', 'hopkins test environment');

my $config = new Test::MockObject;
isa_ok($config, 'Test::MockObject', 'fake config');

$config->mock('fetch', sub { return $env->scratch->mkdir('state') });

my $queue = new Hopkins::Queue { kernel => $poe_kernel, config => $config, name => 'general' };
isa_ok($queue, 'Hopkins::Queue', 'queue');

$env->fake_work->set_always('queue', $queue);

isa_ok($queue->tasks, 'Tie::IxHash', 'task list');
isa_ok($queue->cache, 'Cache::FileCache', 'queue state');

is($queue->alias, 'queue.general', 'queue alias');

ok(!$queue->halted, 'queue not halted');
ok(!$queue->frozen, 'queue not frozen');

$queue->freeze;	ok($queue->frozen, 'queue frozen');
$queue->thaw;	ok(!$queue->frozen, 'queue not frozen');

my $worker = $queue->spawn_worker('A', $env->fake_work);
isa_ok($worker, 'Hopkins::Worker', 'spawned worker');

# test adding Work to the Queue

cmp_ok($queue->num_queued, '==', 0, 'queue->num_queued');

$queue->tasks->Push(AABBCCDD => $env->fake_work);
cmp_ok($queue->num_queued, '==', 1, 'queue->num_queued');

# test state loading by instantiating a new Queue object.
# in order to thoroughly test this, we'll do it twice - once
# without a mock Config->get_task_info method and once with.
# only the second attempt should result in the Queue being
# populated.

$queue->write_state;

$config->set_always('get_task_info', undef);

$queue = new Hopkins::Queue { kernel => $poe_kernel, config => $config, name => 'general' };
isa_ok($queue, 'Hopkins::Queue', 'queue');
cmp_ok($queue->num_queued, '==', 0, 'queue->num_queued');

$config->set_always('get_task_info', $env->fake_task);

$queue = new Hopkins::Queue { kernel => $poe_kernel, config => $config, name => 'general' };
isa_ok($queue, 'Hopkins::Queue', 'queue');
cmp_ok($queue->num_queued, '==', 1, 'queue->num_queued');

my $work = $queue->tasks->Shift;
isa_ok($work, 'Hopkins::Work', 'queue work');
is($work->id,				'DEADBEEF',				'work->id');
is($work->task->name,		'counter',				'work->task');
is($work->date_enqueued,	'2009-06-01T20:24:42',	'work->date_enqueued');
is_deeply($work->options,	{ fruit => 'apple' },	'work->options');

# test prioritization

cmp_ok(Hopkins::Queue::prioritize([ 0, 0, 0, 0, 0, { priority => 2 } ], []), '==', -1, 'prioritize: a=2');
cmp_ok(Hopkins::Queue::prioritize([ 0, 0, 0, 0, 0, { priority => 5 } ], []), '==', 0, 'prioritize: a=5');
cmp_ok(Hopkins::Queue::prioritize([ 0, 0, 0, 0, 0, { priority => 6 } ], []), '==', 1, 'prioritize: a=6');

cmp_ok(Hopkins::Queue::prioritize([], [ 0, 0, 0, 0, { priority => 2 } ]), '==', 1, 'prioritize: b=2');
cmp_ok(Hopkins::Queue::prioritize([], [ 0, 0, 0, 0, { priority => 5 } ]), '==', 0, 'prioritize: b=5');
cmp_ok(Hopkins::Queue::prioritize([], [ 0, 0, 0, 0, { priority => 6 } ]), '==', -1, 'prioritize: b=6');

cmp_ok(Hopkins::Queue::prioritize([ 0, 0, 0, 0, 0, { priority => -94 } ], [ 0, 0, 0, 0, { priority => 2 } ]), '==', -1, 'prioritize: a=-94; b=2');
cmp_ok(Hopkins::Queue::prioritize([ 0, 0, 0, 0, 0, { priority => 2 } ], [ 0, 0, 0, 0, { priority => 2 } ]), '==', 0, 'prioritize: a=2; b=2');
cmp_ok(Hopkins::Queue::prioritize([ 0, 0, 0, 0, 0, { priority => 2 } ], [ 0, 0, 0, 0, { priority => 4 } ]), '==', -1, 'prioritize: a=2; b=4');
cmp_ok(Hopkins::Queue::prioritize([ 0, 0, 0, 0, 0, { priority => 354 } ], [ 0, 0, 0, 0, { priority => 9 } ]), '==', 0, 'prioritize: a=354; b=9');
cmp_ok(Hopkins::Queue::prioritize([ 0, 0, 0, 0, 0, { priority => 9 } ], [ 0, 0, 0, 0, { priority => 8 } ]), '==', 1, 'prioritize: a=354; b=9');

