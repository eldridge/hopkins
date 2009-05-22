package Hopkins::State;

use strict;
use warnings;

=head1 NAME

Hopkins::State - local task state

=head1 DESCRIPTION

Hopkins::State encapsulates all of the busywork associated
with keeping a list of all running tasks in each individual
queue.  i'd prefer not to have to do this, but it appears
that POE::Component::JobQueue provides no means by which i
may access this information.

=cut

use POE;
use Class::Accessor::Fast;

#use Cache;
use Data::UUID;

use Path::Class ();

use Hopkins::State::Schema;

use base 'Class::Accessor::Fast';

__PACKAGE__->mk_accessors(qw(cache config schema));

my $ug = new Data::UUID;

sub new
{
	my $self = shift->SUPER::new(@_);

	#$self->cache(new File::Cache cache_root => $self->config->{root});

	my $dir		= new Path::Class::Dir $self->config->{root};
	my $file	= $dir->file('state');

	$file->touch;
	my $schema	= Hopkins::State::Schema->connect("dbi:SQLite:dbname=$file");

	$schema->deploy;

	die $@ if not defined $schema;

	my @work = $schema->resultset('Work')->all;

	$self->schema(Hopkins::State::Schema->connect("dbi:SQLite:dbname=$file"));

	POE::Session->create
	(
		object_states =>
		[
			$self =>
			{
				_start			=> 'start',
				_stop			=> 'stop',

				task_enqueued	=> 'task_enqueued',
				task_stored		=> 'task_stored',
				task_completed	=> 'task_completed'
			}
		]
	);

	return $self;
}

sub start
{
	my $self	= $_[OBJECT];
	my $kernel	= $_[KERNEL];

	$kernel->alias_set('state');

	# wake up periodically and write state to disk
}

sub stop
{
	my $self = $_[OBJECT];

	Hopkins->log_debug('state exiting');
}

sub task_enqueued
{
	my $self	= $_[OBJECT];
	my $kernel	= $_[KERNEL];
	my $task	= $_[ARG0];

	$task->id($ug->create_str);

	#$self->tasks->{$task->id} = $task;

	return $task->id;
}

sub task_stored
{
	my $self	= $_[OBJECT];
	my $id		= $_[ARG0];
	my $row		= $_[ARG1];

	$self->state->tasks->{$id}->row($row);
}

sub task_completed
{
}

sub update
{
	
}

=back

=head1 AUTHOR

Mike Eldridge <diz@cpan.org>

=head1 LICENSE

=cut

1;

