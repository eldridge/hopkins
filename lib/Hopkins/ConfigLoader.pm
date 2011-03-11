package Hopkins::Config;

use Moose::Role;

=head1 NAME

Hopkins::Config - hopkins configuration

=head1 DESCRIPTION

Hopkins::Config is a framework for configuring queues and
tasks as well as state tracking and storage backends.
hopkins supports a pluggable configuration model, allowing
configuration via XML, YAML, RDBMS -- pretty much anything
that implements the methods below.

=cut


requires 'load';
requires 'scan';
requires 'get_queue_names';
requires 'get_task_names';
requires 'get_task_info';
requires 'get_queue_info';
requires 'get_plugin_names';
requires 'get_plugin_info';
requires 'has_plugin';
requires 'fetch';
requires 'loaded';

has config =>
	isa			=> 'Hopkins::Config',
	is			=> 'ro',
	required	=> 1;

method register_task(HopkinsTask:
{
	m
}

sub store_modified
{
}

=back

=head1 AUTHOR

Mike Eldridge <diz@cpan.org>

=head1 LICENSE

=cut

no Moose::Role;

1;
