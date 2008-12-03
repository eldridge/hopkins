package Hopkins;

use strict;
use warnings;

our $VERSION = '0.01';

=head1 NAME

Hopkins - POE powered job management system

=head1 DESCRIPTION

=cut

#sub POE::Kernel::TRACE_REFCNT () { 1 }

use POE qw(Component::JobQueue Component::Server::SOAP Wheel::Run);

use Log::Log4perl;

use Hopkins::Config;
use Hopkins::Manager;
use Hopkins::Store;
use Hopkins::Queue;
use Hopkins::RPC;

sub POE::Component::Server::SOAP::DEBUG () { 0 }
sub POE::Wheel::SocketFactory::DEBUG { 0 }
sub POE::Kernel::alias { (shift->alias_list(@_))[0] }

=head1 METHODS

=over 4

=item new

instantiates a new Hopkins object.  the Hopkins constructor
accepts a hash of options.  currently supported options are:

=over 4

=item conf

path to the hopkins XML configuration file

=item lp4conf

path to the log4perl configuration file

=item scan

configuration scan resolution (in seconds)

=item poll

scheduler poll resolution (in seconds)

=cut

sub new
{
	my $proto	= shift;
	my $class	= ref($proto) || $proto;
	my $opts	= { @_ };

	# defaults
	$opts->{conf}		||= '/etc/hopkins/hopkins.xml';
	$opts->{lp4conf}	||= '/etc/hopkins/log4perl.conf';
	$opts->{scan}		||= 30;
	$opts->{poll}		||= 30;

	# initialize log4perl
	Log::Log4perl::init_and_watch($opts->{l4pconf}, $opts->{scan});

	# bless me, father.  make me magical.
	my $self = bless {}, $class;

	# create the POE Session that will be the bread and butter
	# of the job daemon's normal function.  the manager session
	# will read the configuration upon execution and will begin
	# the rest of the startup process in the following order:
	#
	#	- storage initialization via DBIx::Class
	#	- queue creation via POE::Component::JobQueue
	#	- RPC session creation via POE::Component::Server::SOAP

	# create manager session
	POE::Session->create
	(
		inline_states => {
			_start		=> \&Hopkins::Manager::start,
			_stop		=> \&Hopkins::Manager::stop,

			confscan	=> \&Hopkins::Config::scan,
			confload	=> \&Hopkins::Config::load,
			queueinit	=> \&Hopkins::Queue::init,
			queuefail	=> \&Hopkins::Queue::fail,
			storeinit	=> \&Hopkins::Store::init,
			rpcinit		=> \&Hopkins::RPC::init,
			enqueue		=> \&Hopkins::Manager::enqueue,
			postback	=> \&Hopkins::Manager::postback,
			scheduler	=> \&Hopkins::Manager::scheduler,
			shutdown	=> \&Hopkins::Manager::shutdown
		},

		args => $opts
	);

	return $self;
}

=item run

a simple shortcut to conceal the fact that we are using POE.
aren't we pretty?  giggity giggity.

=cut

sub run { POE::Kernel->run }

=item get_logger

returns a Log::Log4perl logger for the current session.
the get_logger expects the POE kernel to be passed to
it.  if no POE::Kernel is passed, it will default to
$poe_kernel.

=cut

my $loggers = {};

sub get_logger
{
	my $self	= shift;
	my $kernel	= shift || $poe_kernel;
	my $alias	= ($kernel->alias_list)[0];

	if (not exists $loggers->{$alias}) {
		$loggers->{$alias} = Log::Log4perl->get_logger("hopkins.$alias");
	}

	return $loggers->{$alias};
}

sub get_worker_logger
{
	my $self	= shift;
	my $name	= shift;
	my $alias	= "task.$name";

	if (not exists $loggers->{$alias}) {
		$loggers->{$alias} = Log::Log4perl->get_logger("hopkins.$alias");
	}

	return $loggers->{$alias};
}

sub log_debug	{ return shift->get_logger->debug(@_)	}
sub log_info	{ return shift->get_logger->info(@_)	}
sub log_warn	{ return shift->get_logger->warn(@_)	}
sub log_error	{ return shift->get_logger->error(@_)	}
sub log_fatal	{ return shift->get_logger->fatal(@_)	}

sub log_worker_stdout { return shift->get_worker_logger(shift)->info(@_) }
sub log_worker_stderr { return shift->get_worker_logger(shift)->warn(@_) }

=head1 BUGS

this is my first foray into POE territory.  the way
the system is architected may be horribly inefficient,
cause cancer, or otherwise be a general nuisance to
its intended user(s).  my bad.

=head1 AUTHOR

Mike Eldridge <diz@cpan.org>

=head1 LICENSE

Copyright (c) 2008 Mike Eldridge.  All rights reserved.

This program is free software; you may redistribute it
and/or modify it under the same terms as Perl itself.

=cut

1;
