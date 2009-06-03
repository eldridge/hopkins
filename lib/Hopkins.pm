package Hopkins;

use strict;
use warnings;

our $VERSION = '0.900';

=head1 NAME

Hopkins - POE powered job management system

=head1 DESCRIPTION

=cut

#sub POE::Kernel::TRACE_REFCNT () { 1 }

use POE qw(Component::JobQueue Component::Server::SOAP Wheel::Run);

use POE::API::Peek;

use Class::Accessor::Fast;

use Log::Log4perl;
use Log::Log4perl::Level;

use Hopkins::Manager;

use base 'Class::Accessor::Fast';

__PACKAGE__->mk_accessors(qw(conf l4pconf scan poll manager));

# prevent perl from bitching and complaining about prototype
# mismatches and constant subroutine redefinitions.  the
# warnings pragma doesn't prevent ALL of them from spewing,
# so we have to get raunchy with perl by defining them at
# runtime with a localized no-op warn handler.

{
	local $SIG{__WARN__} = sub { 1 };

	# forcefully disable the unavoidable debugging output
	# from several of the POE components.  also install a
	# shortcut into the POE::Kernel namespace to retrieve
	# the first alias returned by POE::Kernel->alias_list

	eval q/
		sub POE::Component::Server::SOAP::DEBUG () { 0 }
		sub POE::Wheel::SocketFactory::DEBUG { 0 }
		sub POE::Kernel::alias { (shift->alias_list(@_))[0] }
	/;
}

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
	my $opts	= shift;

	# defaults

	$opts->{conf}		||= [ XML => { file => '/etc/hopkins/hopkins.xml' } ];
	$opts->{lp4conf}	||= '/etc/hopkins/log4perl.conf';
	$opts->{scan}		||= 30;
	$opts->{poll}		||= 30;

	# make me majikal wif ur holy waterz.  plzkthx.

	my $self = $proto->SUPER::new($opts);

	# initialize log4perl using the contents of the supplied
	# configuration file.
	#
	# after initialization (which may have failed), we'll
	# create a logger and associated appender for logging
	# all error level log messages to stderr.  this allows
	# logging error messages to the console using log4perl
	# regardless of the configuration that the user loads.

	eval { Log::Log4perl::init_and_watch($opts->{l4pconf}, $opts->{scan}) };

	my $l4perr = $@;
	my $logger = Log::Log4perl->get_logger('hopkins');
	my $layout = new Log::Log4perl::Layout::PatternLayout '%X{session}: %p: %m%n';

	my $appender = new Log::Log4perl::Appender
		'Log::Log4perl::Appender::Screen',
		name	=> 'stderr',
		stderr	=> 1;

	$appender->layout($layout);
	$appender->threshold($ERROR);
	$logger->add_appender($appender);

	Hopkins->log_error("unable to load log4perl configuration file: $l4perr")
		if $l4perr;

	$self->manager(new Hopkins::Manager { hopkins => $self });

	return $self;
}

=item run

a simple shortcut to conceal the fact that we are using POE.
aren't we pretty?  giggity giggity.

=cut

sub run { POE::Kernel->run }

=item is_session_running

returns a truth value indicating whether or not a session
exists with the specified alias.

=cut

sub is_session_active
{
	my $self = shift;
	my $name = shift;

	my $api			= new POE::API::Peek;
	my @sessions	= map { POE::Kernel->alias($_) } $api->session_list;

	#Hopkins->log_debug("checking for session $name");

	return scalar grep { $name eq $_ } @sessions;
}

=item get_running_sessions

returns a list of currently active session aliases

=cut

sub get_running_sessions
{
	my $self = shift;

	my $api = new POE::API::Peek;

	return map { POE::Kernel->alias($_) } $api->session_list;
}

=item get_logger

returns a Log::Log4perl logger for the current session.  the
get_logger expects the POE kernel to be passed to it.  if no
POE::Kernel is passed, it will default to $poe_kernel.

=cut

my $loggers = {};

sub get_logger
{
	my $self	= shift;
	my $kernel	= shift || $poe_kernel;
	my $alias	= $kernel->alias;
	my $session	= 'hopkins' . ($alias ? '.' . $alias : '');
	my $name	= lc ((caller(2))[3]);

	$alias = 'unknown' if not defined $alias;

	if (not exists $loggers->{$name}) {
		$loggers->{$name} = Log::Log4perl->get_logger($name);
	}

	Log::Log4perl::MDC->put('session', $session);

	return $loggers->{$name};
}

sub get_worker_logger
{
	my $self	= shift;
	my $task	= shift;
	my $kernel	= shift || $poe_kernel;
	my $alias	= $kernel->alias;
	my $session	= 'hopkins' . ($alias ? '.' . $alias : '');
	my $name	= "hopkins.task.$task";

	$alias = 'unknown' if not defined $alias;

	if (not exists $loggers->{$name}) {
		$loggers->{$name} = Log::Log4perl->get_logger($name);
	}

	Log::Log4perl::MDC->put('session',	$session);
	Log::Log4perl::MDC->put('task',		$task);

	return $loggers->{$name};
}

sub log_debug	{ return shift->get_logger->debug(@_)	}
sub log_info	{ return shift->get_logger->info(@_)	}
sub log_warn	{ return shift->get_logger->warn(@_)	}
sub log_error	{ return shift->get_logger->error(@_)	}
sub log_fatal	{ return shift->get_logger->fatal(@_)	}

sub log_worker_stdout { return shift->get_worker_logger(shift)->info(@_) }
sub log_worker_stderr { return shift->get_worker_logger(shift)->warn(@_) }

=head1 BUGS

this is my first foray into POE territory.  the way the
system is architected may be horribly inefficient, cause
cancer, or otherwise be a general nuisance to its intended
user(s).  my bad.

=head1 AUTHOR

Mike Eldridge <diz@cpan.org>

=head1 LICENSE

Copyright (c) 2008 Mike Eldridge.  All rights reserved.

This program is free software; you may redistribute it
and/or modify it under the same terms as Perl itself.

=cut

1;
