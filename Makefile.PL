#!/usr/bin/perl -w

use warnings;
use strict;

use inc::Module::Install;

name			'Hopkins';
author			'Mike Eldridge <diz@cpan.org>';
license			'perl';
version_from	'lib/Hopkins.pm';

# date manipulation and testing
requires 'DateTime'						=> '0.37';
requires 'DateTime::Event::MultiCron'	=> '0.01';
requires 'DateTime::Set'				=> '0.25';

# configuration and logging
requires 'File::Monitor'				=> '0.10';
requires 'Log::Log4perl'				=> '1.12';
requires 'XML::Simple'					=> '2.18';

# DBI/POE infrastructure
requires 'DBIx::Class'					=> '0.08007';
requires 'POE'							=> '0.9999';
requires 'POE::API::Peek'				=> '1.34';
requires 'POE::Component::JobQueue'		=> '0.55';
requires 'POE::Wheel::Run'				=> '1.2179';

# required for HMI plugin
requires 'POE::Component::Server::HTTP'	=> '0.09';
requires 'Sys::Hostname::FQDN'			=> '0.10';
requires 'Catalyst::Engine::Embeddable'	=> '0.0.1';

# required for RPC plugin
requires 'POE::Component::Server::SOAP'	=> '1.11';

build_requires 'Class::Accessor::Fast';
build_requires 'Directory::Scratch';
build_requires 'FindBin';
build_requires 'Template';

#auto_install;
WriteAll;

