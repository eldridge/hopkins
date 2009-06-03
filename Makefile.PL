#!/usr/bin/perl -w

use warnings;
use strict;

use inc::Module::Install;

name			'Hopkins';
author			'Mike Eldridge <diz@cpan.org>';
license			'perl';
version_from	'lib/Hopkins.pm';

# command line tools
requires 'Getopt::Attribute'			=> '1.44';
requires 'String::Escape'				=> '2002.001';

# supporting classes
requires 'List::Object'					=> '0.01';
requires 'Tie::IxHash'					=> '1.21';
requires 'Cache::FileCache'				=> 0;

# date manipulation and testing
requires 'DateTime'						=> '0.37';
requires 'DateTime::Event::MultiCron'	=> '0.01';
requires 'DateTime::Set'				=> '0.25';

# configuration and logging
requires 'File::Monitor'				=> '0.10';
requires 'Log::Log4perl'				=> '1.12';
requires 'XML::Simple'					=> '2.18';

# DBI/POE infrastructure
requires 'DBIx::Class'					=> '0.08103';
requires 'POE'							=> '1.005';
requires 'POE::API::Peek'				=> '1.34';
requires 'POE::Component::JobQueue'		=> '0.55';
requires 'POE::Wheel::Run'				=> '1.2179';

# required for HMI plugin
requires 'POE::Component::Server::HTTP'	=> '0.09';
requires 'Sys::Hostname::FQDN'			=> '0.10';
requires 'Catalyst::Engine::Embeddable'	=> '0.000002';

# required for RPC plugin
requires 'POE::Component::Server::SOAP'	=> '1.11';

# required for the test suite
test_requires 'Class::Accessor::Fast';
test_requires 'Directory::Scratch';
test_requires 'FindBin';
test_requires 'Template';
test_requires 'Test::MockObject';
test_requires 'POE::API::Peek';

#auto_install;
WriteAll;

