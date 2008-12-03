#!/usr/bin/perl -w

use warnings;
use strict;

use inc::Module::Install;

name			'Hopkins';
author			'Mike Eldridge <diz@cpan.org>';
license			'perl';
version_from	'lib/Hopkins.pm';

requires 'DateTime'						=> '0.37';
requires 'DateTime::Event::MultiCron'	=> '0.01';
requires 'DateTime::Set'				=> '0.25';
requires 'DBIx::Class'					=> '0.08007';
requires 'File::Monitor'				=> '0.10';
requires 'Log::Log4perl'				=> '1.12';
requires 'POE'							=> '0.9999';
requires 'POE::Component::JobQueue'		=> '0.55';
requires 'POE::Component::Server::SOAP'	=> '1.11';
requires 'POE::Wheel::Run'				=> '1.2179';
requires 'XML::Simple'					=> '2.18';

build_requires 'Directory::Scratch';

auto_install;
WriteAll;

