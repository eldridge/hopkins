use inc::Module::Install;

name 'Greenspan';
all_from 'lib/Greenspan.pm';

requires Catalyst => '5.64';

catalyst;

version 1;
install_script glob('script/*.pl');
auto_install;
WriteAll;
