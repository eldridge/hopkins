package Hopkins::MapReduce::Mapper;

requires 'read';
requires 'reducers';
requires 'map';
requires 'partition';

use SOAP::Lite;

sub run
{
	my $self = shift;

	my @reducers = ();

	foreach my $reducer ($self->reducers) {
		my $target	= $reducer->[0];
		my $uri		= new URI $reducer->[1];

		$uri->path('');
		$uri->query('');

		my $soap = SOAP::Lite->uri($uri->canonical);

		$uri->query_form(session => 'rpc');

		push @reducers, $soap->proxy($uri->canonical);
	}


	while (my $data = $self->read) {
		my @data = $self->map($data);

		$reducers[$self->partition(@data)]->;
	}
}
