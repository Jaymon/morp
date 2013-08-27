import urlparse

class Connection(object):
    """The base connection class, you will most likely always use DsnConnection"""

    name = ""
    """string -- the name of this connection, handy when you have more than one used interface (eg, nsq)"""

    hosts = None
    """list -- a list of (hostname, port) tuples"""

    interface_name = ""
    """string -- full Interface class name -- the interface that will connect to the messaging backend"""

    options = None
    """dict -- any other interface specific options you need"""

    def __init__(self, **kwargs):
        """
        set all the values by passing them into this constructor, any unrecognized kwargs get put into .options

        example --
        c = Connection(
            interface_name="...",
            hosts=[("host", port), ("host2", port2)],
            some_random_thing="foo"
        )

        print c.port # 5000
        print c.options # {"some_random_thing": "foo"}
        """
        self.options = {}
        self.hosts = []

        for key, val in kwargs.iteritems():
            if hasattr(self, key):
                setattr(self, key, val)
            else:
                self.options[key] = val

    def get_netlocs(self, default_port):
        return ["{}:{}".format(h[0], default_port if h[1] is None else h[1]) for h in self.hosts]

    def get_option(self, key, default_val):
        return getattr(self.options, key, default_val)


class DsnConnection(Connection):
    """
    Create a connection object from a dsn in the form

        InterfaceName://host1:port1+host2:port2?opt1=val1&opt2=val2#connection_name

    example -- connect to nsq with 2 lookupd instances

        morp.interface.nsq.Nsq://host1.com:4161+host2.com:4161#nsq
    """
    def __init__(self, dsn):

        d = {'options': {}, 'hosts': []}

        # get the scheme, which is actually our interface_name
        first_colon = dsn.find(u':')
        d['interface_name'] = dsn[0:first_colon]
        dsn_url = dsn[first_colon+1:]
        dsn_hosts = []

        dsn_bits = dsn_url.split(u'?')
        dsn_hosts = dsn_bits[0].split(u'+')
        if len(dsn_bits) == 2:
            dsn_hosts[-1] += u'?' + dsn_bits[1]

        for dsn_host in dsn_hosts:
            if not dsn_host.startswith(u'//'):
                dsn_host = u'//' + dsn_host

            url = urlparse.urlparse(dsn_host)

            # parse the query into options, multiple dsns
            if url.query:
                for k, kv in urlparse.parse_qs(url.query, True).iteritems():
                    if len(kv) > 1:
                        self.options[k] = kv
                        d['options'][k] = kv
                    else:
                        d['options'][k] = kv[0]

            d['hosts'].append((url.hostname, getattr(url, 'port', None)))

            if url.username:
                d['username'] = url.username

            if url.password:
                d['password'] = url.password

            if url.fragment:
                d['name'] = url.fragment

        super(DsnConnection, self).__init__(**d)

