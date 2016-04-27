import hashlib

import dsnparse


class Connection(object):
    """The base connection class, you will most likely always use DsnConnection"""
    name = ""
    """string -- the name of this connection, handy when you have more than one used interface (eg, nsq)"""

    username = ""
    """the username (if needed)"""

    password = ""
    """the password (if needed)"""

    hosts = None
    """list -- a list of (hostname, port) tuples"""

    interface_name = ""
    """string -- full Interface class name -- the interface that will connect to the messaging backend"""

    options = None
    """dict -- any other interface specific options you need"""

    @property
    def key(self):
        """string -- an encryption key loaded from options['keyfile'], or options['key'],
        it must be 32 bytes long so this makes sure it is"""
        if not hasattr(self, '_key'):
            key = ""
            keyfile = self.options.get('keyfile')
            if keyfile:
                with open(keyfile, 'r') as f:
                    key = f.read().strip()

            else:
                key = self.options.get('key', "")

            # key must be 32 characters long
            self._key = hashlib.sha256(key).digest() if key else ""

        return self._key

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
        self.options = kwargs.pop('options', {})
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

        InterfaceName://username:password@host:port?opt1=val1&opt2=val2#connection_name

    example -- connect to amazon SQS

        morp.interface.sqs.SQS://AWS_ID:AWS_KEY@
    """
    def __init__(self, dsn):

        d = {'options': {}, 'hosts': []}
        p = dsnparse.parse(dsn)

        # get the scheme, which is actually our interface_name
        d['interface_name'] = p.scheme

        dsn_hosts = []
        if p.hostname:
            d['hosts'].append((p.hostname, getattr(p, 'port', None)))

        if p.query:
            d['options'] = p.query

        if p.username:
            d['username'] = p.username

        if p.password:
            d['password'] = p.password

        if p.fragment:
            d['name'] = p.fragment

        super(DsnConnection, self).__init__(**d)

