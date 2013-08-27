# Morp

Send messages without really thinking about it. Currently works with [NSQ](https://github.com/bitly/nsq).

## DSN

You configure your connection using a dsn in the form:

    InterfaceName://lookupdhost1:port1+lookupdhost2:port2?opt1=val1&opt2=val2#connection_name

So, to connect to nsq using one localhost lookupd instance:

    morp.interface.nsq.Nsq://127.0.0.1:4161

## Example

Send and consume `Foo` messages.

We are going to create a series of files to run this example:

    rootdir/
      setup.py
      send.py
      consume.py

### setup.py

    # connect to our nsq backend
    import morp
    morp.configure("morp.interface.nsq.Nsq://127.0.0.1:4161")

### send.py

Let's create a `Foo` class to send our `Foo` messages:

    import time
    import morp
    import setup.py # configure nsq

    class Foo(morp.Message):

        def handle(self):
            """this method gets called each time a Foo message is received"""
            print self.bar
            return True


    # start sendind some messages:
    for x in xrange(100):
        f = Foo()
        f.bar = x
        f.send()
        time.sleep(1)

In one terminal, we can start `send.py` to begin sending messages:

    $ python send.py

### consume.py

Now, let's consume our `Foo` messages that `send.py` is sending

    import morp
    import setup.py # configure nsq

    morp.consume("Foo")

In another terminal, we can start consuming our sent `Foo` messages:

    $ python consume.py

That's it, we our now sending and receiving messages.

## Installation

Use pip:

    pip install morp

## License

MIT


