# Morp

Send messages without really thinking about it. Currently works with Amazon's [SQS](http://aws.amazon.com/sqs/).

Morp was crafted with love for [First Opinion](http://firstopinionapp.com).


## 1 Minute Getting Started

Send and receive a `Foo` message.

First, let's set our environment variable:

    export MORP_DSN=morp.interface.sqs.SQS://AWS_ID:AWS_KEY@

Second, let's create a `Foo` class:

```python
import morp

class Foo(morp.Message):
    pass
```

Third, let's get `Foo` ready to receive messages:

```python
while True:
    with Foo.recv() as foo_msg:
        print foo_msg.fields
```

Fourth, let's send a message:

```python
f = Foo()
f.some_field = 1
f.some_other_field = 2
f.send()
```

And we're done, you can check out the actual code of the above example in the `/example` folder on [Github](https://github.com/firstopinion/morp/tree/master/example).


## DSN

You configure your connection using a dsn in the form:

    InterfaceName://username:password@?param1=value1&param2=value2

So, to connect to SQS, you would do:

    morp.interface.sqs.SQS://AWS_ID:AWS_KEY@

You can also override some default values like `region` and `read_lock`:

    morp.interface.sqs.SQS://AWS_ID:AWS_KEY@?region=us-west-1&read_lock=120


## Encryption

If you would like to encrypt all your messages, you can pass in a `keyfile` argument to your dsn that contains a path to a key file and Morp will take care of encrypting and decrypting the messages for you transparently.

Let's generate a key file:

    openssl rand -base64 256 > /tmp/keyfile.key

And modify our dsn:

    morp.interface.sqs.SQS://AWS_ID:AWS_KEY@?keyfile=/tmp/keyfile.key

That's it, every message will now be encrypted on send and decrypted on receive.


## Environment configuration

### MORP_QUEUE_OFF

By default every message will be sent, if you just want to test functionality without actually sending the message you can set this environment variable to turn off all the queues.

    MORP_QUEUE_OFF = 1 # queue is off
    MORP_QUEUE_OFF = 0 # queue is on

### MORP_QUEUE_PREFIX

By default, the queue name is just the class name, but if you would like to have that prefixed with something (eg, `prod` or `dev`) then you can set this environment variable and it will be prefixed to the queue name.

### MORP_DSN

If you set the environment variable `MORP_DSN` with your connection dsn, morp will automatically configure itself on first import.


## Installation

Use pip:

    pip install morp


## License

MIT

