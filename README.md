# Morp

Send messages without really thinking about it. Currently works with Amazon's [SQS](http://aws.amazon.com/sqs/).

## DSN

You configure your connection using a dsn in the form:

    InterfaceName://username:password@?param1=value1&param2=value2

So, to connect to SQS, you would do:

    morp.interface.sqs.SQS://AWS_ID:AWS_KEY@

You can also override some default values like `region` and `read_lock`:

    morp.interface.sqs.SQS://AWS_ID:AWS_KEY@?region=us-west-1&read_lock=120

Finally, if you would like to encrypt all your messages, you can pass in `keyfile` with a path to a key file and Morp will take care of encrypting and decrypting the messages for you transparently to you:

    morp.interface.sqs.SQS://AWS_ID:AWS_KEY@?keyfile=/some/path/keyfile.key

If you set the environment variable `MORP_DSN` with your connection dsn then morp will automatically configure itself on first import.


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

And we're done, you can check out the actual example in the `/example` folder on Github to see similar code to the above in action.


## Installation

Use pip:

    pip install morp

## License

MIT

