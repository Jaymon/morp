# Morp

Send [Amazon SQS](http://aws.amazon.com/sqs/) messages without really thinking about it.


## 1 Minute Getting Started

Send and receive a `Foo` message.

First, let's set our environment variable:

    export MORP_DSN=morp.interface.sqs.SQS://AWS_ID:AWS_KEY@

Second, let's create a `Foo` class:

```python
import morp

class Foo(morp.Message):
    def target(self):
        # this will be run when a message is consumed
        print(self.fields)
```

Third, let's start our message consumer

```
$ morp --quiet
```

Fourth, let's send a message:

```python
f = Foo()
f.some_field = 1
f.some_other_field = 2
f.send()
```

And we're done, take a look in the [example folder](https://github.com/firstopinion/morp/tree/master/example) for the actual code.


## DSN

You configure your connection using a dsn in the form:

    InterfaceName://username:password@?param1=value1&param2=value2

So, to connect to SQS, you would do:

    morp.interface.sqs.SQS://AWS_ID:AWS_KEY@

You can also override some default values like `region` and `read_lock`:

    morp.interface.sqs.SQS://AWS_ID:AWS_KEY@?region=us-west-1&read_lock=120


## Encryption

If you would like to encrypt all your messages, you can pass in a `key` argument to your dsn that either contains a key or a path to a key file and Morp will take care of encrypting and decrypting the messages for you transparently.

Let's just modify our dsn to pass in our key:

    morp.interface.sqs.SQS://AWS_ID:AWS_KEY@?key=jy4XWRuEsrH98RD2VeLG62uVLCPWpdUh

That's it, every message will now be encrypted on send and decrypted on receive.


### Amazon KMS

What if you want to use [Amazon's Key Management Service](https://aws.amazon.com/kms/)? Then just set the `KmsMasterKeyId` value on your dsn:

    morp.interface.sqs.SQS://AWS_ID:AWS_KEY@?KmsMasterKeyId=alias/aws/sqs
    
You can also use [your own key](https://docs.aws.amazon.com/kms/latest/developerguide/create-keys.html) by using the key ID, which would be a value like: `XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXXX` as the value for `KmsMasterKeyId`


## Environment configuration

### MORP_DISABLED

By default every message will be sent, if you just want to test functionality without actually sending the message you can set this environment variable to turn off all the queues.

    MORP_DISABLED = 1 # queue is off
    MORP_DISABLED = 0 # queue is on

### MORP_PREFIX

By default, the queue name is just the class name, but if you would like to have that prefixed with something (eg, `prod` or `dev`) then you can set this environment variable and it will be prefixed to the queue name.

### MORP_DSN

If you set the environment variable `MORP_DSN` with your connection dsn, morp will automatically configure itself on first import.


## FAQ

### I would like to have multiple queues

By default, Morp will send any message from any `morp.Message` derived class to `Message.get_name()`, you can override this behavior by giving your child class a `.name` attribute:

```python
class childMsg(morp.Message):
    name = "custom-queue-name"
```

Now, you can have the Morp command line consumer read from that queue instead:

```
$ morp --quiet="" custom-queue-name
```


## Installation

Use pip to install the latest stable version:

    pip install morp
    
To install the development version:

    pip install -U "git+https://github.com/Jaymon/morp#egg=morp"

