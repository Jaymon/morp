# Morp

Send messages without really thinking about it.


## Installation

Use pip to install the latest stable version:

    pip install morp
    
To install the development version:

    pip install -U "git+https://github.com/Jaymon/morp#egg=morp"


## 1 Minute Getting Started

Send and receive a `Foo` message.

First, let's set our environment variable to use the dropfile (local files suitable for development and prototyping) interface:

    export MORP_DSN=dropfile:///${TMPDIR}

Second, let's create a `Foo` Message class:

```python
from morp import Message

class Foo(Message):
    def target(self):
        # this will be run when a Foo message is consumed
        print(self.fields)
```

Third, let's start our message consumer in a shell:

```
$ morp
```

Fourth, let's send a message:

```python
f = Foo()
f.some_field = 1
f.some_other_field = 2
f.send()
```

That's it!


## DSN

You configure your connection using a dsn in the form:

    InterfaceName://username:password@host:port/path?param1=value1&param2=value2

So, to connect to [Amazon SQS](http://aws.amazon.com/sqs/), you would do:

    sqs://${AWS_ACCESS_KEY_ID}:${AWS_SECRET_ACCESS_KEY}@

You can also override some default values like `region` and `read_lock`:

    sqs://${AWS_ACCESS_KEY_ID}:${AWS_SECRET_ACCESS_KEY}@?region=${AWS_DEFAULT_REGION}&read_lock=120


## Encryption

If you would like to encrypt all your messages, you can pass in a `key` argument to your dsn and Morp will take care of encrypting and decrypting the messages for you transparently.

Let's just modify our dsn to pass in our key:

    sqs://${AWS_ACCESS_KEY_ID}:${AWS_SECRET_ACCESS_KEY}@?key=jy4XWRuEsrH98RD2VeLG62uVLCPWpdUh

That's it, every message will now be encrypted on send and decrypted on receive. If you're using SQS you can also use [Amazon's key management service](https://github.com/Jaymon/morp/blob/master/docs/KMS.md) to handle the encryption for you.


## Environment configuration

### MORP_DISABLED

By default every message will be sent, if you just want to test functionality without actually sending the message you can set this environment variable to turn off all the queues.

    MORP_DISABLED = 1 # queue is off
    MORP_DISABLED = 0 # queue is on


### MORP_PREFIX

If you would like to have your queue names prefixed with something (eg, `prod` or `dev`) then you can set this environment variable and it will be prefixed to the queue name.


### MORP_DSN

Set this environment variable with your connection dsn so morp can automatically configure itself when the interface is first requested.


## FAQ

### I would like to have multiple queues

By default, Morp will send any message from any `morp.Message` derived class to `Message.get_name()`, you can override this behavior by giving your child class a `.name` attribute:

```python
from morp import Message

class childMsg(Message):
    name = "custom-queue-name"
```

Now, you can have the Morp command line consumer read from that queue instead:

```
$ morp custom-queue-name
```