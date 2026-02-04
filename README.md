# Morp

Simple message processing without really thinking about it. Morp can use dropfiles (simple text files), Postgres, and [Amazon SQS](http://aws.amazon.com/sqs/).


## Installation

Use pip to install the latest stable version:

    pip install morp
    
Morp only supports the dropfiles interface out of the box, you'll need to install certain dependencies depending on what interface you want to use:

    pip install morp[sqs]
    pip install morp[postgres]
    
To install the development version:

    pip install -U "git+https://github.com/Jaymon/morp#egg=morp"


## 1 Minute Getting Started

Send and receive a `Foo` message.

First, let's set our environment variable to use dropfiles (local files suitable for development and prototyping) interface:

    export MORP_DSN=dropfile:${TMPDIR}

Then, let's create three files in our working directory:

* `tasks.py` - We'll define our `Message` classes here.
* `send.py` - We'll send messages from this script.
* `recv.py` - We'll receive messages from this script.


Let's create our `Message` class in `tasks.py`:

```python
# tasks.py
from morp import Message

class Foo(Message):
    some_field: int
    some_other_field: str
    
    def handle(self):
        # this will be run when a Foo message is consumed
        print(self.fields)
```

Now, let's flesh out our `recv.py` file:

```python
# recv.py

import asyncio

# import our Foo message class from our tasks.py file
from tasks import Foo

# Foo's `process` method will call `Foo.handle` for each Foo instance received
asyncio.run(Foo.process())
```

And start it up:

```
$ python recv.py
```


Finally, let's send some messages by fleshing out `send.py`:

```python
# send.py

import asyncio

from tasks import Foo

async def send_messages():
    # create a message and send it manually
    f = Foo()
    f.some_field = 1
    f.some_other_field = "one"
    f.ignored_field = True
    await f.send()

    # quickly send a message
    await Foo.create(
        some_field=2,
        some_other_field="two",
    )
    
asyncio.run(send_messages())
```

And running it in a separate shell from the shell running our `recv.py` script (it should send two messages):

```
$ python send.py
```

That's it! Our running `recv.py` script should've received the messages we sent when we ran our `send.py` script.


## DSN

You configure your connection using a dsn in the form:

    InterfaceName://username:password@host:port/path?param1=value1&param2=value2

So, to connect to [Amazon SQS](http://aws.amazon.com/sqs/), you would do:

    sqs://${AWS_ACCESS_KEY_ID}:${AWS_SECRET_ACCESS_KEY}@

You can also override some default values like `region` and `read_lock`:

    sqs://${AWS_ACCESS_KEY_ID}:${AWS_SECRET_ACCESS_KEY}@?region=${AWS_DEFAULT_REGION}&read_lock=120


### Serializers

* `pickle` (default)
* `json`

```
MORP_DSN="sqs://x:x@?serializer=json"
```


## Encryption

You might need to install some dependencies:

```
pip install morp[encryption]
```

If you would like to encrypt all your messages, you can pass in a `key` argument to your DSN and Morp will take care of encrypting and decrypting the messages for you transparently.

Let's just modify our DSN to pass in our key:

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

Set this environment variable with your connection DSN so Morp can automatically configure itself when the interface is first requested.


## FAQ

### I would like to have multiple queues

By default, Morp will send any message from any `morp.Message` derived class to `Message.get_name()`, you can override this behavior by giving your child class a `._name` property:

```python
from morp import Message

class childMsg(Message):
    _name = "custom-queue-name"
```
