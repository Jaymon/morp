#from unittest import TestCase
import logging
import sys
import threading
import time
from Queue import Queue

import nsq
from pyt import TestCase, Assert

import morp
from morp import Message, Connection
from morp.interface.nsq import Nsq
from morp import interface
import morp.nsq

# configure root logger
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
log_handler = logging.StreamHandler(stream=sys.stderr)
log_formatter = logging.Formatter('[%(levelname)s] %(message)s')
log_handler.setFormatter(log_formatter)
logger.addHandler(log_handler)

def consume_messages(message_names=None):
    """starts consuming messages in another thread"""
    def run():
        i = get_interface()
        i.consume(message_names)

    thread = threading.Thread(target=run)
    thread.daemon = True
    thread.start()

def get_interface(connection_name=""):
    """get a connected interface"""
    i = None
    try:
        i = morp.get_interface(connection_name)

    except KeyError:
        c = Connection(hosts=[(Nsq.default_host, Nsq.default_port)])
        c = Connection(hosts=[("127.0.0.1", 4161)])
        i = Nsq(c)
        i.connect()
        a = Assert(i)
        a.connected == True
        a.connection != None
        morp.set_interface(connection_name, i)

    return i

class TestMsg(Message):
    """the test message class that is useful for making sure messages get passed back and forth"""

    callback = None

    def handle(self):
        self.callback(self)

class ConnectionTest(TestCase):

    def test_dsn_connection(self):

        tests = [
            (
                'path.to.Interface://127.0.0.1:4151',
                dict(
                    hosts=[('127.0.0.1', 4151)],
                    interface_name="path.to.Interface",
                    name=''
                )
            ),
            (
                'module.path.to.Interface://example.com:4161#name',
                dict(
                    hosts=[('example.com', 4161)],
                    interface_name='module.path.to.Interface',
                    name="name"
                )
            ),
            (
                'module.path.to.Interface://example.com:4161?foo=bar&bar=che#name',
                dict(
                    hosts=[('example.com', 4161)],
                    interface_name='module.path.to.Interface',
                    options={"foo": "bar", "bar": "che"},
                    name="name"
                )
            ),
            (
                'module.Classname://host1.com:1000+host2.com:2000+host3.com:3000',
                dict(
                    hosts=[('host1.com', 1000), ('host2.com', 2000), ('host3.com', 3000)],
                    interface_name='module.Classname',
                )
            ),
            (
                'module.Classname://host1.com:1000+host2.com:2000#foo',
                dict(
                    hosts=[('host1.com', 1000), ('host2.com', 2000)],
                    interface_name='module.Classname',
                    name="foo"
                )
            ),
            (
                'module.Classname://host1.com:1000+host2.com:2000?foo=bar',
                dict(
                    hosts=[('host1.com', 1000), ('host2.com', 2000)],
                    interface_name='module.Classname',
                    options={"foo": "bar"},
                )
            ),
            (
                'module.Classname://host1.com:1000+host2.com:2000?foo=bar&bar=che#name',
                dict(
                    hosts=[('host1.com', 1000), ('host2.com', 2000)],
                    interface_name='module.Classname',
                    options={"foo": "bar", "bar": "che"},
                    name="name"
                )
            ),
        ]

        for t in tests:
            c = morp.DsnConnection(t[0])
            a = Assert(c)
            a ** t[1]


class NsqTest(TestCase):

    def test_connect_and_close(self):
        #c = morp.Connection(hosts=[("localhost", 4150)])
        n = get_interface()

        n.close()
        self.assertFalse(n.connected)
        self.assertIsNone(n.connection)

    def test_denormalize_message(self):

        i = get_interface()
        m = Message("foo")
        m.foo = 1
        m.bar = 2
        m.che = "str"
        m_str = i.normalize_message(m)

        rm = i.denormalize_message(m_str)
        a = Assert(rm)
        a % Message
        a.foo == m.foo
        a.bar == m.bar
        a.che == m.che

    def test_consume_success(self):

        q = Queue()
        def handle_queue(self, *args):
            q.put(self)


        i = get_interface()

        message_name = "test_consume_success_{}".format(int(time.time()))
        TestMsg.callback = handle_queue
        m = TestMsg(message_name)
        m.foo = 1
        m.send()

        consume_messages([message_name])
        check_count = 0
        while check_count < 5 and q.empty():
            time.sleep(1)
            check_count += 1

        if q.qsize() > 0:
            a = Assert(q.get())
            a.name == m.name
            a.foo == m.foo


class MessageTest(TestCase):

    def test_general(self):
        m = Message("foo")
        a = Assert(m)

        m.bar = 1
        m.che = "this is a string"

        a.name == "foo"
        a.bar == 1
        a['bar'] == 1
        a.che == "this is a string"
        a['che'] == "this is a string"

        a.msg ** dict(bar=1, che="this is a string")

        with Assert(KeyError):
            a['msg']

        a = Assert(Message())
        a.name == "Message"
        a.class_name == "morp.Message"

        class TestGeneralMessage(Message): pass
        a = Assert(TestGeneralMessage())
        a.name == "TestGeneralMessage"
        a.class_name == "morp_test.TestGeneralMessage"

    def test_send(self):

        i = get_interface()

        m = TestMsg()
        m.foo = 1
        m.bar = 2
        m.send()


class InterfaceTest(TestCase):

    def test_get_class(self):
        m = TestMsg()
        c = interface.get_class(m.class_name)
        Assert(m) % c


