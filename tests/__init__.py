# -*- coding: utf-8 -*-
import os
import inspect
import subprocess

import testdata
from datatypes import NamingConvention

from morp.compat import *
from morp.interface.dropfile import Dropfile
from morp.interface import find_environ
from morp.message import Message
from morp.config import DsnConnection


testdata.basic_logging(
    levels={
        "boto3": "WARNING",
        "botocore": "WARNING",
        "datatypes": "WARNING",
    }
)


class Client(object):
    def __init__(self, data, config):
        module_info = testdata.create_module(data=data)
        self.directory = module_info.basedir
        self.module = module_info.module()
        self.config = config
        inter = self.config.interface
        self.message_classes = []

        clear_names = {}
        for _, message_class in inspect.getmembers(self.module, inspect.isclass):
            if issubclass(message_class, Message):
                message_class.interface = inter
                clear_names[message_class.get_name()] = message_class
                self.message_classes.append(message_class)

        for message_class in clear_names.values():
            message_class.unsafe_clear()

    def send(self, **fields):
        return self.message_classes[0].create(fields)

    def recv(self):
        return self.run(self.message_classes[0].name)

    def environ(self):
        environ = os.environ
        environ.update({
            "MORP_DSN": self.config.dsn,
        })
        environ.pop("MORP_DSN_1", None)
        return environ

    def run(self, name, count=1, **options):
        python_cmd = String(subprocess.check_output(["which", "python"]).strip())
        cmd = "{} -m morp --count={} --directory={} {}".format(
            python_cmd,
            count,
            self.directory,
            name
        )
        expected_ret_code = options.get('code', 0)

        def get_output_str(output):
            return "\n".join(String(o) for o in output)

        process = None
        output = []
        try:
            process = subprocess.Popen(
                cmd,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                cwd=os.getcwd(),
                env=self.environ(),
            )

            for line in iter(process.stdout.readline, b""):
                line = line.rstrip()
                print(line)
                output.append(line)

            process.wait()
            if process.returncode != expected_ret_code:
                raise RuntimeError("cmd returned {} with output: {}".format(
                    process.returncode,
                    get_output_str(output)
                ))

        except subprocess.CalledProcessError as e:
            raise RuntimeError("cmd returned {} with output: {}".format(e.returncode, e.output))

        finally:
            if process:
                process.stdout.close()

        return get_output_str(output)


class TestCase(testdata.TestCase):

    interface_class = Dropfile

    queues = []

    interfaces = []

    DSN_ENV_NAME = "MORP_TEST_DSN"

    @classmethod
    def tearDownClass(cls):
        # clean up all the queues we made and close all the interfaces
        if cls.interfaces:
            inter = cls.interfaces[0]
            for name in cls.queues:
                inter.unsafe_delete(name)

            for inter in cls.interfaces:
                inter.close()

    def get_client(self, data, **kwargs):
        return Client(
            data,
            config=kwargs.get("config", self.get_config()),
        )

    def get_config(self, dsn="", config=None, **options):
        if dsn:
            config = DsnConnection(dsn)

        else:
            if not config:
                for c in find_environ(self.DSN_ENV_NAME):
                    if issubclass(self.interface_class, c.interface_class):
                        config = c
                        break

        if config:
            options.setdefault("backoff_multiplier", 1)
            options.setdefault("backoff_amplifier", 1)

            for k, v in options.items():
                config.options[k] = v

        else:
            raise ValueError(f"Could not find a MORP_TEST_DSN for {self.interface_class}")

        return config

    def get_interface(self, config=None, interface=None, **options):
        """get a connected interface"""
        config = self.get_config(config=config, **options)
        inter = interface or self.interface_class(config)
        inter.connect()
        #type(self).interfaces[""].append(inter)
        type(self).interfaces.append(inter)
        self.assertTrue(inter.connected)
        return inter

    def get_encrypted_interface(self, config=None, interface=None, **options):
        """get a connected interface"""
        options.setdefault('key', testdata.get_ascii(testdata.get_int(10, 200)))
        return self.get_interface(config=config, interface=interface, **options)

    def get_name(self, name=""):
        if not name:
            name = f"morp-test-{testdata.get_ascii(12)}"
        type(self).queues.append(name)
        return name

    def get_message_class(self, name=None, interface=None, config=None):
        name = self.get_name(name)
        inter = self.get_interface(config=config, interface=interface)

        return type(
            NamingConvention(name).camelcase(),
            (Message,),
            dict(name=name, interface=inter, connection_name=inter.connection_config.name),
        )

        return orm_class

    def get_message(self, name=None, interface=None, config=None, **fields):
        fields = self.get_fields(**fields)

        return self.get_message_class(name=name, interface=interface, config=config)(**fields)

    def get_fields(self, **fields):
        if not fields:
            fields.update({
                "foo": testdata.get_int(),
                "bar": testdata.get_words(),
            })
        return fields

