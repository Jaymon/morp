# -*- coding: utf-8 -*-
import os
import inspect
import subprocess

import testdata
from testdata import skipIf
from datatypes import NamingConvention, ReflectType

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


class TestCase(testdata.IsolatedAsyncioTestCase):

    interface_class = Dropfile

    queues = []

    interfaces = []

    DSN_ENV_NAME = "MORP_TEST_DSN"

    async def asyncSetUp(self):
        await self.asyncTearDown()

    async def asyncTearDown(self):
        """clean up all the queues we made and close all the interfaces"""
        if self.interfaces:
            inter = self.interfaces[0]
            for name in self.queues:
                await inter.unsafe_delete(name)

            for inter in self.interfaces:
                await inter.close()

        type(self).queues = []
        type(self).interfaces = []

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
            raise ValueError(
                f"Could not find a MORP_TEST_DSN for {self.interface_class}"
            )

        return config

    def get_interface(self, config=None, interface=None, **options):
        """get a connected interface"""
        config = self.get_config(config=config, **options)
        inter = interface or self.interface_class(config)
        #await inter.connect()
        #type(self).interfaces[""].append(inter)
        type(self).interfaces.append(inter)
        #self.assertTrue(inter.connected)
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

    def get_message_class(
        self,
        name=None,
        interface=None,
        config=None,
        target=None,
        schema=None,
        message_class=None
    ):
        name = self.get_name(name)
        inter = self.get_interface(config=config, interface=interface)

        message_class = message_class or Message
        properties = dict(
            _name=name,
            interface=inter,
            handle=target or message_class.handle,
            connection_name=inter.connection_config.name,
        )

        if schema:
            properties["schema"] = schema

        child_class = type(
            NamingConvention(name).camelcase(),
            (message_class,),
            properties,
        )

        return child_class

    def get_message(
        self,
        name=None,
        interface=None,
        config=None,
        target=None,
        message_class=None,
        **fields
    ):
        fields = self.get_fields(**fields)
        schema = self.get_schema(fields)
        return self.get_message_class(
            name=name,
            interface=interface,
            config=config,
            target=target,
            schema=schema,
            message_class=message_class
        )(**fields)

    def get_fields(self, **fields):
        if not fields:
            fields.update({
                "foo": testdata.get_int(),
                "bar": testdata.get_words(),
            })
        return fields

    def get_schema(self, fields):
        schema = {}
        for field_name, field_value in fields.items():
            schema[field_name] = ReflectType(type(field_value))

        return schema

    def assertEqualFields(self, fields1, fields2, **kwargs):
        return self.assertEqual(
            {k:v for k, v in fields1.items() if not k.startswith("_")},
            {k:v for k, v in fields2.items() if not k.startswith("_")}
        )

