from morp.interface.base import InterfaceABC

from .. import TestCase, skipIf


class _InterfaceTest(TestCase):
    async def assertCount(
        self,
        expected: int,
        inter: InterfaceABC,
        name: str,
        attempts: int = 10
    ) -> None:
        for _ in range(attempts):
            count = await inter.count(name)
            if count == expected:
                break
        if count != expected:
            raise AssertError(f"{count} != {expected}")

    async def test_connection_open_close(self):
        inter = self.get_interface()
        await inter.connect()
        self.assertTrue(inter.connected)

        await inter.close()
        self.assertFalse(inter.connected)

    async def test_queue_auto_create(self):
        """queues should auto-create, this just makes sure that works as
        intended"""
        m = self.get_message()
        name = m.get_name()
        inter = m.interface

        await inter.unsafe_delete(name)

    async def test__fields_body_lifecycle(self):
        name = self.get_name()
        inter = self.get_encrypted_interface()
        fields1 = self.get_fields()

        body = inter._fields_to_body(fields1)
        fields2 = inter._body_to_fields(body)
        self.assertEqual(fields1, fields2)

    async def test__fields_body_encrypted_lifecycle(self):
        name = self.get_name()
        inter = self.get_encrypted_interface()
        fields1 = self.get_fields()

        body = inter._fields_to_body(fields1)
        fields2 = inter._body_to_fields(body)
        self.assertEqual(fields1, fields2)

    async def test_send_count_recv(self):
        msg = self.get_message()
        name = msg.get_name()
        inter = msg.interface

        await inter.send(name, msg.fields)

        self.assertEqual(1, await inter.count(name))

        fields = await inter.recv(name)
        self.assertEqualFields(msg.fields, fields)

        await inter.ack(name, fields)

        await self.assertCount(0, inter, name)
        #self.assertEventuallyEqual(0, lambda: await inter.count(name))

    async def test_recv_timeout(self):
        timeout = 1 # 1s as an int is minimum for SQS
        m = self.get_message()
        with self.assertWithin(1.5):
            await m.interface.recv(m.get_name(), timeout=timeout) 

    async def test_recv_atomic(self):
        name = self.get_name()
        inter1 = self.get_interface()
        inter2 = self.get_interface()

        await inter1.send(name, self.get_fields())

        m2 = await inter1.recv(name)
        m3 = await inter2.recv(name)
        self.assertIsNotNone(m2)
        self.assertIsNone(m3)

    async def test_send_recv_encrypted(self):
        m1 = self.get_message(interface=self.get_encrypted_interface())
        name = m1.get_name()
        await m1.interface.send(name, m1.fields)

        fields = await m1.interface.recv(name)
        self.assertEqualFields(m1.fields, fields)

        await m1.interface.ack(name, fields)

    async def test_ack_message(self):
        m1 = self.get_message()
        await m1.send()

        async with type(m1).recv() as m2:
            await m2.ack()

        inter = m2.interface
        name = m2.get_name()

        await self.assertCount(0, inter, name)
        #self.assertEventuallyEqual(0, lambda: await inter.count(name))

    async def test_release_interface(self):
        m = self.get_message()
        name = m.get_name()
        inter = m.interface
        await inter.send(name, m.fields)

        fields = await inter.recv(name)
        self.assertEqual(1, fields["_count"])
        await inter.release(name, fields)

        fields = await inter.recv(name)
        self.assertFalse(fields)
        await self.assertCount(1, inter, name)
        #self.assertEventuallyEqual(1, lambda: await inter.count(name))

    async def test_release_message(self):
        m = self.get_message()
        await m.send()

        async with type(m).recv() as m2:
            self.assertEqual(1, m2._count)
            await m2.release()

