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
        name = self.get_name()
        inter = self.get_interface()

        # clear the queue, this shouldn't throw an error even if the queue
        # doesn't exist
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
        name = self.get_name()
        fields = self.get_fields()
        inter = self.get_interface()

        await inter.send(name, fields)

        self.assertEqual(1, await inter.count(name))

        rf = await inter.recv(name)
        self.assertEqualFields(fields, rf)

        await inter.ack(name, rf)
        await self.assertCount(0, inter, name)

    async def test_recv_timeout(self):
        timeout = 1 # 1s as an int is minimum for SQS
        name = self.get_name()
        inter = self.get_interface()

        with self.assertWithin(1.5):
            await inter.recv(name, timeout=timeout) 

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
        inter = self.get_encrypted_interface()
        name = self.get_name()
        fields = self.get_fields()

        await inter.send(name, fields)

        rf = await inter.recv(name)
        self.assertEqualFields(fields, rf)
        await inter.ack(name, rf)

    async def test_ack_message(self):
        name = self.get_name()
        fields = self.get_fields()
        inter = self.get_interface()

        await inter.send(name, fields)
        rf = await inter.recv(name)
        await inter.ack(name, rf)

        await self.assertCount(0, inter, name)

    async def test_release_interface(self):
        name = self.get_name()
        fields = self.get_fields()
        inter = self.get_interface()

        await inter.send(name, fields)

        fields = await inter.recv(name)
        self.assertEqual(1, fields["_count"])
        await inter.release(name, fields)

        fields = await inter.recv(name)
        self.assertFalse(fields)
        await self.assertCount(1, inter, name)

    async def test_release_message(self):
        name = self.get_name()
        fields = self.get_fields()
        inter = self.get_interface()

        await inter.send(name, fields)
        rf = await inter.recv(name)
        self.assertEqual(1, rf["_count"])
        await inter.release(name, rf)

