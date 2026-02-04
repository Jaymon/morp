# -*- coding: utf-8 -*-
import os
from multiprocessing import cpu_count, Process
from collections import Counter
import asyncio

from morp.compat import *
from morp.interface.postgres import Postgres
from morp.message import Message
from . import TestCase, skipIf


counts = Counter()

def send(count, message_class):
    async def send():
        pid = os.getpid()
        for x in range(count):
            print(f"{x}/{count}. {pid} is sending {x}")
            m = message_class(x=x)
            await m.send()

        await message_class.interface.close()

    asyncio.run(send())


def recv(count, message_class):
    async def recv():
        await message_class.process(count)
        await message_class.interface.close()

    asyncio.run(recv())


class StressTask(Message):
    x: int

    def handle(self):
        pid = os.getpid()
        print(f"{pid} is receiving {self.x}")
        counts[pid] += 1


@skipIf(
    "MORP_DSN" not in os.environ,
    "Skipping stress testing because no MORP_DSN defined"
)
class StressTest(TestCase):
    def test_multi(self):
        """
        https://docs.python.org/3/library/multiprocessing.html
        """
        processes = []
        count = cpu_count()
        send_count = 2
        recv_count = max(2, count - 2)
        recv_message_count = 100
        send_message_count = (recv_message_count * recv_count) // send_count

#         pout.v(count, send_count, recv_count, recv_message_count, send_message_count)

        for c in range(recv_count):
            p = Process(target=recv, args=(recv_message_count, StressTask))
            p.start()
            processes.append(p)

        for c in range(send_count):
            p = Process(target=send, args=(send_message_count, StressTask))
            p.start()
            processes.append(p)

        for p in processes:
            p.join()

