"""Run inside the API container against the running Compose stack."""
import asyncio
import json
import os
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from uuid import uuid4

import aio_pika
import httpx
from sqlalchemy import text

from app.db import engine


class PaymentsTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.calls = []
        self.failures = 0
        case = self

        class Receiver(BaseHTTPRequestHandler):
            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                case.calls.append((time.monotonic(), self.headers["Idempotency-Key"], body))
                self.send_response(503 if len(case.calls) <= case.failures else 204)
                self.end_headers()

            def log_message(self, *args):
                pass

        receiver = ThreadingHTTPServer(("0.0.0.0", 0), Receiver)
        threading.Thread(target=receiver.serve_forever, daemon=True).start()
        self.addCleanup(receiver.server_close)
        self.addAsyncCleanup(asyncio.to_thread, receiver.shutdown)
        self.addAsyncCleanup(engine.dispose)
        self.api = httpx.AsyncClient(
            base_url="http://127.0.0.1:8000", headers={"X-API-Key": os.environ["API_KEY"]}, timeout=15
        )
        self.addAsyncCleanup(self.api.aclose)
        self.url = "/api/v1/payments"
        self.body = {
            "amount": "42.10", "currency": "RUB", "description": "integration test",
            "metadata": {"order": 123}, "webhook_url": f"http://api:{receiver.server_port}/webhook",
        }
        self.key = {"Idempotency-Key": str(uuid4())}

    async def test_request_errors(self):
        response = await self.api.get(f"{self.url}/{uuid4()}", headers={"X-API-Key": "wrong"})
        self.assertEqual(response.status_code, 401)
        self.assertEqual((await self.api.get(f"{self.url}/{uuid4()}")).status_code, 404)
        self.assertEqual((await self.api.post(self.url, json=self.body)).status_code, 422)
        for invalid in ({"amount": "0"}, {"amount": "1.001"}, {"currency": "GBP"}, {"webhook_url": "ftp://host"}):
            with self.subTest(invalid=invalid):
                response = await self.api.post(self.url, headers=self.key, json=self.body | invalid)
                self.assertEqual(response.status_code, 422)

    async def test_concurrent_idempotency_and_retry(self):
        self.failures = 1
        replies = await asyncio.gather(*(
            self.api.post(self.url, headers=self.key, json=self.body) for _ in range(8)
        ))
        for reply in replies:
            self.assertEqual(reply.status_code, 202, reply.text)
        payment_id, = {r.json()["payment_id"] for r in replies}
        self.assertEqual(len({r.json()["created_at"] for r in replies}), 1)
        conflict = await self.api.post(self.url, headers=self.key, json=self.body | {"amount": "1.01"})
        self.assertEqual(conflict.status_code, 409)
        async with asyncio.timeout(40):
            while True:
                async with engine.connect() as db:
                    delivered = await db.scalar(text(
                        "SELECT webhook_delivered_at FROM payments WHERE id = :id"
                    ), {"id": payment_id})
                if delivered:
                    break
                await asyncio.sleep(0.2)
        result = (await self.api.get(f"{self.url}/{payment_id}")).json()
        self.assertIn(result["status"], {"succeeded", "failed"})
        self.assertEqual(result["metadata"], self.body["metadata"])
        self.assertEqual(result["amount"], "42.10")
        self.assertEqual(len(self.calls), 2)
        self.assertGreaterEqual(self.calls[1][0] - self.calls[0][0], 0.9)
        self.assertEqual(self.calls[0][1:], self.calls[1][1:])
        self.assertEqual(self.calls[1][1], payment_id)
        self.assertEqual(self.calls[1][2]["status"], result["status"])
        async with engine.connect() as db:
            self.assertEqual(await db.scalar(text(
                "SELECT count(*) FROM outbox WHERE payment_id = :id AND published_at IS NOT NULL"
            ), {"id": payment_id}), 1)

    async def test_three_failures_dead_letter(self):
        self.failures = 3
        connection = await aio_pika.connect(os.getenv("AMQP_URL", "amqp://payments:payments@rabbitmq/"))
        self.addAsyncCleanup(connection.close)
        channel = await connection.channel()
        # Observe the same dead-letter routing key without consuming the actual DLQ.
        exchange = await channel.get_exchange("payments")
        dead = await channel.get_queue("payments.dlq")
        observer = await channel.declare_queue(exclusive=True)
        await observer.bind(exchange, routing_key=dead.name)
        response = await self.api.post(self.url, headers=self.key, json=self.body)
        self.assertEqual(response.status_code, 202, response.text)
        payment_id = response.json()["payment_id"]
        async with asyncio.timeout(45), observer.iterator() as messages:
            async for message in messages:
                async with message.process():
                    if json.loads(message.body)["payment_id"] == payment_id:
                        self.assertEqual(message.headers["attempt"], 3)
                        self.assertEqual(message.headers["x-death"][0]["reason"], "rejected")
                        break
        self.assertEqual(len(self.calls), 3)
        self.assertGreaterEqual(self.calls[2][0] - self.calls[1][0], 1.9)
        self.assertTrue(all(call[1:] == self.calls[0][1:] for call in self.calls))
        detail = (await self.api.get(f"{self.url}/{payment_id}")).json()
        self.assertEqual(detail["status"], self.calls[0][2]["status"])
