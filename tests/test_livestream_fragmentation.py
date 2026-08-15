"""Regression tests for fragmented IMMI livestream reads."""

import asyncio
from unittest import IsolatedAsyncioTestCase, mock

from blinkpy.livestream import BlinkLiveStream


class TestBlinkLiveStreamFragmentation(IsolatedAsyncioTestCase):
    """Verify IMMI packets survive normal TCP fragmentation."""

    async def test_recv_waits_for_fragmented_header_and_payload(self):
        """Read a complete packet even when header and payload arrive in chunks."""
        camera = mock.Mock()
        camera.serial = "PONLMKJIHGFED"
        response = {
            "command_id": 987654321,
            "polling_interval": 15,
            "server": (
                "immis://1.2.3.4:443/ABCDEFGHIJKML__IMDS_1234567812345678"
                "?client_id=123456"
            ),
        }
        livestream = BlinkLiveStream(camera, response)

        reader = asyncio.StreamReader()
        target_writer = mock.Mock()
        client = mock.Mock()
        client.is_closing.return_value = False
        client.write = mock.Mock()
        client.drain = mock.AsyncMock()

        livestream.target_reader = reader
        livestream.target_writer = target_writer
        livestream.clients = [client]

        header = bytes(
            [
                0x00,  # video packet
                0x00,
                0x00,
                0x00,
                0x01,  # sequence
                0x00,
                0x00,
                0x00,
                0xBC,  # 188-byte MPEG-TS payload
            ]
        )
        payload = bytes([0x47] + [0x00] * 187)

        recv_task = asyncio.create_task(livestream.recv())

        # Deliver less than the 9-byte header first. StreamReader.read(9)
        # may return these 4 bytes immediately, which used to abort the stream.
        reader.feed_data(header[:4])
        await asyncio.sleep(0)
        self.assertFalse(recv_task.done())

        reader.feed_data(header[4:])
        await asyncio.sleep(0)
        self.assertFalse(recv_task.done())

        # Fragment the declared 188-byte payload as well.
        reader.feed_data(payload[:31])
        await asyncio.sleep(0)
        self.assertFalse(recv_task.done())

        reader.feed_data(payload[31:])
        reader.feed_eof()

        await recv_task

        client.write.assert_called_once_with(payload)
        client.drain.assert_awaited_once()
        target_writer.close.assert_called_once()
