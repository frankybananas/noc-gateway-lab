"""Demo FIX 4.4 initiator: logs on to the fix-engine and prints decoded messages.

Usage (on the VPS, inside the venv):
    python -m gateway.fix_client_demo [--seconds 30]

Used to validate the FIX session layer and for live demos: you can watch
Market Data Incremental Refresh (35=X) messages stream in human-readable form.
"""

import argparse
import asyncio
import time

import simplefix

from gateway import config

MSG_NAMES = {
    "A": "Logon", "0": "Heartbeat", "1": "TestRequest",
    "5": "Logout", "X": "MarketDataIncrementalRefresh",
}
ENTRY_TYPES = {"0": "BID", "1": "ASK", "2": "TRADE"}


def describe(msg: simplefix.FixMessage) -> str:
    msg_type = msg.get(35).decode()
    name = MSG_NAMES.get(msg_type, msg_type)
    if msg_type == "X":
        symbol = (msg.get(55) or b"?").decode()
        entry = ENTRY_TYPES.get((msg.get(269) or b"").decode(), "?")
        price = (msg.get(270) or b"?").decode()
        size = (msg.get(271) or b"?").decode()
        return f"{name} {symbol} {entry} px={price} sz={size}"
    return name


async def main(seconds: int) -> None:
    reader, writer = await asyncio.open_connection(config.BIND_HOST, config.FIX_TCP_PORT)
    print(f"connected to FIX acceptor at {config.BIND_HOST}:{config.FIX_TCP_PORT}")

    logon = simplefix.FixMessage()
    logon.append_pair(8, "FIX.4.4")
    logon.append_pair(35, "A")
    logon.append_pair(49, config.FIX_TARGET_COMP_ID)  # we are the client
    logon.append_pair(56, config.FIX_SENDER_COMP_ID)
    logon.append_pair(34, 1)
    logon.append_utc_timestamp(52)
    logon.append_pair(98, 0)
    logon.append_pair(108, config.FIX_HEARTBEAT_INTERVAL)
    writer.write(logon.encode())
    await writer.drain()
    print(">> sent Logon (35=A)")

    parser = simplefix.FixParser()
    deadline = time.monotonic() + seconds
    count = 0
    while time.monotonic() < deadline:
        try:
            data = await asyncio.wait_for(reader.read(4096), timeout=max(0.1, deadline - time.monotonic()))
        except asyncio.TimeoutError:
            break
        if not data:
            print("!! server closed the connection")
            break
        parser.append_buffer(data)
        while (msg := parser.get_message()) is not None:
            count += 1
            print(f"<< [{count}] {describe(msg)}")

    writer.close()
    await writer.wait_closed()
    print(f"done: received {count} messages in {seconds}s window")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=int, default=30, help="how long to listen")
    args = ap.parse_args()
    asyncio.run(main(args.seconds))
