import asyncio
import logging
import websockets
from websockets.client import WebSocketClientProtocol
import ssl
import json
import sys


async def subscribe(websocket: WebSocketClientProtocol) -> None:
    for symbol in symbols:
        msg = json.dumps(
            {
                "method": "SUBSCRIBE",
                "params": [
                    f"{symbol}@bookTicker",
                ],
                "id": 123
            }
        )
        await websocket.send(msg)

async def consumer_handler(websocket: WebSocketClientProtocol) -> None:
    async for message in websocket:
        log_message(message)

async def consume(uri: str):
    async with websockets.connect(
        uri=uri, 
        ssl=ssl.SSLContext(ssl.PROTOCOL_TLS)
        ) as websocket:
        await subscribe(websocket)
        await consumer_handler(websocket)

def log_message(message):
    message = json.loads(message)
    bid_price = None
    bid_qty = None
    ask_price = None
    ask_qty = None
    try:
        bid_price = message['b']
        bid_qty = message['B']
        ask_price = message['a']
        ask_qty = message['A']
    except:
        logging.debug(message)
    logging.info(f'ask: {ask_price} qty: {ask_qty} | bid: {bid_price} qty: {bid_qty}')


if __name__=='__main__':

    logging.basicConfig(
        stream=sys.stdout,
        format='%(asctime)s%(msecs)03d\t%(message)s',
        datefmt='%Y-%m-%dT%H:%M:%S.',
        level=logging.INFO
        )

    uri = 'wss://stream.binance.com:9443/ws/stream'
    symbols = ['btcusdt']

    loop = asyncio.get_event_loop()
    loop.run_until_complete(consume(uri=uri))
    loop.run_forever()