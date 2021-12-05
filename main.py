import asyncio
import os

import nats
import ujson
from nats.aio.msg import Msg
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

from exceptions import NotImproperlyConfigure


async def main():
    nats_dsn = os.getenv('nats_dsn')

    sendgrid_api_key = os.getenv('sendgrid_api_key')

    if nats_dsn is None:
        raise NotImproperlyConfigure('define nats dsn')

    if sendgrid_api_key is None:
        raise NotImproperlyConfigure('define sendgrid api key')

    nc = await nats.connect(nats_dsn)

    sendgrid_client = SendGridAPIClient(sendgrid_api_key)

    async def message_handler(msg: Msg):

        subject = msg.subject
        reply = msg.reply
        data = ujson.loads(msg.data)

        body = Mail(
            from_email=data['from-email'],
            to_emails=[data['to-email']],
            subject=data['email-subject'],
            html_content=data['email-payload']
        )

        try:
            response = sendgrid_client.send(body)
        except Exception:
            pass

    sub = await nc.subscribe('send-email-topic')

    async for message in sub.messages:
        await message_handler(message)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.gather(main()))
