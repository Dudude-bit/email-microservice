import asyncio
import os

import nats
import ujson
from nats.aio.msg import Msg
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

from exceptions import NotImproperlyConfigure


async def main():
    nats_dsn = os.getenv('nats_host')

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
            to_emails=[data['to']],
            subject=data['email-subject'],
            html_content=data['payload']
        )

        try:
            response = sendgrid_client.send(body)
        except Exception:
            await msg.nak()
        else:
            await msg.ack()

    sub = await nc.subscribe('send-email-topic', cb=message_handler)


if __name__ == '__main__':
    asyncio.run(main())
