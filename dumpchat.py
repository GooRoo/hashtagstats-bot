import datetime
import os

from telethon import TelegramClient
from telethon.tl.types import MessageEntityUrl, MessageEntityTextUrl, MessageEntityHashtag

import db

api_id = 123456
api_hash = 'deadbeef01010101010100101010101'

client = TelegramClient('dumpchat_hashtags', api_id, api_hash)


def get_entity_text(m, e):
    if isinstance(e, MessageEntityTextUrl):
        return e.url
    else:
        return m.message[e.offset : e.offset + e.length]


def get_urls(message):
    try:
        return [
            get_entity_text(message, e)
                for e in message.entities
                if isinstance(e, (MessageEntityUrl, MessageEntityTextUrl))
        ]
    except (AttributeError, TypeError):
        return []


def get_hashtags(message):
    try:
        return [
            get_entity_text(message, e)
                for e in message.entities
                if isinstance(e, MessageEntityHashtag)
        ]
    except (AttributeError, TypeError):
        return []


async def unroll_message(message):
    if not hasattr(message, 'is_reply') or not message.is_reply:
        return None

    original = await message.get_reply_message()

    if len(get_urls(original)) > 0:
        return original
    else:
        return await unroll_message(original)


async def dump_chat():
    music_vibes = 12345678

    d = db.DB(
        user='postgres',
        password='password',
        db='postgres'
    )
    d.create_all()

    print('Processing "Music Vibes"...')

    d.add_chat(music_vibes, 'group')
    print(f'Registered the chat with ID={music_vibes}')

    r = d.add_users([
        d.make_user(u.id, u.first_name, u.last_name, u.username, u.bot)
        async for u in client.iter_participants(music_vibes)
    ])
    print(f'Added {r.rowcount} new users')

    i = -1
    dummy_messages = []
    async for u in client.iter_participants(music_vibes):
        dummy_messages.append(
            d.make_message(
                i,
                u.id,
                datetime.datetime(2018, 1, 1, 0, 0, 0),
                music_vibes
            )
        )
        i -= 1
    d.add_messages(dummy_messages)

    # async for message in client.iter_messages("me", limit=1):
    async for message in client.iter_messages(music_vibes, reverse=True):
        urls = get_urls(message)
        hashtags = get_hashtags(message)
        linked_message = None

        # if the message contains neither links nor tags,
        # skip to the next one
        if len(urls) == 0 and len(hashtags) == 0:
            continue

        # if the current message contains tags but not links,
        # we have to find another one if it's a reply or
        # skip to the next one otherwise
        if len(urls) == 0 and len(hashtags) != 0:
            linked_message = await unroll_message(message)
            if linked_message is None:
                continue


        if hasattr(message, 'edit_date') and message.edit_date is not None:
            date = message.edit_date
        else:
            date = message.date

        inserted_message = d.add_message(
            message_id=message.id,
            from_=message.from_id,
            date=date,
            chat=music_vibes,
            urls=urls,
            text=message.message
        )
        # if the message is already there, skip to next one
        if inserted_message.rowcount == 0:
            continue

        # skip hashtags for forwarded messages
        if message.forward is not None:
            continue

        m_id = inserted_message.first()[0]

        if linked_message is not None:
            res = d.find_message(linked_message.id).first()
            l_id = res[0] if res is not None else None
        else:
            l_id = None

        hs = [
            d.make_hashtag(
                message=m_id,
                hashtag=hashtag,
                linked_message=l_id
            ) for hashtag in get_hashtags(message)
        ]

        d.add_hashtags(hs)


with client:
    client.loop.run_until_complete(dump_chat())
