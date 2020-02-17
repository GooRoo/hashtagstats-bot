from sqlalchemy import create_engine, select, text
from sqlalchemy import \
    BigInteger,        \
    Boolean,           \
    Column,            \
    DateTime,          \
    ForeignKey,        \
    Integer,           \
    MetaData,          \
    String,            \
    Table,             \
    UniqueConstraint
from sqlalchemy.dialects import postgresql


class DB(object):
    meta = MetaData()

    users = Table(
        'users', meta,
        Column('id', Integer, primary_key=True),
        Column('first_name', String, nullable=False),
        Column('last_name', String),
        Column('username', String),
        Column('is_bot', Boolean, server_default='false')
    )

    chat_type = postgresql.ENUM(
        'private',
        'group',
        'supergroup',
        'channel',
        name='chat_type', metadata=meta
    )

    chats = Table(
        'chats', meta,
        Column('id', BigInteger, primary_key=True),
        Column('type', chat_type, nullable=False)
    )

    text_type = String(4096)
    messages = Table(
        'messages', meta,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('message_id', Integer, nullable=False),
        Column('from', ForeignKey(users.c.id), nullable=False),
        Column('date', DateTime, nullable=False),
        Column('chat', ForeignKey(chats.c.id), nullable=False),
        Column('urls', postgresql.ARRAY(text_type)),
        Column('text', text_type),
        UniqueConstraint('message_id', 'chat')
    )

    hashtag_type = String(255)

    hashtags = Table(
        'hashtags', meta,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('message', ForeignKey(messages.c.id), nullable=False),
        Column('linked_message', ForeignKey(messages.c.id)),
        Column('hashtag', hashtag_type, nullable=False),
        UniqueConstraint('message', 'hashtag')
    )

    users2hashtags = Table(
        'users2hashtags', meta,
        Column('chat', ForeignKey(chats.c.id), primary_key=True),
        Column('user', ForeignKey(users.c.id), primary_key=True),
        Column('hashtag', hashtag_type, primary_key=True)
    )

    def __init__(self, user='', password='', db='', host='localhost', port=5432, *, full_uri='', echo=False):
        if full_uri:
            self.engine = create_engine(full_uri, echo=echo)
        else:
            self.engine = create_engine(
                f'postgresql://{user}:{password}@{host}:{port}/{db}',
                echo=echo
            )

    def create_all(self):
        self.meta.create_all(self.engine)

    def make_user(self, id, first_name, last_name=None, username=None, is_bot=False):
        return {
            'id': id,
            'first_name': first_name,
            'last_name': last_name,
            'username': username,
            'is_bot': is_bot
        }

    def _insert_user(self, upsert=False):
        ins = postgresql.insert(self.users)
        if upsert:
            new_user = self.make_user(
                ins.excluded.id,
                ins.excluded.first_name,
                ins.excluded.last_name,
                ins.excluded.username,
                ins.excluded.is_bot,
            )
            del new_user['id']
            del new_user['is_bot']
            return ins.on_conflict_do_update(
                constraint=self.users.primary_key,
                set_=new_user
            )
        else:
            return ins.on_conflict_do_nothing()

    def add_user(self, id, first_name, last_name=None, username=None, is_bot=False, *, overwrite=False):
        return self.engine.execute(self._insert_user(overwrite), **self.make_user(
            id,
            first_name,
            last_name,
            username,
            is_bot
        ))

    def add_users(self, users, *, overwrite=False):
        if len(users) == 0:
            return None

        return self.engine.execute(self._insert_user(overwrite), users)

    def find_user(self, username):
        return self.engine.execute(
            select([self.users.c.id]).where(self.users.c.username == username)
        )

    def make_chat(self, id, type_):
        return {
            'id': id,
            'type': type_
        }

    def _insert_chat(self, upsert=False):
        ins = postgresql.insert(self.chats)
        if upsert:
            return ins.on_conflict_do_update(
                constraint=self.chats.primary_key,
                set_={'type': ins.excluded.type}
            )
        else:
            return ins.on_conflict_do_nothing()

    def add_chat(self, id, type_, *, overwrite=False):
        return self.engine.execute(self._insert_chat(overwrite), **self.make_chat(
            id, type_
        ))

    def add_chats(self, chats, *, overwrite=False):
        if len(chats) == 0:
            return None

        return self.engine.execute(self._insert_chat(overwrite), chats)

    def make_message(self, message_id, from_, date, chat, urls=[], text=''):
        return {
            'message_id': message_id,
            'from': from_,
            'date': date,
            'chat': chat,
            'urls': urls,
            'text': text
        }

    def _insert_message(self, upsert=False):
        ins = postgresql.insert(self.messages)
        if upsert:
            new_message = self.make_message(
                ins.excluded.message_id,
                ins.excluded['from'],
                ins.excluded.date,
                ins.excluded.chat,
                ins.excluded.urls,
                ins.excluded.text
            )
            del new_message['message_id']
            del new_message['from']
            del new_message['chat']
            return ins.on_conflict_do_update(
                index_elements=[self.messages.c.message_id, self.messages.c.chat],
                set_=new_message
            )
        else:
            return ins.on_conflict_do_nothing()

    def add_message(self, message_id, from_, date, chat, urls=[], text='', *, overwrite=False):
        ins = self._insert_message(overwrite).returning(self.messages.c.id)
        return self.engine.execute(ins, **self.make_message(
            message_id,
            from_,
            date,
            chat,
            urls,
            text
        ))

    def add_messages(self, messages, *, overwrite=False):
        if len(messages) == 0:
            return None

        return self.engine.execute(self._insert_message(overwrite), messages)

    def find_message(self, id):
        return self.engine.execute(
            select([self.messages.c.id]).where(self.messages.c.message_id == id)
        )

    def make_hashtag(self, message, hashtag, linked_message=None):
        return {
            'message': message,
            'hashtag': hashtag,
            'linked_message': linked_message
        }

    def _insert_hashtag(self, upsert=False):
        ins = postgresql.insert(self.hashtags)
        if upsert:
            return ins.on_conflict_do_update(
                constraint=self.hashtags.primary_key,
                set_={'hashtag': ins.excluded.hashtag}
            ).on_conflict_do_nothing(
                index_elements=[self.hashtags.c.message, self.hashtags.c.hashtag]
            )
        else:
            return ins.on_conflict_do_nothing()

    def add_hashtag(self, message, hashtag, linked_message=None, *, overwrite=False):
        return self.engine.execute(self._insert_hashtag(overwrite), **self.make_hashtag(
            message,
            hashtag,
            linked_message
        ))

    def add_hashtags(self, hashtags, *, overwrite=False):
        if len(hashtags) == 0:
            return None

        return self.engine.execute(self._insert_hashtag(overwrite), hashtags)

    def links_by_tag(self, hashtag, chat_id):
        return self.engine.execute(text('''
            SELECT h.hashtag, sum(array_length(m.urls, 1)) as links
            FROM hashtags h
                INNER JOIN messages m ON h.message = m.id OR h.linked_message = m.id
                INNER JOIN chats c on m.chat = c.id
            WHERE h.hashtag = :tag
            AND c.id = :chat_id
            GROUP BY h.hashtag
            ORDER BY links DESC
        '''), tag=hashtag, chat_id=chat_id)

    def author_of_tag(self, hashtag, chat_id):
        return self.engine.execute(text('''
            SELECT h.hashtag, u.id, u.first_name, u.last_name, u.username, m.text, m.date
            FROM hashtags h
                INNER JOIN messages m ON h.message = m.id
                INNER JOIN users u on m."from" = u.id
                INNER JOIN (
                    SELECT h.hashtag, min(m.date) as first_date
                    FROM hashtags h
                        INNER JOIN messages m ON h.message = m.id
                        INNER JOIN chats c on m.chat = c.id
                    WHERE h.hashtag = :tag
                      AND c.id = :chat_id
                    GROUP BY h.hashtag
                ) hh ON h.hashtag = hh.hashtag AND m.date = hh.first_date
        '''), tag=hashtag, chat_id=chat_id)

    def contributor_of_tag(self, hashtag, chat_id):
        return self.engine.execute(text('''
            SELECT h.hashtag, u.id, u.first_name, u.last_name, u.username, count(h.message) as count
            FROM hashtags h
                INNER JOIN messages m ON h.message = m.id
                INNER JOIN users u ON m."from" = u.id
                INNER JOIN chats c on m.chat = c.id
            WHERE h.hashtag = :tag
            AND c.id = :chat_id
            GROUP BY h.hashtag, u.id, u.first_name, u.last_name, u.username
            ORDER BY count DESC
        '''), tag=hashtag, chat_id=chat_id)

    def tags_by_author(self, user_id, chat_id):
        return self.engine.execute(text('''
            SELECT u.id, u.first_name, u.last_name, u.username, count(h.hashtag) AS count, array_agg(h.hashtag) AS tags
            FROM hashtags h
                INNER JOIN messages m ON h.message = m.id
                INNER JOIN users u ON m."from" = u.id
                INNER JOIN (
                    SELECT h.hashtag, min(m.date) as first_date
                    FROM hashtags h
                        INNER JOIN messages m ON h.message = m.id
                        INNER JOIN chats c on m.chat = c.id
                    WHERE c.id = :chat_id
                    GROUP BY h.hashtag
                ) hh ON h.hashtag = hh.hashtag AND m.date = hh.first_date
            WHERE u.id = :user_id
            GROUP BY u.id, u.first_name, u.last_name, u.username
            ORDER BY count DESC
        '''), user_id=user_id, chat_id=chat_id)

    def links_by_author(self, user_id, chat_id):
        return self.engine.execute(text('''
            SELECT u.id, u.first_name, u.last_name, u.username, sum(array_length(m.urls, 1))
            FROM users u
                INNER JOIN messages m on u.id = m."from"
                INNER JOIN chats c on m.chat = c.id
            WHERE u.id = :user_id
                AND c.id = :chat_id
            GROUP BY u.id, u.first_name, u.last_name, u.username
        '''), user_id=user_id, chat_id=chat_id)

    def tagged_foreign_by_author(self, user_id, chat_id):
        return self.engine.execute(text('''
            SELECT h.hashtag, m.id AS tagged_message, u.id AS tagger, m2.id AS message_with_link, u2.id AS reply_to
            FROM hashtags h
                INNER JOIN messages m on h.message = m.id
                INNER JOIN chats c on m.chat = c.id
                INNER JOIN users u on m."from" = u.id
                INNER JOIN messages m2 ON h.linked_message = m2.id
                INNER JOIN users u2 ON m2."from" = u2.id
            WHERE u.id <> u2.id
            AND u.id = :user_id
            AND c.id = :chat_id
        '''), user_id=user_id, chat_id=chat_id)

    def all_tags(self, chat_id):
        return self.engine.execute(text('''
            SELECT DISTINCT h.hashtag
            FROM hashtags h
                INNER JOIN messages m ON m.id = h.message
                INNER JOIN chats c ON c.id = m.chat
                LEFT JOIN users2hashtags u2h on h.hashtag = u2h.hashtag
            WHERE u2h.hashtag IS NULL
            AND c.id = :chat_id
            ORDER BY h.hashtag
        '''), chat_id=chat_id)

    def top_tags(self, chat_id, limit=10):
        return self.engine.execute(text('''
            SELECT h.hashtag, sum(array_length(m.urls, 1)) as links
            FROM hashtags h
                INNER JOIN messages m ON h.message = m.id OR h.linked_message = m.id
                INNER JOIN chats c ON m.chat = c.id
                LEFT JOIN users2hashtags u2h ON h.hashtag = u2h.hashtag
            WHERE u2h IS NULL
              AND c.id = :chat_id
            GROUP BY h.hashtag
            ORDER BY links DESC, hashtag ASC
            LIMIT :limit
        '''), chat_id=chat_id, limit=limit)

    def top_contributors(self, chat_id, limit=5):
        return self.engine.execute(text('''
            SELECT u.id, u.first_name, u.last_name, u.username, sum(array_length(m.urls, 1)) AS sum
            FROM users u
                INNER JOIN messages m on u.id = m."from"
                INNER JOIN chats c on m.chat = c.id
            WHERE c.id = :chat_id
            GROUP BY u.id, u.first_name, u.last_name, u.username
            ORDER BY sum DESC
            LIMIT :limit
        '''), chat_id=chat_id, limit=limit)

    def bottom_contributers(self, chat_id, limit=5):
        return self.engine.execute(text('''
            SELECT u.id, u.first_name, u.last_name, u.username, coalesce(sum(array_length(m.urls, 1)), 0) AS sum
            FROM users u
                LEFT JOIN messages m ON u.id = m."from"
                INNER JOIN chats c ON m.chat = c.id
            WHERE c.id = :chat_id
            GROUP BY u.id, u.first_name, u.last_name, u.username
            ORDER BY sum ASC
            LIMIT :limit
        '''), chat_id=chat_id, limit=limit)

    def top_music_services(self, chat_id):
        return self.engine.execute(text(r'''
            WITH all_urls AS (
                SELECT unnest(urls) AS link
                FROM messages m
                    INNER JOIN chats c on m.chat = c.id
                WHERE c.id = :chat_id
            ), categorized_urls AS (
                SELECT lower(coalesce (
                    substring (m.link FROM '^https://open.(spotify)\.com.+$'),
                    replace (substring (m.link FROM '^https://.*?(youtu.?be).+$'), '.', ''),
                    substring (m.link FROM '^https://.*?(deezer)\.com.+$'),
                    substring (m.link FROM '^https://(itunes)\.apple\.com.+$'),
                    substring (m.link FROM '^https://play\.(google)\.com.+$'),
                    substring (m.link FROM '^https://(soundcloud)\.com.+$')
                    )) AS category, m.link
                FROM all_urls m
            )
            SELECT category, coalesce(count(link), 0) as count
            FROM categorized_urls
            GROUP BY category
            ORDER BY count DESC
        '''), chat_id=chat_id)
