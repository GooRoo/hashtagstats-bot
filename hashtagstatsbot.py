import logging
import os
import telegram

from datetime import timedelta
from delorean import Delorean
from telegram import MessageEntity, ReplyKeyboardMarkup, ReplyKeyboardRemove, ParseMode
from telegram.ext import CommandHandler, Filters, Job, MessageHandler, Updater
from telegram.ext.jobqueue import Days

import db

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.DEBUG
    # level=logging.INFO
)

logger = logging.Logger(__name__)

d = db.DB(full_uri=os.environ['DATABASE_URL'])


def error(update, context):
    """Log Errors caused by Updates."""
    logger.warning('Update "%s" caused error "%s"', update, context.error)


def on_help(update, context):
    update.message.reply_markdown(
        'Привет! Я *Hashtag Stats Bot*.\n\n'
        'Я сохраняю статистику по тегам в групповых чатах. '
        'Просто добавьте меня в один из оных.\n\n'
        '_Список команд:_\n'
        '/stats — Различного рода глобальная статистика\n'
        '/tag `#hashtag` — Статистика по конкретному тегу\n'
        '/user `@mention` — Статистика по конкретному пользователю\n'
        '/help — Показывает это сообщение\n'
    )
    # context.bot.send_message(chat_id=update.effective_chat.id, text="I'm a bot, please talk to me!")


def get_entity_text(m, e):
    if e.type == MessageEntity.TEXT_LINK:
        return e.url
    else:
        text = m.text or m.caption
        return text[e.offset : e.offset + e.length]


def get_urls(message):
    return [
        get_entity_text(message, e)
            for e in message.entities or message.caption_entities
            if e.type == MessageEntity.URL
            or e.type == MessageEntity.TEXT_LINK
    ]


def get_hashtags(message):
    return [
        get_entity_text(message, e)
            for e in message.entities or message.caption_entities
            if e.type == MessageEntity.HASHTAG
    ]


def on_new_message(update: telegram.Update, context):
    is_edit = update.edited_message is not None
    m = update.edited_message if is_edit else update.message
    u = m.from_user
    c = m.chat

    d.add_user(
        id=u.id,
        first_name=u.first_name,
        last_name=u.last_name,
        username=u.username,
        is_bot=u.is_bot,
        overwrite=True
    )

    d.add_chat(
        id=c.id,
        type_=c.type
    )

    urls = get_urls(m)
    hashtags = get_hashtags(m)

    # neither urls nor tags? Weird... Should never happen
    if len(urls) == 0 and len(hashtags) == 0:
        logger.error("Something went wrong: no tags and no urls")
        return

    # message with tags only?
    l_id = None
    if len(urls) == 0 and len(hashtags) > 0:
        # let's check if there were some links in the message
        # which someone has just replied
        if m.reply_to_message is not None:
            other_urls = get_urls(m.reply_to_message)
            if len(other_urls) > 0:
                # okay, we've found something
                # let's check if we have that message in the database
                res = d.find_message(m.reply_to_message.message_id).first()
                l_id = res[0] if res is not None else None
            else:
                # seems like just a message with list of tags.
                # just skipping this one
                return
        else:
            # seems like just a message with list of tags.
            # just skipping this one
            return

    inserted_message = d.add_message(
        message_id=m.message_id,
        from_=u.id,
        date=m.date,
        chat=c.id,
        urls=urls,
        text=m.text or m.caption,
        overwrite=is_edit
    )

    if inserted_message.rowcount == 0:
        return

    m_id = inserted_message.first()[0]

    hs = [
        d.make_hashtag(
            message=m_id,
            hashtag=hashtag,
            linked_message=l_id
        ) for hashtag in hashtags
    ]

    d.add_hashtags(hs, overwrite=is_edit)


def mention_user(id, first_name, last_name=None, username=None):
    if username:
        return f'[@{username}](tg://user?id={id})'
    else:
        full_name = first_name + (f' {last_name}' if last_name else '')
        return f'[{full_name}](tg://user?id={id})'


def nice_date(d):
    return d.strftime('%d.%m.%Y')


def tr(word, n):
    words = {
        'сообщение': ['сообщения', 'сообщений'],
        'раз': ['раза', 'раз'],
        'тега': ['тегов', 'тегов'],
        'ссылки': ['ссылок', 'ссылок'],
        'штука': ['штуки', 'штук']
    }

    if 5 <= (n % 100) <= 20:
        return words[word][1]
    elif n % 10 == 1:
        return word
    elif 2 <= (n % 10) <= 4:
        return words[word][0]
    else:
        return words[word][1]


def on_tag_stats(update, context):
    try:
        chat_id = update.effective_chat.id
        hashtag = context.args[0]

        if not hashtag.startswith('#'):
            raise ValueError()

        reply = ''

        count = d.links_by_tag(hashtag, chat_id).fetchone()
        if count is not None:
            reply += f'Хэштег {count["hashtag"]} использовался *{count["links"]} {tr("раз", count["links"])}*.'
        else:
            reply += f'Хэштег {hashtag} в этом чате пока не использовался.'

        author = d.author_of_tag(hashtag, chat_id).fetchone()
        contrib = d.contributor_of_tag(hashtag, chat_id).fetchone()

        if author is not None:
            reply += f''' Впервые был введён {
                mention_user(
                    author["id"],
                    author["first_name"],
                    author["last_name"],
                    author["username"]
                )
            } в сообщении от *{nice_date(author["date"])}*'''

        if author is not None and contrib is not None:
            if contrib["count"] == 1:
                reply += f', которое остаётся единственным и по сей день.'
            elif author["id"] != contrib["id"]:
                reply += f''', но самым активным контрибьютером на данный момент является {
                    mention_user(
                        contrib["id"],
                        contrib["first_name"],
                        contrib["last_name"],
                        contrib["username"]
                    )
                }, прислав *{contrib["count"]} {tr("сообщение", contrib["count"])}*.'''
            else:
                reply += f'''. На счету автора уже *{contrib["count"]} {
                    tr("сообщение", contrib["count"])
                }* под этим тегом, что является абсолютным большинством. Так держать!'''

        update.message.reply_markdown(reply)

    except (IndexError, ValueError):
        update.message.reply_text('Использование: /tag #hashtag')


def escape_markdown_tag(tag):
    return tag.replace('_', '\\_')


def escape_markdown_tags(tags):
    return [escape_markdown_tag(t) for t in tags]


def on_user_stats(update, context):
    def get_user_id(m, e):
        if e.type == MessageEntity.TEXT_MENTION:
            return e.user.id
        else:
            message = m.text or m.caption
            username = message[e.offset : e.offset + e.length]
            if username.startswith('@'):
                username = username[1:]
            u = d.find_user(username).fetchone()
            return u['id'] if u is not None else None

    try:
        chat_id = update.effective_chat.id
        user_id = [
            get_user_id(update.message, e)
            for e in (update.message.entities or update.message.caption_entities)
            if e.type == MessageEntity.MENTION or e.type == MessageEntity.TEXT_MENTION
        ][0]

        if user_id is None:
            raise ValueError()

        reply = ''

        tags = d.tags_by_author(user_id, chat_id).fetchone()
        if tags is not None:
            reply += f'''{
                mention_user(
                    tags["id"],
                    tags["first_name"],
                    tags["last_name"],
                    tags["username"]
                )
            } является автором *{tags["count"]} {tr("тега", tags["count"])}* в этом чате.'''
        else:
            reply += f'''{context.args[0]} — практически тёмная лошадка.'''

        links = d.links_by_author(user_id, chat_id).fetchone()
        if links is not None:
            if links["sum"] == 0 or links["sum"] is None:
                reply += ' При этом умудряется сохранять молчание в плане ссылок (их — *ноль*).'
                reply += ' Ни на что намекать мы, конечно, не будем.'
            else:
                reply += f' Также является отправителем *{links["sum"]} {tr("ссылки", links["sum"])}*.'

        tagged = d.tagged_foreign_by_author(user_id, chat_id).fetchall()
        if tagged is not None and len(tagged) > 0:
            if links is not None and links["sum"] == 0:
                reply += ' Зато '
            else:
                reply += ' Даже более того, ещё и '
            reply += f'''находит время, чтобы тегать чужие ссылки: и таких уже аж *{len(tagged)} {
                tr("штука", len(tagged))
            }*.'''

        if tags is not None and len(tags["tags"]) > 0:
            reply += f'\n\nАвтор тегов: {" ".join(sorted(escape_markdown_tags(tags["tags"])))}'

        update.message.reply_markdown(reply)
    except (IndexError, ValueError):
        update.message.reply_markdown('Использование: /user @mention')


def on_stats(update, context):
    update.message.reply_markdown('Что вы хотите увидеть?', reply_markup=ReplyKeyboardMarkup([
        ['ТОП-10 тегов', 'Все теги'],
        ['ТОП-5 контрибьютеров', 'БОТТОМ-5 контрибьютеров'],
        ['ТОП музыкальных сервисов']
    ], one_time_keyboard=True))


def leaderboard(iterable):
    return zip(
        ['1️⃣', '2️⃣', '3️⃣', '4️⃣', '5️⃣', '6️⃣', '7️⃣', '8️⃣', '9️⃣', '🔟'],
        iterable
    )


def weekly_contributors(chat_id):
    def format_date(date):
        return date.strftime('%d.%m.%Y')

    now = Delorean()
    step_from = 1 if now.date.isoweekday() == 1 else 2
    from_ = now.last_monday(step_from).date
    to = now.last_sunday().date
    contribs = d.top_contributors_by_date(chat_id, from_=from_, to=to).fetchall()
    if contribs is not None and len(contribs) > 0:
        reply = f'*Результаты недели* ({format_date(from_)}–{format_date(to)}):\n\n'

        cs = [
            f'{n} {mention_user(c["id"], c["first_name"], c["last_name"], c["username"])} ({c["sum"]})'
            for n, c in leaderboard(contribs)
        ]
        reply += '\n'.join(cs)
        reply += f'\n\n{escape_markdown_tag("#weekly_stats")}'
    else:
        reply = f'''Никто не прислал ничего полезного за целую неделю ({format_date(from_)}–{format_date(to)}). Стыдно должно быть, товарищи!

{escape_markdown_tag("#weekly_stats")}'''

    return reply


def on_weekly_stats(context):
    chat_id = context.job.context
    reply = weekly_contributors(chat_id)
    context.bot.send_message(chat_id, reply, parse_mode=ParseMode.MARKDOWN, disable_notification=True)


def enable_weekly_stats(update, context):
    chat_id = update.effective_chat.id

    if 'weekly_stats' in context.chat_data:
        old_job = context.chat_data['weekly_stats']
        old_job.schedule_removal()

    new_job = context.job_queue.run_repeating(
        on_weekly_stats,
        interval=timedelta(weeks=1),
        first=Delorean(timezone='Europe/Berlin').next_monday().midnight + timedelta(hours=8),
        context=chat_id,
        name='weekly_stats'
    )
    context.chat_data['weekly_stats'] = new_job

    context.bot.send_message(chat_id, 'Еженедельные дайджесты включены.')


def disable_weekly_stats(update, context):
    if 'weekly_stats' in context.chat_data:
        old_job = context.chat_data['weekly_stats']
        old_job.schedule_removal()


def on_detailed_stats(update, context):
    def nice_category(category):
        names = {
            'spotify': 'Spotify',
            'youtube': 'YouTube',
            'deezer': 'Deezer',
            'google': 'Google Play Music',
            'itunes': 'Apple Music',
            'soundcloud': 'SoundCloud'
        }
        return names.get(category, category)

    chat_id = update.effective_chat.id
    m = update.message

    if m.text == 'ТОП-10 тегов':
        tags = d.top_tags(chat_id).fetchall()
        if tags is not None:
            reply = '*ТОП тегов* (по количеству ссылок):\n\n'

            hs = [
                f'{n} {escape_markdown_tag(t["hashtag"])} ({t["links"]})'
                for n, t in leaderboard(tags)
            ]

            reply += '\n'.join(hs)
        else:
            reply = 'Похоже, в этом чате пока нет полезных тегов (или я о них не знаю).'

    elif m.text == 'Все теги':
        tags = d.all_tags(chat_id).fetchall()
        if tags is not None:
            hs = escape_markdown_tags([t['hashtag'] for t in tags])
            reply = ' '.join(hs)
        else:
            reply = 'Похоже, в этом чате пока нет полезных тегов (или я о них не знаю).'

    elif m.text == 'ТОП-5 контрибьютеров':
        contribs = d.top_contributors(chat_id).fetchall()
        if contribs is not None:
            reply = '*ТОП контрибьютеров* (по количеству ссылок):\n\n'

            cs = [
                f'{n} {mention_user(c["id"], c["first_name"], c["last_name"], c["username"])} ({c["sum"]})'
                for n, c in leaderboard(contribs)
            ]
            reply += '\n'.join(cs)
        else:
            reply = 'Увы, я пока не в курсе ни о каких ссылках в этом чате.'

    elif m.text == 'БОТТОМ-5 контрибьютеров':
        contribs = d.bottom_contributers(chat_id).fetchall()
        if contribs is not None:
            reply = '*БОТТОМ контрибьютеров* (по количеству ссылок):\n\n'

            cs = [
                f'{n} {mention_user(c["id"], c["first_name"], c["last_name"], c["username"])} ({c["sum"]})'
                for n, c in leaderboard(contribs)
            ]
            reply += '\n'.join(cs)
        else:
            reply = 'Похоже, в списке должен был оказаться весь чат (или же никто не кидал ссылок с тех пор, как меня сюда добавили).'

    elif m.text == 'ТОП музыкальных сервисов':

        music_services = d.top_music_services(chat_id).fetchall()
        if music_services is not None:
            reply = '*ТОП музыкальных сервисов* (по количеству ссылок):\n\n'

            ms = [
                f'{n} {nice_category(m["category"])} ({m["count"]})'
                for n, m in leaderboard(music_services)
                if m["category"] is not None
            ]
            reply += '\n'.join(ms)

            others = [m for m in music_services if m["category"] is None]
            if len(others) != 0:
                reply += f'\n\nСсылки на всё прочее: {others[0]["count"]} {tr("штука", others[0]["count"])}'
        else:
            reply = 'Увы, я пока не в курсе ни о каких ссылках в этом чате.'

    m.reply_markdown(reply, reply_markup=ReplyKeyboardRemove())




def main(webhook=False):
    d.create_all()

    TOKEN = os.environ['TG_TOKEN']
    updater = Updater(token=TOKEN, use_context=True)

    dispatcher = updater.dispatcher
    job_queue = updater.job_queue


    start_handler = CommandHandler('start', on_help)
    help_handler = CommandHandler('help', on_help)
    dispatcher.add_handler(start_handler)
    dispatcher.add_handler(help_handler)

    tag_stats_handler = CommandHandler('tag', on_tag_stats)
    dispatcher.add_handler(tag_stats_handler)

    user_stats_handler = CommandHandler('user', on_user_stats)
    dispatcher.add_handler(user_stats_handler)

    enable_weekly_handler = CommandHandler('disable_weekly', disable_weekly_stats)
    dispatcher.add_handler(enable_weekly_handler)

    disable_weekly_handler = CommandHandler('weekly', enable_weekly_stats)
    dispatcher.add_handler(disable_weekly_handler)

    stats_handler = CommandHandler('stats', on_stats)
    dispatcher.add_handler(stats_handler)

    stats_details_handler = MessageHandler(
        Filters.regex('^(ТОП-10 тегов|Все теги|ТОП-5 контрибьютеров|БОТТОМ-5 контрибьютеров|ТОП музыкальных сервисов)$'),
        on_detailed_stats
    )
    dispatcher.add_handler(stats_details_handler)

    new_msg_handler = MessageHandler(
        Filters.entity(MessageEntity.HASHTAG) |
            Filters.entity(MessageEntity.URL) |
            Filters.entity(MessageEntity.TEXT_LINK),
        on_new_message
    )
    dispatcher.add_handler(new_msg_handler)

    dispatcher.add_error_handler(error)

    new_job = Job(
        on_weekly_stats,
        interval=timedelta(weeks=1),
        repeat=True,
        context=int(os.environ['TG_INIT_CHAT_ID']),
        name='weekly_stats',
        days=(Days.MON,),
        job_queue=job_queue
    )
    job_queue._put(
        new_job,
        Delorean(timezone='Europe/Berlin').next_monday().midnight + timedelta(hours=8)
    )

    if webhook:
        logger.info('Creating a webhook...')
        updater.start_webhook(
            listen='0.0.0.0',
            port=int(os.environ.get('PORT', '8443')),
            url_path=TOKEN
        )
        updater.bot.set_webhook(f'''{os.environ['URL']}/{TOKEN}''')
    else:
        logger.info('Starting polling...')
        updater.start_polling()

    updater.idle()


if __name__ == "__main__":
    main(webhook=True)
