"""
Telegram бот — парсер комментаторов каналов
Оптимизирован по памяти: ленивые импорты, автоочистка, gc после парсинга

Запуск: python tg_bot.py
"""

import asyncio
import gc
import os
import logging
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, Document, FSInputFile
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.filters import CommandStart, Command

from telethon import TelegramClient
from telethon.errors import (
    FloodWaitError, ChannelPrivateError,
    ChatAdminRequiredError, SessionPasswordNeededError,
    AuthKeyUnregisteredError
)

# ==========================================
# НАСТРОЙКИ
# ==========================================

BOT_TOKEN = "8542734719:AAEEyZVGcHTBj1va5v2cY5QIKsXIJEp11rM"

API_ID = 37443553
API_HASH = "a9a89f77413936f88b395a27ff956102"

POSTS_LIMIT = 200

SESSIONS_DIR = "sessions"
OUTPUT_DIR = "outputs"
os.makedirs(SESSIONS_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Через сколько минут неактивности отключать клиент (0 = не отключать)
IDLE_DISCONNECT_MINUTES = 30

# ==========================================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# Хранилище клиентов: user_id -> {"client": ..., "last_used": datetime}
clients: dict = {}


class ParseState(StatesGroup):
    waiting_phone = State()
    waiting_code = State()
    waiting_2fa = State()
    waiting_channels_file = State()
    parsing = State()


# ==========================================
# УПРАВЛЕНИЕ КЛИЕНТАМИ
# ==========================================

async def get_client(user_id: int) -> TelegramClient | None:
    """Получить клиент и обновить время последнего использования."""
    if user_id in clients:
        clients[user_id]["last_used"] = datetime.now()
        return clients[user_id]["client"]
    return None


async def set_client(user_id: int, client: TelegramClient):
    """Сохранить клиент."""
    clients[user_id] = {
        "client": client,
        "last_used": datetime.now()
    }


async def drop_client(user_id: int):
    """Отключить и удалить клиент из памяти."""
    if user_id not in clients:
        return
    try:
        await clients[user_id]["client"].disconnect()
    except Exception:
        pass
    del clients[user_id]
    gc.collect()


async def cleanup_idle_clients():
    """Фоновая задача: отключать клиентов которые давно не используются."""
    while True:
        await asyncio.sleep(300)  # проверяем каждые 5 минут
        if IDLE_DISCONNECT_MINUTES <= 0:
            continue
        cutoff = datetime.now() - timedelta(minutes=IDLE_DISCONNECT_MINUTES)
        idle_users = [
            uid for uid, data in clients.items()
            if data["last_used"] < cutoff
        ]
        for uid in idle_users:
            logger.info(f"Отключаем idle клиент user_id={uid}")
            await drop_client(uid)


# ==========================================
# ХЕЛПЕРЫ
# ==========================================

DC_MAP = {1: "US", 2: "EU", 3: "US", 4: "EU", 5: "AS"}


def get_status(user):
    if not hasattr(user, "status") or not user.status:
        return "n/a"
    t = type(user.status).__name__
    if t == "UserStatusOnline":       return "Онлайн"
    if t == "UserStatusRecently":     return "Был(а) недавно"
    if t == "UserStatusLastWeek":     return "На этой неделе"
    if t == "UserStatusLastMonth":    return "В этом месяце"
    if t == "UserStatusOffline":
        wo = user.status.was_online
        return wo.strftime("%d/%m/%Y %H:%M:%S") if wo else "n/a"
    return "n/a"


def get_user_info(user):
    bio = getattr(user, "about", None) or "n/a"
    photo = getattr(user, "photo", None)
    dc_id = ""
    if photo and hasattr(photo, "dc_id"):
        dc_id = DC_MAP.get(photo.dc_id, str(photo.dc_id))
    return bio, dc_id


def make_row(user, channel_username, post_id, comment):
    bio, dc_id = get_user_info(user)
    return {
        "ID канала":                      channel_username,
        "ID":                             user.id,
        "Имя":                            user.first_name or "",
        "Фамилия":                        user.last_name or "",
        "Имя пользователя":               user.username or "",
        "Есть фото":                      "TRUE" if user.photo else "FALSE",
        "Телефон":                        getattr(user, "phone", "") or "",
        "Есть премиум":                   "TRUE" if getattr(user, "premium", False) else "FALSE",
        "Статус":                         get_status(user),
        "Язык":                           "",
        "О себе":                         bio,
        "Пол":                            "n/a",
        "Удален":                         "TRUE" if getattr(user, "deleted", False) else "FALSE",
        "Бот":                            "TRUE" if getattr(user, "bot", False) else "FALSE",
        "Фейк":                           "TRUE" if getattr(user, "fake", False) else "FALSE",
        "Скам":                           "TRUE" if getattr(user, "scam", False) else "FALSE",
        "Требуется премиум для контакта": "FALSE",
        "DC ID":                          dc_id,
        "Истории недоступны":             "TRUE",
        "Макс ID истории":                "",
        "ID поста":                       post_id,
        "Дата сообщения":                 comment.date.strftime("%d/%m/%Y %H:%M:%S") if comment.date else "",
        "Текст сообщения":                comment.text or "",
        "ID сообщения":                   comment.id,
        "Ссылка на комментарий":          f"https://t.me/{channel_username}/{post_id}?comment={comment.id}",
    }


async def parse_channel(client, channel_username, status_msg: Message):
    rows = []
    seen = set()

    try:
        channel = await client.get_entity(channel_username)
    except ChannelPrivateError:
        await status_msg.answer(f"❌ Канал @{channel_username} закрытый или нет доступа")
        return rows
    except ChatAdminRequiredError:
        await status_msg.answer(f"❌ @{channel_username} — нет прав доступа")
        return rows
    except AuthKeyUnregisteredError:
        await status_msg.answer("❌ Сессия устарела. Напиши /start и авторизуйся заново.")
        return rows
    except Exception as e:
        await status_msg.answer(f"❌ Ошибка канала @{channel_username}: {e}")
        return rows

    async for post in client.iter_messages(channel, limit=POSTS_LIMIT or None):
        if not post.replies or post.replies.replies == 0:
            continue

        post_id = post.id
        try:
            async for comment in client.iter_messages(channel, reply_to=post_id):
                if not comment.sender_id:
                    continue
                user = comment.sender
                if not user or getattr(user, "bot", False):
                    continue

                key = (user.id, post_id)
                if key in seen:
                    continue
                seen.add(key)

                rows.append(make_row(user, channel_username, post_id, comment))

        except FloodWaitError as e:
            await asyncio.sleep(e.seconds)
        except Exception:
            continue

    return rows


def save_excel(rows: list, user_id: int) -> str:
    """Сохраняет данные в Excel и возвращает путь к файлу.
    pandas и openpyxl импортируются здесь — не при старте бота."""
    import pandas as pd  # ленивый импорт — экономит ~40MB в idle

    output_file = os.path.join(
        OUTPUT_DIR,
        f"комментаторы_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    )
    df = pd.DataFrame(rows)

    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Комментаторы")
        ws = writer.sheets["Комментаторы"]
        for col in ws.columns:
            max_len = max((len(str(c.value)) for c in col if c.value), default=10)
            ws.column_dimensions[col[0].column_letter].width = min(max_len + 2, 50)

    return output_file


# ==========================================
# ХЭНДЛЕРЫ
# ==========================================

@dp.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    user_id = message.from_user.id
    session_file = f"{SESSIONS_DIR}/session_{user_id}"

    # Сбрасываем старый клиент
    await drop_client(user_id)
    await state.clear()

    # Проверяем сохранённую сессию
    if os.path.exists(f"{session_file}.session"):
        try:
            client = TelegramClient(session_file, API_ID, API_HASH)
            await client.connect()
            if await client.is_user_authorized():
                await set_client(user_id, client)
                await state.set_state(ParseState.waiting_channels_file)
                await message.answer(
                    "👋 С возвращением!\n\n"
                    "📁 Отправь .txt файл со списком каналов (по одному на строку):\n\n"
                    "<code>channel1\nchannel2</code>\n\n"
                    "Команды:\n"
                    "/clear — очистить сессию и память\n"
                    "/status — состояние бота",
                    parse_mode="HTML"
                )
                return
            else:
                await client.disconnect()
                os.remove(f"{session_file}.session")
        except Exception:
            if os.path.exists(f"{session_file}.session"):
                os.remove(f"{session_file}.session")

    await state.set_state(ParseState.waiting_phone)
    await message.answer(
        "👋 Привет! Я парсер комментаторов Telegram каналов.\n\n"
        "📱 Введи номер телефона в формате:\n"
        "<code>+79991234567</code>",
        parse_mode="HTML"
    )


@dp.message(Command("clear"))
async def cmd_clear(message: Message, state: FSMContext):
    user_id = message.from_user.id
    was_connected = user_id in clients

    await drop_client(user_id)
    await state.clear()

    # Удаляем сессию с диска
    session_file = f"{SESSIONS_DIR}/session_{user_id}.session"
    if os.path.exists(session_file):
        os.remove(session_file)

    # Удаляем временные файлы пользователя
    for f in os.listdir(OUTPUT_DIR):
        if str(user_id) in f:
            try:
                os.remove(os.path.join(OUTPUT_DIR, f))
            except Exception:
                pass

    gc.collect()

    msg = "🧹 Готово!\n\n"
    msg += "✅ Telethon-клиент отключён\n" if was_connected else ""
    msg += "✅ Сессия удалена с диска\n"
    msg += "✅ Временные файлы удалены\n"
    msg += "✅ Память освобождена\n\n"
    msg += "Для нового парсинга — /start"
    await message.answer(msg)


@dp.message(Command("status"))
async def cmd_status(message: Message, state: FSMContext):
    user_id = message.from_user.id
    current_state = await state.get_state()

    connected = user_id in clients
    last_used = ""
    if connected:
        lu = clients[user_id]["last_used"]
        last_used = f"\n🕐 Последняя активность: {lu.strftime('%H:%M:%S')}"

    total_clients = len(clients)

    await message.answer(
        f"📊 <b>Статус бота</b>\n\n"
        f"{'🟢' if connected else '🔴'} Telethon: {'подключён' if connected else 'не подключён'}{last_used}\n"
        f"📋 Состояние: {current_state or 'нет'}\n"
        f"👥 Активных сессий: {total_clients}",
        parse_mode="HTML"
    )


@dp.message(ParseState.waiting_phone)
async def handle_phone(message: Message, state: FSMContext):
    phone = message.text.strip()
    user_id = message.from_user.id
    session_file = f"{SESSIONS_DIR}/session_{user_id}"

    await state.update_data(phone=phone)
    await message.answer("⏳ Отправляю код...")

    client = TelegramClient(session_file, API_ID, API_HASH)
    await client.connect()

    try:
        result = await client.send_code_request(phone)
        await state.update_data(phone_code_hash=result.phone_code_hash)
        await set_client(user_id, client)
        await state.set_state(ParseState.waiting_code)
        await message.answer(
            "✅ Код отправлен!\n\n"
            "📨 Введи код из Telegram (слитно или через пробел):\n"
            "Пример: <code>12345</code>",
            parse_mode="HTML"
        )
    except Exception as e:
        await client.disconnect()
        await message.answer(f"❌ Ошибка: {e}\n\nПопробуй ещё раз /start")
        await state.clear()


@dp.message(ParseState.waiting_code)
async def handle_code(message: Message, state: FSMContext):
    code = message.text.strip().replace(" ", "")
    user_id = message.from_user.id
    data = await state.get_data()
    phone = data.get("phone")
    phone_code_hash = data.get("phone_code_hash")
    client = await get_client(user_id)

    if not client:
        await message.answer("❌ Сессия потеряна. Начни заново /start")
        await state.clear()
        return

    try:
        await client.sign_in(phone=phone, code=code, phone_code_hash=phone_code_hash)
        await state.set_state(ParseState.waiting_channels_file)
        await message.answer(
            "✅ Авторизация успешна!\n\n"
            "📁 Отправь .txt файл со списком каналов (по одному на строку):\n\n"
            "<code>channel1\nchannel2</code>",
            parse_mode="HTML"
        )
    except SessionPasswordNeededError:
        await state.set_state(ParseState.waiting_2fa)
        await message.answer("🔐 Аккаунт защищён 2FA. Введи пароль:")
    except Exception as e:
        await message.answer(f"❌ Неверный код: {e}\n\nНачни заново /start")
        await state.clear()


@dp.message(ParseState.waiting_2fa)
async def handle_2fa(message: Message, state: FSMContext):
    password = message.text.strip()
    user_id = message.from_user.id
    client = await get_client(user_id)

    if not client:
        await message.answer("❌ Сессия потеряна. Начни заново /start")
        await state.clear()
        return

    try:
        await client.sign_in(password=password)
        await state.set_state(ParseState.waiting_channels_file)
        await message.answer(
            "✅ Авторизация успешна!\n\n"
            "📁 Отправь .txt файл со списком каналов (по одному на строку):\n\n"
            "<code>channel1\nchannel2</code>",
            parse_mode="HTML"
        )
    except Exception as e:
        await message.answer(f"❌ Неверный пароль: {e}\n\nНачни заново /start")
        await state.clear()


@dp.message(ParseState.waiting_channels_file, F.document)
async def handle_channels_file(message: Message, state: FSMContext):
    document: Document = message.document
    user_id = message.from_user.id

    if not document.file_name.endswith(".txt"):
        await message.answer("❌ Нужен файл формата .txt")
        return

    # Скачиваем файл
    file_path = f"{OUTPUT_DIR}/channels_{user_id}.txt"
    await bot.download(document, destination=file_path)

    with open(file_path, "r", encoding="utf-8") as f:
        channels = [
            line.strip().lstrip("@").replace("https://t.me/", "")
            for line in f.readlines()
            if line.strip()
        ]
    os.remove(file_path)  # сразу удаляем временный файл

    if not channels:
        await message.answer("❌ Файл пустой или неверный формат")
        return

    await message.answer(
        f"✅ Найдено каналов: {len(channels)}\n"
        f"📋 {chr(10).join(['@' + c for c in channels])}\n\n"
        f"⏳ Начинаю парсинг..."
    )

    await state.set_state(ParseState.parsing)

    # Получаем или создаём клиент
    client = await get_client(user_id)
    if not client:
        session_file = f"{SESSIONS_DIR}/session_{user_id}"
        client = TelegramClient(session_file, API_ID, API_HASH)
        await client.connect()
        await set_client(user_id, client)

    # ========== ПАРСИНГ ==========
    all_rows = []
    for i, channel in enumerate(channels, 1):
        status_msg = await message.answer(f"📡 [{i}/{len(channels)}] Парсим @{channel}...")
        rows = await parse_channel(client, channel, status_msg)
        all_rows.extend(rows)
        await status_msg.edit_text(
            f"✅ [{i}/{len(channels)}] @{channel} — {len(rows)} комментаторов"
        )

    if not all_rows:
        await message.answer("❌ Данные не собраны. Проверь каналы и наличие комментариев.")
        await state.set_state(ParseState.waiting_channels_file)
        return

    # Сохраняем Excel (pandas загружается только здесь)
    output_file = save_excel(all_rows, user_id)

    # Отправляем файл
    await message.answer_document(
        FSInputFile(output_file),
        caption=(
            f"✅ Готово!\n"
            f"📊 Записей: {len(all_rows)}\n"
            f"📋 Каналов: {len(channels)}"
        )
    )

    # ========== ОЧИСТКА ПОСЛЕ ПАРСИНГА ==========
    os.remove(output_file)

    # Освобождаем память от данных парсинга
    all_rows.clear()
    del all_rows
    gc.collect()

    await state.set_state(ParseState.waiting_channels_file)
    await message.answer(
        "📁 Можешь отправить новый .txt файл для следующего парсинга.\n\n"
        "💡 Чтобы освободить память — /clear"
    )


@dp.message(ParseState.waiting_channels_file)
async def handle_wrong_input(message: Message):
    await message.answer(
        "📁 Отправь .txt файл со списком каналов.\n\n"
        "Команды:\n"
        "/clear — очистить память и сессию\n"
        "/status — состояние бота"
    )


# ==========================================
# ЗАПУСК
# ==========================================

async def main():
    # Запускаем фоновую очистку idle-клиентов
    asyncio.create_task(cleanup_idle_clients())
    print("🤖 Бот запущен!")
    print(f"⏱ Idle-таймаут клиентов: {IDLE_DISCONNECT_MINUTES} мин.")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
