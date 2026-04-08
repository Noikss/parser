"""
Telegram бот — парсер комментаторов каналов
Запуск: python tg_bot.py
"""

import asyncio
import os
import logging
from datetime import datetime

import pandas as pd
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, Document, FSInputFile
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.filters import CommandStart

from telethon import TelegramClient
from telethon.tl.functions.users import GetFullUserRequest
from telethon.errors import (
    FloodWaitError, ChannelPrivateError,
    ChatAdminRequiredError, SessionPasswordNeededError
)

# ==========================================
# НАСТРОЙКИ
# ==========================================

BOT_TOKEN = "8542734719:AAEEyZVGcHTBj1va5v2cY5QIKsXIJEp11rM"

# Публичные данные TDesktop (если нет своих)
API_ID = 2040
API_HASH = "b18441a1ff607e10a989891a5462e627"

# Лимит постов (0 = все)
POSTS_LIMIT = 200

# Задержка между запросами
DELAY = 0.5

# Папка для сессий и файлов
SESSIONS_DIR = "sessions"
OUTPUT_DIR = "outputs"
os.makedirs(SESSIONS_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ==========================================

logging.basicConfig(level=logging.INFO)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# Хранилище клиентов Telethon по user_id
clients: dict = {}


class ParseState(StatesGroup):
    waiting_phone = State()
    waiting_code = State()
    waiting_2fa = State()
    waiting_channels_file = State()
    parsing = State()


# ========== ХЕЛПЕРЫ ==========

def get_status(user):
    if not hasattr(user, "status") or not user.status:
        return "n/a"
    t = type(user.status).__name__
    if t == "UserStatusOnline":
        return "Онлайн"
    elif t == "UserStatusRecently":
        return "Был(а) недавно"
    elif t == "UserStatusLastWeek":
        return "На этой неделе"
    elif t == "UserStatusLastMonth":
        return "В этом месяце"
    elif t == "UserStatusOffline":
        wo = user.status.was_online
        return wo.strftime("%d/%m/%Y %H:%M:%S") if wo else "n/a"
    return "n/a"


DC_MAP = {1: "US", 2: "EU", 3: "US", 4: "EU", 5: "AS"}


async def get_user_full(client, user):
    """Возвращает (bio, dc_id)"""
    try:
        full = await client(GetFullUserRequest(user))
        bio = full.full_user.about or "n/a"
        photo = getattr(user, "photo", None)
        if photo and hasattr(photo, "dc_id"):
            dc_id = DC_MAP.get(photo.dc_id, str(photo.dc_id))
        else:
            dc_id = ""
        return bio, dc_id
    except Exception:
        return "n/a", ""


async def parse_channel(client, channel_username, status_msg: Message):
    rows = []
    seen_users = set()

    try:
        channel = await client.get_entity(channel_username)
    except (ChannelPrivateError, ChatAdminRequiredError):
        await status_msg.answer(f"❌ Канал @{channel_username} закрытый или нет доступа")
        return rows
    except Exception as e:
        await status_msg.answer(f"❌ Ошибка канала @{channel_username}: {e}")
        return rows

    post_count = 0
    total_comments = 0

    async for post in client.iter_messages(channel, limit=POSTS_LIMIT if POSTS_LIMIT > 0 else None):
        if not post.replies or post.replies.replies == 0:
            continue

        post_count += 1
        post_id = post.id

        try:
            async for comment in client.iter_messages(channel, reply_to=post_id):
                if not comment.sender_id:
                    continue
                user = comment.sender
                if not user or getattr(user, "bot", False):
                    continue

                comment_link = f"https://t.me/{channel_username}/{post_id}?comment={comment.id}"
                unique_key = (user.id, post_id)
                if unique_key in seen_users:
                    continue
                seen_users.add(unique_key)

                try:
                    bio, dc_id = await get_user_full(client, user)
                    await asyncio.sleep(DELAY)
                except FloodWaitError as e:
                    await asyncio.sleep(e.seconds)
                    bio, dc_id = "n/a", ""
                except Exception:
                    bio, dc_id = "n/a", ""

                rows.append({
                    "ID канала": channel_username,
                    "ID": user.id,
                    "Имя": user.first_name or "",
                    "Фамилия": user.last_name or "",
                    "Имя пользователя": user.username or "",
                    "Есть фото": "TRUE" if user.photo else "FALSE",
                    "Телефон": getattr(user, "phone", "") or "",
                    "Есть премиум": "TRUE" if getattr(user, "premium", False) else "FALSE",
                    "Статус": get_status(user),
                    "Язык": "",
                    "О себе": bio,
                    "Пол": "n/a",
                    "Удален": "TRUE" if getattr(user, "deleted", False) else "FALSE",
                    "Бот": "TRUE" if getattr(user, "bot", False) else "FALSE",
                    "Фейк": "TRUE" if getattr(user, "fake", False) else "FALSE",
                    "Скам": "TRUE" if getattr(user, "scam", False) else "FALSE",
                    "Требуется премиум для контакта": "FALSE",
                    "DC ID": dc_id,
                    "Истории недоступны": "TRUE",
                    "Макс ID истории": "",
                    "ID поста": post_id,
                    "Дата сообщения": comment.date.strftime("%d/%m/%Y %H:%M:%S") if comment.date else "",
                    "Текст сообщения": comment.text or "",
                    "ID сообщения": comment.id,
                    "Ссылка на комментарий": comment_link,
                })
                total_comments += 1

        except FloodWaitError as e:
            await asyncio.sleep(e.seconds)
        except Exception:
            continue

    return rows


# ========== ХЭНДЛЕРЫ ==========

@dp.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    user_id = message.from_user.id
    session_file = f"{SESSIONS_DIR}/session_{user_id}"

    # Если сессия уже есть — сразу к файлу
    if os.path.exists(f"{session_file}.session"):
        await state.set_state(ParseState.waiting_channels_file)
        await message.answer(
            "👋 С возвращением!\n\n"
            "📁 Отправь мне .txt файл со списком каналов (по одному на строку):\n\n"
            "<code>channel1\nchannel2\nchannel3</code>",
            parse_mode="HTML"
        )
    else:
        await state.set_state(ParseState.waiting_phone)
        await message.answer(
            "👋 Привет! Я парсер комментаторов Telegram каналов.\n\n"
            "📱 Введи номер телефона своего Telegram аккаунта в формате:\n"
            "<code>+79991234567</code>",
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
        clients[user_id] = client
        await state.set_state(ParseState.waiting_code)
        await message.answer(
            "✅ Код отправлен!\n\n"
            "📨 Введи код из Telegram (цифры через пробел или слитно):\n"
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
    client = clients.get(user_id)

    if not client:
        await message.answer("❌ Сессия потеряна. Начни заново /start")
        await state.clear()
        return

    try:
        await client.sign_in(phone=phone, code=code, phone_code_hash=phone_code_hash)
        await state.set_state(ParseState.waiting_channels_file)
        await message.answer(
            "✅ Авторизация успешна!\n\n"
            "📁 Теперь отправь .txt файл со списком каналов (по одному на строку):\n\n"
            "<code>channel1\nchannel2\nchannel3</code>",
            parse_mode="HTML"
        )
    except SessionPasswordNeededError:
        await state.set_state(ParseState.waiting_2fa)
        await message.answer("🔐 Аккаунт защищён двухфакторной аутентификацией.\nВведи пароль:")
    except Exception as e:
        await message.answer(f"❌ Неверный код или ошибка: {e}\n\nНачни заново /start")
        await state.clear()


@dp.message(ParseState.waiting_2fa)
async def handle_2fa(message: Message, state: FSMContext):
    password = message.text.strip()
    user_id = message.from_user.id
    client = clients.get(user_id)

    try:
        await client.sign_in(password=password)
        await state.set_state(ParseState.waiting_channels_file)
        await message.answer(
            "✅ Авторизация успешна!\n\n"
            "📁 Отправь .txt файл со списком каналов (по одному на строку):\n\n"
            "<code>channel1\nchannel2\nchannel3</code>",
            parse_mode="HTML"
        )
    except Exception as e:
        await message.answer(f"❌ Неверный пароль: {e}\n\nНачни заново /start")
        await state.clear()


@dp.message(ParseState.waiting_channels_file, F.document)
async def handle_channels_file(message: Message, state: FSMContext):
    document: Document = message.document

    if not document.file_name.endswith(".txt"):
        await message.answer("❌ Нужен файл формата .txt")
        return

    # Скачиваем файл
    file_path = f"{OUTPUT_DIR}/channels_{message.from_user.id}.txt"
    await bot.download(document, destination=file_path)

    # Читаем каналы
    with open(file_path, "r", encoding="utf-8") as f:
        channels = [
            line.strip().lstrip("@").replace("https://t.me/", "")
            for line in f.readlines()
            if line.strip()
        ]

    if not channels:
        await message.answer("❌ Файл пустой или неверный формат")
        return

    await message.answer(
        f"✅ Найдено каналов: {len(channels)}\n"
        f"📋 {chr(10).join(['@' + c for c in channels])}\n\n"
        f"⏳ Начинаю парсинг... Это может занять некоторое время."
    )

    await state.set_state(ParseState.parsing)

    # Получаем клиент Telethon
    user_id = message.from_user.id
    session_file = f"{SESSIONS_DIR}/session_{user_id}"

    client = clients.get(user_id)
    if not client:
        client = TelegramClient(session_file, API_ID, API_HASH)
        await client.connect()
        clients[user_id] = client

    # Парсим
    all_rows = []
    for i, channel in enumerate(channels, 1):
        status_msg = await message.answer(f"📡 [{i}/{len(channels)}] Парсим @{channel}...")
        rows = await parse_channel(client, channel, status_msg)
        all_rows.extend(rows)
        await status_msg.edit_text(
            f"✅ [{i}/{len(channels)}] @{channel} — собрано {len(rows)} комментаторов"
        )

    if not all_rows:
        await message.answer("❌ Данные не собраны. Проверь каналы и наличие комментариев.")
        await state.set_state(ParseState.waiting_channels_file)
        return

    # Сохраняем Excel
    output_file = f"{OUTPUT_DIR}/комментаторы_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    df = pd.DataFrame(all_rows)

    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Комментаторы")
        ws = writer.sheets["Комментаторы"]
        for col in ws.columns:
            max_len = max((len(str(cell.value)) for cell in col if cell.value), default=10)
            ws.column_dimensions[col[0].column_letter].width = min(max_len + 2, 50)

    # Отправляем файл
    await message.answer_document(
        FSInputFile(output_file),
        caption=(
            f"✅ Готово!\n"
            f"📊 Собрано записей: {len(all_rows)}\n"
            f"📋 Каналов обработано: {len(channels)}"
        )
    )

    # Чистим файл
    os.remove(output_file)

    await state.set_state(ParseState.waiting_channels_file)
    await message.answer("📁 Можешь отправить новый .txt файл для следующего парсинга.")


@dp.message(ParseState.waiting_channels_file)
async def handle_wrong_input(message: Message):
    await message.answer("📁 Отправь .txt файл со списком каналов")


# ========== ЗАПУСК ==========

async def main():
    print("🤖 Бот запущен!")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
