# -*- coding: utf-8 -*-
import asyncio
import logging
import os
import sqlite3
import sys
import math
import json
import time
import random
import shutil
from collections import deque
from functools import wraps
from logging.handlers import RotatingFileHandler
from datetime import datetime

# Импорт ошибок Telethon
from telethon import TelegramClient, events, Button
from telethon.errors.rpcerrorlist import (
    FloodWaitError, SessionPasswordNeededError, PhoneCodeInvalidError,
    MessageNotModifiedError, ApiIdInvalidError, UserIsBotError, MediaCaptionTooLongError,
    UserDeactivatedError, UserBannedInChannelError, PeerFloodError
)
from telethon.sessions import SQLiteSession

# --- 0. КОНСТАНТЫ ---
RESTART_EXIT_CODE = 5
RESTART_EVENT = asyncio.Event()
DB_PATH = 'manager.db'
ACCOUNTS_PER_PAGE = 5
MAX_LOGGED_ERRORS = 10
DELETED_MESSAGE_CHECK_DELAY = 3 

# --- 1. НАСТРОЙКА ЛОГИРОВАНИЯ ---
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(formatter)
log.addHandler(stream_handler)
file_handler = RotatingFileHandler('bot_multimanager.log', maxBytes=5 * 1024 * 1024, backupCount=5, encoding='utf-8')
file_handler.setFormatter(formatter)
log.addHandler(file_handler)

# --- 1.5. УТИЛИТА SPINTAX ---
def process_spintax(text, depth=0):
    """Рекурсивно обрабатывает строку со Spintax синтаксисом с защитой от зацикливания."""
    if depth > 50: # Защита от переполнения стека
        return text
        
    if '{' not in text:
        return text
    
    start_index = text.rfind('{')
    end_index = text.find('}', start_index)
    
    if start_index == -1 or end_index == -1:
        return text
        
    substring = text[start_index + 1 : end_index]
    choices = substring.split('|')
    chosen = random.choice(choices)
    
    new_text = text[:start_index] + chosen + text[end_index + 1:]
    
    return process_spintax(new_text, depth + 1)

# --- 2. МЕНЕДЖЕР БАЗЫ ДАННЫХ ---
class DatabaseManager:
    """Управляет всеми операциями с базой данных SQLite."""
    def __init__(self, db_path):
        self.db_path = db_path
        # timeout=30.0 предотвращает ошибки блокировки базы
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False, isolation_level='DEFERRED', timeout=30.0)
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()
        self.init_db()

    def init_db(self):
        self.cursor.execute("PRAGMA foreign_keys = ON")
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                api_id INTEGER NOT NULL,
                api_hash TEXT NOT NULL,
                session_name TEXT NOT NULL UNIQUE,
                phone TEXT,
                text TEXT DEFAULT 'Default message with {option1|option2}.',
                image_path TEXT DEFAULT '',
                send_with_image BOOLEAN DEFAULT FALSE,
                chats TEXT DEFAULT '',
                send_interval INTEGER DEFAULT 120,
                message_delay INTEGER DEFAULT 10,
                is_healthy BOOLEAN DEFAULT FALSE,
                last_errors TEXT DEFAULT '[]',
                is_running BOOLEAN DEFAULT 0
            )
        ''')
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS stats (
                account_id INTEGER PRIMARY KEY,
                messages_sent INTEGER DEFAULT 0,
                messages_failed INTEGER DEFAULT 0,
                FOREIGN KEY (account_id) REFERENCES accounts (id) ON DELETE CASCADE
            )
        ''')
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        ''')
        self.conn.commit()
        
        # Миграции (добавление колонок при обновлении)
        self.cursor.execute("PRAGMA table_info(accounts)")
        columns = [column['name'] for column in self.cursor.fetchall()]
        
        if 'last_errors' not in columns:
            self.cursor.execute("ALTER TABLE accounts ADD COLUMN last_errors TEXT DEFAULT '[]'")
            log.info("Added 'last_errors' column.")
        
        if 'is_running' not in columns:
            self.cursor.execute("ALTER TABLE accounts ADD COLUMN is_running BOOLEAN DEFAULT 0")
            log.info("Added 'is_running' column.")
            
        self.conn.commit()

    def get_setting(self, key, default=None):
        self.cursor.execute("SELECT value FROM settings WHERE key = ?", (key,))
        row = self.cursor.fetchone()
        return row['value'] if row else default

    def set_setting(self, key, value):
        self.cursor.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, str(value)))
        self.conn.commit()

    def add_account(self, api_id, api_hash, session_name, phone):
        try:
            self.cursor.execute(
                "INSERT INTO accounts (api_id, api_hash, session_name, phone) VALUES (?, ?, ?, ?)",
                (api_id, api_hash, session_name, phone)
            )
            account_id = self.cursor.lastrowid
            self.cursor.execute("INSERT INTO stats (account_id) VALUES (?)", (account_id,))
            self.conn.commit()
            return account_id
        except sqlite3.IntegrityError:
            log.error(f"Account '{session_name}' already exists.")
            return None

    def get_account(self, account_id):
        self.cursor.execute("SELECT * FROM accounts WHERE id = ?", (account_id,))
        return self.cursor.fetchone()

    def get_all_accounts(self):
        self.cursor.execute("SELECT * FROM accounts ORDER BY id ASC")
        return self.cursor.fetchall()

    def update_account(self, account_id, **kwargs):
        fields = ', '.join([f"{key} = ?" for key in kwargs])
        values = list(kwargs.values())
        values.append(account_id)
        self.cursor.execute(f"UPDATE accounts SET {fields} WHERE id = ?", tuple(values))
        self.conn.commit()

    def delete_account(self, account_id):
        try:
            self.cursor.execute("DELETE FROM accounts WHERE id = ?", (account_id,))
            self.conn.commit()
            log.info(f"Account {account_id} successfully deleted from DB.")
        except Exception as e:
            self.conn.rollback()
            log.error(f"Failed to delete account {account_id}: {e}", exc_info=True)

    def get_stats(self, account_id):
        self.cursor.execute("SELECT * FROM stats WHERE account_id = ?", (account_id,))
        return self.cursor.fetchone()

    def increment_stat(self, account_id, stat_field):
        self.cursor.execute(f"UPDATE stats SET {stat_field} = {stat_field} + 1 WHERE account_id = ?", (account_id,))
        self.conn.commit()

# --- 3. КЛАССЫ ДАННЫХ И МЕНЕДЖЕР АККАУНТОВ ---
class AccountState:
    def __init__(self, db_row):
        self.id = db_row['id']
        self.name = f"Аккаунт {self.id}"
        
        # FIX: Исправлена ошибка AttributeError 'sqlite3.Row' object has no attribute 'get'
        # Используем безопасную проверку наличия ключа
        if 'is_running' in db_row.keys():
            self.is_running = bool(db_row['is_running'])
        else:
            self.is_running = False

        self.next_run_time = 0
        self.client = None
        self.task = None
        self.logger = logging.getLogger(self.name)
        self.user_info = "Неизвестно"
        self.force_run = asyncio.Event()

    def toggle_running(self, interval=None):
        self.is_running = not self.is_running
        if self.is_running and interval:
            now = time.time()
            self.next_run_time = (math.floor(now / interval) + 1) * interval
            self.logger.info("▶️ Scheduler state changed to: RUNNING")
        else:
            self.logger.info("⏸️ Scheduler state changed to: PAUSED")

class AccountManager:
    def __init__(self, db_path=DB_PATH):
        self.db = DatabaseManager(db_path)
        self.accounts = {} 
        self.sent_messages = deque(maxlen=2000)
        self.load_bot_config()
        self.load_accounts()

    def load_bot_config(self):
        self.bot_token = self.db.get_setting('bot_token')
        self.owner_id = int(self.db.get_setting('owner_id', 0))
        self.bot_api_id = int(self.db.get_setting('bot_api_id', 0))
        self.bot_api_hash = self.db.get_setting('bot_api_hash')
        self.main_api_id = self.db.get_setting('main_api_id')
        self.main_api_hash = self.db.get_setting('main_api_hash')
        admin_ids_raw = self.db.get_setting('additional_admin_ids', '')
        self.admin_ids = {self.owner_id} if self.owner_id else set()
        if admin_ids_raw:
            self.admin_ids.update([int(i.strip()) for i in admin_ids_raw.split(',') if i.strip().isdigit()])
        log.info(f"Loaded config. Owner: {self.owner_id}, Admins: {self.admin_ids}")

    def load_accounts(self):
        """Загружает аккаунты в словарь."""
        accounts_data = self.db.get_all_accounts()
        self.accounts = {acc['id']: AccountState(acc) for acc in accounts_data}
        log.info(f"Loaded {len(self.accounts)} accounts from database.")

    def get_account_state_by_id(self, acc_id):
        return self.accounts.get(acc_id)
            
    def add_error_log(self, acc_id, error_message: str):
        account_data = self.db.get_account(acc_id)
        try:
            last_errors = json.loads(account_data['last_errors'])
        except (json.JSONDecodeError, TypeError):
            last_errors = []
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
        clean_error = str(error_message).split('(caused by')[0].strip()
        last_errors.insert(0, f"{timestamp}: {clean_error}")
        
        self.db.update_account(acc_id, last_errors=json.dumps(last_errors[:MAX_LOGGED_ERRORS], ensure_ascii=False))

# --- 4. ЛОГИКА ПЛАНИРОВЩИКА И ОТПРАВКИ ---
async def run_scheduler(acc_state, manager):
    acc_state.logger.info(f"Scheduler loop started. Initial State: {'RUNNING' if acc_state.is_running else 'PAUSED'}")
    while True:
        try:
            acc_data = manager.db.get_account(acc_state.id)
            if not acc_data:
                acc_state.logger.warning("Account data not found. Stopping scheduler.")
                break

            force_run_task = asyncio.create_task(acc_state.force_run.wait())
            sleep_task = asyncio.create_task(asyncio.sleep(1))
            done, pending = await asyncio.wait({force_run_task, sleep_task}, return_when=asyncio.FIRST_COMPLETED)
            for task in pending: task.cancel()

            send_now = force_run_task in done
            if send_now:
                acc_state.logger.info("🚀 'Send Now' triggered!")
                acc_state.force_run.clear()
            
            # Проверяем условия запуска
            if (acc_state.is_running and time.time() >= acc_state.next_run_time) or send_now:
                if acc_state.is_running or send_now:
                    acc_state.logger.info("🚀 Starting new message cycle...")
                    await send_messages(acc_state, acc_data, manager)
                    acc_state.logger.info("✅ Cycle finished.")
                    
                    now = time.time()
                    interval = acc_data['send_interval']
                    last_run_base = acc_state.next_run_time if not send_now and acc_state.next_run_time > 0 else now
                    acc_state.next_run_time = (math.floor(last_run_base / interval) + 1) * interval
                    
                    if acc_state.next_run_time <= now:
                        acc_state.next_run_time = (math.floor(now / interval) + 1) * interval
                    
                    next_run_dt = datetime.fromtimestamp(acc_state.next_run_time).strftime('%Y-%m-%d %H:%M:%S')
                    acc_state.logger.info(f"Next run scheduled for {next_run_dt}")

        except asyncio.CancelledError:
            acc_state.logger.info("Scheduler loop cancelled.")
            break
        except Exception as e:
            acc_state.logger.error(f"Unhandled error in scheduler: {e}", exc_info=True)
            await asyncio.sleep(5)

async def send_messages(acc_state, acc_data, manager):
    # Разбираем чаты. Теперь поддерживаем формат ID:TOPIC (например -100123456:9995)
    raw_chats = [c.strip() for c in acc_data['chats'].split(',') if c.strip()]
    
    image = acc_data['image_path'] if acc_data['send_with_image'] and os.path.exists(acc_data['image_path']) else None
    
    for chat_entry in raw_chats:
        try:
            # Логика разделения Чат и Топик
            topic_id = None
            if ':' in chat_entry:
                chat_str, topic_str = chat_entry.split(':')
                chat = int(chat_str) if chat_str.lstrip('-').isdigit() else chat_str
                if topic_str.isdigit():
                    topic_id = int(topic_str)
            else:
                chat = int(chat_entry) if chat_entry.lstrip('-').isdigit() else chat_entry

            message_text = process_spintax(acc_data['text'])
            
            # Отправка (добавлен аргумент reply_to для топиков)
            if image:
                sent_message = await acc_state.client.send_file(chat, file=image, caption=message_text, parse_mode='md', reply_to=topic_id)
            else:
                sent_message = await acc_state.client.send_message(chat, message_text, parse_mode='md', reply_to=topic_id)
            
            if sent_message:
                await asyncio.sleep(DELETED_MESSAGE_CHECK_DELAY)
                # Проверка (нужно учитывать топик, но get_messages обычно находит по ID)
                check_message = await acc_state.client.get_messages(chat, ids=sent_message.id)
                
                if check_message:
                    manager.sent_messages.append((sent_message.chat_id, sent_message.id, acc_state.name))
                    acc_state.logger.info(f"  -> Sent to {chat} (Topic: {topic_id}) (ID: {sent_message.id})")
                    manager.db.increment_stat(acc_state.id, 'messages_sent')
                else:
                    error_text = f"Message to {chat} was deleted shortly after sending."
                    acc_state.logger.warning(f"  -> {error_text}")
                    manager.db.increment_stat(acc_state.id, 'messages_failed')
                    manager.add_error_log(acc_state.id, error_text)

        except FloodWaitError as e:
            error_text = f"Flood wait for {e.seconds}s on {chat}"
            acc_state.logger.warning(f"  -> {error_text}")
            manager.db.increment_stat(acc_state.id, 'messages_failed')
            manager.add_error_log(acc_state.id, error_text)
            await asyncio.sleep(e.seconds + 2)
        
        except (UserDeactivatedError, UserBannedInChannelError) as e:
            error_text = f"CRITICAL: Account banned/deactivated: {e}"
            acc_state.logger.critical(error_text)
            manager.db.increment_stat(acc_state.id, 'messages_failed')
            manager.add_error_log(acc_state.id, error_text)
            
            manager.db.update_account(acc_state.id, is_healthy=False, is_running=0) 
            acc_state.is_running = False 
            break 

        except Exception as e:
            error_text = f"FAILED to send to {chat}: {e}"
            acc_state.logger.error(f"  -> {error_text}")
            manager.db.increment_stat(acc_state.id, 'messages_failed')
            manager.add_error_log(acc_state.id, error_text)
        
        if acc_state.is_running:
            await asyncio.sleep(acc_data['message_delay'])
        else:
            break

# --- 5. ИНТЕРФЕЙС БОТА И КОМАНДЫ ---
def admin_only(func):
    @wraps(func)
    async def wrapped(event, *args, **kwargs):
        manager = event.client.manager
        if event.sender_id not in manager.admin_ids:
            if isinstance(event, events.CallbackQuery.Event):
                await event.answer("❌ Доступ запрещен.", alert=True)
            return
        return await func(event, *args, **kwargs)
    return wrapped

def owner_only(func):
    @wraps(func)
    async def wrapped(event, *args, **kwargs):
        manager = event.client.manager
        if event.sender_id != manager.owner_id:
            if isinstance(event, events.CallbackQuery.Event):
                await event.answer("❌ Доступ запрещен. Только для владельца.", alert=True)
            else:
                await event.respond("❌ Эта команда доступна только главному владельцу.")
            return
        return await func(event, *args, **kwargs)
    return wrapped

def create_code_handler(acc_state, manager, bot_client):
    async def code_handler(event):
        try:
            phone = manager.db.get_account(acc_state.id)['phone']
            log.info(f"Login code detected for {acc_state.name} ({phone}). Forwarding to owner.")
            await bot_client.send_message(
                manager.owner_id,
                f"🔔 **Новый код входа для {acc_state.name}**\n"
                f"**Телефон:** `{phone}`\n\n"
                f"```\n{event.message.message}\n```",
                parse_mode='md'
            )
        except Exception as e:
            log.error(f"Failed to forward login code from {acc_state.name}: {e}", exc_info=True)
    return code_handler

def generate_main_menu(manager, page=1):
    text = "**🤖 Главное меню менеджера аккаунтов (v8.8)**\n\n"
    text += "Управление: `/add_account`, `/remove_account <ID>`\n"
    text += "Админы: `/list_admins`, `/add_admin <ID>` (только владелец)\n\n"
    
    accounts_list = sorted(list(manager.accounts.values()), key=lambda acc: acc.id)
    total_pages = max(1, math.ceil(len(accounts_list) / ACCOUNTS_PER_PAGE))
    page = max(1, min(page, total_pages))
    start_index = (page - 1) * ACCOUNTS_PER_PAGE
    accounts_on_page = accounts_list[start_index:start_index + ACCOUNTS_PER_PAGE]
    
    buttons = []
    for state in accounts_on_page:
        data = manager.db.get_account(state.id)
        status_emoji = "▶️" if state.is_running else "⏸️"
        health_emoji = "✅" if data and data['is_healthy'] else "❌"
        user_first_name = state.user_info.split('`')[1] if '`' in state.user_info else "Инфо"
        buttons.append([Button.inline(f"{status_emoji} {health_emoji} {state.name}: {user_first_name}", f"control:{state.id}")])
    
    if total_pages > 1:
        row = []
        if page > 1: row.append(Button.inline("⬅️ Пред.", f"page:{page-1}"))
        row.append(Button.inline(f"📄 {page}/{total_pages}", "dummy"))
        if page < total_pages: row.append(Button.inline("След. ➡️", f"page:{page+1}"))
        buttons.append(row)
    
    buttons.extend([
        [Button.inline("▶️ Зап. все", "start_all"), Button.inline("⏸️ Ост. все", "stop_all")],
        [Button.inline("❤️ Проверить все", "health_check_all"), Button.inline("ℹ️ Статус всех", "status_all")]
    ])
    return text, buttons

async def get_status_details(manager, acc_id):
    acc_state = manager.get_account_state_by_id(acc_id)
    acc_data = manager.db.get_account(acc_id)
    if not acc_state or not acc_data: return "❌ Аккаунт не найден.", None
    
    stats = manager.db.get_stats(acc_id)
    status = "▶️ Работает" if acc_state.is_running else "⏸️ На паузе"
    with_img = "✅ Да" if acc_data['send_with_image'] else "❌ Нет"
    remaining = int(acc_state.next_run_time - time.time()) if acc_state.is_running else -1
    next_run_dt = datetime.fromtimestamp(acc_state.next_run_time).strftime('%H:%M:%S')
    next_run_info = f"{next_run_dt} (~{remaining // 60} мин {remaining % 60:02d} сек)" if remaining > 0 else "N/A"
    
    chat_count = len([c for c in acc_data['chats'].split(',') if c.strip()])
    
    text = (f"**ℹ️ Статус: {acc_state.name}**\n\n"
            f"**Пользователь:** {acc_state.user_info}\n"
            f"**Телефон:** `{acc_data['phone']}`\n"
            f"**Состояние:** {status}\n"
            f"**Здоровье сессии:** {'✅ OK' if acc_data['is_healthy'] else '❌ Проблема'}\n"
            f"**Следующий запуск:** {next_run_info}\n"
            f"**Интервал рассылки:** {acc_data['send_interval']} сек\n"
            f"**Отправка с фото:** {with_img}\n"
            f"**Кол-во чатов:** {chat_count}\n\n"
            f"**Статистика:**\n"
            f"  - Успешно: `{stats['messages_sent']}`\n"
            f"  - Ошибок: `{stats['messages_failed']}`\n\n"
            f"**Текст сообщения (шаблон):**\n"
            f"```\n{acc_data['text'][:1000]}\n```")
    
    image_path = acc_data['image_path'] if acc_data['send_with_image'] and os.path.exists(acc_data['image_path']) else None
    return text, image_path

def generate_control_menu(manager, acc_id):
    acc_state = manager.get_account_state_by_id(acc_id)
    if not acc_state: return "❌ Аккаунт не найден.", []
    toggle_text = "⏸️ Пауза" if acc_state.is_running else "▶️ Старт"
    return f"**⚙️ Управление: {acc_state.name}**", [
        [Button.inline(toggle_text, f"toggle_run:{acc_id}")],
        [Button.inline("🚀 Отправить сейчас", f"send_now:{acc_id}")],
        [Button.inline("📝 Редактировать", f"edit:{acc_id}")],
        [Button.inline("🔄 Клонировать", f"clone:{acc_id}")],
        [Button.inline("ℹ️ Статус", f"status:{acc_id}"), Button.inline("🚨 Ошибки", f"view_errors:{acc_id}")],
        [Button.inline("⬅️ Назад", "main_menu")]
    ]

def generate_edit_menu(manager, acc_id):
    acc_state = manager.get_account_state_by_id(acc_id)
    if not acc_state: return "❌ Аккаунт не найден.", []
    text = f"**📝 Редактирование: {acc_state.name}**"
    buttons = [
        [Button.inline("💬 Изменить текст", f"set_text:{acc_id}")],
        [Button.inline("🖼️ Изменить/удалить фото", f"set_image:{acc_id}")],
        [Button.inline("🔁 Вкл/Выкл фото", f"toggle_image:{acc_id}")],
        [Button.inline("📊 Управление чатами", f"edit_chats:{acc_id}")],
        [Button.inline("⏱ Изменить интервал", f"set_interval:{acc_id}")],
        [Button.inline("⬅️ Назад", f"control:{acc_id}")]
    ]
    return text, buttons
    
def generate_chat_menu(manager, acc_id):
    acc_state = manager.get_account_state_by_id(acc_id)
    acc_data = manager.db.get_account(acc_id)
    if not acc_state or not acc_data: return "❌ Аккаунт не найден.", []
    chats = [c.strip() for c in acc_data['chats'].split(',') if c.strip()]
    text = f"**📊 Управление чатами для {acc_state.name}**\n\n"
    text += "**Текущие чаты:**\n" + "\n".join(f"— `{c}`" for c in chats) if chats else "Список чатов пуст."
    buttons = [
        [Button.inline("➕ Добавить чат(ы)", f"add_chat:{acc_id}")],
        [Button.inline("🗑️ Удалить чат", f"del_chat_menu:{acc_id}")],
        [Button.inline("⬅️ Назад", f"edit:{acc_id}")]
    ]
    return text, buttons

def generate_delete_chat_menu(manager, acc_id):
    acc_data = manager.db.get_account(acc_id)
    if not acc_data: return "❌ Аккаунт не найден.", []
    chats = [c.strip() for c in acc_data['chats'].split(',') if c.strip()]
    text = "Нажмите на чат, который хотите удалить:"
    buttons = [[Button.inline(f"❌ `{chat}`", f"del_chat:{acc_id}:{i}")] for i, chat in enumerate(chats)]
    buttons.append([Button.inline("⬅️ Назад", f"edit_chats:{acc_id}")])
    return text, buttons

def generate_clone_target_menu(manager, source_id):
    source_state = manager.get_account_state_by_id(source_id)
    if not source_state: return "❌ Аккаунт не найден.", []
    
    text = f"**🔄 Клонирование настроек с {source_state.name}**\n\n"
    text += "Выберите аккаунт, на который нужно скопировать **все** настройки (текст, фото, чаты, интервалы):"
    
    buttons = []
    sorted_accounts = sorted(manager.accounts.values(), key=lambda acc: acc.id) 
    
    for acc_state in sorted_accounts:
        if acc_state.id == source_id:
            continue
        
        user_first_name = acc_state.user_info.split('`')[1] if '`' in acc_state.user_info else "Инфо"
        buttons.append([
            Button.inline(
                f"➡️ {acc_state.name}: {user_first_name}", 
                f"clone_to:{source_id}:{acc_state.id}"
            )
        ])
    
    if not buttons:
        text += "\n\n⚠️ *Нет других аккаунтов для клонирования.*"
        
    buttons.append([Button.inline("⬅️ Назад", f"control:{source_id}")])
    return text, buttons

async def add_account_conversation(conv, manager):
    try:
        if not manager.main_api_id or not manager.main_api_hash:
            await conv.send_message("❌ **Критическая ошибка!**\nГлобальные `main_api_id` и `main_api_hash` не настроены.", parse_mode='md')
            return

        await conv.send_message("**Шаг 1/2: Введите номер телефона в международном формате** (например, `+1234567890`).\n\n"
                                "Для отмены напишите `отмена`.", parse_mode='md')
        phone_msg = await conv.get_response()
        if phone_msg.text.lower() == 'отмена': return await conv.send_message("⛔️ Операция отменена.")
        phone = phone_msg.text.strip()
        
        session_name = f"user_account_{phone.replace('+', '')}"
        temp_client = TelegramClient(SQLiteSession(session_name), int(manager.main_api_id), manager.main_api_hash)
        
        await conv.send_message("⏳ Пытаюсь подключиться к Telegram...")
        try:
            code_sent = False
            async def get_code():
                nonlocal code_sent
                if not code_sent:
                    await conv.send_message("**Шаг 2/2: Введите код подтверждения из Telegram.**\n\n"
                                            "Для отмены напишите `отмена`.", parse_mode='md')
                    code_sent = True
                response = await conv.get_response()
                if response.text.lower() == 'отмена': raise asyncio.CancelledError("User cancelled")
                return response.text.strip()

            async def get_password():
                await conv.send_message("**Аккаунт защищен 2FA. Введите пароль:**\n\n"
                                        "Для отмены напишите `отмена`.", parse_mode='md')
                response = await conv.get_response()
                if response.text.lower() == 'отмена': raise asyncio.CancelledError("User cancelled")
                return response.text.strip()

            await temp_client.start(phone=phone, code_callback=get_code, password=get_password)
        
        except (PhoneCodeInvalidError, ApiIdInvalidError) as e:
            return await conv.send_message(f"❌ Неверный код или глобальные API данные невалидны. Операция отменена. ({e})")
        except UserIsBotError:
            return await conv.send_message("❌ Нельзя добавлять аккаунты ботов. Операция отменена.")
        except asyncio.CancelledError:
             return await conv.send_message("⛔️ Операция отменена пользователем.")
        except Exception as e:
             if temp_client.is_connected(): await temp_client.disconnect()
             log.error(f"Sign in error: {e}", exc_info=True)
             return await conv.send_message(f"❌ Произошла ошибка при входе: `{e}`. Операция отменена.")
        
        me = await temp_client.get_me()
        if temp_client.is_connected(): await temp_client.disconnect()
        
        db_id = manager.db.add_account(int(manager.main_api_id), manager.main_api_hash, session_name, phone)
        if db_id:
            await conv.send_message(f"✅ **Аккаунт `{me.first_name}` успешно добавлен с ID {db_id}!**\n\n"
                                    f"Перезапускаюсь для активации...", parse_mode='md')
            await asyncio.sleep(2)
            RESTART_EVENT.set()
        else:
            await conv.send_message("❌ Не удалось сохранить аккаунт в базу данных (возможно, сессия уже существует).")
    
    except asyncio.TimeoutError:
        await conv.send_message("⌛ Время вышло. Операция отменена.")
    except Exception as e:
        log.error(f"Error in add_account conversation: {e}", exc_info=True)
        await conv.send_message(f"❌ Произошла критическая ошибка: {e}")


def register_bot_commands(bot_client, manager):
    @bot_client.on(events.NewMessage(pattern='/start'))
    @admin_only
    async def start_handler(event):
        text, buttons = generate_main_menu(manager)
        await event.respond(text, buttons=buttons)

    @bot_client.on(events.NewMessage(pattern='/add_account'))
    @admin_only
    async def add_account_handler(event):
        try:
            async with bot_client.conversation(event.sender_id, timeout=600) as conv:
                await add_account_conversation(conv, manager)
        except (asyncio.TimeoutError, TypeError):
            pass

    @bot_client.on(events.NewMessage(pattern=r'/remove_account (\d+)'))
    @admin_only
    async def remove_account_handler(event):
        try:
            acc_id = int(event.pattern_match.group(1))
            acc_state = manager.get_account_state_by_id(acc_id)
            
            async with bot_client.conversation(event.sender_id, timeout=60) as conv:
                name = acc_state.name if acc_state else f"ID {acc_id}"
                await conv.send_message(f"⚠️ Вы уверены, что хотите удалить **{name}**? "
                                        f"Это действие необратимо.\n"
                                        f"Отправьте `да` для подтверждения.", parse_mode='md')
                response = await conv.get_response()
                
                if response.text.lower() == 'да':
                    await conv.send_message("⏳ Останавливаю и удаляю...")
                    
                    if acc_state:
                        if acc_state.task and not acc_state.task.done():
                            acc_state.task.cancel()
                            try:
                                await acc_state.task
                            except asyncio.CancelledError:
                                pass
                        
                        if acc_state.client and acc_state.client.is_connected():
                            await acc_state.client.disconnect()

                    acc_data = manager.db.get_account(acc_id)
                    if acc_data:
                        session_path = acc_data['session_name'] + ".session"
                        if os.path.exists(session_path):
                            try:
                                os.remove(session_path)
                            except Exception as e:
                                log.error(f"Failed to remove session file: {e}")
                    
                    manager.db.delete_account(acc_id)
                    
                    await conv.send_message(f"✅ Аккаунт удален. Перезапускаюсь...", parse_mode='md')
                    await asyncio.sleep(1)
                    RESTART_EVENT.set()
                else:
                    await conv.send_message("⛔️ Удаление отменено.")
        except asyncio.TimeoutError:
             await event.respond("⌛ Время вышло. Операция отменена.")
        except Exception as e:
            log.error(f"Error in remove_account: {e}", exc_info=True)
            await event.respond(f"❌ Произошла ошибка: {e}")

    @bot_client.on(events.NewMessage(pattern=r'/add_admin (\d+)'))
    @owner_only
    async def add_admin_handler(event):
        try:
            user_id = int(event.pattern_match.group(1))
            if user_id == manager.owner_id:
                return await event.respond("❌ Нельзя добавить владельца в список админов (он уже владелец).")
            
            if user_id in manager.admin_ids:
                return await event.respond(f"✅ Пользователь `{user_id}` уже является админом.")

            admin_ids_raw = manager.db.get_setting('additional_admin_ids', '')
            current_admin_ids = set([int(i.strip()) for i in admin_ids_raw.split(',') if i.strip().isdigit()])
            current_admin_ids.add(user_id)
            
            new_admin_ids_str = ','.join(map(str, current_admin_ids))
            manager.db.set_setting('additional_admin_ids', new_admin_ids_str)
            manager.load_bot_config() 
            
            await event.respond(f"✅ **Успех!** Пользователь `{user_id}` назначен администратором.")
        except Exception as e:
            log.error(f"Error in add_admin_handler: {e}", exc_info=True)
            await event.respond(f"❌ Произошла ошибка: {e}")

    @bot_client.on(events.NewMessage(pattern=r'/remove_admin (\d+)'))
    @owner_only
    async def remove_admin_handler(event):
        try:
            user_id = int(event.pattern_match.group(1))
            if user_id == manager.owner_id:
                return await event.respond("❌ Нельзя удалить владельца.")

            admin_ids_raw = manager.db.get_setting('additional_admin_ids', '')
            current_admin_ids = set([int(i.strip()) for i in admin_ids_raw.split(',') if i.strip().isdigit()])

            if user_id not in current_admin_ids:
                    return await event.respond(f"❌ Пользователь `{user_id}` не найден в списке дополнительных админов.")
            
            current_admin_ids.remove(user_id)
            new_admin_ids_str = ','.join(map(str, current_admin_ids))
            manager.db.set_setting('additional_admin_ids', new_admin_ids_str)
            manager.load_bot_config()
            
            await event.respond(f"✅ **Успех!** Пользователь `{user_id}` удален из администраторов.")
        except Exception as e:
            log.error(f"Error in remove_admin_handler: {e}", exc_info=True)
            await event.respond(f"❌ Произошла ошибка: {e}")

    @bot_client.on(events.NewMessage(pattern='/list_admins'))
    @admin_only
    async def list_admins_handler(event):
        owner_id = manager.owner_id
        additional_ids = manager.admin_ids - {owner_id}
        
        text = f"**👑 Владелец (Owner):**\n— `{owner_id}`\n\n"
        if additional_ids:
            text += "**👮‍♂️ Дополнительные администраторы:**\n"
            text += "\n".join(f"— `{admin_id}`" for admin_id in sorted(list(additional_ids)))
        else:
            text += "**👮‍♂️ Дополнительные администраторы:**\n"
            text += "*(список пуст)*"
        
        text += "\n\nВладелец может добавлять админов: `/add_admin <ID>`\n"
        text += "Владелец может удалять админов: `/remove_admin <ID>`"
        await event.respond(text, parse_mode='md')

    @bot_client.on(events.CallbackQuery)
    @admin_only
    async def callback_handler(event):
        data = event.data.decode('utf-8')
        parts = data.split(':')
        action = parts[0]
        acc_id = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0
        
        try:
            if action == "dummy":
                return await event.answer()
            
            if action == "page":
                page = int(parts[1])
                text, buttons = generate_main_menu(manager, page=page)
                await event.edit(text, buttons=buttons)

            elif action == "main_menu":
                text, buttons = generate_main_menu(manager)
                await event.edit(text, buttons=buttons)

            elif action == "control":
                text, buttons = generate_control_menu(manager, acc_id)
                await event.edit(text, buttons=buttons)

            elif action == "status":
                await event.answer("Загружаю статус...")
                text, image_path = await get_status_details(manager, acc_id)
                await event.delete()
                
                buttons = [Button.inline("⬅️ Назад", f"control:{acc_id}")]
                if image_path:
                    try:
                        await bot_client.send_file(event.chat_id, file=image_path, caption=text, buttons=buttons, link_preview=False)
                    except MediaCaptionTooLongError:
                        await bot_client.send_file(event.chat_id, file=image_path)
                        await bot_client.send_message(event.chat_id, text, buttons=buttons, link_preview=False)
                else:
                    await bot_client.send_message(event.chat_id, text, buttons=buttons, link_preview=False)
            
            elif action == "view_errors":
                acc_data = manager.db.get_account(acc_id)
                if not acc_data: return await event.answer("Аккаунт не найден", alert=True)
                
                try:
                    errors = json.loads(acc_data['last_errors'])
                except: errors = []

                if not errors:
                    return await event.answer("✅ Ошибок для этого аккаунта не зафиксировано.", alert=True)
                
                error_log_content = f"Последние {len(errors)} ошибок для Аккаунта {acc_id}:\n\n" + "\n".join(errors)
                await event.answer("Высылаю лог ошибок...")
                with open(f"errors_acc_{acc_id}.txt", "w", encoding="utf-8") as f:
                    f.write(error_log_content)
                await bot_client.send_file(event.chat_id, f"errors_acc_{acc_id}.txt", caption=f"📄 Лог ошибок для **Аккаунта {acc_id}**")
                os.remove(f"errors_acc_{acc_id}.txt")

            elif action == "toggle_run":
                acc_state = manager.get_account_state_by_id(acc_id)
                acc_data = manager.db.get_account(acc_id)
                if not acc_state or not acc_data: return await event.answer("Аккаунт не найден", alert=True)
                
                acc_state.toggle_running(acc_data['send_interval'])
                
                # СОХРАНЯЕМ СТАТУС В БД
                manager.db.update_account(acc_id, is_running=int(acc_state.is_running))
                
                await event.answer("Статус изменен!")
                text, buttons = generate_control_menu(manager, acc_id)
                await event.edit(text, buttons=buttons)

            elif action == "send_now":
                acc_state = manager.get_account_state_by_id(acc_id)
                if not acc_state: return await event.answer("Аккаунт не найден", alert=True)
                acc_state.force_run.set()
                await event.answer("✅ Команда на немедленную отправку дана!", alert=True)

            elif action == "edit":
                text, buttons = generate_edit_menu(manager, acc_id)
                await event.edit(text, buttons=buttons)

            elif action == "clone":
                text, buttons = generate_clone_target_menu(manager, acc_id)
                await event.edit(text, buttons=buttons)
            
            elif action == "clone_to":
                try:
                    source_id = int(parts[1])
                    target_id = int(parts[2])
                    
                    source_data = manager.db.get_account(source_id)
                    target_data = manager.db.get_account(target_id)
                    source_state = manager.get_account_state_by_id(source_id)
                    target_state = manager.get_account_state_by_id(target_id)

                    if not source_data or not target_data or not source_state or not target_state:
                        return await event.answer("❌ Ошибка: Один из аккаунтов не найден.", alert=True)

                    await event.answer("⏳ Клонирую настройки...")
                    
                    settings_to_clone = {
                        'text': source_data['text'],
                        'send_with_image': source_data['send_with_image'],
                        'chats': source_data['chats'],
                        'send_interval': source_data['send_interval'],
                        'message_delay': source_data['message_delay'],
                        'image_path': '' 
                    }
                    
                    source_image_path = source_data['image_path']
                    if source_data['send_with_image'] and source_image_path and os.path.exists(source_image_path):
                        try:
                            target_image_path = f"img_acc_{target_id}.jpg"
                            if os.path.exists(target_image_path):
                                try: os.remove(target_image_path)
                                except: pass
                            shutil.copy2(source_image_path, target_image_path)
                            settings_to_clone['image_path'] = target_image_path
                            log.info(f"Cloned image from {source_image_path} to {target_image_path}")
                        except Exception as e:
                            log.error(f"Failed to copy image during clone {source_id}->{target_id}: {e}")
                            settings_to_clone['send_with_image'] = False
                    
                    manager.db.update_account(target_id, **settings_to_clone)
                    
                    await event.answer(f"✅ Настройки с {source_state.name} скопированы на {target_state.name}!", alert=True)
                    text, buttons = generate_control_menu(manager, target_id)
                    await event.edit(text, buttons=buttons)

                except Exception as e:
                    log.error(f"Error in clone_to: {e}", exc_info=True)
                    await event.answer(f"❌ Ошибка: {e}", alert=True)

            elif action in ["set_text", "set_interval", "set_image", "add_chat"]:
                await event.answer()
                original_menu_func, original_menu_args = (generate_chat_menu, (manager, acc_id)) if action == "add_chat" else (generate_edit_menu, (manager, acc_id))
                
                try:
                    async with bot_client.conversation(event.sender_id, timeout=300) as conv:
                        if action == "set_text":
                            current_text = manager.db.get_account(acc_id)['text']
                            await conv.send_message(f"**Текущий текст:**\n\n`{current_text}`\n\nПришлите новый (поддерживается Spintax). Для отмены напишите `отмена`.", parse_mode='md')
                            response = await conv.get_response()
                            if response.text.lower() != 'отмена':
                                manager.db.update_account(acc_id, text=response.text)
                                await conv.send_message("✅ Текст обновлен.")
                            else: await conv.send_message("⛔️ Отменено.")
                        
                        elif action == "set_interval":
                            current_interval = manager.db.get_account(acc_id)['send_interval']
                            await conv.send_message(f"**Текущий интервал:** `{current_interval}` сек.\n\nВведите новый (минимум 10). Для отмены напишите `отмена`.", parse_mode='md')
                            response = await conv.get_response()
                            if response.text.lower() != 'отмена':
                                if response.text.isdigit() and int(response.text) >= 10:
                                    interval = int(response.text)
                                    manager.db.update_account(acc_id, send_interval=interval)
                                    acc_state = manager.get_account_state_by_id(acc_id)
                                    if acc_state and acc_state.is_running:
                                        now = time.time()
                                        acc_state.next_run_time = (math.floor(now / interval) + 1) * interval
                                    await conv.send_message("✅ Интервал обновлен.")
                                else: await conv.send_message("❌ Неверное значение.")
                            else: await conv.send_message("⛔️ Отменено.")

                        elif action == "add_chat":
                            await conv.send_message("Пришлите ID или @юзернеймы чатов (можно несколько, через запятую). Для отмены напишите `отмена`.")
                            response = await conv.get_response()
                            if response.text.lower() != 'отмена':
                                current_chats = {c.strip() for c in manager.db.get_account(acc_id)['chats'].split(',') if c.strip()}
                                new_chats = {c.strip() for c in response.text.split(',') if c.strip()}
                                all_chats = sorted(list(current_chats.union(new_chats)))
                                manager.db.update_account(acc_id, chats=','.join(all_chats))
                                await conv.send_message(f"✅ Чаты добавлены/обновлены.")
                            else: await conv.send_message("⛔️ Отменено.")

                        elif action == "set_image":
                            await conv.send_message("Отправьте новое фото или напишите `удалить`, чтобы убрать его. Для отмены напишите `отмена`.")
                            response = await conv.get_response()
                            if response.text and response.text.lower() == 'отмена':
                               await conv.send_message("⛔️ Отменено.")
                            elif response.photo:
                                path = await bot_client.download_media(response.photo, file=f"img_acc_{acc_id}.jpg")
                                manager.db.update_account(acc_id, image_path=path, send_with_image=True)
                                await conv.send_message("✅ Изображение обновлено, отправка с фото включена.")
                            elif response.text and response.text.lower() == 'удалить':
                                manager.db.update_account(acc_id, image_path='', send_with_image=False)
                                await conv.send_message("✅ Изображение удалено.")
                            else:
                                await conv.send_message("❌ Это не фото. Операция отменена.")

                except asyncio.TimeoutError:
                    await bot_client.send_message(event.sender_id, "⌛ Время вышло. Операция отменена.")
                
                text, buttons = original_menu_func(*original_menu_args)
                await event.edit(text, buttons=buttons)

            elif action == "toggle_image":
                acc_data = manager.db.get_account(acc_id)
                if not acc_data: return await event.answer("Аккаунт не найден", alert=True)
                new_status = not acc_data['send_with_image']
                manager.db.update_account(acc_id, send_with_image=new_status)
                await event.answer(f"Отправка с фото теперь {'ВКЛ' if new_status else 'ВЫКЛ'}.")
            
            elif action == "edit_chats":
                text, buttons = generate_chat_menu(manager, acc_id)
                await event.edit(text, buttons=buttons)

            elif action == "del_chat_menu":
                text, buttons = generate_delete_chat_menu(manager, acc_id)
                await event.edit(text, buttons=buttons)

            elif action == "del_chat":
                chat_index = int(parts[2])
                current_chats = [c.strip() for c in manager.db.get_account(acc_id)['chats'].split(',') if c.strip()]
                if 0 <= chat_index < len(current_chats):
                    current_chats.pop(chat_index)
                    manager.db.update_account(acc_id, chats=','.join(current_chats))
                    text, buttons = generate_delete_chat_menu(manager, acc_id)
                    await event.edit(text, buttons=buttons)
                else:
                    await event.answer("Чат уже удален.", alert=True)

            elif action == "start_all" or action == "stop_all":
                is_start = action == "start_all"
                count = 0
                for acc_state in manager.accounts.values():
                    if acc_state.is_running != is_start:
                        interval = manager.db.get_account(acc_state.id)['send_interval'] if is_start else None
                        acc_state.toggle_running(interval)
                        # СОХРАНЯЕМ ВСЕ
                        manager.db.update_account(acc_state.id, is_running=int(is_start))
                        count += 1
                await event.answer(f"✅ {'Запущено' if is_start else 'Остановлено'} аккаунтов: {count}")
                text, buttons = generate_main_menu(manager)
                await event.edit(text, buttons=buttons)

            elif action == "health_check_all" or action == "status_all":
                is_health = action == "health_check_all"
                await event.answer("❤️ Начинаю проверку...")
                msg = await event.respond("Проверка...")
                total = len(manager.accounts)
                ok_count = 0
                
                for i, acc_state in enumerate(manager.accounts.values(), 1):
                    await msg.edit(f"❤️ Проверяю {i}/{total}: **{acc_state.name}**")
                    if is_health:
                        is_ok = False
                        try:
                            if acc_state.client and acc_state.client.is_connected():
                                await acc_state.client.get_me()
                                is_ok = True
                                ok_count += 1
                        except Exception as e:
                            log.warning(f"Health check failed for {acc_state.name}: {e}")
                            is_ok = False
                        manager.db.update_account(acc_state.id, is_healthy=is_ok)
                    else:  # status_all
                        text, image_path = await get_status_details(manager, acc_state.id)
                        try:
                            if image_path:
                                await bot_client.send_file(event.chat_id, file=image_path, caption=text, link_preview=False)
                            else:
                                await bot_client.send_message(event.chat_id, text, link_preview=False)
                        except MediaCaptionTooLongError:
                            await bot_client.send_file(event.chat_id, file=image_path)
                            await bot_client.send_message(event.chat_id, text, link_preview=False)
                        except Exception as e:
                            log.error(f"Failed to send status for {acc_state.name}: {e}")
                            await bot_client.send_message(event.chat_id, f"⚠️ Не удалось отправить статус для {acc_state.name}:\n`{type(e).__name__}`")
                        await asyncio.sleep(0.5)

                await msg.delete()
                if is_health:
                    final_text = f"✅ Проверка завершена! Здоровых сессий: {ok_count}/{total}\n\n"
                    text, buttons = generate_main_menu(manager)
                    try:
                        await event.edit(final_text + text, buttons=buttons)
                    except MessageNotModifiedError: pass
                else:
                    await event.answer("✅ Статусы всех аккаунтов отправлены.")
                    text, buttons = generate_main_menu(manager)
                    await event.respond(text, buttons=buttons)

        except MessageNotModifiedError:
            await event.answer()
        except Exception as e:
            log.error(f"Callback handler error: {e}", exc_info=True)
            await event.answer("❌ Произошла ошибка.", alert=True)

# --- 6. ГЛАВНОЕ ПРИЛОЖЕНИЕ ---
async def main():
    manager = AccountManager()

    if not all([manager.bot_token, manager.owner_id, manager.bot_api_id, manager.bot_api_hash, manager.main_api_id, manager.main_api_hash]):
        log.warning("!!! CONFIGURATION MISSING. Starting initial setup mode.")
        if not os.path.exists(DB_PATH): DatabaseManager(DB_PATH)
        try:
            print("--- Initial Bot Setup ---")
            setup_bot_api_id = int(input(">>> Enter your BOT's api_id: "))
            setup_bot_api_hash = input(">>> Enter your BOT's api_hash: ")
            setup_bot_token = input(">>> Enter your BOT's token: ")
            print("\n--- Main User Account API Credentials ---")
            setup_main_api_id = int(input(">>> Enter the MAIN api_id for adding accounts: "))
            setup_main_api_hash = input(">>> Enter the MAIN api_hash for adding accounts: ")
        except ValueError:
            log.critical("Invalid input. api_id must be a number")
            return

        temp_bot_client = TelegramClient(SQLiteSession('control_bot_session'), setup_bot_api_id, setup_bot_api_hash)
        
        @temp_bot_client.on(events.NewMessage(pattern='/start'))
        async def temp_start_handler(event):
            sender_id = event.sender_id
            db = DatabaseManager(DB_PATH)
            db.set_setting('bot_api_id', setup_bot_api_id)
            db.set_setting('bot_api_hash', setup_bot_api_hash)
            db.set_setting('bot_token', setup_bot_token)
            db.set_setting('main_api_id', setup_main_api_id)
            db.set_setting('main_api_hash', setup_main_api_hash)
            db.set_setting('owner_id', sender_id)
            await event.respond(f"✅ **Настройка завершена!**\n\n"
                                f"Ваш ID `{sender_id}` установлен как владелец.\n"
                                f"Данные сохранены в `{DB_PATH}`.\n\n"
                                f"**Теперь, пожалуйста, остановите скрипт (Ctrl+C) и запустите его снова.**")
            log.info("Initial setup complete. Please restart the script.")
        
        log.info("Starting in initial setup mode... Send /start to your bot.")
        await temp_bot_client.start(bot_token=setup_bot_token)
        await temp_bot_client.run_until_disconnected()
        return

    bot_client = TelegramClient(SQLiteSession('control_bot_session'), manager.bot_api_id, manager.bot_api_hash)
    bot_client.manager = manager
    tasks = []
    
    try:
        log.info("Starting control bot client...")
        await bot_client.start(bot_token=manager.bot_token)
        log.info("Control bot connected.")
        register_bot_commands(bot_client, manager)
    except Exception as e:
        log.critical(f"FATAL: Could not start control bot. {e}", exc_info=True)
        return

    for acc_state in manager.accounts.values():
        try:
            acc_data = manager.db.get_account(acc_state.id)
            acc_state.client = TelegramClient(SQLiteSession(acc_data['session_name']), acc_data['api_id'], acc_data['api_hash'])
            await acc_state.client.start()
            
            me = await acc_state.client.get_me()
            username = f"@{me.username}" if me.username else "N/A"
            acc_state.user_info = f"`{me.first_name}` (`{username}`, ID: `{me.id}`)"
            acc_state.logger.info(f"Client for {acc_state.name} connected as {acc_state.user_info}")
            
            handler = create_code_handler(acc_state, manager, bot_client)
            acc_state.client.add_event_handler(handler, events.NewMessage(from_users=777000))
            acc_state.logger.info("Added login code forwarder.")
            
            # ВОССТАНОВЛЕНИЕ СТАТУСА РАБОТЫ
            if acc_state.is_running:
                acc_state.logger.info("Restoring ACTIVE state from DB...")
                now = time.time()
                interval = acc_data['send_interval']
                acc_state.next_run_time = (math.floor(now / interval) + 1) * interval
            
            acc_state.task = asyncio.create_task(run_scheduler(acc_state, manager))
            tasks.append(acc_state.task)
            manager.db.update_account(acc_state.id, is_healthy=True)
        except Exception as e:
            error_msg = f"Failed to start client for {acc_state.name}: {e}"
            if isinstance(e, SessionPasswordNeededError):
                error_msg = f"Failed to start {acc_state.name}: Session needs 2FA password."
            acc_state.logger.critical(error_msg)
            manager.db.update_account(acc_state.id, is_healthy=False)
            manager.add_error_log(acc_state.id, str(e))
    
    try:
        await bot_client.send_message(
            manager.owner_id, 
            f"✅ **Bot Manager Started (v8.8 Stable)**\n\n"
            f"Все аккаунты ({len(manager.accounts)}) загружены.\n"
            f"Память состояния активна.\n"
            "Отправьте /start, чтобы открыть меню.", 
            parse_mode='md'
        )
    except Exception as e:
        log.warning(f"Could not send start message to owner: {e}")

    log.info("All services running. Press Ctrl+C or trigger a restart command.")
    await RESTART_EVENT.wait()
    
    log.info("Shutdown or Restart initiated...")
    for task in tasks: task.cancel()
    
    for acc in manager.accounts.values():
        if acc.client and acc.client.is_connected():
            try:
                await acc.client.disconnect()
            except Exception as e:
                log.error(f"Error disconnecting {acc.name}: {e}")
                
    if bot_client.is_connected():
        await bot_client.disconnect()
    log.info("All clients disconnected.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Shutdown by user (Ctrl+C).")
        sys.exit(0)
    except Exception as e:
        log.critical(f"An unhandled error occurred in main: {e}", exc_info=True)
        sys.exit(1)
    
    if RESTART_EVENT.is_set():
        log.info(f"Exiting with status {RESTART_EXIT_CODE} for automatic restart.")
        sys.exit(RESTART_EXIT_CODE)