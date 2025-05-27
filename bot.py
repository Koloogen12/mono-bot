"""Mono‑Fabrique Telegram bot – Production-ready marketplace bot
================================================================
Connects garment factories («Фабрика») with buyers («Заказчик»).
Full-featured implementation with persistent storage, user management,
comprehensive error handling, and production-ready features.

Main flows
----------
* Factory onboarding → payment (₂ 000 ₽/month) → PRO status → receives leads & dashboard
* Buyer creates order → payment (₇ 00 ₽) → order stored → auto-dispatch to matching factories
* Factories browse leads or get push notifications → send proposals → Buyer receives offers
* Secure escrow system for payments with full status tracking
* Rating system with reviews and reputation management

Features
--------
* Persistent user profiles with role detection
* Advanced search and filtering for orders
* Notification preferences and settings
* Analytics dashboard for both sides
* Automated reminders and follow-ups
* Support ticket system
* Admin panel for platform management

Runtime
-------
* Works in **long-polling** (default) or **webhook** mode (`BOT_MODE=WEBHOOK`)
* SQLite persistence (`fabrique.db`) with automatic migrations
* Graceful shutdown, error recovery, and comprehensive logging
* Background tasks for notifications and cleanup

Env variables
-------------
* `BOT_TOKEN`    – Telegram token (required)
* `BOT_MODE`     – `POLLING` (default) or `WEBHOOK`
* `WEBHOOK_BASE` – public HTTPS URL when in webhook mode
* `PORT`         – HTTP port for webhook (default: 8080)
* `ADMIN_IDS`    – comma-separated admin Telegram IDs
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
import sqlite3
import json
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union
from enum import Enum

from aiogram import Bot, Dispatcher, F, Router
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    Document,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    PhotoSize,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiohttp import web

try:
    from dotenv import load_dotenv
    load_dotenv()
except ModuleNotFoundError:
    pass

# ---------------------------------------------------------------------------
#  Config & bootstrap
# ---------------------------------------------------------------------------
TOKEN = os.getenv("BOT_TOKEN", "").strip()
if not TOKEN:
    raise RuntimeError("Set BOT_TOKEN env var with @BotFather token")

BOT_MODE = os.getenv("BOT_MODE", "POLLING").upper()
WEBHOOK_BASE = os.getenv("WEBHOOK_BASE", "").rstrip("/")
PORT = int(os.getenv("PORT", 8080))
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]

logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)
logger = logging.getLogger("fabrique-bot")

bot = Bot(TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)

DB_PATH = "fabrique.db"
DB_VERSION = 2  # Increment when schema changes

# ---------------------------------------------------------------------------
#  Constants and Enums
# ---------------------------------------------------------------------------

class UserRole(Enum):
    UNKNOWN = "unknown"
    FACTORY = "factory"
    BUYER = "buyer"
    ADMIN = "admin"

class OrderStatus(Enum):
    DRAFT = "DRAFT"
    SAMPLE_PASS = "SAMPLE_PASS"
    PRODUCTION = "PRODUCTION"
    READY_TO_SHIP = "READY_TO_SHIP"
    IN_TRANSIT = "IN_TRANSIT"
    DELIVERED = "DELIVERED"
    CANCELLED = "CANCELLED"
    DISPUTED = "DISPUTED"

ORDER_STATUS_DESCRIPTIONS = {
    OrderStatus.DRAFT: "Образец оплачивается. Ожидаем фото QC.",
    OrderStatus.SAMPLE_PASS: "Образец одобрен. Оплатите 30% предоплаты (Escrow).",
    OrderStatus.PRODUCTION: "Производство. Инспекция в процессе.",
    OrderStatus.READY_TO_SHIP: "Фабрика загрузила B/L. Оплатите остаток 70%.",
    OrderStatus.IN_TRANSIT: "Товар в пути. Отслеживание активно.",
    OrderStatus.DELIVERED: "Груз получен. Escrow разблокирован. Оцените сделку.",
    OrderStatus.CANCELLED: "Заказ отменен.",
    OrderStatus.DISPUTED: "Спорная ситуация. Ожидается решение администрации.",
}

# Categories for clothing production
CATEGORIES = [
    "футерки", "трикотаж", "пековые", "джинсы", "куртки", 
    "платья", "брюки", "рубашки", "спортивная одежда", 
    "нижнее белье", "детская одежда", "верхняя одежда"
]

# ---------------------------------------------------------------------------
#  Enhanced DB helpers with migrations
# ---------------------------------------------------------------------------

def get_db_version() -> int:
    """Get current database schema version."""
    try:
        with sqlite3.connect(DB_PATH) as db:
            result = db.execute("SELECT version FROM schema_version").fetchone()
            return result[0] if result else 0
    except:
        return 0

def init_db() -> None:
    """Initialize database with migrations support."""
    current_version = get_db_version()
    
    with sqlite3.connect(DB_PATH) as db:
        # Create schema version table
        db.execute("""
            CREATE TABLE IF NOT EXISTS schema_version (
                version INTEGER PRIMARY KEY
            )
        """)
        
        # Initial schema (version 1)
        if current_version < 1:
            logger.info("Creating initial database schema...")
            
            # Users table - central user management
            db.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    tg_id         INTEGER PRIMARY KEY,
                    username      TEXT,
                    full_name     TEXT,
                    phone         TEXT,
                    email         TEXT,
                    role          TEXT DEFAULT 'unknown',
                    is_active     INTEGER DEFAULT 1,
                    is_banned     INTEGER DEFAULT 0,
                    language      TEXT DEFAULT 'ru',
                    notifications INTEGER DEFAULT 1,
                    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_active   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Enhanced factories table
            db.execute("""
                CREATE TABLE IF NOT EXISTS factories (
                    tg_id        INTEGER PRIMARY KEY,
                    name         TEXT NOT NULL,
                    inn          TEXT NOT NULL,
                    legal_name   TEXT,
                    address      TEXT,
                    categories   TEXT,
                    min_qty      INTEGER,
                    max_qty      INTEGER,
                    avg_price    INTEGER,
                    portfolio    TEXT,
                    description  TEXT,
                    certificates TEXT,
                    rating       REAL DEFAULT 0,
                    rating_count INTEGER DEFAULT 0,
                    completed_orders INTEGER DEFAULT 0,
                    is_pro       INTEGER DEFAULT 0,
                    pro_expires  TIMESTAMP,
                    balance      INTEGER DEFAULT 0,
                    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (tg_id) REFERENCES users(tg_id)
                )
            """)
            
            # Enhanced orders table
            db.execute("""
                CREATE TABLE IF NOT EXISTS orders (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    buyer_id    INTEGER NOT NULL,
                    title       TEXT,
                    category    TEXT NOT NULL,
                    quantity    INTEGER NOT NULL,
                    budget      INTEGER NOT NULL,
                    destination TEXT NOT NULL,
                    lead_time   INTEGER NOT NULL,
                    description TEXT,
                    file_id     TEXT,
                    requirements TEXT,
                    paid        INTEGER DEFAULT 0,
                    is_active   INTEGER DEFAULT 1,
                    views       INTEGER DEFAULT 0,
                    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    expires_at  TIMESTAMP,
                    FOREIGN KEY (buyer_id) REFERENCES users(tg_id)
                )
            """)
            
            # Proposals with more details
            db.execute("""
                CREATE TABLE IF NOT EXISTS proposals (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_id     INTEGER NOT NULL,
                    factory_id   INTEGER NOT NULL,
                    price        INTEGER NOT NULL,
                    lead_time    INTEGER NOT NULL,
                    sample_cost  INTEGER NOT NULL,
                    message      TEXT,
                    attachments  TEXT,
                    is_accepted  INTEGER DEFAULT 0,
                    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(order_id, factory_id),
                    FOREIGN KEY (order_id) REFERENCES orders(id),
                    FOREIGN KEY (factory_id) REFERENCES factories(tg_id)
                )
            """)
            
            # Enhanced deals table
            db.execute("""
                CREATE TABLE IF NOT EXISTS deals (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_id     INTEGER NOT NULL,
                    factory_id   INTEGER NOT NULL,
                    buyer_id     INTEGER NOT NULL,
                    amount       INTEGER NOT NULL,
                    status       TEXT DEFAULT 'DRAFT',
                    deposit_paid INTEGER DEFAULT 0,
                    final_paid   INTEGER DEFAULT 0,
                    sample_photos TEXT,
                    production_photos TEXT,
                    tracking_num TEXT,
                    carrier      TEXT,
                    eta          TEXT,
                    notes        TEXT,
                    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(order_id, factory_id, buyer_id),
                    FOREIGN KEY (order_id) REFERENCES orders(id),
                    FOREIGN KEY (factory_id) REFERENCES factories(tg_id),
                    FOREIGN KEY (buyer_id) REFERENCES users(tg_id)
                )
            """)
            
            # Ratings with more detail
            db.execute("""
                CREATE TABLE IF NOT EXISTS ratings (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    deal_id      INTEGER NOT NULL,
                    factory_id   INTEGER NOT NULL,
                    buyer_id     INTEGER NOT NULL,
                    rating       INTEGER NOT NULL CHECK(rating >= 1 AND rating <= 5),
                    quality_rating INTEGER CHECK(quality_rating >= 1 AND quality_rating <= 5),
                    time_rating  INTEGER CHECK(time_rating >= 1 AND time_rating <= 5),
                    comm_rating  INTEGER CHECK(comm_rating >= 1 AND comm_rating <= 5),
                    comment      TEXT,
                    photos       TEXT,
                    is_verified  INTEGER DEFAULT 1,
                    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(deal_id, factory_id, buyer_id),
                    FOREIGN KEY (deal_id) REFERENCES deals(id),
                    FOREIGN KEY (factory_id) REFERENCES factories(tg_id),
                    FOREIGN KEY (buyer_id) REFERENCES users(tg_id)
                )
            """)
            
            # Payments tracking
            db.execute("""
                CREATE TABLE IF NOT EXISTS payments (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id      INTEGER NOT NULL,
                    type         TEXT NOT NULL,
                    amount       INTEGER NOT NULL,
                    currency     TEXT DEFAULT 'RUB',
                    status       TEXT DEFAULT 'pending',
                    reference_id INTEGER,
                    reference_type TEXT,
                    payment_method TEXT,
                    transaction_id TEXT,
                    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    completed_at TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(tg_id)
                )
            """)
            
            # Notifications queue
            db.execute("""
                CREATE TABLE IF NOT EXISTS notifications (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id      INTEGER NOT NULL,
                    type         TEXT NOT NULL,
                    title        TEXT,
                    message      TEXT NOT NULL,
                    data         TEXT,
                    is_read      INTEGER DEFAULT 0,
                    is_sent      INTEGER DEFAULT 0,
                    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    sent_at      TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(tg_id)
                )
            """)
            
            # Support tickets
            db.execute("""
                CREATE TABLE IF NOT EXISTS tickets (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id      INTEGER NOT NULL,
                    subject      TEXT NOT NULL,
                    category     TEXT,
                    status       TEXT DEFAULT 'open',
                    priority     TEXT DEFAULT 'normal',
                    assigned_to  INTEGER,
                    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    closed_at    TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(tg_id)
                )
            """)
            
            # Ticket messages
            db.execute("""
                CREATE TABLE IF NOT EXISTS ticket_messages (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticket_id    INTEGER NOT NULL,
                    user_id      INTEGER NOT NULL,
                    message      TEXT NOT NULL,
                    attachments  TEXT,
                    is_internal  INTEGER DEFAULT 0,
                    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (ticket_id) REFERENCES tickets(id),
                    FOREIGN KEY (user_id) REFERENCES users(tg_id)
                )
            """)
            
            # Analytics events
            db.execute("""
                CREATE TABLE IF NOT EXISTS analytics (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id      INTEGER,
                    event_type   TEXT NOT NULL,
                    event_data   TEXT,
                    ip_address   TEXT,
                    user_agent   TEXT,
                    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create indexes for performance
            db.execute("CREATE INDEX IF NOT EXISTS idx_orders_buyer ON orders(buyer_id)")
            db.execute("CREATE INDEX IF NOT EXISTS idx_orders_category ON orders(category)")
            db.execute("CREATE INDEX IF NOT EXISTS idx_proposals_order ON proposals(order_id)")
            db.execute("CREATE INDEX IF NOT EXISTS idx_deals_status ON deals(status)")
            db.execute("CREATE INDEX IF NOT EXISTS idx_notifications_user ON notifications(user_id, is_sent)")
            db.execute("CREATE INDEX IF NOT EXISTS idx_analytics_user ON analytics(user_id, event_type)")
            
            db.execute("INSERT OR REPLACE INTO schema_version (version) VALUES (1)")
            db.commit()
        
        # Migration to version 2 - Add factory photos and documents
        if current_version < 2:
            logger.info("Migrating database to version 2...")
            
            db.execute("""
                CREATE TABLE IF NOT EXISTS factory_photos (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    factory_id   INTEGER NOT NULL,
                    file_id      TEXT NOT NULL,
                    type         TEXT DEFAULT 'workshop',
                    caption      TEXT,
                    is_primary   INTEGER DEFAULT 0,
                    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (factory_id) REFERENCES factories(tg_id)
                )
            """)
            
            db.execute("""
                CREATE TABLE IF NOT EXISTS saved_searches (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id      INTEGER NOT NULL,
                    name         TEXT,
                    filters      TEXT NOT NULL,
                    is_active    INTEGER DEFAULT 1,
                    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(tg_id)
                )
            """)
            
            db.execute("INSERT OR REPLACE INTO schema_version (version) VALUES (2)")
            db.commit()
        
    logger.info(f"Database initialized successfully (version {DB_VERSION}) ✔")

def q(sql: str, params: Iterable[Any] | None = None) -> list[sqlite3.Row]:
    """Execute query and return all rows."""
    with sqlite3.connect(DB_PATH) as db:
        db.row_factory = sqlite3.Row
        return db.execute(sql, params or []).fetchall()

def q1(sql: str, params: Iterable[Any] | None = None) -> sqlite3.Row | None:
    """Execute query and return first row."""
    rows = q(sql, params)
    return rows[0] if rows else None

def run(sql: str, params: Iterable[Any] | None = None) -> None:
    """Execute query without returning results."""
    with sqlite3.connect(DB_PATH) as db:
        db.execute(sql, params or [])
        db.commit()

def insert_and_get_id(sql: str, params: Iterable[Any] | None = None) -> int:
    """Insert row and return its ID."""
    with sqlite3.connect(DB_PATH) as db:
        cursor = db.execute(sql, params or [])
        db.commit()
        return cursor.lastrowid

# ---------------------------------------------------------------------------
#  User management functions
# ---------------------------------------------------------------------------

def get_or_create_user(tg_user) -> dict:
    """Get existing user or create new one."""
    user = q1("SELECT * FROM users WHERE tg_id = ?", (tg_user.id,))
    
    if not user:
        # Create new user
        run("""
            INSERT INTO users (tg_id, username, full_name)
            VALUES (?, ?, ?)
        """, (
            tg_user.id,
            tg_user.username or "",
            tg_user.full_name or f"User_{tg_user.id}"
        ))
        user = q1("SELECT * FROM users WHERE tg_id = ?", (tg_user.id,))
    else:
        # Update last active
        run("""
            UPDATE users 
            SET last_active = CURRENT_TIMESTAMP,
                username = ?,
                full_name = ?
            WHERE tg_id = ?
        """, (
            tg_user.username or user['username'],
            tg_user.full_name or user['full_name'],
            tg_user.id
        ))
    
    return dict(user)

def get_user_role(tg_id: int) -> UserRole:
    """Get user's role."""
    user = q1("SELECT role FROM users WHERE tg_id = ?", (tg_id,))
    if not user:
        return UserRole.UNKNOWN
    
    role_str = user['role']
    if tg_id in ADMIN_IDS:
        return UserRole.ADMIN
    
    try:
        return UserRole(role_str)
    except ValueError:
        return UserRole.UNKNOWN

def is_user_banned(tg_id: int) -> bool:
    """Check if user is banned."""
    user = q1("SELECT is_banned FROM users WHERE tg_id = ?", (tg_id,))
    return bool(user and user['is_banned'])

# ---------------------------------------------------------------------------
#  Analytics tracking
# ---------------------------------------------------------------------------

def track_event(user_id: int | None, event_type: str, data: dict | None = None):
    """Track analytics event."""
    try:
        run("""
            INSERT INTO analytics (user_id, event_type, event_data)
            VALUES (?, ?, ?)
        """, (
            user_id,
            event_type,
            json.dumps(data) if data else None
        ))
    except Exception as e:
        logger.error(f"Failed to track event: {e}")

# ---------------------------------------------------------------------------
#  Notification system
# ---------------------------------------------------------------------------

async def send_notification(user_id: int, type: str, title: str, message: str, data: dict | None = None):
    """Send notification to user."""
    # Save to database
    notification_id = insert_and_get_id("""
        INSERT INTO notifications (user_id, type, title, message, data)
        VALUES (?, ?, ?, ?, ?)
    """, (user_id, type, title, message, json.dumps(data) if data else None))
    
    # Check if user has notifications enabled
    user = q1("SELECT notifications FROM users WHERE tg_id = ?", (user_id,))
    if not user or not user['notifications']:
        return
    
    # Send via Telegram
    try:
        await bot.send_message(user_id, f"<b>{title}</b>\n\n{message}")
        run("UPDATE notifications SET is_sent = 1, sent_at = CURRENT_TIMESTAMP WHERE id = ?", (notification_id,))
    except Exception as e:
        logger.error(f"Failed to send notification {notification_id}: {e}")

async def notify_admins(event_type: str, title: str, message: str, data: dict | None = None, 
                       buttons: list | None = None):
    """Send notification to all admins."""
    if not ADMIN_IDS:
        return
    
    # Format admin message
    admin_message = (
        f"🔔 <b>{title}</b>\n\n"
        f"{message}\n\n"
        f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    
    # Add data details if provided
    if data:
        admin_message += "\n\n📊 <b>Детали:</b>"
        for key, value in data.items():
            admin_message += f"\n• {key}: {value}"
    
    # Create keyboard if buttons provided
    kb = None
    if buttons:
        kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    # Send to all admins
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, admin_message, reply_markup=kb)
            
            # Also save to admin's notifications
            await send_notification(
                admin_id,
                f"admin_{event_type}",
                title,
                message,
                data
            )
        except Exception as e:
            logger.error(f"Failed to notify admin {admin_id}: {e}")

# ---------------------------------------------------------------------------
#  Enhanced FSM States
# ---------------------------------------------------------------------------

class FactoryForm(StatesGroup):
    checking_existing = State()
    inn = State()
    legal_name = State()
    address = State()
    photos = State()
    categories = State()
    min_qty = State()
    max_qty = State()
    avg_price = State()
    description = State()
    portfolio = State()
    confirm_pay = State()

class BuyerForm(StatesGroup):
    title = State()
    category = State()
    quantity = State()
    budget = State()
    destination = State()
    lead_time = State()
    description = State()
    requirements = State()
    file = State()
    confirm_pay = State()

class ProposalForm(StatesGroup):
    price = State()
    lead_time = State()
    sample_cost = State()
    message = State()
    attachments = State()

class DealForm(StatesGroup):
    choose_factory = State()
    confirm_sample = State()
    sample_photos = State()
    payment_deposit = State()
    production_photos = State()
    payment_final = State()
    confirm_delivery = State()
    rate_factory = State()
    rate_comment = State()

class TrackingForm(StatesGroup):
    order_id = State()
    tracking_num = State()
    carrier = State()
    eta = State()

class SettingsForm(StatesGroup):
    main_menu = State()
    notifications = State()
    language = State()
    phone = State()
    email = State()

class TicketForm(StatesGroup):
    subject = State()
    category = State()
    message = State()

class BroadcastForm(StatesGroup):
    message = State()
    confirm = State()

# ---------------------------------------------------------------------------
#  Enhanced keyboards
# ---------------------------------------------------------------------------

def kb_main(user_role: UserRole = UserRole.UNKNOWN) -> ReplyKeyboardMarkup:
    """Main menu keyboard based on user role."""
    if user_role == UserRole.FACTORY:
        return kb_factory_menu()
    elif user_role == UserRole.BUYER:
        return kb_buyer_menu()
    elif user_role == UserRole.ADMIN:
        return kb_admin_menu()
    else:
        keyboard = [
            [
                KeyboardButton(text="🛠 Я – Фабрика"), 
                KeyboardButton(text="🛒 Мне нужна фабрика")
            ],
            [
                KeyboardButton(text="ℹ️ Как работает"), 
                KeyboardButton(text="💰 Тарифы")
            ],
            [
                KeyboardButton(text="📞 Поддержка"),
                KeyboardButton(text="⚙️ Настройки")
            ],
        ]
        return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def kb_factory_menu() -> ReplyKeyboardMarkup:
    """Factory main menu."""
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text="📂 Заявки"), 
                KeyboardButton(text="📊 Аналитика")
            ],
            [
                KeyboardButton(text="👤 Профиль"), 
                KeyboardButton(text="💼 Мои сделки")
            ],
            [
                KeyboardButton(text="⭐ Рейтинг"),
                KeyboardButton(text="💳 Баланс")
            ],
            [
                KeyboardButton(text="⚙️ Настройки"),
                KeyboardButton(text="📞 Поддержка")
            ],
        ],
        resize_keyboard=True,
    )

def kb_buyer_menu() -> ReplyKeyboardMarkup:
    """Buyer main menu."""
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text="➕ Новый заказ"),
                KeyboardButton(text="📋 Мои заказы")
            ],
            [
                KeyboardButton(text="💌 Предложения"),
                KeyboardButton(text="💼 Мои сделки")
            ],
            [
                KeyboardButton(text="🔍 Поиск фабрик"),
                KeyboardButton(text="👤 Профиль")
            ],
            [
                KeyboardButton(text="⚙️ Настройки"),
                KeyboardButton(text="📞 Поддержка")
            ],
        ],
        resize_keyboard=True,
    )

def kb_admin_menu() -> ReplyKeyboardMarkup:
    """Admin menu."""
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text="👥 Пользователи"),
                KeyboardButton(text="📊 Статистика")
            ],
            [
                KeyboardButton(text="🎫 Тикеты"),
                KeyboardButton(text="💰 Платежи")
            ],
            [
                KeyboardButton(text="📢 Рассылка"),
                KeyboardButton(text="⚙️ Настройки")
            ],
        ],
        resize_keyboard=True,
    )

def kb_categories() -> InlineKeyboardMarkup:
    """Categories selection keyboard."""
    buttons = []
    for i in range(0, len(CATEGORIES), 2):
        row = []
        row.append(InlineKeyboardButton(
            text=CATEGORIES[i].capitalize(), 
            callback_data=f"cat:{CATEGORIES[i]}"
        ))
        if i + 1 < len(CATEGORIES):
            row.append(InlineKeyboardButton(
                text=CATEGORIES[i+1].capitalize(), 
                callback_data=f"cat:{CATEGORIES[i+1]}"
            ))
        buttons.append(row)
    
    buttons.append([InlineKeyboardButton(text="✅ Готово", callback_data="cat:done")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# ---------------------------------------------------------------------------
#  Helper functions
# ---------------------------------------------------------------------------

def parse_digits(text: str) -> int | None:
    """Extract digits from text."""
    digits = re.sub(r"\D", "", text)
    return int(digits) if digits else None

def format_price(price: int) -> str:
    """Format price with thousands separator."""
    return f"{price:,}".replace(",", " ")

def order_caption(row: sqlite3.Row, detailed: bool = False) -> str:
    """Format order information."""
    caption = (
        f"<b>Заявка #Z-{row['id']}</b>\n"
        f"📦 Категория: {row['category'].capitalize()}\n"
        f"🔢 Тираж: {format_price(row['quantity'])} шт.\n"
        f"💰 Бюджет: {format_price(row['budget'])} ₽/шт.\n"
        f"📅 Срок: {row['lead_time']} дн.\n"
        f"📍 Город: {row['destination']}"
    )
    
    if detailed and row.get('description'):
        caption += f"\n\n📝 Описание:\n{row['description']}"
    
    if row.get('views'):
        caption += f"\n\n👁 Просмотров: {row['views']}"
    
    return caption

def proposal_caption(proposal: sqlite3.Row, factory: sqlite3.Row | None = None)
