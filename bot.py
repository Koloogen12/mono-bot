"""
Mono‑Fabrique Telegram bot – Production-ready marketplace bot
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
* Group chats for deals
* Complete order management

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
import sys
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union
from enum import Enum
from aiogram import Bot, Dispatcher, F, Router
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from payments import create_payment, check_payment

class TicketForm(StatesGroup):
    subject = State()
    message = State()

class ProfileEditForm(StatesGroup):
    field_selection = State()
    new_value = State()

class PhotoManagementForm(StatesGroup):
    action = State()
    upload = State()

class EditOrderForm(StatesGroup):
    field_selection = State()
    title = State()
    category = State()
    quantity = State()
    budget = State()
    destination = State()
    lead_time = State()
    description = State()
    requirements = State()
    file = State()

class EditProposalForm(StatesGroup):
    field_selection = State()
    price = State()
    lead_time = State()
    sample_cost = State()
    message = State()

from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import BotCommand
from aiogram.fsm.context import FSMContext
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
    Chat,
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

try:
    from group_creator import create_deal_chat_real, TelegramGroupCreator
    GROUP_CREATOR_AVAILABLE = True
    logger.info("Group creator module loaded successfully")
except ImportError as e:
    logger.error(f"Failed to import group_creator: {e}")
    GROUP_CREATOR_AVAILABLE = False
except Exception as e:
    logger.error(f"Error loading group_creator: {e}")
    GROUP_CREATOR_AVAILABLE = False

bot = Bot(TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)

DB_PATH = "fabrique.db"
DB_VERSION = 3  # Increment when schema changes

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
                    sample_cost  INTEGER DEFAULT 0,
                    deposit_paid INTEGER DEFAULT 0,
                    final_paid   INTEGER DEFAULT 0,
                    sample_photos TEXT,
                    production_photos TEXT,
                    tracking_num TEXT,
                    carrier      TEXT,
                    eta          TEXT,
                    notes        TEXT,
                    chat_id      INTEGER,
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
                    payment_data TEXT,
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
        
        # Migration to version 3 - Add deal chats
        if current_version < 3:
            logger.info("Migrating database to version 3...")
            
            # Add chat_id column to deals if not exists
            try:
                db.execute("ALTER TABLE deals ADD COLUMN chat_id INTEGER")
            except sqlite3.OperationalError:
                pass  # Column already exists
            
            # Add sample_cost column to deals if not exists
            try:
                db.execute("ALTER TABLE deals ADD COLUMN sample_cost INTEGER DEFAULT 0")
            except sqlite3.OperationalError:
                pass  # Column already exists
            
            db.execute("INSERT OR REPLACE INTO schema_version (version) VALUES (3)")
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
    
    if detailed and 'description' in row and row['description']:
        caption += f"\n\n📝 Описание:\n{row['description']}"
    
    if 'views' in row and row['views']:
        caption += f"\n\n👁 Просмотров: {row['views']}"
    
    return caption

def proposal_caption(proposal: sqlite3.Row, factory: sqlite3.Row | None = None) -> str:
    """Format proposal information."""
    factory_name = factory['name'] if factory else f"Фабрика #{proposal['factory_id']}"
    
    caption = (
        f"<b>Предложение от {factory_name}</b>\n"
        f"💰 Цена: {format_price(proposal['price'])} ₽/шт.\n"
        f"📅 Срок: {proposal['lead_time']} дн.\n"
        f"🧵 Образец: {format_price(proposal['sample_cost'])} ₽"
    )
    
    if factory:
        if factory['rating_count'] > 0:
            caption += f"\n⭐ Рейтинг: {factory['rating']:.1f}/5.0 ({factory['rating_count']})"
        if factory['completed_orders'] > 0:
            caption += f"\n✅ Выполнено: {factory['completed_orders']} заказов"
    
    if 'message' in proposal and proposal['message']:
        caption += f"\n\n💬 Сообщение:\n{proposal['message']}"
    
    return caption

def deal_status_caption(deal: sqlite3.Row) -> str:
    """Format deal status information."""
    status = OrderStatus(deal['status'])
    status_text = ORDER_STATUS_DESCRIPTIONS.get(status, "Статус неизвестен")
    
    factory = q1("SELECT * FROM factories WHERE tg_id=?", (deal['factory_id'],))
    factory_name = factory['name'] if factory else "Неизвестная фабрика"
    
    order = q1("SELECT * FROM orders WHERE id=?", (deal['order_id'],))
    
    caption = (
        f"<b>Сделка #{deal['id']}</b>\n"
        f"📦 Заказ: #Z-{deal['order_id']}\n"
        f"🏭 Фабрика: {factory_name}\n"
        f"💰 Сумма: {format_price(deal['amount'])} ₽\n"
        f"📊 Статус: {status.value}\n"
        f"<i>{status_text}</i>"
    )
    
    if deal['tracking_num']:
        caption += f"\n\n🚚 Трек: {deal['tracking_num']}"
        if deal['carrier']:
            caption += f" ({deal['carrier']})"
    
    if deal['eta']:
        caption += f"\n📅 ETA: {deal['eta']}"
    
    # Payment status
    if status in [OrderStatus.SAMPLE_PASS, OrderStatus.PRODUCTION]:
        if deal['deposit_paid']:
            caption += "\n\n✅ Предоплата 30% получена"
        else:
            caption += "\n\n⏳ Ожидается предоплата 30%"
    elif status == OrderStatus.READY_TO_SHIP:
        if deal['final_paid']:
            caption += "\n\n✅ Оплата 100% получена"
        else:
            caption += "\n\n⏳ Ожидается доплата 70%"
    
    return caption

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
#  ПРОДОЛЖЕНИЕ: Admin commands (дописываем прерванную функцию)
# ---------------------------------------------------------------------------

@router.message(F.text == "📊 Статистика")
async def cmd_admin_stats(msg: Message) -> None:
    """Show platform statistics for admin."""
    if msg.from_user.id not in ADMIN_IDS:
        return

    # Get comprehensive stats
    stats = q1("""
        SELECT 
            (SELECT COUNT(*) FROM orders WHERE paid = 1) as total_orders,
            (SELECT COUNT(*) FROM orders WHERE paid = 1 AND created_at > datetime('now', '-7 days')) as orders_week,
            (SELECT COUNT(*) FROM deals) as total_deals,
            (SELECT COUNT(*) FROM deals WHERE status = 'DELIVERED') as completed_deals,
            (SELECT SUM(amount) FROM deals WHERE status = 'DELIVERED') as total_turnover,
            (SELECT SUM(amount) FROM payments WHERE status = 'completed') as total_payments,
            (SELECT COUNT(*) FROM factories WHERE is_pro = 1) as pro_factories,
            (SELECT AVG(rating) FROM factories WHERE rating_count > 0) as avg_rating
    """)
    
    # Revenue stats
    revenue = q1("""
        SELECT 
            SUM(CASE WHEN type = 'factory_pro' THEN amount ELSE 0 END) as factory_revenue,
            SUM(CASE WHEN type = 'order_placement' THEN amount ELSE 0 END) as order_revenue,
            COUNT(CASE WHEN type = 'factory_pro' THEN 1 END) as pro_subscriptions,
            COUNT(CASE WHEN type = 'order_placement' THEN 1 END) as paid_orders
        FROM payments 
        WHERE status = 'completed' AND created_at > datetime('now', '-30 days')
    """)
    
    text = (
        "<b>📊 Статистика платформы</b>\n\n"
        "<b>Заказы:</b>\n"
        f"├ Всего размещено: {stats['total_orders']}\n"
        f"├ За последнюю неделю: {stats['orders_week']}\n"
        f"└ Оплачено размещений: {revenue['paid_orders']} ({format_price(revenue['order_revenue'] or 0)} ₽)\n\n"
        "<b>Сделки:</b>\n"
        f"├ Всего сделок: {stats['total_deals']}\n"
        f"├ Завершено успешно: {stats['completed_deals']}\n"
        f"└ Общий оборот: {format_price(stats['total_turnover'] or 0)} ₽\n\n"
        "<b>Фабрики:</b>\n"
        f"├ PRO-подписок: {stats['pro_factories']}\n"
        f"├ Продано подписок (30д): {revenue['pro_subscriptions']} ({format_price(revenue['factory_revenue'] or 0)} ₽)\n"
        f"└ Средний рейтинг: {stats['avg_rating']:.1f}/5.0\n\n" if stats['avg_rating'] else "└ Средний рейтинг: нет данных\n\n"
    )
    
    text += f"<b>💰 Выручка за 30 дней: {format_price((revenue['factory_revenue'] or 0) + (revenue['order_revenue'] or 0))} ₽</b>"
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📈 График", callback_data="admin_stats_chart"),
            InlineKeyboardButton(text="💾 Экспорт", callback_data="admin_export_stats")
        ],
        [
            InlineKeyboardButton(text="🏭 Топ фабрик", callback_data="admin_top_factories"),
            InlineKeyboardButton(text="🛍 Топ заказчиков", callback_data="admin_top_buyers")
        ]
    ])

    await msg.answer(text, reply_markup=kb)

@router.message(F.text == "👥 Пользователи")
async def cmd_admin_users(msg: Message) -> None:
    """Show users statistics for admin."""
    if msg.from_user.id not in ADMIN_IDS:
        return
    
    stats = q1("""
        SELECT 
            COUNT(*) as total,
            COUNT(CASE WHEN role = 'factory' THEN 1 END) as factories,
            COUNT(CASE WHEN role = 'buyer' THEN 1 END) as buyers,
            COUNT(CASE WHEN is_banned = 1 THEN 1 END) as banned,
            COUNT(CASE WHEN created_at > datetime('now', '-1 day') THEN 1 END) as new_today
        FROM users
    """)
    
    recent_users = q("""
        SELECT tg_id, username, full_name, role, created_at
        FROM users
        ORDER BY created_at DESC
        LIMIT 10
    """)
    
    text = (
        "<b>👥 Статистика пользователей</b>\n\n"
        f"Всего: {stats['total']}\n"
        f"├ 🏭 Фабрик: {stats['factories']}\n"
        f"├ 🛍 Заказчиков: {stats['buyers']}\n"
        f"├ 🆕 Новых сегодня: {stats['new_today']}\n"
        f"└ 🚫 Заблокировано: {stats['banned']}\n\n"
        "<b>Последние регистрации:</b>\n"
    )
    
    for user in recent_users:
        role_emoji = {'factory': '🏭', 'buyer': '🛍'}.get(user['role'], '👤')
        username = f"@{user['username']}" if user['username'] else f"ID:{user['tg_id']}"
        text += f"\n{role_emoji} {username} - {user['created_at'][:16]}"
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🔍 Поиск пользователя", callback_data="admin_search_user"),
            InlineKeyboardButton(text="📊 Детальная статистика", callback_data="admin_user_stats")
        ]
    ])
    
    await msg.answer(text, reply_markup=kb)

@router.message(F.text == "🎫 Тикеты")
async def cmd_admin_tickets(msg: Message) -> None:
    """Show support tickets for admin."""
    if msg.from_user.id not in ADMIN_IDS:
        return
    
    # Get tickets stats
    ticket_stats = q1("""
        SELECT 
            COUNT(*) as total,
            COUNT(CASE WHEN status = 'open' THEN 1 END) as open,
            COUNT(CASE WHEN status = 'in_progress' THEN 1 END) as in_progress,
            COUNT(CASE WHEN status = 'closed' THEN 1 END) as closed,
            COUNT(CASE WHEN priority = 'high' THEN 1 END) as high_priority
        FROM tickets
    """)
    
    # Get recent tickets
    recent_tickets = q("""
        SELECT t.*, u.username, u.full_name
        FROM tickets t
        JOIN users u ON t.user_id = u.tg_id
        WHERE t.status IN ('open', 'in_progress')
        ORDER BY 
            CASE t.priority 
                WHEN 'high' THEN 1 
                WHEN 'normal' THEN 2 
                ELSE 3 
            END,
            t.created_at DESC
        LIMIT 10
    """)
    
    text = (
        "<b>🎫 Тикеты поддержки</b>\n\n"
        f"Всего: {ticket_stats['total']}\n"
        f"├ 🔴 Открытых: {ticket_stats['open']}\n"
        f"├ 🟡 В работе: {ticket_stats['in_progress']}\n"
        f"├ 🟢 Закрытых: {ticket_stats['closed']}\n"
        f"└ ⚡ Высокий приоритет: {ticket_stats['high_priority']}\n\n"
    )
    
    if recent_tickets:
        text += "<b>Активные обращения:</b>\n"
        for ticket in recent_tickets:
            priority_emoji = {'high': '🔴', 'normal': '🟡'}.get(ticket['priority'], '🟢')
            username = f"@{ticket['username']}" if ticket['username'] else ticket['full_name']
            text += f"\n{priority_emoji} #{ticket['id']} - {ticket['subject'][:30]}... ({username})"
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🔴 Открытые", callback_data="admin_tickets:open"),
            InlineKeyboardButton(text="🟡 В работе", callback_data="admin_tickets:in_progress")
        ],
        [
            InlineKeyboardButton(text="📋 Все тикеты", callback_data="admin_tickets:all"),
            InlineKeyboardButton(text="📊 Статистика", callback_data="admin_tickets:stats")
        ]
    ])
    
    await msg.answer(text, reply_markup=kb)

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

# ---------------------------------------------------------------------------
#  Main command handlers
# ---------------------------------------------------------------------------

@router.message(CommandStart())
async def cmd_start(msg: Message, state: FSMContext) -> None:
    """Handle /start command."""
    await state.clear()
    
    # Check if user is banned
    if is_user_banned(msg.from_user.id):
        await msg.answer("⛔ Ваш аккаунт заблокирован. Обратитесь в поддержку.")
        return
    
    # Get or create user
    user = get_or_create_user(msg.from_user)
    role = UserRole(user['role'])
    
    # Track start event
    track_event(msg.from_user.id, 'start_command', {'role': role.value})
    
    # Send appropriate greeting based on role
    if role == UserRole.FACTORY:
        factory = q1("SELECT * FROM factories WHERE tg_id = ?", (msg.from_user.id,))
        if factory and factory['is_pro']:
            await msg.answer(
                f"👋 С возвращением, {factory['name']}!\n\n"
                f"Ваш PRO-статус активен. Выберите действие:",
                reply_markup=kb_factory_menu()
            )
        else:
            await msg.answer(
                f"👋 С возвращением!\n\n"
                f"⚠️ Ваш PRO-статус неактивен. Оформите подписку для получения заявок.",
                reply_markup=kb_main(role)
            )
    elif role == UserRole.BUYER:
        orders_count = q1("SELECT COUNT(*) as cnt FROM orders WHERE buyer_id = ?", (msg.from_user.id,))
        await msg.answer(
            f"👋 С возвращением!\n\n"
            f"У вас {orders_count['cnt']} заказов. Что будем делать?",
            reply_markup=kb_buyer_menu()
        )
    elif role == UserRole.ADMIN:
        await msg.answer(
            f"👋 Добро пожаловать в админ-панель!",
            reply_markup=kb_admin_menu()
        )
    else:
        # New user
        await msg.answer(
            "<b>Добро пожаловать в Mono-Fabrique!</b> 🎉\n\n"
            "Мы соединяем швейные фабрики с заказчиками.\n\n"
            "• Фабрики получают прямые заказы\n"
            "• Заказчики находят проверенных производителей\n"
            "• Безопасные сделки через Escrow\n\n"
            "Кто вы?",
            reply_markup=kb_main()
        )

@router.message(Command("help"))
async def cmd_help(msg: Message) -> None:
    """Show help information."""
    user_role = get_user_role(msg.from_user.id)
    
    help_text = "<b>Команды бота:</b>\n\n"
    
    if user_role == UserRole.FACTORY:
        help_text += (
            "/start — главное меню\n"
            "/profile — ваш профиль фабрики\n"
            "/leads — активные заявки\n"
            "/deals — ваши сделки\n"
            "/analytics — статистика\n"
            "/balance — баланс и платежи\n"
            "/settings — настройки\n"
            "/support — поддержка"
        )
    elif user_role == UserRole.BUYER:
        help_text += (
            "/start — главное меню\n"
            "/neworder — создать заказ\n"
            "/myorders — мои заказы\n"
            "/proposals — предложения от фабрик\n"
            "/deals — мои сделки\n"
            "/factories — поиск фабрик\n"
            "/settings — настройки\n"
            "/support — поддержка"
        )
    else:
        help_text += (
            "/start — начать работу\n"
            "/help — эта справка\n"
            "/support — связаться с поддержкой\n\n"
            "Выберите в меню, кто вы — фабрика или заказчик"
        )
    
    await msg.answer(help_text, reply_markup=kb_main(user_role))

@router.message(Command("loopinfo"))
async def cmd_loop_info(msg: Message) -> None:
    """Show event loop info for admin."""
    if msg.from_user.id not in ADMIN_IDS:
        return
    
    try:
        loop = asyncio.get_running_loop()
        loop_info = (
            f"🔄 <b>Event Loop Info:</b>\n\n"
            f"Loop ID: {id(loop)}\n"
            f"Running: {loop.is_running()}\n"
            f"Closed: {loop.is_closed()}\n"
            f"Debug: {loop.get_debug()}\n"
        )
        
        # Check if we can create tasks
        try:
            test_task = loop.create_task(asyncio.sleep(0))
            await test_task
            loop_info += f"Task creation: ✅\n"
        except Exception as e:
            loop_info += f"Task creation: ❌ {e}\n"
        
        loop_info += f"\nGroup creator available: {'✅' if GROUP_CREATOR_AVAILABLE else '❌'}"
        
    except Exception as e:
        loop_info = f"❌ Error getting loop info: {e}"
    
    await msg.answer(loop_info)

# ---------------------------------------------------------------------------
#  ДОРАБОТКА: Мои заказы для покупателей
# ---------------------------------------------------------------------------

@router.message(F.text == "📋 Мои заказы")
async def cmd_my_orders(msg: Message) -> None:
    """Show buyer's orders without selected factory."""
    user_role = get_user_role(msg.from_user.id)
    if user_role != UserRole.BUYER:
        return
    
    # Get active orders without selected factory
    active_orders = q("""
        SELECT * FROM orders 
        WHERE buyer_id = ? 
          AND is_active = 1 
          AND NOT EXISTS (
              SELECT 1 FROM deals d 
              WHERE d.order_id = orders.id 
                AND d.status NOT IN ('CANCELLED')
          )
        ORDER BY created_at DESC
    """, (msg.from_user.id,))
    
    if not active_orders:
        await msg.answer(
            "У вас нет активных заказов без выбранной фабрики.\n\n"
            "Создайте новый заказ или проверьте раздел «Мои сделки».",
            reply_markup=kb_buyer_menu()
        )
        return
    
    await msg.answer(
        f"<b>Ваши активные заказы ({len(active_orders)})</b>\n\n"
        "Заказы, по которым еще не выбрана фабрика:",
        reply_markup=kb_buyer_menu()
    )
    
    for order in active_orders:
        # Get proposals count
        proposals_count = q1(
            "SELECT COUNT(*) as cnt FROM proposals WHERE order_id = ?",
            (order['id'],)
        )
        
        buttons = [
            [
                InlineKeyboardButton(text="✏️ Изменить", callback_data=f"edit_order:{order['id']}"),
                InlineKeyboardButton(text="❌ Отменить", callback_data=f"cancel_order:{order['id']}")
            ]
        ]
        
        if proposals_count and proposals_count['cnt'] > 0:
            buttons.append([
                InlineKeyboardButton(
                    text=f"👀 Предложения ({proposals_count['cnt']})", 
                    callback_data=f"view_proposals:{order['id']}"
                )
            ])
        
        kb = InlineKeyboardMarkup(inline_keyboard=buttons)
        
        caption = order_caption(order, detailed=True)
        caption += f"\n\n💌 Предложений: {proposals_count['cnt'] if proposals_count else 0}"
        
        await msg.answer(caption, reply_markup=kb)

@router.callback_query(F.data.startswith("edit_order:"))
async def edit_order_start(call: CallbackQuery, state: FSMContext) -> None:
    """Start editing order."""
    order_id = int(call.data.split(":", 1)[1])
    
    # Verify ownership
    order = q1("SELECT * FROM orders WHERE id = ? AND buyer_id = ?", (order_id, call.from_user.id))
    if not order:
        await call.answer("Заказ не найден", show_alert=True)
        return
    
    # Check if order has active deal
    deal = q1("SELECT * FROM deals WHERE order_id = ? AND status NOT IN ('CANCELLED')", (order_id,))
    if deal:
        await call.answer("Нельзя изменить заказ с активной сделкой", show_alert=True)
        return
    
    await state.update_data(edit_order_id=order_id)
    await state.set_state(EditOrderForm.field_selection)
    
    buttons = [
        [InlineKeyboardButton(text="📝 Название", callback_data="edit_order_field:title")],
        [InlineKeyboardButton(text="📦 Категория", callback_data="edit_order_field:category")],
        [InlineKeyboardButton(text="🔢 Количество", callback_data="edit_order_field:quantity")],
        [InlineKeyboardButton(text="💰 Бюджет", callback_data="edit_order_field:budget")],
        [InlineKeyboardButton(text="📍 Город", callback_data="edit_order_field:destination")],
        [InlineKeyboardButton(text="📅 Срок", callback_data="edit_order_field:lead_time")],
        [InlineKeyboardButton(text="📝 Описание", callback_data="edit_order_field:description")],
        [InlineKeyboardButton(text="⚙️ Требования", callback_data="edit_order_field:requirements")],
        [InlineKeyboardButton(text="📎 Файл", callback_data="edit_order_field:file")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_edit_order")]
    ]
    
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    await call.message.edit_text(
        f"<b>Редактирование заказа #Z-{order_id}</b>\n\n"
        f"Что хотите изменить?",
        reply_markup=kb
    )
    await call.answer()

@router.callback_query(F.data.startswith("edit_order_field:"))
async def edit_order_field(call: CallbackQuery, state: FSMContext) -> None:
    """Handle order field editing."""
    field = call.data.split(":", 1)[1]
    
    field_names = {
        'title': 'название заказа',
        'category': 'категория',
        'quantity': 'количество',
        'budget': 'бюджет за единицу',
        'destination': 'город доставки',
        'lead_time': 'срок изготовления',
        'description': 'описание',
        'requirements': 'требования к фабрике',
        'file': 'файл с техническим заданием'
    }
    
    await state.update_data(edit_field=field)
    
    if field == 'category':
        await state.set_state(EditOrderForm.category)
        await call.message.edit_text(
            "Выберите новую категорию:",
            reply_markup=kb_categories()
        )
    elif field == 'file':
        await state.set_state(EditOrderForm.file)
        await call.message.edit_text(
            "Отправьте новый файл с техническим заданием\nили напишите «удалить» для удаления текущего файла:"
        )
    else:
        # Set appropriate state
        state_map = {
            'title': EditOrderForm.title,
            'quantity': EditOrderForm.quantity,
            'budget': EditOrderForm.budget,
            'destination': EditOrderForm.destination,
            'lead_time': EditOrderForm.lead_time,
            'description': EditOrderForm.description,
            'requirements': EditOrderForm.requirements
        }
        
        await state.set_state(state_map[field])
        await call.message.edit_text(
            f"Введите новое значение для поля «{field_names[field]}»:"
        )
    
    await call.answer()

@router.message(EditOrderForm.title)
async def edit_order_title(msg: Message, state: FSMContext) -> None:
    """Edit order title."""
    if not msg.text or len(msg.text) < 5:
        await msg.answer("❌ Введите название заказа (минимум 5 символов):")
        return
    
    data = await state.get_data()
    order_id = data['edit_order_id']
    
    run("UPDATE orders SET title = ? WHERE id = ?", (msg.text.strip(), order_id))
    
    await msg.answer(
        "✅ Название заказа обновлено!",
        reply_markup=kb_buyer_menu()
    )
    await state.clear()

@router.message(EditOrderForm.quantity)
async def edit_order_quantity(msg: Message, state: FSMContext) -> None:
    """Edit order quantity."""
    qty = parse_digits(msg.text or "")
    if not qty or qty < 1:
        await msg.answer("❌ Укажите корректное количество:")
        return
    
    data = await state.get_data()
    order_id = data['edit_order_id']
    
    run("UPDATE orders SET quantity = ? WHERE id = ?", (qty, order_id))
    
    await msg.answer(
        "✅ Количество обновлено!",
        reply_markup=kb_buyer_menu()
    )
    await state.clear()

@router.message(EditOrderForm.budget)
async def edit_order_budget(msg: Message, state: FSMContext) -> None:
    """Edit order budget."""
    price = parse_digits(msg.text or "")
    if not price or price < 1:
        await msg.answer("❌ Укажите корректную цену:")
        return
    
    data = await state.get_data()
    order_id = data['edit_order_id']
    
    run("UPDATE orders SET budget = ? WHERE id = ?", (price, order_id))
    
    await msg.answer(
        "✅ Бюджет обновлен!",
        reply_markup=kb_buyer_menu()
    )
    await state.clear()

@router.message(EditOrderForm.destination)
async def edit_order_destination(msg: Message, state: FSMContext) -> None:
    """Edit order destination."""
    if not msg.text or len(msg.text) < 2:
        await msg.answer("❌ Введите название города:")
        return
    
    data = await state.get_data()
    order_id = data['edit_order_id']
    
    run("UPDATE orders SET destination = ? WHERE id = ?", (msg.text.strip(), order_id))
    
    await msg.answer(
        "✅ Город доставки обновлен!",
        reply_markup=kb_buyer_menu()
    )
    await state.clear()

@router.message(EditOrderForm.lead_time)
async def edit_order_lead_time(msg: Message, state: FSMContext) -> None:
    """Edit order lead time."""
    days = parse_digits(msg.text or "")
    if not days or days < 1:
        await msg.answer("❌ Укажите количество дней:")
        return
    
    data = await state.get_data()
    order_id = data['edit_order_id']
    
    run("UPDATE orders SET lead_time = ? WHERE id = ?", (days, order_id))
    
    await msg.answer(
        "✅ Срок изготовления обновлен!",
        reply_markup=kb_buyer_menu()
    )
    await state.clear()

@router.message(EditOrderForm.description)
async def edit_order_description(msg: Message, state: FSMContext) -> None:
    """Edit order description."""
    if not msg.text or len(msg.text) < 20:
        await msg.answer("❌ Опишите заказ подробнее (минимум 20 символов):")
        return
    
    data = await state.get_data()
    order_id = data['edit_order_id']
    
    run("UPDATE orders SET description = ? WHERE id = ?", (msg.text.strip(), order_id))
    
    await msg.answer(
        "✅ Описание обновлено!",
        reply_markup=kb_buyer_menu()
    )
    await state.clear()

@router.message(EditOrderForm.requirements)
async def edit_order_requirements(msg: Message, state: FSMContext) -> None:
    """Edit order requirements."""
    requirements = ""
    if msg.text and msg.text.lower() not in ["нет", "no", "skip"]:
        requirements = msg.text.strip()
    
    data = await state.get_data()
    order_id = data['edit_order_id']
    
    run("UPDATE orders SET requirements = ? WHERE id = ?", (requirements, order_id))
    
    await msg.answer(
        "✅ Требования обновлены!",
        reply_markup=kb_buyer_menu()
    )
    await state.clear()

@router.message(EditOrderForm.file, F.document | F.photo | F.text)
async def edit_order_file(msg: Message, state: FSMContext) -> None:
    """Edit order file."""
    file_id = None
    
    if msg.text and msg.text.lower() in ["удалить", "delete"]:
        file_id = None
    elif msg.text and msg.text.lower() in ["пропустить", "skip"]:
        await msg.answer("Файл не изменен.", reply_markup=kb_buyer_menu())
        await state.clear()
        return
    elif msg.document:
        file_id = msg.document.file_id
    elif msg.photo:
        file_id = msg.photo[-1].file_id
    else:
        await msg.answer("Отправьте файл/фото или напишите «удалить»/«пропустить»:")
        return
    
    data = await state.get_data()
    order_id = data['edit_order_id']
    
    run("UPDATE orders SET file_id = ? WHERE id = ?", (file_id, order_id))
    
    if file_id:
        await msg.answer("✅ Файл обновлен!", reply_markup=kb_buyer_menu())
    else:
        await msg.answer("✅ Файл удален!", reply_markup=kb_buyer_menu())
    
    await state.clear()

@router.callback_query(F.data.startswith("cat:"), EditOrderForm.category)
async def edit_order_category(call: CallbackQuery, state: FSMContext) -> None:
    """Edit order category."""
    category = call.data.split(":", 1)[1]
    
    if category == "done":
        await call.answer("Выберите одну категорию!", show_alert=True)
        return
    
    data = await state.get_data()
    order_id = data['edit_order_id']
    
    run("UPDATE orders SET category = ? WHERE id = ?", (category, order_id))
    
    await call.message.edit_text(
        f"✅ Категория изменена на: {category.capitalize()}"
    )
    
    await asyncio.sleep(2)
    await bot.send_message(
        call.from_user.id,
        "Категория обновлена!",
        reply_markup=kb_buyer_menu()
    )
    
    await state.clear()
    await call.answer()

@router.callback_query(F.data.startswith("cancel_order:"))
async def cancel_order_confirm(call: CallbackQuery) -> None:
    """Confirm order cancellation."""
    order_id = int(call.data.split(":", 1)[1])
    
    # Verify ownership
    order = q1("SELECT * FROM orders WHERE id = ? AND buyer_id = ?", (order_id, call.from_user.id))
    if not order:
        await call.answer("Заказ не найден", show_alert=True)
        return
    
    # Check if order has active deal
    deal = q1("SELECT * FROM deals WHERE order_id = ? AND status NOT IN ('CANCELLED')", (order_id,))
    if deal:
        await call.answer("Нельзя отменить заказ с активной сделкой", show_alert=True)
        return
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="❌ Да, отменить", callback_data=f"confirm_cancel_order:{order_id}"),
            InlineKeyboardButton(text="✅ Нет, оставить", callback_data="cancel_order_cancel")
        ]
    ])
    
    await call.message.edit_text(
        f"<b>Отмена заказа #Z-{order_id}</b>\n\n"
        f"⚠️ Вы уверены, что хотите отменить этот заказ?\n\n"
        f"Это действие необратимо. Заказ будет деактивирован,\n"
        f"а все предложения от фабрик будут отклонены.",
        reply_markup=kb
    )
    await call.answer()

@router.callback_query(F.data.startswith("confirm_cancel_order:"))
async def cancel_order_execute(call: CallbackQuery) -> None:
    """Execute order cancellation."""
    order_id = int(call.data.split(":", 1)[1])
    
    # Deactivate order
    run("UPDATE orders SET is_active = 0 WHERE id = ?", (order_id,))
    
    # Notify factories with proposals
    proposals = q("""
        SELECT factory_id FROM proposals 
        WHERE order_id = ? AND is_accepted = 0
    """, (order_id,))
    
    for proposal in proposals:
        await send_notification(
            proposal['factory_id'],
            'order_cancelled',
            'Заказ отменен',
            f'Заказчик отменил заказ #Z-{order_id}, на который вы отправляли предложение.',
            {'order_id': order_id}
        )
    
    await call.message.edit_text(
        f"✅ Заказ #Z-{order_id} успешно отменен.\n\n"
        f"Все заинтересованные фабрики получили уведомление."
    )
    
    await call.answer("Заказ отменен")

@router.callback_query(F.data == "cancel_order_cancel")
async def cancel_order_cancel(call: CallbackQuery) -> None:
    """Cancel order cancellation."""
    await call.message.edit_text("❌ Отмена заказа отменена")
    await call.answer()

@router.callback_query(F.data == "cancel_edit_order")
async def cancel_edit_order(call: CallbackQuery, state: FSMContext) -> None:
    """Cancel order editing."""
    await state.clear()
    await call.message.edit_text("❌ Редактирование заказа отменено")
    await call.answer()

# ---------------------------------------------------------------------------
#  ДОРАБОТКА: Исправленный раздел "Предложения" для покупателей
# ---------------------------------------------------------------------------

@router.message(F.text == "💌 Предложения")
async def cmd_buyer_proposals_fixed(msg: Message) -> None:
    """Show all proposals for buyer's orders - ИСПРАВЛЕННАЯ ВЕРСИЯ."""
    user_role = get_user_role(msg.from_user.id)
    if user_role != UserRole.BUYER:
        return
    
    # Get orders with proposals that don't have selected factory yet
    orders_with_proposals = q("""
        SELECT DISTINCT o.*, 
               COUNT(p.id) as proposal_count,
               MAX(p.created_at) as last_proposal
        FROM orders o
        JOIN proposals p ON o.id = p.order_id
        WHERE o.buyer_id = ? 
          AND o.is_active = 1
          AND NOT EXISTS (
              SELECT 1 FROM deals d 
              WHERE d.order_id = o.id 
                AND d.status NOT IN ('CANCELLED')
          )
        GROUP BY o.id
        ORDER BY last_proposal DESC
    """, (msg.from_user.id,))
    
    if not orders_with_proposals:
        await msg.answer(
            "У вас пока нет предложений от фабрик.\n\n"
            "Создайте заказ, и фабрики начнут присылать предложения!",
            reply_markup=kb_buyer_menu()
        )
        return
    
    await msg.answer(
        f"<b>Заказы с предложениями ({len(orders_with_proposals)})</b>\n\n"
        "Предложения от фабрик по вашим активным заказам:",
        reply_markup=kb_buyer_menu()
    )
    
    for order in orders_with_proposals:
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text=f"👀 Смотреть {order['proposal_count']} предложений", 
                callback_data=f"view_proposals:{order['id']}"
            )
        ]])
        
        caption = order_caption(order)
        caption += f"\n\n💌 Предложений: {order['proposal_count']}"
        caption += f"\n📅 Последнее: {order['last_proposal'][:16]}"
        
        await msg.answer(caption, reply_markup=kb)

# ---------------------------------------------------------------------------
#  ДОРАБОТКА: Кнопка "О фабрике" в предложениях
# ---------------------------------------------------------------------------

@router.callback_query(F.data.startswith("factory_info:"))
async def show_factory_info(call: CallbackQuery) -> None:
    """Show detailed factory information."""
    factory_id = int(call.data.split(":", 1)[1])
    
    # Get factory details
    factory = q1("SELECT * FROM factories WHERE tg_id = ?", (factory_id,))
    if not factory:
        await call.answer("Фабрика не найдена", show_alert=True)
        return
    
    # Get factory photos
    photos = q("SELECT * FROM factory_photos WHERE factory_id = ? ORDER BY is_primary DESC, created_at", (factory_id,))
    
    # Get factory stats
    stats = q1("""
        SELECT 
            COUNT(DISTINCT d.id) as total_deals,
            COUNT(CASE WHEN d.status = 'DELIVERED' THEN 1 END) as completed_deals,
            SUM(CASE WHEN d.status = 'DELIVERED' THEN d.amount ELSE 0 END) as total_revenue,
            AVG(CASE WHEN d.status = 'DELIVERED' THEN d.amount ELSE NULL END) as avg_deal_size
        FROM deals d
        WHERE d.factory_id = ?
    """, (factory_id,))
    
    # Get recent reviews
    recent_reviews = q("""
        SELECT r.*, u.full_name as buyer_name
        FROM ratings r
        JOIN users u ON r.buyer_id = u.tg_id
        WHERE r.factory_id = ?
        ORDER BY r.created_at DESC
        LIMIT 3
    """, (factory_id,))
    
    # Build factory info text
    info_text = (
        f"<b>🏭 {factory['name']}</b>\n\n"
        f"📍 Адрес: {factory['address']}\n"
        f"🏷 ИНН: {factory['inn']}\n"
    )
    
    # Categories
    if factory['categories']:
        categories = factory['categories'].split(',')
        categories_text = ", ".join([c.capitalize() for c in categories[:5]])
        if len(categories) > 5:
            categories_text += f" +{len(categories) - 5}"
        info_text += f"📦 Категории: {categories_text}\n"
    
    # Production capacity
    info_text += (
        f"📊 Партии: {format_price(factory['min_qty'])} - {format_price(factory['max_qty'])} шт.\n"
        f"💰 Средняя цена: {format_price(factory['avg_price'])} ₽\n\n"
    )
    
    # Rating and stats
    if factory['rating_count'] > 0:
        info_text += f"⭐ Рейтинг: {factory['rating']:.1f}/5.0 ({factory['rating_count']} отзывов)\n"
    else:
        info_text += "⭐ Рейтинг: пока нет отзывов\n"
    
    info_text += f"✅ Выполнено заказов: {factory['completed_orders']}\n"
    
    if stats and stats['total_deals'] > 0:
        info_text += f"🤝 Всего сделок: {stats['total_deals']}\n"
        if stats['total_revenue']:
            info_text += f"💵 Общий оборот: {format_price(stats['total_revenue'])} ₽\n"
    
    # Description
    if factory['description']:
        info_text += f"\n📝 <b>О фабрике:</b>\n{factory['description'][:300]}"
        if len(factory['description']) > 300:
            info_text += "..."
    
    # Portfolio link
    if factory['portfolio']:
        info_text += f"\n\n🔗 Портфолио: {factory['portfolio']}"
    
    # Recent reviews
    if recent_reviews:
        info_text += f"\n\n<b>Последние отзывы:</b>\n"
        for review in recent_reviews:
            stars = "⭐" * review['rating']
            info_text += f"\n{stars} — {review['buyer_name']}"
            if review['comment']:
                info_text += f"\n💬 {review['comment'][:100]}"
                if len(review['comment']) > 100:
                    info_text += "..."
            info_text += "\n"
    
    # PRO status
    info_text += f"\n<b>Статус:</b> "
    if factory['is_pro']:
        if factory['pro_expires']:
            info_text += f"✅ PRO до {factory['pro_expires'][:10]}"
        else:
            info_text += "✅ PRO (активен)"
    else:
        info_text += "❌ Базовый"
    
    buttons = []
    
    # Contact button
    buttons.append([
        InlineKeyboardButton(text="💬 Написать фабрике", url=f"tg://user?id={factory_id}")
    ])
    
    # Back button
    buttons.append([
        InlineKeyboardButton(text="◀️ Назад к предложениям", callback_data="back_to_proposals")
    ])
    
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    # If there are photos, send them first
    if photos:
        try:
            # Send primary photo with caption
            primary_photo = next((p for p in photos if p['is_primary']), photos[0])
            await call.message.answer_photo(
                primary_photo['file_id'],
                caption=info_text,
                reply_markup=kb
            )
            
            # Send additional photos if any
            other_photos = [p for p in photos if not p['is_primary']]
            if other_photos:
                media_group = []
                for photo in other_photos[:3]:  # Max 3 additional photos
                    media_group.append({"type": "photo", "media": photo['file_id']})
                
                if media_group:
                    # Note: media_group sending would need additional import and handling
                    # For now, just send them one by one
                    for photo in other_photos[:2]:
                        await call.message.answer_photo(photo['file_id'])
        except Exception as e:
            logger.error(f"Error sending factory photos: {e}")
            # Fallback to text message
            await call.message.answer(info_text, reply_markup=kb)
    else:
        # No photos, send text message
        await call.message.answer(info_text, reply_markup=kb)
    
    await call.answer()

@router.callback_query(F.data == "back_to_proposals")
async def back_to_proposals(call: CallbackQuery) -> None:
    """Go back to proposals list."""
    await call.message.delete()
    await call.answer()

# ---------------------------------------------------------------------------
#  ДОРАБОТКА: Система оплат для образцов
# ---------------------------------------------------------------------------

@router.callback_query(F.data.startswith("pay_sample:"))
async def pay_sample_init(call: CallbackQuery, state: FSMContext) -> None:
    """Initialize sample payment."""
    deal_id = int(call.data.split(":", 1)[1])
    
    # Get deal info
    deal = q1("""
        SELECT d.*, o.title, f.name as factory_name, p.sample_cost
        FROM deals d
        JOIN orders o ON d.order_id = o.id
        JOIN factories f ON d.factory_id = f.tg_id
        LEFT JOIN proposals p ON d.order_id = p.order_id AND d.factory_id = p.factory_id
        WHERE d.id = ? AND d.buyer_id = ?
    """, (deal_id, call.from_user.id))
    
    if not deal:
        await call.answer("Сделка не найдена", show_alert=True)
        return
    
    sample_cost = deal['sample_cost'] if deal['sample_cost'] else 0
    
    if sample_cost == 0:
        # Skip sample payment
        run("""
            UPDATE deals 
            SET status = 'SAMPLE_PASS', sample_cost = 0
            WHERE id = ?
        """, (deal_id,))
        
        await call.message.edit_text(
            "✅ Образец бесплатный!\n\n"
            "Сделка переходит к следующему этапу.\n"
            "Ожидайте фото образца от фабрики."
        )
        
        # Notify factory
        await send_notification(
            deal['factory_id'],
            'sample_approved',
            'Образец одобрен',
            f'Заказчик принял бесплатный образец по сделке #{deal_id}. Можете приступать к производству.',
            {'deal_id': deal_id}
        )
        
        await call.answer()
        return
    
    # Create payment for sample
    try:
        user_id = call.from_user.id
        amount = sample_cost
        description = f"Оплата образца по сделке #{deal_id}"
        return_url = "https://t.me/your_bot_username"  # Замените на ваш бот
        
        payment = create_payment(amount, description, return_url, metadata={
            "user_id": user_id,
            "deal_id": deal_id,
            "type": "sample"
        })
        
        payment_id = payment.id
        pay_url = payment.confirmation.confirmation_url
        
        # Save payment info
        payment_db_id = insert_and_get_id("""
            INSERT INTO payments 
            (user_id, type, amount, status, reference_type, reference_id, transaction_id, payment_data)
            VALUES (?, 'sample', ?, 'pending', 'deal', ?, ?, ?)
        """, (user_id, amount, deal_id, payment_id, json.dumps(payment.json())))
        
        # Save payment info to state for checking
        await state.update_data(
            payment_id=payment_id,
            payment_db_id=payment_db_id,
            deal_id=deal_id
        )
        
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💳 Оплатить образец", url=pay_url)],
            [InlineKeyboardButton(text="🔄 Проверить оплату", callback_data=f"check_sample_payment:{deal_id}")]
        ])
        
        await call.message.edit_text(
            f"💳 <b>Оплата образца</b>\n\n"
            f"Сделка: #{deal_id}\n"
            f"Фабрика: {deal['factory_name']}\n"
            f"Заказ: {deal['title']}\n\n"
            f"Стоимость образца: {format_price(sample_cost)} ₽\n\n"
            f"После оплаты фабрика изготовит образец и пришлет фото для согласования.",
            reply_markup=kb
        )
        
        await call.answer()
        
    except Exception as e:
        logger.error(f"Error creating sample payment: {e}")
        await call.answer("Ошибка при создании платежа", show_alert=True)

@router.callback_query(F.data.startswith("check_sample_payment:"))
async def check_sample_payment(call: CallbackQuery, state: FSMContext) -> None:
    """Check sample payment status."""
    deal_id = int(call.data.split(":", 1)[1])
    
    data = await state.get_data()
    payment_id = data.get('payment_id')
    payment_db_id = data.get('payment_db_id')
    
    if not payment_id:
        await call.answer("Платеж не найден", show_alert=True)
        return
    
    try:
        # Check payment status with YooKassa
        payment_status = check_payment(payment_id)
        
        if payment_status == 'succeeded':
            # Update payment in DB
            run("""
                UPDATE payments 
                SET status = 'completed', completed_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (payment_db_id,))
            
            # Update deal status
            run("""
                UPDATE deals 
                SET status = 'SAMPLE_PASS'
                WHERE id = ?
            """, (deal_id,))
            
            # Get deal info for notification
            deal = q1("""
                SELECT d.*, o.title, f.name as factory_name
                FROM deals d
                JOIN orders o ON d.order_id = o.id
                JOIN factories f ON d.factory_id = f.tg_id
                WHERE d.id = ?
            """, (deal_id,))
            
            # Track event
            track_event(call.from_user.id, 'sample_paid', {
                'deal_id': deal_id,
                'amount': data.get('amount', 0)
            })
            
            await call.message.edit_text(
                "✅ <b>Образец оплачен!</b>\n\n"
                f"Сделка #{deal_id} переходит к следующему этапу.\n\n"
                "Фабрика получила уведомление и приступит к изготовлению образца.\n"
                "Вы получите фото для согласования."
            )
            
            # Notify factory
            await send_notification(
                deal['factory_id'],
                'sample_paid',
                'Образец оплачен!',
                f'Заказчик оплатил образец по сделке #{deal_id}. Приступайте к изготовлению и пришлите фото для согласования.',
                {'deal_id': deal_id}
            )
            
            # Notify admins
            await notify_admins(
                'sample_paid',
                '💰 Образец оплачен',
                f"Сделка #{deal_id}\n"
                f"Заказ: {deal['title']}\n"
                f"Фабрика: {deal['factory_name']}\n"
                f"Заказчик оплатил образец.",
                {
                    'deal_id': deal_id,
                    'order_id': deal['order_id']
                }
            )
            
            await state.clear()
            await call.answer("Образец оплачен!")
            
        elif payment_status == 'canceled':
            await call.answer("Платеж отменен", show_alert=True)
            
        else:
            await call.answer("Платеж еще не завершен. Попробуйте позже.", show_alert=True)
            
    except Exception as e:
        logger.error(f"Error checking sample payment: {e}")
        await call.answer("Ошибка при проверке платежа", show_alert=True)

# ---------------------------------------------------------------------------
#  ДОРАБОТКА: Каталог фабрик с пагинацией
# ---------------------------------------------------------------------------

@router.message(F.text == "🔍 Поиск фабрик")
async def cmd_factories_catalog(msg: Message) -> None:
    """Show factories catalog with pagination."""
    await show_factories_page(msg.from_user.id, 0, msg.message_id)

async def show_factories_page(user_id: int, page: int = 0, edit_message_id: int = None):
    """Show factories catalog page."""
    page_size = 5
    offset = page * page_size
    
    # Get total count
    total_count = q1("SELECT COUNT(*) as cnt FROM factories WHERE is_pro = 1")['cnt']
    
    if total_count == 0:
        text = "В каталоге пока нет PRO-фабрик."
        if edit_message_id:
            await bot.edit_message_text(text, user_id, edit_message_id)
        else:
            await bot.send_message(user_id, text, reply_markup=kb_buyer_menu())
        return
    
    # Get factories for current page
    factories = q("""
        SELECT * FROM factories 
        WHERE is_pro = 1 
        ORDER BY rating DESC, completed_orders DESC, created_at DESC
        LIMIT ? OFFSET ?
    """, (page_size, offset))
    
    total_pages = (total_count + page_size - 1) // page_size
    
    # Header
    text = (
        f"<b>🏭 Каталог фабрик</b>\n\n"
        f"Страница {page + 1} из {total_pages} (всего: {total_count})\n\n"
    )
    
    # Navigation buttons
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton(text="◀️ Назад", callback_data=f"factories_page:{page-1}"))
    if page < total_pages - 1:
        nav_buttons.append(InlineKeyboardButton(text="Вперед ▶️", callback_data=f"factories_page:{page+1}"))
    
    buttons = []
    if nav_buttons:
        buttons.append(nav_buttons)
    
    # Add filter button
    buttons.append([
        InlineKeyboardButton(text="🔍 Фильтры", callback_data="factories_filters")
    ])
    
    kb = InlineKeyboardMarkup(inline_keyboard=buttons) if buttons else None
    
    if edit_message_id:
        await bot.edit_message_text(text, user_id, edit_message_id, reply_markup=kb)
    else:
        await bot.send_message(user_id, text, reply_markup=kb)
    
    # Send individual factory cards
    for factory in factories:
        await send_factory_card(user_id, factory)

async def send_factory_card(user_id: int, factory: dict):
    """Send individual factory card."""
    # Get factory photos
    photos = q("SELECT * FROM factory_photos WHERE factory_id = ? ORDER BY is_primary DESC LIMIT 1", (factory['tg_id'],))
    
    # Build factory card text
    card_text = (
        f"<b>🏭 {factory['name']}</b>\n"
        f"📍 {factory['address']}\n"
    )
    
    # Categories
    if factory['categories']:
        categories = factory['categories'].split(',')
        categories_text = ", ".join([c.capitalize() for c in categories[:3]])
        if len(categories) > 3:
            categories_text += f" +{len(categories) - 3}"
        card_text += f"📦 {categories_text}\n"
    
    # Stats
    card_text += (
        f"📊 Партии: {format_price(factory['min_qty'])} - {format_price(factory['max_qty'])} шт.\n"
        f"💰 От {format_price(factory['avg_price'])} ₽/шт.\n"
    )
    
    # Rating
    if factory['rating_count'] > 0:
        card_text += f"⭐ {factory['rating']:.1f}/5.0 ({factory['rating_count']} отзывов)\n"
    else:
        card_text += "⭐ Пока нет отзывов\n"
    
    card_text += f"✅ Выполнено: {factory['completed_orders']} заказов"
    
    # Description snippet
    if factory['description']:
        desc_snippet = factory['description'][:100]
        if len(factory['description']) > 100:
            desc_snippet += "..."
        card_text += f"\n\n📝 {desc_snippet}"
    
    buttons = [
        [
            InlineKeyboardButton(text="👀 Подробнее", callback_data=f"factory_info:{factory['tg_id']}"),
            InlineKeyboardButton(text="💬 Написать", url=f"tg://user?id={factory['tg_id']}")
        ]
    ]
    
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    # Send with photo if available
    if photos:
        try:
            await bot.send_photo(
                user_id,
                photos[0]['file_id'],
                caption=card_text,
                reply_markup=kb
            )
        except Exception as e:
            logger.error(f"Error sending factory photo: {e}")
            await bot.send_message(user_id, card_text, reply_markup=kb)
    else:
        await bot.send_message(user_id, card_text, reply_markup=kb)

@router.callback_query(F.data.startswith("factories_page:"))
async def factories_page_handler(call: CallbackQuery) -> None:
    """Handle factories pagination."""
    page = int(call.data.split(":", 1)[1])
    await show_factories_page(call.from_user.id, page, call.message.message_id)
    await call.answer()

@router.callback_query(F.data == "factories_filters")
async def factories_filters(call: CallbackQuery) -> None:
    """Show factory filters (placeholder for now)."""
    await call.answer("Фильтры будут добавлены в следующем обновлении", show_alert=True)

# ---------------------------------------------------------------------------
#  ДОРАБОТКА: История заказов для профиля
# ---------------------------------------------------------------------------

@router.callback_query(F.data == "order_history")
async def show_order_history(call: CallbackQuery) -> None:
    """Show order history for buyer."""
    # Get all orders (active and inactive)
    all_orders = q("""
        SELECT o.*, 
               CASE 
                   WHEN EXISTS(SELECT 1 FROM deals d WHERE d.order_id = o.id AND d.status = 'DELIVERED') 
                   THEN 'COMPLETED'
                   WHEN EXISTS(SELECT 1 FROM deals d WHERE d.order_id = o.id AND d.status != 'CANCELLED')
                   THEN 'IN_PROGRESS'
                   WHEN o.is_active = 0 
                   THEN 'CANCELLED'
                   ELSE 'ACTIVE'
               END as order_status,
               (SELECT COUNT(*) FROM proposals p WHERE p.order_id = o.id) as proposals_count
        FROM orders o
        WHERE o.buyer_id = ?
        ORDER BY o.created_at DESC
    """, (call.from_user.id,))
    
    if not all_orders:
        await call.message.edit_text(
            "У вас пока нет истории заказов.\n\n"
            "Создайте первый заказ, чтобы начать работу с фабриками!"
        )
        return
    
    # Group orders by status
    active_orders = [o for o in all_orders if o['order_status'] == 'ACTIVE']
    in_progress_orders = [o for o in all_orders if o['order_status'] == 'IN_PROGRESS']
    completed_orders = [o for o in all_orders if o['order_status'] == 'COMPLETED']
    cancelled_orders = [o for o in all_orders if o['order_status'] == 'CANCELLED']
    
    history_text = (
        f"<b>📋 История заказов</b>\n\n"
        f"Всего заказов: {len(all_orders)}\n\n"
    )
    
    if active_orders:
        history_text += f"🔄 <b>Активные ({len(active_orders)})</b>\n"
        for order in active_orders[:3]:
            history_text += (
                f"#Z-{order['id']} - {order['title'] or order['category']}\n"
                f"  💌 Предложений: {order['proposals_count']}\n"
                f"  📅 {order['created_at'][:10]}\n\n"
            )
        if len(active_orders) > 3:
            history_text += f"... и еще {len(active_orders) - 3}\n\n"
    
    if in_progress_orders:
        history_text += f"⚙️ <b>В работе ({len(in_progress_orders)})</b>\n"
        for order in in_progress_orders[:3]:
            history_text += (
                f"#Z-{order['id']} - {order['title'] or order['category']}\n"
                f"  📅 {order['created_at'][:10]}\n\n"
            )
        if len(in_progress_orders) > 3:
            history_text += f"... и еще {len(in_progress_orders) - 3}\n\n"
    
    if completed_orders:
        history_text += f"✅ <b>Завершенные ({len(completed_orders)})</b>\n"
        for order in completed_orders[:3]:
            history_text += (
                f"#Z-{order['id']} - {order['title'] or order['category']}\n"
                f"  📅 {order['created_at'][:10]}\n\n"
            )
        if len(completed_orders) > 3:
            history_text += f"... и еще {len(completed_orders) - 3}\n\n"
    
    if cancelled_orders:
        history_text += f"❌ <b>Отмененные ({len(cancelled_orders)})</b>\n"
        for order in cancelled_orders[:2]:
            history_text += (
                f"#Z-{order['id']} - {order['title'] or order['category']}\n"
                f"  📅 {order['created_at'][:10]}\n\n"
            )
        if len(cancelled_orders) > 2:
            history_text += f"... и еще {len(cancelled_orders) - 2}\n\n"
    
    buttons = [
        [InlineKeyboardButton(text="◀️ Назад к профилю", callback_data="back_to_profile")]
    ]
    
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    await call.message.edit_text(history_text, reply_markup=kb)
    await call.answer()

@router.callback_query(F.data == "back_to_profile")
async def back_to_profile(call: CallbackQuery) -> None:
    """Go back to profile."""
    await call.message.delete()
    # Trigger profile command
    await cmd_profile(call.message)
    await call.answer()

# ---------------------------------------------------------------------------
#  ДОРАБОТКА: Редактирование предложений фабрик
# ---------------------------------------------------------------------------

@router.callback_query(F.data == "edit_proposal")
async def edit_proposal_start(call: CallbackQuery, state: FSMContext) -> None:
    """Start editing proposal (from proposal creation flow)."""
    await state.set_state(EditProposalForm.field_selection)
    
    buttons = [
        [InlineKeyboardButton(text="💰 Цена", callback_data="edit_prop_field:price")],
        [InlineKeyboardButton(text="📅 Срок", callback_data="edit_prop_field:lead_time")],
        [InlineKeyboardButton(text="🧵 Образец", callback_data="edit_prop_field:sample_cost")],
        [InlineKeyboardButton(text="💬 Сообщение", callback_data="edit_prop_field:message")],
        [InlineKeyboardButton(text="✅ Готово", callback_data="confirm_proposal")]
    ]
    
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    await call.message.edit_text(
        "<b>Редактирование предложения</b>\n\n"
        "Что хотите изменить?",
        reply_markup=kb
    )
    await call.answer()

@router.callback_query(F.data.startswith("edit_existing_proposal:"))
async def edit_existing_proposal_start(call: CallbackQuery, state: FSMContext) -> None:
    """Start editing existing proposal."""
    proposal_id = int(call.data.split(":", 1)[1])
    
    # Get proposal
    proposal = q1("SELECT * FROM proposals WHERE id = ? AND factory_id = ?", (proposal_id, call.from_user.id))
    if not proposal:
        await call.answer("Предложение не найдено", show_alert=True)
        return
    
    # Check if proposal is not accepted yet
    if proposal['is_accepted']:
        await call.answer("Нельзя изменить принятое предложение", show_alert=True)
        return
    
    await state.update_data(edit_proposal_id=proposal_id)
    await state.set_state(EditProposalForm.field_selection)
    
    buttons = [
        [InlineKeyboardButton(text="💰 Цена", callback_data="edit_prop_field:price")],
        [InlineKeyboardButton(text="📅 Срок", callback_data="edit_prop_field:lead_time")],
        [InlineKeyboardButton(text="🧵 Образец", callback_data="edit_prop_field:sample_cost")],
        [InlineKeyboardButton(text="💬 Сообщение", callback_data="edit_prop_field:message")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_edit_proposal")]
    ]
    
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    current_data = (
        f"<b>Текущее предложение:</b>\n\n"
        f"💰 Цена: {format_price(proposal['price'])} ₽/шт.\n"
        f"📅 Срок: {proposal['lead_time']} дней\n"
        f"🧵 Образец: {format_price(proposal['sample_cost'])} ₽\n"
    )
    
    if proposal['message']:
        current_data += f"💬 Сообщение: {proposal['message'][:100]}...\n"
    
    current_data += "\nЧто хотите изменить?"
    
    await call.message.edit_text(current_data, reply_markup=kb)
    await call.answer()

@router.callback_query(F.data.startswith("edit_prop_field:"))
async def edit_proposal_field(call: CallbackQuery, state: FSMContext) -> None:
    """Handle proposal field editing."""
    field = call.data.split(":", 1)[1]
    
    field_names = {
        'price': 'цену за единицу',
        'lead_time': 'срок изготовления',
        'sample_cost': 'стоимость образца',
        'message': 'сообщение для заказчика'
    }
    
    await state.update_data(edit_prop_field=field)
    
    # Set appropriate state
    state_map = {
        'price': EditProposalForm.price,
        'lead_time': EditProposalForm.lead_time,
        'sample_cost': EditProposalForm.sample_cost,
        'message': EditProposalForm.message
    }
    
    await state.set_state(state_map[field])
    
    if field == 'message':
        await call.message.edit_text(
            f"Введите новое сообщение для заказчика\n"
            f"(или напишите «—» чтобы убрать):"
        )
    else:
        await call.message.edit_text(
            f"Введите новое значение для поля «{field_names[field]}»:"
        )
    
    await call.answer()

@router.message(EditProposalForm.price)
async def edit_proposal_price(msg: Message, state: FSMContext) -> None:
    """Edit proposal price."""
    price = parse_digits(msg.text or "")
    if not price or price < 1:
        await msg.answer("❌ Укажите корректную цену:")
        return
    
    data = await state.get_data()
    
    # Check if editing existing proposal or creating new
    if 'edit_proposal_id' in data:
        proposal_id = data['edit_proposal_id']
        run("UPDATE proposals SET price = ? WHERE id = ?", (price, proposal_id))
        await msg.answer("✅ Цена предложения обновлена!", reply_markup=kb_factory_menu())
        await state.clear()
    else:
        # Editing during creation
        await state.update_data(price=price)
        await edit_proposal_start(msg, state)

@router.message(EditProposalForm.lead_time)
async def edit_proposal_lead_time(msg: Message, state: FSMContext) -> None:
    """Edit proposal lead time."""
    days = parse_digits(msg.text or "")
    if not days or days < 1:
        await msg.answer("❌ Укажите количество дней:")
        return
    
    data = await state.get_data()
    
    if 'edit_proposal_id' in data:
        proposal_id = data['edit_proposal_id']
        run("UPDATE proposals SET lead_time = ? WHERE id = ?", (days, proposal_id))
        await msg.answer("✅ Срок изготовления обновлен!", reply_markup=kb_factory_menu())
        await state.clear()
    else:
        await state.update_data(lead_time=days)
        await edit_proposal_start(msg, state)

@router.message(EditProposalForm.sample_cost)
async def edit_proposal_sample_cost(msg: Message, state: FSMContext) -> None:
    """Edit proposal sample cost."""
    cost = parse_digits(msg.text or "0")
    if cost is None or cost < 0:
        await msg.answer("❌ Укажите корректную стоимость (или 0):")
        return
    
    data = await state.get_data()
    
    if 'edit_proposal_id' in data:
        proposal_id = data['edit_proposal_id']
        run("UPDATE proposals SET sample_cost = ? WHERE id = ?", (cost, proposal_id))
        await msg.answer("✅ Стоимость образца обновлена!", reply_markup=kb_factory_menu())
        await state.clear()
    else:
        await state.update_data(sample_cost=cost)
        await edit_proposal_start(msg, state)

@router.message(EditProposalForm.message)
async def edit_proposal_message(msg: Message, state: FSMContext) -> None:
    """Edit proposal message."""
    message = ""
    if msg.text and msg.text not in ["—", "-", "–"]:
        message = msg.text.strip()
    
    data = await state.get_data()
    
    if 'edit_proposal_id' in data:
        proposal_id = data['edit_proposal_id']
        run("UPDATE proposals SET message = ? WHERE id = ?", (message, proposal_id))
        await msg.answer("✅ Сообщение предложения обновлено!", reply_markup=kb_factory_menu())
        await state.clear()
    else:
        await state.update_data(message=message)
        await edit_proposal_start(msg, state)

@router.callback_query(F.data == "cancel_edit_proposal")
async def cancel_edit_proposal(call: CallbackQuery, state: FSMContext) -> None:
    """Cancel proposal editing."""
    await state.clear()
    await call.message.edit_text("❌ Редактирование предложения отменено")
    await call.answer()

# ---------------------------------------------------------------------------
#  ДОРАБОТКА: Групповые чаты для сделок
# ---------------------------------------------------------------------------

async def create_deal_chat(deal_id: int) -> tuple[int | None, str | None]:
    """Create group chat for deal using invite link only. Returns (chat_id, invite_link) or (None, None) on fail."""
    if not GROUP_CREATOR_AVAILABLE:
        logger.warning("Group creator not available, using fallback notification")
        await send_fallback_chat_notification(deal_id)
        return None, None

    try:
        deal = q1("""
            SELECT d.*, o.title, f.name as factory_name, u.full_name as buyer_name
            FROM deals d
            JOIN orders o ON d.order_id = o.id
            JOIN factories f ON d.factory_id = f.tg_id
            JOIN users u ON d.buyer_id = u.tg_id
            WHERE d.id = ?
        """, (deal_id,))
        if not deal:
            logger.error(f"Deal {deal_id} not found for chat creation")
            return None, None

        api_id = os.getenv("TELEGRAM_API_ID")
        api_hash = os.getenv("TELEGRAM_API_HASH")

        if not api_id or not api_hash:
            logger.error("Missing TELEGRAM_API_ID or TELEGRAM_API_HASH in environment")
            await send_fallback_chat_notification(deal_id, error="Missing TELEGRAM_API_ID or TELEGRAM_API_HASH")
            return None, None

        logger.info(f"Creating real group chat for deal {deal_id}")

        try:
            # Новая логика — только нужные данные, без user_id
            chat_id, status_message, invite_link = await create_deal_chat_real(
                deal_id=deal_id,
                deal_title=deal['title'],
                factory_name=deal['factory_name'],
                buyer_name=deal['buyer_name']
            )
        except RuntimeError as e:
            if "event loop" in str(e).lower():
                logger.error(f"Event loop conflict in chat creation: {e}")
                await send_fallback_chat_notification(deal_id, error="Event loop conflict")
                return None, None
            else:
                raise

        if chat_id and invite_link:
            if abs(chat_id) < 1000000000:
                logger.error(f"Invalid chat_id received: {chat_id}")
                await send_fallback_chat_notification(deal_id, error="Invalid chat_id")
                return None, None

            run("UPDATE deals SET chat_id = ? WHERE id = ?", (chat_id, deal_id))
            logger.info(f"Created real group chat {chat_id} for deal {deal_id}")
            await notify_chat_created(deal_id, chat_id, invite_link)
            return chat_id, invite_link
        else:
            logger.error(f"Failed to create group for deal {deal_id}: {status_message}")
            await send_fallback_chat_notification(deal_id, error=status_message)
            return None, None

    except Exception as e:
        logger.error(f"Error creating deal chat for deal {deal_id}: {e}")
        await send_fallback_chat_notification(deal_id, error=str(e))
        return None, None

async def send_fallback_chat_notification(deal_id: int, error: str = None):
    """Send fallback notification when group chat creation fails."""
    deal = q1("""
        SELECT d.*, o.title, f.name as factory_name, u.full_name as buyer_name, d.buyer_id, d.factory_id
        FROM deals d
        JOIN orders o ON d.order_id = o.id
        JOIN factories f ON d.factory_id = f.tg_id
        JOIN users u ON d.buyer_id = u.tg_id
        WHERE d.id = ?
    """, (deal_id,))
    if not deal:
        return

    fallback_message = (
        f"💬 <b>Сделка #{deal_id} создана!</b>\n\n"
        f"📦 Заказ: {deal['title']}\n"
        f"🏭 Фабрика: {deal['factory_name']}\n"
        f"👤 Заказчик: {deal['buyer_name']}\n\n"
        f"⚠️ Групповой чат временно недоступен.\n"
        f"Вы можете общаться напрямую через профили или обратиться в поддержку.\n\n"
        f"<i>Мы работаем над восстановлением функции групповых чатов.</i>"
    )
    # Send to buyer
    try:
        await bot.send_message(deal['buyer_id'], fallback_message)
    except Exception as e:
        logger.error(f"Failed to send fallback to buyer {deal['buyer_id']}: {e}")
    # Send to factory
    try:
        await bot.send_message(deal['factory_id'], fallback_message)
    except Exception as e:
        logger.error(f"Failed to send fallback to factory {deal['factory_id']}: {e}")
    # Notify admins
    admin_message = f"🚨 Не удалось создать чат для сделки #{deal_id}"
    if error:
        admin_message += f"\nОшибка: {error}"
    admin_message += f"\nПокупатель: ID {deal['buyer_id']}\nФабрика: ID {deal['factory_id']}"
    await notify_admins(
        'chat_creation_failed',
        '🚨 Ошибка создания чата',
        admin_message,
        {'deal_id': deal_id, 'buyer_id': deal['buyer_id'], 'factory_id': deal['factory_id'], 'error': error}
    )

async def notify_chat_created(deal_id: int, chat_id: int, invite_link: str):
    """Notify participants that chat was created successfully, with invite link."""
    deal = q1("""
        SELECT d.*, o.title, f.name as factory_name, u.full_name as buyer_name, d.buyer_id, d.factory_id
        FROM deals d
        JOIN orders o ON d.order_id = o.id
        JOIN factories f ON d.factory_id = f.tg_id
        JOIN users u ON d.buyer_id = u.tg_id
        WHERE d.id = ?
    """, (deal_id,))
    if not deal:
        return

    success_message = (
        f"✅ <b>Групповой чат сделки #{deal_id} создан!</b>\n\n"
        f"📦 Заказ: {deal['title']}\n"
        f"🏭 Фабрика: {deal['factory_name']}\n"
        f"👤 Заказчик: {deal['buyer_name']}\n\n"
        f"💬 Теперь вы можете общаться в общем чате. "
        f"Нажмите на кнопку <b>\"💬 Чат по сделке\"</b> чтобы перейти в группу."
    )
    # Кнопка
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="💬 Чат по сделке", url=invite_link)
    ]])
    # Send to buyer
    try:
        await bot.send_message(deal['buyer_id'], success_message, reply_markup=kb)
    except Exception as e:
        logger.error(f"Failed to notify buyer {deal['buyer_id']} about chat creation: {e}")
    # Send to factory
    try:
        await bot.send_message(deal['factory_id'], success_message, reply_markup=kb)
    except Exception as e:
        logger.error(f"Failed to notify factory {deal['factory_id']} about chat creation: {e}")

@router.callback_query(F.data.startswith("deal_chat:"))
async def deal_chat_handler(call: CallbackQuery) -> None:
    """Handle deal chat access with improved logic (invite link only)."""
    deal_id = int(call.data.split(":", 1)[1])
    deal = q1("""
        SELECT d.*, o.title, f.name as factory_name, u.full_name as buyer_name, d.buyer_id, d.factory_id, d.chat_id
        FROM deals d
        JOIN orders o ON d.order_id = o.id
        JOIN factories f ON d.factory_id = f.tg_id
        JOIN users u ON d.buyer_id = u.tg_id
        WHERE d.id = ? AND (d.buyer_id = ? OR d.factory_id = ?)
    """, (deal_id, call.from_user.id, call.from_user.id))
    if not deal:
        await call.answer("Сделка не найдена", show_alert=True)
        return

    if not GROUP_CREATOR_AVAILABLE:
        chat_info = (
            f"💬 <b>Чат сделки #{deal_id}</b>\n\n"
            f"📦 {deal['title']}\n"
            f"🏭 {deal['factory_name']}\n"
            f"👤 {deal['buyer_name']}\n\n"
            f"⚠️ Групповые чаты временно недоступны.\n"
            f"Обратитесь в поддержку или общайтесь напрямую через профили пользователей."
        )
        await call.message.answer(chat_info)
        await call.answer()
        return

    if deal['chat_id']:
        chat_id = deal['chat_id']
        # Получаем новую инвайт-ссылку на группу
        api_id = os.getenv("TELEGRAM_API_ID")
        api_hash = os.getenv("TELEGRAM_API_HASH")
        creator = TelegramGroupCreator(api_id, api_hash)
        invite_link = await creator.create_invite_link(chat_id)
        if invite_link:
            kb = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="💬 Чат по сделке", url=invite_link)
            ]])
            chat_info = (
                f"💬 <b>Чат сделки #{deal_id}</b>\n\n"
                f"📦 {deal['title']}\n"
                f"🏭 {deal['factory_name']}\n"
                f"👤 {deal['buyer_name']}\n\n"
                f"👥 Для перехода используйте кнопку ниже."
            )
        else:
            chat_info = (
                f"❌ <b>Не удалось получить ссылку на чат</b>\n\n"
                f"Обратитесь к поддержке."
            )
            kb = None
    else:
        # Создать новый чат и получить ссылку
        chat_id, invite_link = await create_deal_chat(deal_id)
        if chat_id and invite_link:
            kb = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="💬 Чат по сделке", url=invite_link)
            ]])
            chat_info = (
                f"✅ <b>Чат сделки #{deal_id} создан!</b>\n\n"
                f"📦 {deal['title']}\n"
                f"🏭 {deal['factory_name']}\n"
                f"👤 {deal['buyer_name']}\n\n"
                f"💬 Для перехода используйте кнопку ниже."
            )
        else:
            chat_info = (
                f"❌ <b>Не удалось создать чат</b>\n\n"
                f"Временные проблемы. Обратитесь в поддержку."
            )
            kb = None

    await call.message.answer(chat_info, reply_markup=kb)
    await call.answer()

@router.callback_query(F.data.startswith("recreate_chat:"))
async def recreate_chat_handler(call: CallbackQuery) -> None:
    """Handle chat recreation with invite link logic."""
    deal_id = int(call.data.split(":", 1)[1])
    deal = q1("SELECT * FROM deals WHERE id = ? AND (buyer_id = ? OR factory_id = ?)", (deal_id, call.from_user.id, call.from_user.id))
    if not deal:
        await call.answer("Доступ запрещен", show_alert=True)
        return

    run("UPDATE deals SET chat_id = NULL WHERE id = ?", (deal_id,))
    chat_id, invite_link = await create_deal_chat(deal_id)
    if chat_id and invite_link:
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="💬 Чат по сделке", url=invite_link)
        ]])
        await call.message.edit_text(
            f"✅ <b>Новый чат для сделки #{deal_id} создан!</b>\n\n"
            f"Для перехода используйте кнопку ниже.",
            reply_markup=kb
        )
    else:
        await call.message.edit_text(
            f"❌ <b>Не удалось создать новый чат</b>\n\n"
            f"Обратитесь в поддержку."
        )
    await call.answer()

# 8. Добавьте команду для проверки переменных окружения (только для админов):
@router.message(Command("checkenv"))
async def cmd_check_env(msg: Message) -> None:
    """Check environment variables for admin."""
    if msg.from_user.id not in ADMIN_IDS:
        return
    
    api_id = os.getenv("TELEGRAM_API_ID")
    api_hash = os.getenv("TELEGRAM_API_HASH") 
    
    env_status = f"🔧 <b>Статус переменных окружения:</b>\n\n"
    env_status += f"TELEGRAM_API_ID: {'✅' if api_id else '❌'} {f'({api_id[:4]}***)' if api_id else ''}\n"
    env_status += f"TELEGRAM_API_HASH: {'✅' if api_hash else '❌'} {f'({api_hash[:4]}***)' if api_hash else ''}\n"
    env_status += f"GROUP_CREATOR_AVAILABLE: {'✅' if GROUP_CREATOR_AVAILABLE else '❌'}\n"
    
    if GROUP_CREATOR_AVAILABLE:
        env_status += f"\n🧪 <b>Тест создания группы:</b>\nИспользуйте /testgroup для проверки"
    
    await msg.answer(env_status)

# 9. Добавьте тестовую команду (только для админов):
@router.message(Command("checkenv"))
async def cmd_check_env(msg: Message) -> None:
    """Check environment variables for admin."""
    if msg.from_user.id not in ADMIN_IDS:
        return
    
    api_id = os.getenv("TELEGRAM_API_ID")
    api_hash = os.getenv("TELEGRAM_API_HASH") 
    
    env_status = f"🔧 <b>Статус переменных окружения:</b>\n\n"
    env_status += f"TELEGRAM_API_ID: {'✅' if api_id else '❌'} {f'({api_id[:4]}***)' if api_id else ''}\n"
    env_status += f"TELEGRAM_API_HASH: {'✅' if api_hash else '❌'} {f'({api_hash[:4]}***)' if api_hash else ''}\n"
    env_status += f"GROUP_CREATOR_AVAILABLE: {'✅' if GROUP_CREATOR_AVAILABLE else '❌'}\n"
    
    if GROUP_CREATOR_AVAILABLE:
        env_status += f"\n🧪 <b>Тест создания группы:</b>\nИспользуйте /testgroup для проверки"
    
    await msg.answer(env_status)

@router.message(Command("testgroup"))
async def cmd_test_group(msg: Message) -> None:
    """Test group creation for admin."""
    if msg.from_user.id not in ADMIN_IDS:
        return
    
    if not GROUP_CREATOR_AVAILABLE:
        await msg.answer("❌ Group creator module not available")
        return
    
    await msg.answer("🧪 Тестируем создание группы...")
    
    try:
        api_id = os.getenv("TELEGRAM_API_ID")
        api_hash = os.getenv("TELEGRAM_API_HASH")
        
        if not all([api_id, api_hash]):
            await msg.answer("❌ Отсутствуют переменные окружения")
            return
        
        creator = TelegramGroupCreator(api_id, api_hash)
        
        # Test with admin as both buyer and factory (for testing)
        chat_id, result = await creator.create_deal_group(
            deal_id=999999,
            buyer_id=msg.from_user.id,
            factory_id=msg.from_user.id,
            admin_ids=ADMIN_IDS,
            deal_title="🧪 Test Deal - DELETE ME",
            factory_name="Test Factory",
            buyer_name="Test Buyer"
        )
        
        if chat_id:
            await msg.answer(
                f"✅ <b>Тест успешен!</b>\n\n"
                f"Создана тестовая группа: {chat_id}\n"
                f"Результат: {result}\n\n"
                f"⚠️ Удалите тестовую группу вручную!"
            )
        else:
            await msg.answer(f"❌ <b>Тест провален:</b>\n{result}")
            
    except Exception as e:
        await msg.answer(f"❌ <b>Ошибка теста:</b>\n{str(e)}")

@router.message(Command("cleanfakechats"))
async def cmd_clean_fake_chats(msg: Message) -> None:
    """Clean fake chat IDs from database."""
    if msg.from_user.id not in ADMIN_IDS:
        return
    
    # Находим все сделки с подозрительными chat_id (положительные или очень длинные)
    fake_chats = q("""
        SELECT id, chat_id FROM deals 
        WHERE chat_id IS NOT NULL 
        AND (chat_id > 0 OR LENGTH(CAST(chat_id AS TEXT)) > 15)
    """)
    
    if fake_chats:
        # Очищаем фейковые chat_id
        run("UPDATE deals SET chat_id = NULL WHERE chat_id > 0 OR LENGTH(CAST(chat_id AS TEXT)) > 15")
        
        cleaned_text = f"🧹 Очищено {len(fake_chats)} фейковых chat_id:\n\n"
        for chat in fake_chats[:10]:  # Показываем первые 10
            cleaned_text += f"Deal #{chat['id']}: {chat['chat_id']}\n"
        
        if len(fake_chats) > 10:
            cleaned_text += f"... и еще {len(fake_chats) - 10}"
        
        await msg.answer(cleaned_text)
    else:
        await msg.answer("✅ Фейковых chat_id не найдено")

# ---------------------------------------------------------------------------
#  ДОРАБОТКА: Отмена сделок с предупреждением
# ---------------------------------------------------------------------------

@router.callback_query(F.data.startswith("cancel_deal:"))
async def cancel_deal_confirm(call: CallbackQuery) -> None:
    """Confirm deal cancellation."""
    deal_id = int(call.data.split(":", 1)[1])
    
    # Get deal info
    deal = q1("""
        SELECT d.*, o.title, f.name as factory_name
        FROM deals d
        JOIN orders o ON d.order_id = o.id
        JOIN factories f ON d.factory_id = f.tg_id
        WHERE d.id = ? AND (d.buyer_id = ? OR d.factory_id = ?)
    """, (deal_id, call.from_user.id, call.from_user.id))
    
    if not deal:
        await call.answer("Сделка не найдена", show_alert=True)
        return
    
    # Check if deal can be cancelled
    if deal['status'] == 'DELIVERED':
        await call.answer("Нельзя отменить завершенную сделку", show_alert=True)
        return
    
    if deal['status'] == 'CANCELLED':
        await call.answer("Сделка уже отменена", show_alert=True)
        return
    
    status = OrderStatus(deal['status'])
    
    # Different warnings based on deal status and user role
    user_role = get_user_role(call.from_user.id)
    
    if user_role == UserRole.BUYER:
        if status in [OrderStatus.DRAFT, OrderStatus.SAMPLE_PASS]:
            warning = (
                "⚠️ <b>Отмена сделки</b>\n\n"
                f"Сделка #{deal_id} будет отменена.\n\n"
                f"Если фабрика уже начала работу над образцом, "
                f"вам может потребоваться компенсировать понесенные расходы."
            )
        else:
            warning = (
                "⚠️ <b>Отмена сделки</b>\n\n"
                f"Сделка #{deal_id} будет отменена.\n\n"
                f"🔴 <b>ВНИМАНИЕ:</b> Фабрика уже приступила к производству.\n"
                f"Вам потребуется оплатить все фактически понесенные расходы:\n"
                f"• Материалы\n"
                f"• Производственные затраты\n"
                f"• Образцы\n\n"
                f"Администрация свяжется с вами для расчета компенсации."
            )
    else:  # Factory
        warning = (
            "⚠️ <b>Отмена сделки</b>\n\n"
            f"Сделка #{deal_id} будет отменена.\n\n"
            f"Если вы понесли расходы на материалы или производство, "
            f"вы сможете запросить компенсацию через администрацию платформы."
        )
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="❌ Да, отменить", callback_data=f"confirm_cancel_deal:{deal_id}"),
            InlineKeyboardButton(text="✅ Нет, оставить", callback_data="cancel_deal_cancel")
        ]
    ])
    
    await call.message.edit_text(warning, reply_markup=kb)
    await call.answer()

@router.callback_query(F.data.startswith("confirm_cancel_deal:"))
async def cancel_deal_execute(call: CallbackQuery) -> None:
    """Execute deal cancellation."""
    deal_id = int(call.data.split(":", 1)[1])
    
    # Get deal info before cancellation
    deal = q1("""
        SELECT d.*, o.title, f.name as factory_name, u.full_name as buyer_name
        FROM deals d
        JOIN orders o ON d.order_id = o.id
        JOIN factories f ON d.factory_id = f.tg_id
        JOIN users u ON d.buyer_id = u.tg_id
        WHERE d.id = ?
    """, (deal_id,))
    
    if not deal:
        await call.answer("Сделка не найдена", show_alert=True)
        return
    
    user_role = get_user_role(call.from_user.id)
    cancelled_by = "заказчиком" if user_role == UserRole.BUYER else "фабрикой"
    
    # Cancel deal
    run("""
        UPDATE deals 
        SET status = 'CANCELLED', updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
    """, (deal_id,))
    
    # Reactivate order if cancelled early
    if deal['status'] in ['DRAFT', 'SAMPLE_PASS']:
        run("UPDATE orders SET is_active = 1 WHERE id = ?", (deal['order_id'],))
    
    # Track event
    track_event(call.from_user.id, 'deal_cancelled', {
        'deal_id': deal_id,
        'order_id': deal['order_id'],
        'cancelled_by': cancelled_by,
        'status_when_cancelled': deal['status']
    })
    
    # Notify other party
    if user_role == UserRole.BUYER:
        # Notify factory
        await send_notification(
            deal['factory_id'],
            'deal_cancelled',
            'Сделка отменена заказчиком',
            f'Заказчик отменил сделку #{deal_id} ({deal["title"]}). '
            f'Если вы понесли расходы, обратитесь в поддержку для получения компенсации.',
            {'deal_id': deal_id}
        )
    else:
        # Notify buyer
        await send_notification(
            deal['buyer_id'],
            'deal_cancelled',
            'Сделка отменена фабрикой',
            f'Фабрика {deal["factory_name"]} отменила сделку #{deal_id} ({deal["title"]}). '
            f'Обратитесь в поддержку для выяснения обстоятельств.',
            {'deal_id': deal_id}
        )
    
    # Notify admins
    await notify_admins(
        'deal_cancelled',
        f'🚫 Сделка отменена {cancelled_by}',
        f"Сделка #{deal_id}\n"
        f"Заказ: {deal['title']}\n"
        f"Фабрика: {deal['factory_name']}\n"
        f"Заказчик: {deal['buyer_name']}\n"
        f"Статус на момент отмены: {deal['status']}\n"
        f"Отменена: {cancelled_by}",
        {
            'deal_id': deal_id,
            'order_id': deal['order_id'],
            'cancelled_by': cancelled_by,
            'requires_compensation': deal['status'] not in ['DRAFT', 'SAMPLE_PASS']
        },
        [[
            InlineKeyboardButton(text="📞 Связаться с заказчиком", url=f"tg://user?id={deal['buyer_id']}"),
            InlineKeyboardButton(text="📞 Связаться с фабрикой", url=f"tg://user?id={deal['factory_id']}")
        ]]
    )
    
    await call.message.edit_text(
        f"✅ Сделка #{deal_id} отменена.\n\n"
        f"Другая сторона получила уведомление.\n"
        f"Администрация свяжется с вами при необходимости."
    )
    
    await call.answer("Сделка отменена")

@router.callback_query(F.data == "cancel_deal_cancel")
async def cancel_deal_cancel(call: CallbackQuery) -> None:
    """Cancel deal cancellation."""
    await call.message.edit_text("✅ Отмена сделки отменена")
    await call.answer()

# ---------------------------------------------------------------------------
#  Factory registration flow (продолжение основного кода)
# ---------------------------------------------------------------------------

@router.message(F.text == "🛠 Я – Фабрика")
async def factory_start(msg: Message, state: FSMContext) -> None:
    """Start factory registration or show profile."""
    await state.clear()
    
    # Check if already registered as factory
    user = get_or_create_user(msg.from_user)
    
    if user['role'] == 'factory':
        factory = q1("SELECT * FROM factories WHERE tg_id = ?", (msg.from_user.id,))
        if factory:
            await msg.answer(
                "Вы уже зарегистрированы как фабрика!",
                reply_markup=kb_factory_menu()
            )
            await cmd_profile(msg)
            return
    elif user['role'] == 'buyer':
        await msg.answer(
            "⚠️ Вы зарегистрированы как заказчик.\n\n"
            "Один аккаунт не может быть одновременно и фабрикой, и заказчиком.\n"
            "Используйте другой Telegram-аккаунт для регистрации фабрики.",
            reply_markup=kb_buyer_menu()
        )
        return
    
    # Start registration
    await state.set_state(FactoryForm.inn)
    await msg.answer(
        "Начнем регистрацию вашей фабрики!\n\n"
        "Введите ИНН компании (10 или 12 цифр):",
        reply_markup=ReplyKeyboardRemove()
    )

@router.message(FactoryForm.inn)
async def factory_inn(msg: Message, state: FSMContext) -> None:
    """Process INN input."""
    inn_digits = parse_digits(msg.text or "")
    if inn_digits is None or len(str(inn_digits)) not in (10, 12):
        await msg.answer("❌ ИНН должен содержать 10 или 12 цифр. Попробуйте еще раз:")
        return
    
    # Check if INN already registered
    existing = q1("SELECT name FROM factories WHERE inn = ?", (str(inn_digits),))
    if existing:
        await msg.answer(
            f"⚠️ Этот ИНН уже зарегистрирован ({existing['name']}).\n"
            f"Если это ваша компания, обратитесь в поддержку.",
            reply_markup=kb_main()
        )
        await state.clear()
        return
    
    await state.update_data(inn=str(inn_digits))
    await state.set_state(FactoryForm.legal_name)
    await msg.answer("Введите юридическое название компании:")

@router.message(FactoryForm.legal_name)
async def factory_legal_name(msg: Message, state: FSMContext) -> None:
    """Process legal name input."""
    if not msg.text or len(msg.text) < 3:
        await msg.answer("❌ Введите корректное название компании:")
        return
    
    await state.update_data(legal_name=msg.text.strip())
    await state.set_state(FactoryForm.address)
    await msg.answer("Введите адрес производства (город, район):")

@router.message(FactoryForm.address)
async def factory_address(msg: Message, state: FSMContext) -> None:
    """Process address input."""
    if not msg.text or len(msg.text) < 5:
        await msg.answer("❌ Введите корректный адрес:")
        return
    
    await state.update_data(address=msg.text.strip())
    await state.set_state(FactoryForm.photos)
    await msg.answer(
        "Пришлите 1-3 фото вашего производства (цех, оборудование).\n"
        "Это повысит доверие заказчиков.\n\n"
        "Отправьте фото или напишите «пропустить»:"
    )

@router.message(FactoryForm.photos, F.photo | F.text)
async def factory_photos(msg: Message, state: FSMContext) -> None:
    """Process photos input."""
    data = await state.get_data()
    photos: list[str] = data.get("photos", [])
    
    if msg.text and msg.text.lower() in ["пропустить", "skip", "далее"]:
        if not photos:
            await msg.answer("⚠️ Рекомендуем добавить хотя бы одно фото для привлечения клиентов.")
    elif msg.photo:
        photos.append(msg.photo[-1].file_id)
        await state.update_data(photos=photos)
        
        if len(photos) < 3:
            await msg.answer(f"Фото {len(photos)}/3 добавлено. Отправьте еще или напишите «далее»:")
            return
    else:
        await msg.answer("Отправьте фото или напишите «пропустить»:")
        return
    
    await state.set_state(FactoryForm.categories)
    
    # Show categories keyboard
    await msg.answer(
        "Выберите категории продукции, которую вы производите:",
        reply_markup=kb_categories()
    )
    await state.update_data(selected_categories=[])

@router.callback_query(F.data.startswith("cat:"), FactoryForm.categories)
async def factory_category_select(call: CallbackQuery, state: FSMContext) -> None:
    """Handle category selection."""
    category = call.data.split(":", 1)[1]
    
    if category == "done":
        data = await state.get_data()
        selected = data.get("selected_categories", [])
        
        if not selected:
            await call.answer("Выберите хотя бы одну категорию!", show_alert=True)
            return
        
        await state.update_data(categories=",".join(selected))
        await state.set_state(FactoryForm.min_qty)
        await call.message.edit_text(
            f"Выбрано категорий: {len(selected)}\n\n"
            f"Укажите минимальный размер партии (штук):"
        )
    else:
        data = await state.get_data()
        selected: list = data.get("selected_categories", [])
        
        if category in selected:
            selected.remove(category)
            await call.answer(f"❌ {category} удалена")
        else:
            selected.append(category)
            await call.answer(f"✅ {category} добавлена")
        
        await state.update_data(selected_categories=selected)
    
    await call.answer()

@router.message(FactoryForm.min_qty)
async def factory_min_qty(msg: Message, state: FSMContext) -> None:
    """Process minimum quantity."""
    qty = parse_digits(msg.text or "")
    if not qty or qty < 1:
        await msg.answer("❌ Укажите число больше 0:")
        return
    
    await state.update_data(min_qty=qty)
    await state.set_state(FactoryForm.max_qty)
    await msg.answer("Укажите максимальный размер партии (штук):")

@router.message(FactoryForm.max_qty)
async def factory_max_qty(msg: Message, state: FSMContext) -> None:
    """Process maximum quantity."""
    qty = parse_digits(msg.text or "")
    data = await state.get_data()
    min_qty = data.get("min_qty", 0)
    
    if not qty or qty < min_qty:
        await msg.answer(f"❌ Укажите число больше минимального ({min_qty}):")
        return
    
    await state.update_data(max_qty=qty)
    await state.set_state(FactoryForm.avg_price)
    await msg.answer("Средняя цена за единицу продукции (₽):")

@router.message(FactoryForm.avg_price)
async def factory_avg_price(msg: Message, state: FSMContext) -> None:
    """Process average price."""
    price = parse_digits(msg.text or "")
    if not price or price < 1:
        await msg.answer("❌ Укажите корректную цену:")
        return
    
    await state.update_data(avg_price=price)
    await state.set_state(FactoryForm.description)
    await msg.answer(
        "Расскажите о вашем производстве (оборудование, опыт, преимущества).\n"
        "Это поможет заказчикам выбрать именно вас:"
    )

@router.message(FactoryForm.description)
async def factory_description(msg: Message, state: FSMContext) -> None:
    """Process description."""
    if not msg.text or len(msg.text) < 20:
        await msg.answer("❌ Напишите более подробное описание (минимум 20 символов):")
        return
    
    await state.update_data(description=msg.text.strip())
    await state.set_state(FactoryForm.portfolio)
    await msg.answer(
        "Ссылка на портфолио (Instagram, сайт, Google Drive).\n"
        "Или напишите «нет»:"
    )

@router.message(FactoryForm.portfolio)
async def factory_portfolio(msg: Message, state: FSMContext) -> None:
    """Process portfolio link."""
    portfolio = ""
    if msg.text and msg.text.lower() not in ["нет", "no", "skip"]:
        portfolio = msg.text.strip()
    
    # Сохраняем в FSM
    await state.update_data(portfolio=portfolio)
    
    # Get all data
    data = await state.get_data()
    data['portfolio'] = portfolio
    
    # Show confirmation
    categories_list = data['categories'].split(',')
    categories_text = ", ".join([c.capitalize() for c in categories_list[:3]])
    if len(categories_list) > 3:
        categories_text += f" и еще {len(categories_list) - 3}"
    
    confirmation_text = (
        "<b>Проверьте данные вашей фабрики:</b>\n\n"
        f"🏢 Компания: {data['legal_name']}\n"
        f"📍 Адрес: {data['address']}\n"
        f"🏷 ИНН: {data['inn']}\n"
        f"📦 Категории: {categories_text}\n"
        f"📊 Партия: от {format_price(data['min_qty'])} до {format_price(data['max_qty'])} шт.\n"
        f"💰 Средняя цена: {format_price(data['avg_price'])} ₽\n"
    )
    
    if portfolio:
        confirmation_text += f"🔗 Портфолио: {portfolio}\n"
    
    photos_count = len(data.get('photos', []))
    if photos_count > 0:
        confirmation_text += f"📸 Фото: {photos_count} шт.\n"
    
    confirmation_text += (
        f"\n💳 <b>Стоимость PRO-подписки: 2 000 ₽/месяц</b>\n\n"
        f"После оплаты вы получите:\n"
        f"✅ Все заявки в ваших категориях\n"
        f"✅ Возможность откликаться без ограничений\n"
        f"✅ Приоритет в выдаче\n"
        f"✅ Поддержку менеджера"
    )
    
    # Payment button
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="💳 Оплатить 2 000 ₽", callback_data="pay_factory"),
        InlineKeyboardButton(text="✏️ Изменить", callback_data="edit_factory")
    ]])
    
    await state.set_state(FactoryForm.confirm_pay)
    await msg.answer(confirmation_text, reply_markup=kb)

@router.callback_query(F.data == "pay_factory", FactoryForm.confirm_pay)
async def factory_payment(call: CallbackQuery, state: FSMContext) -> None:
    """Process factory payment - ЗАГЛУШКА."""
    data = await state.get_data()
    
    # ЗАГЛУШКА для оплаты - в реальной версии здесь будет создание платежа
    # Имитируем успешную оплату
    
    # Update user role
    run("UPDATE users SET role = 'factory' WHERE tg_id = ?", (call.from_user.id,))
    
    # Create factory
    run("""
        INSERT OR REPLACE INTO factories
        (tg_id, name, inn, legal_name, address, categories, min_qty, max_qty, 
         avg_price, portfolio, description, is_pro, pro_expires)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, datetime('now', '+1 month'))
    """, (
        call.from_user.id,
        data['legal_name'],  # Use legal name as display name initially
        data['inn'],
        data['legal_name'],
        data['address'],
        data['categories'],
        data['min_qty'],
        data['max_qty'],
        data['avg_price'],
        data['portfolio'],
        data['description']
    ))
    
    # Save photos if any
    factory_photos = data.get('photos', [])
    for idx, photo_id in enumerate(factory_photos):
        run("""
            INSERT INTO factory_photos (factory_id, file_id, type, is_primary)
            VALUES (?, ?, 'workshop', ?)
        """, (call.from_user.id, photo_id, 1 if idx == 0 else 0))
    
    # Create payment record (ЗАГЛУШКА)
    payment_id = insert_and_get_id("""
        INSERT INTO payments 
        (user_id, type, amount, status, reference_type, reference_id)
        VALUES (?, 'factory_pro', 2000, 'completed', 'factory', ?)
    """, (call.from_user.id, call.from_user.id))
    
    # Track event
    track_event(call.from_user.id, 'factory_registered', {
        'categories': data['categories'],
        'min_qty': data['min_qty'],
        'max_qty': data['max_qty']
    })
    
    # Notify admins about new factory registration
    await notify_admins(
        'factory_registered',
        '🏭 Новая фабрика зарегистрирована!',
        f"Компания: {data['legal_name']}\n"
        f"ИНН: {data['inn']}\n"
        f"Категории: {data['categories']}\n"
        f"Мин. партия: {format_price(data['min_qty'])} шт.\n"
        f"Средняя цена: {format_price(data['avg_price'])} ₽",
        {
            'user_id': call.from_user.id,
            'username': call.from_user.username or 'N/A',
            'payment_id': payment_id,
            'amount': '2000 ₽'
        },
        [[
            InlineKeyboardButton(text="👤 Профиль", callback_data=f"admin_view_user:{call.from_user.id}"),
            InlineKeyboardButton(text="💬 Написать", url=f"tg://user?id={call.from_user.id}")
        ]]
    )
    
    await state.clear()
    await call.message.edit_text(
        "✅ <b>Поздравляем! Ваша фабрика зарегистрирована!</b>\n\n"
        "🎯 PRO-статус активирован на 1 месяц\n"
        "📬 Вы будете получать все подходящие заявки\n"
        "💬 Можете откликаться без ограничений\n\n"
        "Начните получать заказы прямо сейчас!"
    )
    
    await asyncio.sleep(2)
    await bot.send_message(
        call.from_user.id,
        "Главное меню фабрики:",
        reply_markup=kb_factory_menu()
    )
    
    await call.answer("✅ Регистрация завершена!")

# ---------------------------------------------------------------------------
#  Buyer order flow (продолжение)
# ---------------------------------------------------------------------------

@router.message(F.text.in_(["🛒 Мне нужна фабрика", "➕ Новый заказ"]))
async def buyer_start(msg: Message, state: FSMContext) -> None:
    """Start buyer order creation."""
    await state.clear()
    
    user = get_or_create_user(msg.from_user)
    
    # Check role conflicts
    if user['role'] == 'factory':
        await msg.answer(
            "⚠️ Вы зарегистрированы как фабрика.\n\n"
            "Один аккаунт не может быть одновременно и фабрикой, и заказчиком.\n"
            "Используйте другой Telegram-аккаунт для размещения заказов.",
            reply_markup=kb_factory_menu()
        )
        return
    
    # Update role if needed
    if user['role'] == 'unknown':
        run("UPDATE users SET role = 'buyer' WHERE tg_id = ?", (msg.from_user.id,))
    
    await state.set_state(BuyerForm.title)
    await msg.answer(
        "Создаем новый заказ!\n\n"
        "Придумайте короткое название для заказа\n"
        "(например: «Футболки с принтом 500шт»):",
        reply_markup=ReplyKeyboardRemove()
    )

@router.message(BuyerForm.title)
async def buyer_title(msg: Message, state: FSMContext) -> None:
    """Process order title."""
    if not msg.text or len(msg.text) < 5:
        await msg.answer("❌ Введите название заказа (минимум 5 символов):")
        return
    
    await state.update_data(title=msg.text.strip())
    await state.set_state(BuyerForm.category)
    
    # Show categories
    await msg.answer(
        "Выберите категорию товара:",
        reply_markup=kb_categories()
    )

@router.callback_query(F.data.startswith("cat:"), BuyerForm.category)
async def buyer_category_select(call: CallbackQuery, state: FSMContext) -> None:
    """Handle category selection for buyer."""
    category = call.data.split(":", 1)[1]
    
    if category == "done":
        await call.answer("Выберите одну категорию!", show_alert=True)
        return
    
    await state.update_data(category=category)
    await state.set_state(BuyerForm.quantity)
    await call.message.edit_text(
        f"Категория: {category.capitalize()}\n\n"
        f"Укажите количество (штук):"
    )
    await call.answer()

@router.message(BuyerForm.quantity)
async def buyer_quantity(msg: Message, state: FSMContext) -> None:
    """Process quantity."""
    qty = parse_digits(msg.text or "")
    if not qty or qty < 1:
        await msg.answer("❌ Укажите корректное количество:")
        return
    
    await state.update_data(quantity=qty)
    await state.set_state(BuyerForm.budget)
    await msg.answer("Ваш бюджет за единицу товара (₽):")

@router.message(BuyerForm.budget)
async def buyer_budget(msg: Message, state: FSMContext) -> None:
    """Process budget per item."""
    price = parse_digits(msg.text or "")
    if not price or price < 1:
        await msg.answer("❌ Укажите корректную цену:")
        return
    
    data = await state.get_data()
    total = price * data['quantity']
    
    await state.update_data(budget=price)
    await state.set_state(BuyerForm.destination)
    await msg.answer(
        f"Общий бюджет: {format_price(total)} ₽\n\n"
        f"Город доставки:"
    )

@router.message(BuyerForm.destination)
async def buyer_destination(msg: Message, state: FSMContext) -> None:
    """Process destination city."""
    if not msg.text or len(msg.text) < 2:
        await msg.answer("❌ Введите название города:")
        return
    
    await state.update_data(destination=msg.text.strip())
    await state.set_state(BuyerForm.lead_time)
    await msg.answer("Желаемый срок изготовления (дней):")

@router.message(BuyerForm.lead_time)
async def buyer_lead_time(msg: Message, state: FSMContext) -> None:
    """Process lead time."""
    days = parse_digits(msg.text or "")
    if not days or days < 1:
        await msg.answer("❌ Укажите количество дней:")
        return
    
    await state.update_data(lead_time=days)
    await state.set_state(BuyerForm.description)
    await msg.answer(
        "Опишите подробнее, что нужно произвести.\n"
        "Материалы, цвета, размеры, особенности:"
    )

@router.message(BuyerForm.description)
async def buyer_description(msg: Message, state: FSMContext) -> None:
    """Process description."""
    if not msg.text or len(msg.text) < 20:
        await msg.answer("❌ Опишите заказ подробнее (минимум 20 символов):")
        return
    
    await state.update_data(description=msg.text.strip())
    await state.set_state(BuyerForm.requirements)
    await msg.answer(
        "Особые требования к фабрике?\n"
        "(сертификаты, опыт, оборудование)\n\n"
        "Или напишите «нет»:"
    )

@router.message(BuyerForm.requirements)
async def buyer_requirements(msg: Message, state: FSMContext) -> None:
    """Process requirements."""
    requirements = ""
    if msg.text and msg.text.lower() not in ["нет", "no", "skip"]:
        requirements = msg.text.strip()
    
    await state.update_data(requirements=requirements)
    await state.set_state(BuyerForm.file)
    await msg.answer(
        "Приложите файл с техническим заданием (фото, документ).\n"
        "Или напишите «пропустить»:"
    )

@router.message(BuyerForm.file, F.document | F.photo | F.text)
async def buyer_file(msg: Message, state: FSMContext) -> None:
    """Process file attachment."""
    file_id = None

    if msg.text and msg.text.lower() in ["пропустить", "skip", "нет"]:
        pass
    elif msg.document:
        file_id = msg.document.file_id
    elif msg.photo:
        file_id = msg.photo[-1].file_id
    else:
        await msg.answer("Отправьте файл/фото или напишите «пропустить»:")
        return

    await state.update_data(file_id=file_id)
    
    # Show order summary
    data = await state.get_data()
    total = data['budget'] * data['quantity']
    
    summary = (
        "<b>Проверьте ваш заказ:</b>\n\n"
        f"📋 {data['title']}\n"
        f"📦 Категория: {data['category'].capitalize()}\n"
        f"🔢 Количество: {format_price(data['quantity'])} шт.\n"
        f"💰 Цена за шт: {format_price(data['budget'])} ₽\n"
        f"💵 Общий бюджет: {format_price(total)} ₽\n"
        f"📅 Срок: {data['lead_time']} дней\n"
        f"📍 Доставка: {data['destination']}\n\n"
        f"📝 <i>{data['description'][:100]}...</i>\n"
    )
    
    if data.get('requirements'):
        summary += f"\n⚠️ Особые требования: да"
    
    if file_id:
        summary += f"\n📎 Вложения: да"
    
    summary += (
        f"\n\n💳 <b>Стоимость размещения: 700 ₽</b>\n\n"
        f"После оплаты ваш заказ увидят все подходящие фабрики"
    )
    
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="💳 Оплатить 700 ₽", callback_data="pay_order"),
        InlineKeyboardButton(text="✏️ Изменить", callback_data="edit_order")
    ]])
    
    await state.set_state(BuyerForm.confirm_pay)
    await msg.answer(summary, reply_markup=kb)

@router.callback_query(F.data == "pay_order", BuyerForm.confirm_pay)
async def buyer_payment(call: CallbackQuery, state: FSMContext) -> None:
    """Process order payment - ЗАГЛУШКА."""
    data = await state.get_data()
    
    # ЗАГЛУШКА для оплаты - имитируем успешную оплату
    
    # Create order
    order_id = insert_and_get_id("""
        INSERT INTO orders
        (buyer_id, title, category, quantity, budget, destination, lead_time, 
         description, requirements, file_id, paid, expires_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, datetime('now', '+30 days'))
    """, (
        call.from_user.id,
        data['title'],
        data['category'],
        data['quantity'],
        data['budget'],
        data['destination'],
        data['lead_time'],
        data['description'],
        data.get('requirements', ''),
        data.get('file_id'),
    ))
    
    # Create payment record (ЗАГЛУШКА)
    payment_id = insert_and_get_id("""
        INSERT INTO payments 
        (user_id, type, amount, status, reference_type, reference_id)
        VALUES (?, 'order_placement', 700, 'completed', 'order', ?)
    """, (call.from_user.id, order_id))
    
    # Track event
    track_event(call.from_user.id, 'order_created', {
        'order_id': order_id,
        'category': data['category'],
        'quantity': data['quantity'],
        'budget': data['budget']
    })
    
    # Calculate total budget
    total_budget = data['quantity'] * data['budget']
    
    # Notify admins about new order
    await notify_admins(
        'order_created',
        '📦 Новый заказ размещен!',
        f"Заказ #Z-{order_id}: {data['title']}\n"
        f"Категория: {data['category']}\n"
        f"Количество: {format_price(data['quantity'])} шт.\n"
        f"Бюджет: {format_price(total_budget)} ₽\n"
        f"Город: {data['destination']}",
        {
            'buyer_id': call.from_user.id,
            'buyer_username': call.from_user.username or 'N/A',
            'payment_id': payment_id,
            'payment_amount': '700 ₽'
        },
        [[
            InlineKeyboardButton(text="📋 Детали заказа", callback_data=f"admin_view_order:{order_id}"),
            InlineKeyboardButton(text="💬 Написать заказчику", url=f"tg://user?id={call.from_user.id}")
        ]]
    )
    
    await state.clear()
    await call.message.edit_text(
        f"✅ <b>Заказ #Z-{order_id} успешно размещен!</b>\n\n"
        f"📬 Уведомления отправлены подходящим фабрикам\n"
        f"⏰ Ожидайте предложения в течение 24-48 часов\n"
        f"💬 Вы получите уведомление о каждом предложении\n\n"
        f"Удачных сделок!"
    )
    
    # Notify matching factories
    order_row = q1("SELECT * FROM orders WHERE id = ?", (order_id,))
    if order_row:
        notified = await notify_factories_about_order(order_row)
        
        await asyncio.sleep(2)
        await bot.send_message(
            call.from_user.id,
            f"📊 Ваш заказ отправлен {notified} фабрикам",
            reply_markup=kb_buyer_menu()
        )
    
    await call.answer("✅ Заказ размещен!")

# ---------------------------------------------------------------------------
#  ДОРАБОТКА: Просмотр заявок и отклики фабрик
# ---------------------------------------------------------------------------

@router.callback_query(F.data.startswith("view_order:"))
async def view_order_details(call: CallbackQuery) -> None:
    """Show detailed order information."""
    order_id = int(call.data.split(":", 1)[1])
    order = q1("SELECT * FROM orders WHERE id = ?", (order_id,))
    
    if not order:
        await call.answer("Заявка не найдена", show_alert=True)
        return
    
    # Check if factory can view
    factory = q1("SELECT * FROM factories WHERE tg_id = ?", (call.from_user.id,))
    if not factory or not factory['is_pro']:
        await call.answer("Доступ только для PRO-фабрик", show_alert=True)
        return
    
    # Get proposals count
    proposals_count = q1(
        "SELECT COUNT(*) as cnt FROM proposals WHERE order_id = ?",
        (order_id,)
    )
    
    # Detailed view
    detail_text = order_caption(order, detailed=True)
    
    if order['requirements']:
        detail_text += f"\n\n⚠️ <b>Особые требования:</b>\n{order['requirements']}"
    
    detail_text += f"\n\n📊 <b>Статистика:</b>"
    detail_text += f"\n👁 Просмотров: {order['views']}"
    detail_text += f"\n👥 Предложений: {proposals_count['cnt']}"
    detail_text += f"\n📅 Размещено: {order['created_at'][:16]}"
    
    # Check if already responded
    has_proposal = q1(
        "SELECT id FROM proposals WHERE order_id = ? AND factory_id = ?",
        (order_id, call.from_user.id)
    )
    
    buttons = []
    
    if order['file_id']:
        buttons.append([
            InlineKeyboardButton(text="📎 Скачать ТЗ", callback_data=f"download:{order_id}")
        ])
    
    if has_proposal:
        buttons.append([
            InlineKeyboardButton(text="✅ Вы откликнулись", callback_data=f"view_proposal:{order_id}")
        ])
    else:
        buttons.append([
            InlineKeyboardButton(text="💌 Откликнуться", callback_data=f"lead:{order_id}")
        ])
    
    buttons.append([
        InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_leads")
    ])
    
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    await call.message.edit_text(detail_text, reply_markup=kb)
    await call.answer()

@router.callback_query(F.data.startswith("download:"))
async def download_tz(call: CallbackQuery):
    """Download technical specification file."""
    try:
        order_id = int(call.data.split(":")[1])
        
        # Get order info
        order = q1("SELECT file_id, title FROM orders WHERE id = ?", (order_id,))
        
        if not order:
            await call.answer("Заказ не найден", show_alert=True)
            return
        
        # Check if file exists and is not empty
        file_id = order['file_id']
        
        if file_id and file_id.strip():  # Проверяем что file_id не None и не пустая строка
            try:
                # Определяем тип файла для caption
                order_title = order['title'] or f"Заказ #{order_id}"
                caption = f"📎 Техническое задание\n📋 {order_title}"
                
                # Отправляем файл
                await bot.send_document(
                    chat_id=call.message.chat.id,
                    document=file_id,
                    caption=caption
                )
                
                # Подтверждаем успешную отправку
                await call.answer("✅ Файл отправлен")
                
                # Логируем успешное скачивание
                logger.info(f"File downloaded for order {order_id} by user {call.from_user.id}")
                
            except Exception as e:
                logger.error(f"Error sending file for order {order_id}: {e}")
                
                # Если файл поврежден или недоступен
                await call.answer(
                    "❌ Ошибка при отправке файла. Возможно, файл поврежден или удален.", 
                    show_alert=True
                )
        else:
            # Файл не прикреплен
            await call.answer(
                "📎 К этому заказу не прикреплен файл с техническим заданием", 
                show_alert=True
            )
            
    except ValueError:
        # Ошибка парсинга order_id
        await call.answer("❌ Неверный формат запроса", show_alert=True)
        logger.error(f"Invalid order_id format in download request: {call.data}")
        
    except Exception as e:
        # Общая ошибка
        logger.error(f"Unexpected error in download_tz: {e}")
        await call.answer("❌ Произошла ошибка при загрузке файла", show_alert=True)

# Дополнительная функция для проверки доступности файла (опционально)
async def check_file_availability(file_id: str) -> bool:
    """
    Проверяет доступность файла в Telegram
    
    Args:
        file_id: ID файла в Telegram
        
    Returns:
        True если файл доступен, False если нет
    """
    try:
        # Пытаемся получить информацию о файле
        file_info = await bot.get_file(file_id)
        return file_info is not None
    except Exception as e:
        logger.error(f"File {file_id} is not available: {e}")
        return False

# Улучшенная версия с предварительной проверкой файла
@router.callback_query(F.data.startswith("download_safe:"))
async def download_tz_safe(call: CallbackQuery):
    """Download technical specification file with pre-check."""
    try:
        order_id = int(call.data.split(":")[1])
        
        # Get order info
        order = q1("SELECT file_id, title FROM orders WHERE id = ?", (order_id,))
        
        if not order:
            await call.answer("Заказ не найден", show_alert=True)
            return
        
        file_id = order['file_id']
        
        if not file_id or not file_id.strip():
            await call.answer(
                "📎 К этому заказу не прикреплен файл с техническим заданием", 
                show_alert=True
            )
            return
        
        # Показываем индикатор загрузки
        await call.answer("⏳ Подготавливаем файл...")
        
        # Проверяем доступность файла
        if not await check_file_availability(file_id):
            await bot.send_message(
                call.message.chat.id,
                "❌ Файл недоступен или был удален из Telegram. Обратитесь к заказчику за новой версией."
            )
            return
        
        # Отправляем файл
        order_title = order['title'] or f"Заказ #{order_id}"
        caption = f"📎 Техническое задание\n📋 {order_title}"
        
        await bot.send_document(
            chat_id=call.message.chat.id,
            document=file_id,
            caption=caption
        )
        
        logger.info(f"File safely downloaded for order {order_id} by user {call.from_user.id}")
        
    except ValueError:
        await call.answer("❌ Неверный формат запроса", show_alert=True)
        
    except Exception as e:
        logger.error(f"Error in download_tz_safe: {e}")
        await bot.send_message(
            call.message.chat.id,
            "❌ Произошла ошибка при загрузке файла. Попробуйте позже или обратитесь в поддержку."
        )

@router.callback_query(F.data.startswith("lead:"))
async def process_lead_response(call: CallbackQuery, state: FSMContext) -> None:
    """Start proposal creation for an order."""
    order_id = int(call.data.split(":", 1)[1])
    
    # Verify factory status
    factory = q1("SELECT * FROM factories WHERE tg_id = ? AND is_pro = 1", (call.from_user.id,))
    if not factory:
        await call.answer("Доступ только для PRO-фабрик", show_alert=True)
        return
    
    # Check order exists and active
    order = q1("SELECT * FROM orders WHERE id = ? AND is_active = 1", (order_id,))
    if not order:
        await call.answer("Заявка недоступна", show_alert=True)
        return
    
    # Check if already has active deal
    active_deal = q1("""
        SELECT 1 FROM deals 
        WHERE order_id = ? AND status NOT IN ('CANCELLED', 'DELIVERED')
    """, (order_id,))
    
    if active_deal:
        await call.answer("По этой заявке уже идет сделка", show_alert=True)
        return
    
    # Check if already responded
    existing_proposal = q1(
        "SELECT * FROM proposals WHERE order_id = ? AND factory_id = ?",
        (order_id, call.from_user.id)
    )
    
    if existing_proposal:
        await call.answer("Вы уже откликнулись на эту заявку", show_alert=True)
        return
    
    await state.update_data(order_id=order_id)
    await state.set_state(ProposalForm.price)
    
    await call.message.answer(
        f"<b>Отклик на заявку #Z-{order_id}</b>\n\n"
        f"Категория: {order['category']}\n"
        f"Количество: {format_price(order['quantity'])} шт.\n"
        f"Бюджет заказчика: {format_price(order['budget'])} ₽/шт.\n\n"
        f"Ваша цена за единицу (₽):",
        reply_markup=ReplyKeyboardRemove()
    )
    await call.answer()

@router.message(ProposalForm.price)
async def proposal_price(msg: Message, state: FSMContext) -> None:
    """Process proposal price."""
    price = parse_digits(msg.text or "")
    if not price or price < 1:
        await msg.answer("❌ Укажите корректную цену:")
        return
    
    data = await state.get_data()
    order = q1("SELECT quantity FROM orders WHERE id = ?", (data['order_id'],))
    
    if order:
        total = price * order['quantity']
        await msg.answer(f"Общая сумма: {format_price(total)} ₽")
    
    await state.update_data(price=price)
    await state.set_state(ProposalForm.lead_time)
    await msg.answer("Срок изготовления (дней):")

@router.message(ProposalForm.lead_time)
async def proposal_lead_time(msg: Message, state: FSMContext) -> None:
    """Process lead time."""
    days = parse_digits(msg.text or "")
    if not days or days < 1:
        await msg.answer("❌ Укажите количество дней:")
        return
    
    await state.update_data(lead_time=days)
    await state.set_state(ProposalForm.sample_cost)
    await msg.answer(
        "Стоимость образца (₽)\n"
        "Введите 0, если образец бесплатный:"
    )

@router.message(ProposalForm.sample_cost)
async def proposal_sample_cost(msg: Message, state: FSMContext) -> None:
    """Process sample cost."""
    cost = parse_digits(msg.text or "0")
    if cost is None or cost < 0:
        await msg.answer("❌ Укажите корректную стоимость (или 0):")
        return
    
    await state.update_data(sample_cost=cost)
    await state.set_state(ProposalForm.message)
    await msg.answer(
        "Добавьте сообщение для заказчика.\n"
        "Расскажите о своих преимуществах, опыте с подобными заказами:\n\n"
        "(или напишите «—» чтобы пропустить)"
    )

@router.message(ProposalForm.message)
async def proposal_message(msg: Message, state: FSMContext) -> None:
    """Process proposal message."""
    message = ""
    if msg.text and msg.text not in ["—", "-", "–"]:
        message = msg.text.strip()
    
    data = await state.get_data()
    data['message'] = message
    
    # Get order details
    order = q1("SELECT * FROM orders WHERE id = ?", (data['order_id'],))
    if not order:
        await msg.answer("Ошибка: заказ не найден")
        await state.clear()
        return
    
    # Show confirmation
    total = data['price'] * order['quantity']
    
    confirm_text = (
        "<b>Проверьте ваше предложение:</b>\n\n"
        f"Заявка: #Z-{order['id']}\n"
        f"Цена за единицу: {format_price(data['price'])} ₽\n"
        f"Общая сумма: {format_price(total)} ₽\n"
        f"Срок: {data['lead_time']} дней\n"
        f"Образец: {format_price(data['sample_cost'])} ₽\n"
    )
    
    if message:
        confirm_text += f"\n💬 Сообщение:\n{message[:200]}"
        if len(message) > 200:
            confirm_text += "..."
    
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Отправить", callback_data="confirm_proposal"),
        InlineKeyboardButton(text="✏️ Изменить", callback_data="edit_proposal")
    ]])
    
    await msg.answer(confirm_text, reply_markup=kb)

@router.callback_query(F.data == "confirm_proposal")
async def confirm_proposal(call: CallbackQuery, state: FSMContext) -> None:
    """Confirm and submit proposal."""
    data = await state.get_data()
    
    # Verify order still available
    order = q1("SELECT * FROM orders WHERE id = ? AND is_active = 1", (data['order_id'],))
    if not order:
        await call.answer("Заявка уже недоступна", show_alert=True)
        await state.clear()
        return
    
    # Insert proposal
    try:
        proposal_id = insert_and_get_id("""
            INSERT INTO proposals
            (order_id, factory_id, price, lead_time, sample_cost, message)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            data['order_id'],
            call.from_user.id,
            data['price'],
            data['lead_time'],
            data['sample_cost'],
            data.get('message', '')
        ))
        
        # Get factory info
        factory = q1("SELECT * FROM factories WHERE tg_id = ?", (call.from_user.id,))
        
        # Track event
        track_event(call.from_user.id, 'proposal_sent', {
            'order_id': data['order_id'],
            'price': data['price'],
            'lead_time': data['lead_time']
        })
        
        await call.message.edit_text(
            "✅ <b>Предложение отправлено!</b>\n\n"
            "Заказчик получил уведомление и рассмотрит ваше предложение.\n"
            "Мы сообщим вам о решении."
        )
        
        # Notify buyer
        proposal_row = dict(
            id=proposal_id,
            price=data['price'],
            lead_time=data['lead_time'],
            sample_cost=data['sample_cost'],
            message=data.get('message', '')
        )
        
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👀 Все предложения", callback_data=f"view_proposals:{order['id']}")],
            [InlineKeyboardButton(text="✅ Выбрать эту фабрику", callback_data=f"choose_factory:{order['id']}:{call.from_user.id}")]
        ])
        
        await send_notification(
            order['buyer_id'],
            'new_proposal',
            f'Новое предложение на заказ #{order["id"]}',
            proposal_caption(proposal_row, factory),
            {'order_id': order['id'], 'factory_id': call.from_user.id}
        )
        
        asyncio.create_task(
            bot.send_message(
                order['buyer_id'],
                f"💌 <b>Новое предложение на ваш заказ!</b>\n\n" +
                order_caption(order) + "\n\n" +
                proposal_caption(proposal_row, factory),
                reply_markup=kb
            )
        )
        
        await state.clear()
        await call.answer("✅ Предложение отправлено!")
        
    except Exception as e:
        logger.error(f"Error creating proposal: {e}")
        if "UNIQUE constraint failed" in str(e):
            await call.answer("Вы уже откликались на эту заявку", show_alert=True)
        else:
            await call.answer("Ошибка при отправке предложения", show_alert=True)

# ---------------------------------------------------------------------------
#  ДОРАБОТКА: Меню фабрики - Заявки
# ---------------------------------------------------------------------------

@router.message(Command("leads"))
@router.message(F.text == "📂 Заявки")
async def cmd_factory_leads(msg: Message) -> None:
    """Show available leads for factory."""
    factory = q1("SELECT * FROM factories WHERE tg_id = ? AND is_pro = 1", (msg.from_user.id,))
    
    if not factory:
        await msg.answer(
            "❌ Доступ к заявкам только для PRO-фабрик.\n\n"
            "Оформите подписку для получения заказов.",
            reply_markup=kb_factory_menu() if get_user_role(msg.from_user.id) == UserRole.FACTORY else kb_main()
        )
        return
    
    # Get matching orders
    matching_orders = q("""
        SELECT o.*, 
               (SELECT COUNT(*) FROM proposals p WHERE p.order_id = o.id) as proposals_count,
               (SELECT COUNT(*) FROM proposals p WHERE p.order_id = o.id AND p.factory_id = ?) as has_proposal
        FROM orders o
        WHERE o.paid = 1 
          AND o.is_active = 1
          AND o.quantity >= ? 
          AND o.budget >= ?
          AND (',' || ? || ',') LIKE ('%,' || o.category || ',%')
          AND NOT EXISTS (
              SELECT 1 FROM deals d 
              WHERE d.order_id = o.id AND d.status != 'CANCELLED'
          )
        ORDER BY o.created_at DESC
        LIMIT 20
    """, (
        msg.from_user.id,
        factory['min_qty'],
        factory['avg_price'],
        factory['categories']
    ))
    
    if not matching_orders:
        await msg.answer(
            "📭 Сейчас нет подходящих заявок.\n\n"
            "Мы уведомим вас, когда появятся новые!",
            reply_markup=kb_factory_menu()
        )
        return
    
    # Send header
    await msg.answer(
        f"<b>Доступные заявки ({len(matching_orders)})</b>\n\n"
        f"Нажмите «Подробнее» для просмотра или «Откликнуться» для отправки предложения:",
        reply_markup=kb_factory_menu()
    )
    
    # Send orders (max 5 at once)
    sent = 0
    for order in matching_orders[:5]:
        # Update views
        run("UPDATE orders SET views = views + 1 WHERE id = ?", (order['id'],))
        
        buttons = []
        
        # First row: View and Respond
        first_row = [
            InlineKeyboardButton(text="👀 Подробнее", callback_data=f"view_order:{order['id']}")
        ]
        
        if order['has_proposal']:
            first_row.append(
                InlineKeyboardButton(text="✅ Вы откликнулись", callback_data=f"view_proposal:{order['id']}")
            )
        else:
            first_row.append(
                InlineKeyboardButton(text="💌 Откликнуться", callback_data=f"lead:{order['id']}")
            )
        
        buttons.append(first_row)
        
        # Second row: Competition info
        if order['proposals_count'] > 0:
            buttons.append([
                InlineKeyboardButton(
                    text=f"👥 Предложений: {order['proposals_count']}", 
                    callback_data=f"competition:{order['id']}"
                )
            ])
        
        kb = InlineKeyboardMarkup(inline_keyboard=buttons)
        
        await msg.answer(order_caption(order), reply_markup=kb)
        sent += 1
    
    if len(matching_orders) > 5:
        load_more_kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="📋 Показать еще", callback_data="load_more_orders:5")
        ]])
        await msg.answer(
            f"Показано {sent} из {len(matching_orders)} заявок",
            reply_markup=load_more_kb
        )

# ---------------------------------------------------------------------------
#  ДОРАБОТКА: Меню фабрики - Аналитика
# ---------------------------------------------------------------------------

@router.message(F.text == "📊 Аналитика")
async def cmd_factory_analytics(msg: Message) -> None:
    """Show factory analytics."""
    factory = q1("SELECT * FROM factories WHERE tg_id = ? AND is_pro = 1", (msg.from_user.id,))
    if not factory:
        await msg.answer(
            "❌ Аналитика доступна только для PRO-фабрик.\n\n"
            "Оформите подписку для получения детальной статистики.",
            reply_markup=kb_factory_menu()
        )
        return

    stats = q1("""
        SELECT 
            COUNT(DISTINCT p.id) as total_proposals,
            COUNT(DISTINCT CASE WHEN p.is_accepted = 1 THEN p.id END) as accepted_proposals,
            COUNT(DISTINCT d.id) as total_deals,
            COUNT(DISTINCT CASE WHEN d.status = 'DELIVERED' THEN d.id END) as completed_deals,
            SUM(CASE WHEN d.status = 'DELIVERED' THEN d.amount ELSE 0 END) as total_revenue
        FROM proposals p
        LEFT JOIN deals d ON p.order_id = d.order_id AND p.factory_id = d.factory_id
        WHERE p.factory_id = ?
    """, (msg.from_user.id,))

    if not stats or stats['total_proposals'] == 0:
        await msg.answer(
            "📊 <b>Аналитика</b>\n\n"
            "На данный момент у нас недостаточно данных для отображения аналитики.\n\n"
            "Мы собираем данные с момента вашей регистрации. "
            "Начните откликаться на заявки, и здесь появится подробная статистика!",
            reply_markup=kb_factory_menu()
        )
        return

    proposal_conversion = (stats['accepted_proposals'] / stats['total_proposals']) * 100 if stats['total_proposals'] > 0 else 0
    deal_conversion = (stats['completed_deals'] / stats['total_deals']) * 100 if stats['total_deals'] > 0 else 0

    analytics_text = (
        f"📊 <b>Аналитика фабрики</b>\n\n"
        f"<b>Предложения:</b>\n"
        f"├ Всего отправлено: {stats['total_proposals']}\n"
        f"├ Принято: {stats['accepted_proposals']}\n"
        f"└ Конверсия: {proposal_conversion:.1f}%\n\n"
        f"<b>Сделки:</b>\n"
        f"├ Всего: {stats['total_deals']}\n"
        f"├ Завершено: {stats['completed_deals']}\n"
        f"└ Успешность: {deal_conversion:.1f}%\n\n"
        f"<b>Финансы:</b>\n"
        f"└ Общий оборот: {format_price(stats['total_revenue'] or 0)} ₽\n\n"
    )

    recent_activity = q1("""
        SELECT 
            COUNT(DISTINCT p.id) as recent_proposals,
            COUNT(DISTINCT d.id) as recent_deals
        FROM proposals p
        LEFT JOIN deals d ON p.order_id = d.order_id AND p.factory_id = d.factory_id
        WHERE p.factory_id = ? AND p.created_at > datetime('now', '-30 days')
    """, (msg.from_user.id,))

    analytics_text += (
        f"<b>За последние 30 дней:</b>\n"
        f"├ Предложений: {recent_activity['recent_proposals']}\n"
        f"└ Новых сделок: {recent_activity['recent_deals']}"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📈 Детальная статистика", callback_data="analytics_detailed"),
            InlineKeyboardButton(text="📊 Рейтинг среди фабрик", callback_data="analytics_rating")
        ]
    ])
    await msg.answer(analytics_text, reply_markup=kb)

# ---------------------------------------------------------------------------
#  ДОРАБОТКА: Меню фабрики - Рейтинг
# ---------------------------------------------------------------------------

@router.message(F.text == "⭐ Рейтинг")
async def cmd_factory_rating(msg: Message) -> None:
    """Show factory rating."""
    factory = q1("SELECT * FROM factories WHERE tg_id = ?", (msg.from_user.id,))
    if not factory:
        await msg.answer(
            "Профиль фабрики не найден",
            reply_markup=kb_factory_menu()
        )
        return

    if factory['rating_count'] == 0:
        await msg.answer(
            "⭐ <b>Рейтинг</b>\n\n"
            "У вас еще нет оценок. Не расстраивайтесь, в ближайшее время "
            "мы найдем для вас заказ и ваш рейтинг вырастет!\n\n"
            "💡 <b>Как получить высокий рейтинг:</b>\n"
            "• Качественно выполняйте заказы\n"
            "• Соблюдайте сроки\n"
            "• Поддерживайте связь с заказчиками\n"
            "• Предоставляйте фото процесса производства",
            reply_markup=kb_factory_menu()
        )
        return

    ratings = q("""
        SELECT r.*, o.title, u.full_name as buyer_name
        FROM ratings r
        JOIN deals d ON r.deal_id = d.id
        JOIN orders o ON d.order_id = o.id
        JOIN users u ON r.buyer_id = u.tg_id
        WHERE r.factory_id = ?
        ORDER BY r.created_at DESC
        LIMIT 5
    """, (msg.from_user.id,))

    rating_text = (
        f"⭐ <b>Ваш рейтинг: {factory['rating']:.1f}/5.0</b>\n"
        f"📊 Основан на {factory['rating_count']} отзывах\n\n"
        f"<b>Последние отзывы:</b>\n"
    )

    for rating in ratings:
        stars = "⭐" * rating['rating']
        rating_text += (
            f"\n{stars} ({rating['rating']}/5)\n"
            f"Заказ: {rating['title'][:30]}...\n"
            f"От: {rating['buyer_name']}\n"
        )
        if rating['comment']:
            rating_text += f"💬 {rating['comment'][:50]}...\n"

    position = q1("""
        SELECT COUNT(*) + 1 as position
        FROM factories
        WHERE rating > ? AND rating_count > 0
    """, (factory['rating'],))

    if position:
        rating_text += f"\n🏆 Ваша позиция: #{position['position']} среди всех фабрик"

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 Все отзывы", callback_data="view_all_ratings")]
    ])
    await msg.answer(rating_text, reply_markup=kb)

# ---------------------------------------------------------------------------
#  ДОРАБОТКА: Меню фабрики - Баланс
# ---------------------------------------------------------------------------

@router.message(F.text == "💳 Баланс")
async def cmd_factory_balance(msg: Message) -> None:
    """Show factory balance."""
    factory = q1("SELECT * FROM factories WHERE tg_id = ?", (msg.from_user.id,))
    if not factory:
        await msg.answer(
            "Профиль фабрики не найден",
            reply_markup=kb_factory_menu()
        )
        return

    active_deals_sum = q1("""
        SELECT SUM(amount) as total
        FROM deals
        WHERE factory_id = ? AND status IN ('PRODUCTION', 'READY_TO_SHIP', 'IN_TRANSIT')
    """, (msg.from_user.id,))

    completed_revenue = q1("""
        SELECT SUM(amount) as total
        FROM deals
        WHERE factory_id = ? AND status = 'DELIVERED'
    """, (msg.from_user.id,))

    pending_payments = q1("""
        SELECT SUM(amount * 0.7) as total
        FROM deals
        WHERE factory_id = ? AND status = 'READY_TO_SHIP' AND final_paid = 0
    """, (msg.from_user.id,))

    current_balance = active_deals_sum['total'] or 0
    total_earned = completed_revenue['total'] or 0
    pending_amount = pending_payments['total'] or 0

    if current_balance == 0 and total_earned == 0:
        await msg.answer(
            "💳 <b>Баланс</b>\n\n"
            "Здесь будет отображаться ваш баланс, равный сумме принятых "
            "в работу заказов, а также статистика по выплатам.\n\n"
            "Начните выполнять заказы, и ваша финансовая статистика появится здесь!",
            reply_markup=kb_factory_menu()
        )
        return

    balance_text = (
        f"💳 <b>Финансы</b>\n\n"
        f"<b>Текущий баланс:</b>\n"
        f"💰 В работе: {format_price(current_balance)} ₽\n"
    )

    if pending_amount > 0:
        balance_text += f"⏳ Ожидается: {format_price(int(pending_amount))} ₽\n"

    balance_text += (
        f"\n<b>Статистика:</b>\n"
        f"✅ Всего заработано: {format_price(total_earned)} ₽\n"
    )

    deals_breakdown = q("""
        SELECT status, COUNT(*) as count, SUM(amount) as total
        FROM deals
        WHERE factory_id = ?
        GROUP BY status
    """, (msg.from_user.id,))

    if deals_breakdown:
        balance_text += f"\n<b>Сделки по статусам:</b>\n"
        for deal in deals_breakdown:
            status_names = {
                'PRODUCTION': '🔄 Производство',
                'READY_TO_SHIP': '📦 Готово к отправке',
                'IN_TRANSIT': '🚚 В пути',
                'DELIVERED': '✅ Доставлено'
            }
            status_name = status_names.get(deal['status'], deal['status'])
            balance_text += f"{status_name}: {deal['count']} ({format_price(deal['total'])} ₽)\n"

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📊 История платежей", callback_data="payment_history"),
            InlineKeyboardButton(text="📈 Динамика доходов", callback_data="revenue_chart")
        ]
    ])
    await msg.answer(balance_text, reply_markup=kb)

# ---------------------------------------------------------------------------
#  ДОРАБОТКА: Мои сделки (универсальная функция)
# ---------------------------------------------------------------------------

@router.message(F.text == "💼 Мои сделки")
async def cmd_my_deals(msg: Message) -> None:
    """Show user's deals."""
    user_role = get_user_role(msg.from_user.id)
    
    if user_role == UserRole.FACTORY:
        deals = q("""
            SELECT d.*, o.title, o.category, o.quantity
            FROM deals d
            JOIN orders o ON d.order_id = o.id
            WHERE d.factory_id = ?
            ORDER BY 
                CASE d.status 
                    WHEN 'DRAFT' THEN 1
                    WHEN 'SAMPLE_PASS' THEN 2
                    WHEN 'PRODUCTION' THEN 3
                    WHEN 'READY_TO_SHIP' THEN 4
                    WHEN 'IN_TRANSIT' THEN 5
                    WHEN 'DELIVERED' THEN 6
                    ELSE 7
                END,
                d.created_at DESC
        """, (msg.from_user.id,))
    elif user_role == UserRole.BUYER:
        deals = q("""
            SELECT d.*, o.title, o.category, o.quantity, f.name as factory_name
            FROM deals d
            JOIN orders o ON d.order_id = o.id
            JOIN factories f ON d.factory_id = f.tg_id
            WHERE d.buyer_id = ?
            ORDER BY 
                CASE d.status 
                    WHEN 'DRAFT' THEN 1
                    WHEN 'SAMPLE_PASS' THEN 2
                    WHEN 'PRODUCTION' THEN 3
                    WHEN 'READY_TO_SHIP' THEN 4
                    WHEN 'IN_TRANSIT' THEN 5
                    WHEN 'DELIVERED' THEN 6
                    ELSE 7
                END,
                d.created_at DESC
        """, (msg.from_user.id,))
    else:
        await msg.answer("Доступ запрещен", reply_markup=kb_main())
        return
    
    if not deals:
        await msg.answer(
            "У вас пока нет активных сделок.",
            reply_markup=kb_factory_menu() if user_role == UserRole.FACTORY else kb_buyer_menu()
        )
        return
        
    # Группировка сделок по статусу
    active_deals = [d for d in deals if d['status'] not in ['DELIVERED', 'CANCELLED']]
    completed_deals = [d for d in deals if d['status'] == 'DELIVERED']

    response = "<b>Ваши сделки</b>\n\n"

    if active_deals:
        response += f"🔄 <b>Активные ({len(active_deals)})</b>\n"
        for deal in active_deals[:3]:
            status = OrderStatus(deal['status'])
            title = deal['title'] if deal['title'] else f"Заказ #{deal['order_id']}"
            response += f"\n#{deal['id']} - {title}\n"
            response += f"Статус: {status.value}\n"
            if user_role == UserRole.BUYER:
                response += f"Фабрика: {deal['factory_name']}\n"

        if len(active_deals) > 3:
            response += f"\n... и еще {len(active_deals) - 3}\n"

    if completed_deals:
        response += f"\n\n✅ <b>Завершенные ({len(completed_deals)})</b>"

    await msg.answer(
        response,
        reply_markup=kb_factory_menu() if user_role == UserRole.FACTORY else kb_buyer_menu()
    )

    # Отправка детальных карточек по активным сделкам (макс 5)
    for deal in active_deals[:5]:
        await send_deal_card(msg.from_user.id, deal, user_role)

async def send_deal_card(user_id: int, deal: dict, user_role: UserRole):
    """Send deal status card with actions."""
    status = OrderStatus(deal['status'])
    caption = deal_status_caption(dict(deal))

    buttons = []

    # Для покупателя
    if user_role == UserRole.BUYER:
        if status == OrderStatus.DRAFT and not deal['deposit_paid']:
            buttons.append([
                InlineKeyboardButton(text="💳 Оплатить образец", callback_data=f"pay_sample:{deal['id']}")
            ])
        elif status == OrderStatus.SAMPLE_PASS and not deal['deposit_paid']:
            buttons.append([
                InlineKeyboardButton(text="💳 Внести предоплату 30%", callback_data=f"pay_deposit:{deal['id']}")
            ])
        elif status == OrderStatus.READY_TO_SHIP and not deal['final_paid']:
            buttons.append([
                InlineKeyboardButton(text="💳 Доплатить 70%", callback_data=f"pay_final:{deal['id']}")
            ])
        elif status == OrderStatus.IN_TRANSIT:
            buttons.append([
                InlineKeyboardButton(text="✅ Подтвердить получение", callback_data=f"confirm_delivery:{deal['id']}")
            ])
        elif status == OrderStatus.DELIVERED:
            rating = q1("SELECT id FROM ratings WHERE deal_id = ? AND buyer_id = ?", (deal['id'], user_id))
            if not rating:
                buttons.append([
                    InlineKeyboardButton(text="⭐ Оставить отзыв", callback_data=f"rate_deal:{deal['id']}")
                ])

    # Для фабрики
    elif user_role == UserRole.FACTORY:
        if status == OrderStatus.DRAFT and deal['deposit_paid']:
            buttons.append([
                InlineKeyboardButton(text="📸 Загрузить фото образца", callback_data=f"upload_sample:{deal['id']}")
            ])
        elif status == OrderStatus.PRODUCTION:
            buttons.append([
                InlineKeyboardButton(text="📸 Фото производства", callback_data=f"upload_production:{deal['id']}"),
                InlineKeyboardButton(text="📦 Готово к отправке", callback_data=f"ready_to_ship:{deal['id']}")
            ])
        elif status == OrderStatus.READY_TO_SHIP and deal['final_paid'] and not deal['tracking_num']:
            buttons.append([
                InlineKeyboardButton(text="🚚 Добавить трек-номер", callback_data=f"add_tracking:{deal['id']}")
            ])
    
    # Common actions - ГЛАВНОЕ: добавляем кнопку перехода в чат
    buttons.append([
        InlineKeyboardButton(text="💬 Перейти в чат", callback_data=f"deal_chat:{deal['id']}")
    ])
    
    if status not in [OrderStatus.DELIVERED, OrderStatus.CANCELLED]:
        buttons.append([
            InlineKeyboardButton(text="🚫 Отменить сделку", callback_data=f"cancel_deal:{deal['id']}")
        ])
    
    kb = InlineKeyboardMarkup(inline_keyboard=buttons) if buttons else None
    
    await bot.send_message(user_id, caption, reply_markup=kb)

# ---------------------------------------------------------------------------
#  ДОРАБОТКА: Обработчики для просмотра предложений
# ---------------------------------------------------------------------------

@router.callback_query(F.data.startswith("view_proposals:"))
async def view_order_proposals(call: CallbackQuery) -> None:
    """Show all proposals for specific order."""
    order_id = int(call.data.split(":", 1)[1])
    
    # Verify ownership
    order = q1("SELECT * FROM orders WHERE id = ? AND buyer_id = ?", (order_id, call.from_user.id))
    if not order:
        await call.answer("Заказ не найден", show_alert=True)
        return
    
    # Get all proposals
    proposals = q("""
        SELECT p.*, f.name, f.rating, f.rating_count, f.completed_orders
        FROM proposals p
        JOIN factories f ON p.factory_id = f.tg_id
        WHERE p.order_id = ?
        ORDER BY p.price ASC, p.lead_time ASC
    """, (order_id,))
    
    if not proposals:
        await call.message.edit_text(
            "По этому заказу пока нет предложений.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_orders")
            ]])
        )
        return
    
    await call.message.edit_text(
        f"<b>Предложения по заказу #Z-{order_id}</b>\n"
        f"Всего предложений: {len(proposals)}\n\n"
        f"Отсортировано по цене ⬆️"
    )
    
    # Send each proposal
    for idx, prop in enumerate(proposals):
        factory = dict(
            name=prop['name'],
            rating=prop['rating'],
            rating_count=prop['rating_count'],
            completed_orders=prop['completed_orders']
        )
        
        buttons = [
            [
                InlineKeyboardButton(text="👤 О фабрике", callback_data=f"factory_info:{prop['factory_id']}"),
                InlineKeyboardButton(text="✅ Выбрать", callback_data=f"choose_factory:{order_id}:{prop['factory_id']}")
            ]
        ]
        
        # Add comparison if multiple proposals
        if len(proposals) > 1:
            buttons.append([
                InlineKeyboardButton(text="📊 Сравнить все", callback_data=f"compare_proposals:{order_id}")
            ])
        
        kb = InlineKeyboardMarkup(inline_keyboard=buttons)
        
        caption = f"<b>#{idx + 1}</b> " + proposal_caption(prop, factory)
        await call.message.answer(caption, reply_markup=kb)
    
    await call.answer()
@router.callback_query(F.data.startswith("choose_factory:"))
async def choose_factory(call: CallbackQuery, state: FSMContext) -> None:
    """Choose factory and create deal - ИСПРАВЛЕННАЯ ВЕРСИЯ."""
    try:
        # Парсим данные из callback
        parts = call.data.split(":")
        if len(parts) < 3:
            logger.error(f"Invalid callback data format: {call.data}")
            await call.answer("❌ Неверный формат запроса", show_alert=True)
            return
            
        order_id = int(parts[1])
        factory_id = int(parts[2])
        
        logger.info(f"User {call.from_user.id} trying to choose factory {factory_id} for order {order_id}")
        
        # Сначала проверим существует ли заказ вообще
        order_exists = q1("SELECT id, buyer_id, is_active FROM orders WHERE id = ?", (order_id,))
        if not order_exists:
            logger.error(f"Order {order_id} does not exist")
            await call.answer("❌ Заказ не найден в системе", show_alert=True)
            return
        
        # Проверим права доступа
        if order_exists['buyer_id'] != call.from_user.id:
            logger.error(f"Access denied: user {call.from_user.id} trying to access order {order_id} owned by {order_exists['buyer_id']}")
            await call.answer("❌ У вас нет прав на этот заказ", show_alert=True)
            return
        
        # Проверим активен ли заказ
        if not order_exists['is_active']:
            logger.warning(f"Order {order_id} is not active")
            await call.answer("❌ Этот заказ больше не активен", show_alert=True)
            return
        
        # Теперь получаем полную информацию о заказе
        order = q1("SELECT * FROM orders WHERE id = ? AND buyer_id = ?", (order_id, call.from_user.id))
        if not order:
            logger.error(f"Failed to get full order info for {order_id}")
            await call.answer("❌ Ошибка при получении данных заказа", show_alert=True)
            return
        
        # Проверяем существует ли уже активная сделка
        existing_deal = q1("""
            SELECT id, status FROM deals 
            WHERE order_id = ? AND status NOT IN ('CANCELLED')
        """, (order_id,))
        
        if existing_deal:
            logger.warning(f"Order {order_id} already has active deal {existing_deal['id']} with status {existing_deal['status']}")
            await call.answer(f"❌ По этому заказу уже есть активная сделка (#{existing_deal['id']})", show_alert=True)
            return
        
        # Проверяем существует ли предложение от этой фабрики
        proposal = q1("""
            SELECT p.*, f.name as factory_name
            FROM proposals p
            JOIN factories f ON p.factory_id = f.tg_id
            WHERE p.order_id = ? AND p.factory_id = ?
        """, (order_id, factory_id))
        
        if not proposal:
            logger.error(f"Proposal not found for order {order_id} and factory {factory_id}")
            await call.answer("❌ Предложение от этой фабрики не найдено", show_alert=True)
            return
        
        # Проверяем не было ли предложение уже принято
        if proposal['is_accepted']:
            logger.warning(f"Proposal for order {order_id} from factory {factory_id} already accepted")
            await call.answer("❌ Это предложение уже было принято ранее", show_alert=True)
            return
        
        # Проверяем существует ли фабрика
        factory = q1("SELECT * FROM factories WHERE tg_id = ?", (factory_id,))
        if not factory:
            logger.error(f"Factory {factory_id} not found")
            await call.answer("❌ Фабрика не найдена", show_alert=True)
            return
        
        # Все проверки пройдены, создаем сделку
        logger.info(f"Creating deal for order {order_id} and factory {factory_id}")
        
        # Calculate total amount
        total_amount = proposal['price'] * order['quantity']
        
        # Create deal
        deal_id = insert_and_get_id("""
            INSERT INTO deals
            (order_id, factory_id, buyer_id, amount, status, sample_cost)
            VALUES (?, ?, ?, ?, 'DRAFT', ?)
        """, (order_id, factory_id, call.from_user.id, total_amount, proposal['sample_cost']))
        
        if not deal_id:
            logger.error(f"Failed to create deal for order {order_id}")
            await call.answer("❌ Ошибка при создании сделки", show_alert=True)
            return
        
        # Update proposal status
        run("UPDATE proposals SET is_accepted = 1 WHERE order_id = ? AND factory_id = ?", 
            (order_id, factory_id))
        
        # Deactivate order
        run("UPDATE orders SET is_active = 0 WHERE id = ?", (order_id,))
        
        # Create deal chat
        try:
            chat_id = await create_deal_chat(deal_id)
            if chat_id:
                logger.info(f"Created chat {chat_id} for deal {deal_id}")
        except Exception as e:
            logger.error(f"Failed to create chat for deal {deal_id}: {e}")
            # Продолжаем выполнение, даже если чат не создался
        
        # Track event
        track_event(call.from_user.id, 'deal_created', {
            'deal_id': deal_id,
            'order_id': order_id,
            'factory_id': factory_id,
            'amount': total_amount
        })

        # Create deal chat automatically
        try:
            await create_deal_chat(deal_id)
        except Exception as e:
            logger.error(f"Failed to create chat for deal {deal_id}: {e}")
        
        # Notify admins about new deal
        await notify_admins(
            'deal_created',
            '🤝 Новая сделка создана!',
            f"Сделка #{deal_id}\n"
            f"Заказ: #Z-{order_id} - {order['title']}\n"
            f"Фабрика: {proposal['factory_name']}\n"
            f"Сумма: {format_price(total_amount)} ₽",
            {
                'buyer_id': call.from_user.id,
                'factory_id': factory_id,
                'category': order['category'],
                'quantity': order['quantity']
            }
        )
        
        # Send confirmation
        deal_text = (
            f"✅ <b>Сделка создана!</b>\n\n"
            f"Сделка: #{deal_id}\n"
            f"Фабрика: {proposal['factory_name']}\n"
            f"Сумма: {format_price(total_amount)} ₽\n\n"
            f"<b>Следующий шаг:</b>\n"
            f"{ORDER_STATUS_DESCRIPTIONS[OrderStatus.DRAFT]}"
        )
        
        buttons = []
        
        if proposal['sample_cost'] > 0:
            deal_text += f"\n\nСтоимость образца: {format_price(proposal['sample_cost'])} ₽"
            buttons.append([
                InlineKeyboardButton(text="💳 Оплатить образец", callback_data=f"pay_sample:{deal_id}")
            ])
        else:
            deal_text += f"\n\n✅ Образец бесплатный!"
        
        # Всегда добавляем кнопку чата
        buttons.append([
            InlineKeyboardButton(text="💬 Перейти в чат", callback_data=f"deal_chat:{deal_id}")
        ])
        
        kb = InlineKeyboardMarkup(inline_keyboard=buttons)
        
        await call.message.edit_text(deal_text, reply_markup=kb)
        
        # Notify factory
        await send_notification(
            factory_id,
            'deal_created',
            'Ваше предложение выбрано!',
            f'Заказчик выбрал ваше предложение по заказу #Z-{order_id}\n'
            f'Сумма сделки: {format_price(total_amount)} ₽\n\n'
            f'Чат по сделке создан.',
            {'deal_id': deal_id, 'order_id': order_id}
        )
        
        # Notify other factories that didn't win
        other_proposals = q("""
            SELECT factory_id FROM proposals 
            WHERE order_id = ? AND factory_id != ? AND is_accepted = 0
        """, (order_id, factory_id))
        
        for prop in other_proposals:
            await send_notification(
                prop['factory_id'],
                'proposal_rejected',
                'Предложение не выбрано',
                f'К сожалению, заказчик выбрал другую фабрику для заказа #Z-{order_id}',
                {'order_id': order_id}
            )
        
        logger.info(f"Deal {deal_id} created successfully for order {order_id}")
        await call.answer("✅ Сделка создана!")
        
    except ValueError as e:
        logger.error(f"ValueError in choose_factory: {e}, callback_data: {call.data}")
        await call.answer("❌ Неверный формат данных", show_alert=True)
        
    except Exception as e:
        logger.error(f"Unexpected error in choose_factory: {e}, callback_data: {call.data}")
        await call.answer("❌ Произошла ошибка. Попробуйте позже или обратитесь в поддержку.", show_alert=True)

# Дополнительная функция для диагностики заказов (для админов)
async def diagnose_order(order_id: int) -> str:
    """Диагностика состояния заказа для отладки"""
    
    order = q1("SELECT * FROM orders WHERE id = ?", (order_id,))
    if not order:
        return f"❌ Заказ {order_id} не существует"
    
    proposals = q("SELECT * FROM proposals WHERE order_id = ?", (order_id,))
    deals = q("SELECT * FROM deals WHERE order_id = ?", (order_id,))
    
    result = f"🔍 Диагностика заказа #{order_id}:\n\n"
    result += f"📋 Заказ: {order['title']}\n"
    result += f"👤 Заказчик ID: {order['buyer_id']}\n"
    result += f"✅ Активен: {'Да' if order['is_active'] else 'Нет'}\n"
    result += f"💳 Оплачен: {'Да' if order['paid'] else 'Нет'}\n"
    result += f"📅 Создан: {order['created_at']}\n\n"
    
    result += f"💌 Предложений: {len(proposals)}\n"
    for prop in proposals:
        result += f"  • Фабрика {prop['factory_id']}: {'✅ Принято' if prop['is_accepted'] else '⏳ Ожидает'}\n"
    
    result += f"\n🤝 Сделок: {len(deals)}\n"
    for deal in deals:
        result += f"  • #{deal['id']}: {deal['status']}\n"
    
    return result

# Команда для админов для диагностики
@router.message(Command("diagnose"))
async def cmd_diagnose_order(msg: Message) -> None:
    """Diagnose order for admin."""
    if msg.from_user.id not in ADMIN_IDS:
        return
    
    try:
        # Ожидаем команду в формате /diagnose 123
        if not msg.text or len(msg.text.split()) < 2:
            await msg.answer("Использование: /diagnose <order_id>")
            return
        
        order_id = int(msg.text.split()[1])
        diagnosis = await diagnose_order(order_id)
        await msg.answer(diagnosis)
        
    except ValueError:
        await msg.answer("❌ Неверный формат order_id")
    except Exception as e:
        await msg.answer(f"❌ Ошибка: {e}")

# ---------------------------------------------------------------------------
#  Background tasks для уведомлений фабрик
# ---------------------------------------------------------------------------

async def notify_factories_about_order(order_row: sqlite3.Row) -> int:
    """Notify matching factories about new order."""
    factories = q("""
        SELECT f.tg_id, f.name, u.notifications 
        FROM factories f
        JOIN users u ON f.tg_id = u.tg_id
        WHERE f.is_pro = 1
          AND f.min_qty <= ?
          AND f.avg_price <= ?
          AND (',' || f.categories || ',') LIKE ('%,' || ? || ',%')
          AND u.is_active = 1
          AND u.is_banned = 0
    """, (order_row['quantity'], order_row['budget'], order_row['category']))
    
    notified_count = 0
    for factory in factories:
        if factory['notifications']:
            try:
                kb = InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="👀 Посмотреть", callback_data=f"view_order:{order_row['id']}"),
                    InlineKeyboardButton(text="💌 Откликнуться", callback_data=f"lead:{order_row['id']}")
                ]])
                
                await bot.send_message(
                    factory['tg_id'],
                    f"🔥 <b>Новая заявка в вашей категории!</b>\n\n" + order_caption(order_row),
                    reply_markup=kb
                )
                notified_count += 1
                
                # Track notification
                await send_notification(
                    factory['tg_id'],
                    'new_order',
                    'Новая заявка',
                    f"Заявка #{order_row['id']} в категории {order_row['category']}",
                    {'order_id': order_row['id']}
                )
            except Exception as e:
                logger.error(f"Failed to notify factory {factory['tg_id']}: {e}")
    
    logger.info(f"Order #{order_row['id']} notified to {notified_count} factories")
    return notified_count

# ---------------------------------------------------------------------------
#  ДОРАБОТКА: Дополнительные обработчики callback'ов
# ---------------------------------------------------------------------------

@router.callback_query(F.data == "back_to_leads")
async def back_to_leads(call: CallbackQuery) -> None:
    """Go back to leads list."""
    await call.message.delete()
    await call.answer()

@router.callback_query(F.data.startswith("view_proposal:"))
async def view_existing_proposal(call: CallbackQuery) -> None:
    """View existing proposal."""
    order_id = int(call.data.split(":", 1)[1])
    
    proposal = q1("""
        SELECT p.*, o.title, o.category, o.quantity
        FROM proposals p
        JOIN orders o ON p.order_id = o.id
        WHERE p.order_id = ? AND p.factory_id = ?
    """, (order_id, call.from_user.id))
    
    if not proposal:
        await call.answer("Предложение не найдено", show_alert=True)
        return
    
    proposal_text = (
        f"<b>Ваше предложение на заказ #Z-{order_id}</b>\n\n"
        f"📦 {proposal['title']}\n"
        f"🔢 Количество: {format_price(proposal['quantity'])} шт.\n\n"
        f"💰 Ваша цена: {format_price(proposal['price'])} ₽/шт.\n"
        f"📅 Срок: {proposal['lead_time']} дней\n"
        f"🧵 Образец: {format_price(proposal['sample_cost'])} ₽\n"
    )
    
    if proposal['message']:
        proposal_text += f"\n💬 Ваше сообщение:\n{proposal['message']}"
    
    status_text = "✅ Принято" if proposal['is_accepted'] else "⏳ Ожидает решения"
    proposal_text += f"\n\n📊 Статус: {status_text}"
    
    buttons = []
    if not proposal['is_accepted']:
        buttons.append([
            InlineKeyboardButton(text="✏️ Редактировать", callback_data=f"edit_existing_proposal:{proposal['id']}")
        ])
    
    kb = InlineKeyboardMarkup(inline_keyboard=buttons) if buttons else None
    
    await call.message.answer(proposal_text, reply_markup=kb)
    await call.answer()

@router.callback_query(F.data.startswith("competition:"))
async def view_competition(call: CallbackQuery) -> None:
    """View competition for order."""
    order_id = int(call.data.split(":", 1)[1])
    
    proposals = q("""
        SELECT COUNT(*) as total,
               AVG(price) as avg_price,
               MIN(price) as min_price,
               MAX(price) as max_price,
               AVG(lead_time) as avg_lead_time
        FROM proposals
        WHERE order_id = ?
    """, (order_id,))
    
    if not proposals or proposals[0]['total'] == 0:
        await call.answer("Нет данных о конкуренции", show_alert=True)
        return
    
    stats = proposals[0]
    competition_text = (
        f"📊 <b>Конкуренция по заказу #Z-{order_id}</b>\n\n"
        f"👥 Предложений: {stats['total']}\n"
        f"💰 Средняя цена: {format_price(int(stats['avg_price']))} ₽\n"
        f"💰 Мин. цена: {format_price(stats['min_price'])} ₽\n"
        f"💰 Макс. цена: {format_price(stats['max_price'])} ₽\n"
        f"📅 Средний срок: {int(stats['avg_lead_time'])} дней"
    )
    
    await call.message.answer(competition_text)
    await call.answer()

@router.callback_query(F.data.startswith("load_more_orders:"))
async def load_more_orders(call: CallbackQuery) -> None:
    """Load more orders."""
    offset = int(call.data.split(":", 1)[1])
    
    factory = q1("SELECT * FROM factories WHERE tg_id = ? AND is_pro = 1", (call.from_user.id,))
    if not factory:
        await call.answer("Доступ запрещен", show_alert=True)
        return
    
    # Get more matching orders
    matching_orders = q("""
        SELECT o.*, 
               (SELECT COUNT(*) FROM proposals p WHERE p.order_id = o.id) as proposals_count,
               (SELECT COUNT(*) FROM proposals p WHERE p.order_id = o.id AND p.factory_id = ?) as has_proposal
        FROM orders o
        WHERE o.paid = 1 
          AND o.is_active = 1
          AND o.quantity >= ? 
          AND o.budget >= ?
          AND (',' || ? || ',') LIKE ('%,' || o.category || ',%')
          AND NOT EXISTS (
              SELECT 1 FROM deals d 
              WHERE d.order_id = o.id AND d.status != 'CANCELLED'
          )
        ORDER BY o.created_at DESC
        LIMIT 5 OFFSET ?
    """, (
        call.from_user.id,
        factory['min_qty'],
        factory['avg_price'],
        factory['categories'],
        offset
    ))
    
    if not matching_orders:
        await call.answer("Больше заявок нет", show_alert=True)
        return
    
    # Send additional orders
    for order in matching_orders:
        buttons = []
        
        first_row = [
            InlineKeyboardButton(text="👀 Подробнее", callback_data=f"view_order:{order['id']}")
        ]
        
        if order['has_proposal']:
            first_row.append(
                InlineKeyboardButton(text="✅ Вы откликнулись", callback_data=f"view_proposal:{order['id']}")
            )
        else:
            first_row.append(
                InlineKeyboardButton(text="💌 Откликнуться", callback_data=f"lead:{order['id']}")
            )
        
        buttons.append(first_row)
        
        if order['proposals_count'] > 0:
            buttons.append([
                InlineKeyboardButton(
                    text=f"👥 Предложений: {order['proposals_count']}", 
                    callback_data=f"competition:{order['id']}"
                )
            ])
        
        kb = InlineKeyboardMarkup(inline_keyboard=buttons)
        await call.message.answer(order_caption(order), reply_markup=kb)
    
    # Update load more button
    new_offset = offset + 5
    total_orders = q1("""
        SELECT COUNT(*) as cnt FROM orders o
        WHERE o.paid = 1 
          AND o.is_active = 1
          AND o.quantity >= ? 
          AND o.budget >= ?
          AND (',' || ? || ',') LIKE ('%,' || o.category || ',%')
          AND NOT EXISTS (
              SELECT 1 FROM deals d 
              WHERE d.order_id = o.id AND d.status != 'CANCELLED'
          )
    """, (factory['min_qty'], factory['avg_price'], factory['categories']))
    
    if new_offset < total_orders['cnt']:
        new_kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="📋 Показать еще", callback_data=f"load_more_orders:{new_offset}")
        ]])
        await call.message.edit_reply_markup(reply_markup=new_kb)
    else:
        await call.message.edit_text("Все заявки показаны")
    
    await call.answer(f"Загружено еще {len(matching_orders)} заявок")

# ---------------------------------------------------------------------------
#  Background tasks and startup
# ---------------------------------------------------------------------------

async def run_background_tasks():
    """Run periodic background tasks with proper event loop handling."""
    last_daily_report = None
    
    # Получаем текущий event loop
    loop = asyncio.get_running_loop()
    logger.info(f"Background tasks starting in loop: {id(loop)}")
    
    while True:
        try:
            current_time = datetime.now()
            
            # Check PRO expiration every hour
            await check_pro_expiration()
            
            # Clean up old notifications
            run("""
                DELETE FROM notifications 
                WHERE is_sent = 1 
                  AND created_at < datetime('now', '-30 days')
            """)
            
            # Send daily report at 9:00 AM
            if current_time.hour == 9 and last_daily_report != current_time.date():
                await send_daily_report()
                last_daily_report = current_time.date()
            
            # Update analytics
            daily_stats = q1("""
                SELECT 
                    COUNT(DISTINCT CASE WHEN role = 'factory' THEN tg_id END) as factories,
                    COUNT(DISTINCT CASE WHEN role = 'buyer' THEN tg_id END) as buyers,
                    COUNT(DISTINCT o.id) as orders,
                    COUNT(DISTINCT d.id) as deals
                FROM users u
                LEFT JOIN orders o ON u.tg_id = o.buyer_id 
                    AND date(o.created_at) = date('now')
                LEFT JOIN deals d ON u.tg_id IN (d.buyer_id, d.factory_id) 
                    AND date(d.created_at) = date('now')
            """)
            
            logger.info(
                f"Daily stats - Factories: {daily_stats['factories']}, "
                f"Buyers: {daily_stats['buyers']}, "
                f"Orders: {daily_stats['orders']}, "
                f"Deals: {daily_stats['deals']}"
            )
            
            # Check for stale deals (no activity for 7 days)
            stale_deals = q("""
                SELECT d.*, o.title, f.name as factory_name, u.username as buyer_username
                FROM deals d
                JOIN orders o ON d.order_id = o.id
                JOIN factories f ON d.factory_id = f.tg_id
                JOIN users u ON d.buyer_id = u.tg_id
                WHERE d.status NOT IN ('DELIVERED', 'CANCELLED')
                  AND d.updated_at < datetime('now', '-7 days')
            """)
            
            if stale_deals:
                stale_report = "<b>⚠️ Застрявшие сделки (нет активности > 7 дней)</b>\n\n"
                for deal in stale_deals[:5]:
                    stale_report += (
                        f"#{deal['id']} - {deal['title']}\n"
                        f"Статус: {deal['status']}\n"
                        f"Покупатель: @{deal['buyer_username']}\n"
                        f"Фабрика: {deal['factory_name']}\n\n"
                    )
                
                await notify_admins(
                    'stale_deals',
                    '⚠️ Обнаружены застрявшие сделки',
                    stale_report,
                    {'count': len(stale_deals)},
                    [[InlineKeyboardButton(text="📋 Все застрявшие", callback_data="admin_stale_deals")]]
                )
            
        except RuntimeError as e:
            if "event loop" in str(e).lower():
                logger.error(f"Event loop error in background tasks: {e}")
                # Пытаемся переключиться на текущий loop
                try:
                    loop = asyncio.get_running_loop()
                    logger.info(f"Switched to loop: {id(loop)}")
                except:
                    pass
            else:
                logger.error(f"Runtime error in background tasks: {e}")
        except Exception as e:
            logger.error(f"Error in background tasks: {e}")
        
        # Run every hour
        await asyncio.sleep(3600)

async def on_startup(bot: Bot) -> None:
    """Run on bot startup with proper event loop handling."""
    init_db()
    
    # Get current event loop
    loop = asyncio.get_running_loop()
    logger.info(f"Bot starting in loop: {id(loop)}")
    
    # Start background tasks in the same loop
    loop.create_task(run_background_tasks())
    
    # Set bot commands
    await bot.set_my_commands([
        BotCommand(command="start", description="Главное меню"),
        BotCommand(command="help", description="Помощь"),
        BotCommand(command="profile", description="Мой профиль"),
        BotCommand(command="support", description="Поддержка"),
    ])
    
    logger.info("Bot startup complete ✅")

# ---------------------------------------------------------------------------
#  Profile commands
# ---------------------------------------------------------------------------

@router.message(Command("profile"))
@router.message(F.text.in_(["👤 Профиль", "🧾 Профиль"]))
async def cmd_profile(msg: Message) -> None:
    """Show user profile."""
    user = get_or_create_user(msg.from_user)
    role = UserRole(user['role'])
    
    if role == UserRole.FACTORY:
        factory = q1("SELECT * FROM factories WHERE tg_id = ?", (msg.from_user.id,))
        if not factory:
            await msg.answer("Профиль фабрики не найден", reply_markup=kb_main())
            return
        
        # Calculate stats
        active_deals = q1(
            "SELECT COUNT(*) as cnt FROM deals WHERE factory_id = ? AND status NOT IN ('DELIVERED', 'CANCELLED')",
            (msg.from_user.id,)
        )
        
        total_revenue = q1(
            "SELECT SUM(amount) as total FROM deals WHERE factory_id = ? AND status = 'DELIVERED'",
            (msg.from_user.id,)
        )
        
        profile_text = (
            f"<b>Профиль фабрики</b>\n\n"
            f"🏢 {factory['name']}\n"
            f"📍 {factory['address']}\n"
            f"🏷 ИНН: {factory['inn']}\n"
        )
        
        # Categories
        categories = factory['categories'].split(',')
        categories_text = ", ".join([c.capitalize() for c in categories[:5]])
        if len(categories) > 5:
            categories_text += f" +{len(categories) - 5}"
        profile_text += f"📦 Категории: {categories_text}\n"
        
        # Production capacity
        profile_text += f"📊 Партии: {format_price(factory['min_qty'])} - {format_price(factory['max_qty'])} шт.\n"
        profile_text += f"💰 Средняя цена: {format_price(factory['avg_price'])} ₽\n\n"
        
        # Stats
        profile_text += "<b>Статистика:</b>\n"
        if factory['rating_count'] > 0:
            profile_text += f"⭐ Рейтинг: {factory['rating']:.1f}/5.0 ({factory['rating_count']} отзывов)\n"
        else:
            profile_text += "⭐ Рейтинг: нет отзывов\n"
        
        profile_text += f"✅ Выполнено: {factory['completed_orders']} заказов\n"
        profile_text += f"🔄 Активных сделок: {active_deals['cnt']}\n"
        
        if total_revenue and total_revenue['total']:
            profile_text += f"💵 Общий оборот: {format_price(total_revenue['total'])} ₽\n"
        
        # PRO status
        profile_text += f"\n<b>Статус:</b> "
        if factory['is_pro']:
            if factory['pro_expires']:
                profile_text += f"✅ PRO до {factory['pro_expires'][:10]}"
            else:
                profile_text += "✅ PRO (бессрочно)"
        else:
            profile_text += "❌ Базовый (оформите PRO для получения заявок)"
        
        # Action buttons
        buttons = []
        if not factory['is_pro']:
            buttons.append([InlineKeyboardButton(text="💳 Оформить PRO", callback_data="upgrade_pro")])
        
        buttons.append([
            InlineKeyboardButton(text="✏️ Изменить данные", callback_data="edit_profile"),
            InlineKeyboardButton(text="📸 Фото", callback_data="manage_photos")
        ])
        
        kb = InlineKeyboardMarkup(inline_keyboard=buttons) if buttons else None
        
        await msg.answer(profile_text, reply_markup=kb)
        
    elif role == UserRole.BUYER:
        # Buyer profile
        stats = q1("""
            SELECT 
                COUNT(DISTINCT o.id) as total_orders,
                COUNT(DISTINCT d.id) as total_deals,
                SUM(CASE WHEN o.is_active = 1 THEN 1 ELSE 0 END) as active_orders
            FROM orders o
            LEFT JOIN deals d ON o.id = d.order_id
            WHERE o.buyer_id = ?
        """, (msg.from_user.id,))
        
        profile_text = (
            f"<b>Профиль заказчика</b>\n\n"
            f"👤 {user['full_name']}\n"
            f"🆔 ID: {msg.from_user.id}\n"
        )
        
        if user['phone']:
            profile_text += f"📱 Телефон: {user['phone']}\n"
        if user['email']:
            profile_text += f"📧 Email: {user['email']}\n"
        
        profile_text += (
            f"\n<b>Статистика:</b>\n"
            f"📋 Всего заказов: {stats['total_orders']}\n"
            f"✅ Завершено сделок: {stats['total_deals']}\n"
            f"🔄 Активных заказов: {stats['active_orders']}\n"
        )
        
        # Last order
        last_order = q1(
            "SELECT * FROM orders WHERE buyer_id = ? ORDER BY created_at DESC LIMIT 1",
            (msg.from_user.id,)
        )
        
        if last_order:
            profile_text += f"\n📅 Последний заказ: {last_order['created_at'][:10]}"
        
        buttons = [[
            InlineKeyboardButton(text="✏️ Изменить данные", callback_data="edit_profile"),
            InlineKeyboardButton(text="📋 История заказов", callback_data="order_history")
        ]]
        
        kb = InlineKeyboardMarkup(inline_keyboard=buttons)
        await msg.answer(profile_text, reply_markup=kb)
        
    else:
        await msg.answer(
            "У вас пока нет профиля. Выберите, кто вы:",
            reply_markup=kb_main()
        )

# ---------------------------------------------------------------------------
#  Settings
# ---------------------------------------------------------------------------

@router.message(F.text == "⚙️ Настройки")
async def cmd_settings(msg: Message, state: FSMContext) -> None:
    """Show simplified settings menu."""
    await state.clear()
    
    settings_text = (
        "<b>Настройки</b>\n\n"
        "Управление вашим аккаунтом:"
    )
    
    buttons = [
        [
            InlineKeyboardButton(
                text="🗑 Удалить аккаунт", 
                callback_data="settings:delete_account"
            )
        ]
    ]
    
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    await msg.answer(settings_text, reply_markup=kb)

@router.callback_query(F.data == "settings:delete_account")
async def delete_account_confirm(call: CallbackQuery) -> None:
    """Confirm account deletion."""
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="❌ Да, удалить", callback_data="confirm_delete_account"),
            InlineKeyboardButton(text="✅ Отмена", callback_data="cancel_delete_account")
        ]
    ])
    
    await call.message.edit_text(
        "⚠️ <b>Вы уверены, что хотите удалить аккаунт?</b>\n\n"
        "Это действие необратимо. Будут удалены:\n"
        "• Ваш профиль\n"
        "• История заказов/предложений\n"
        "• Все данные\n\n"
        "Активные сделки будут завершены через поддержку.",
        reply_markup=kb
    )
    await call.answer()

@router.callback_query(F.data == "confirm_delete_account")
async def delete_account_execute(call: CallbackQuery) -> None:
    """Execute account deletion."""
    user_id = call.from_user.id
    
    # Check active deals
    active_deals = q1("""
        SELECT COUNT(*) as cnt FROM deals 
        WHERE (buyer_id = ? OR factory_id = ?) 
        AND status NOT IN ('DELIVERED', 'CANCELLED')
    """, (user_id, user_id))
    
    if active_deals and active_deals['cnt'] > 0:
        await call.message.edit_text(
            "❌ <b>Невозможно удалить аккаунт</b>\n\n"
            f"У вас есть {active_deals['cnt']} активных сделок.\n"
            "Завершите все сделки или обратитесь в поддержку."
        )
        await call.answer()
        return
    
    try:
        # Delete all user data
        run("DELETE FROM ratings WHERE buyer_id = ? OR factory_id = ?", (user_id, user_id))
        run("DELETE FROM proposals WHERE factory_id = ?", (user_id,))
        run("DELETE FROM factory_photos WHERE factory_id = ?", (user_id,))
        run("DELETE FROM factories WHERE tg_id = ?", (user_id,))
        run("DELETE FROM orders WHERE buyer_id = ?", (user_id,))
        run("DELETE FROM notifications WHERE user_id = ?", (user_id,))
        run("DELETE FROM ticket_messages WHERE user_id = ?", (user_id,))
        run("DELETE FROM tickets WHERE user_id = ?", (user_id,))
        run("DELETE FROM analytics WHERE user_id = ?", (user_id,))
        run("DELETE FROM users WHERE tg_id = ?", (user_id,))
        
        # Notify admins
        await notify_admins(
            'account_deleted',
            '🗑 Аккаунт удален',
            f"Пользователь {call.from_user.username or call.from_user.full_name} удалил свой аккаунт",
            {'user_id': user_id}
        )
        
        await call.message.edit_text(
            "✅ <b>Аккаунт успешно удален</b>\n\n"
            "Все ваши данные удалены из системы.\n"
            "Спасибо за использование Mono-Fabrique!"
        )
        
    except Exception as e:
        logger.error(f"Error deleting account {user_id}: {e}")
        await call.message.edit_text(
            "❌ Ошибка при удалении аккаунта.\n"
            "Обратитесь в поддержку."
        )
    
    await call.answer()

@router.callback_query(F.data == "cancel_delete_account")
async def cancel_delete_account(call: CallbackQuery) -> None:
    """Cancel account deletion."""
    await call.message.edit_text("✅ Удаление аккаунта отменено")
    await call.answer()

# ---------------------------------------------------------------------------
#  Support system
# ---------------------------------------------------------------------------

@router.message(F.text == "📞 Поддержка")
async def cmd_support(msg: Message, state: FSMContext) -> None:
    """Show support menu."""
    await state.clear()
    
    # Check for open tickets
    open_tickets = q("""
        SELECT COUNT(*) as cnt 
        FROM tickets 
        WHERE user_id = ? AND status = 'open'
    """, (msg.from_user.id,))
    
    support_text = (
        "<b>Поддержка Mono-Fabrique</b>\n\n"
        "Мы готовы помочь вам 24/7!\n\n"
        "📧 Email: support@mono-fabrique.ru\n"
        "📱 Телефон: +7 (800) 123-45-67\n"
        "💬 Telegram: @mono_fabrique_support\n\n"
    )
    
    if open_tickets and open_tickets[0]['cnt'] > 0:
        support_text += f"У вас есть {open_tickets[0]['cnt']} открытых обращений\n\n"
    
    support_text += "Выберите тему обращения:"
    
    buttons = [
        [InlineKeyboardButton(text="❓ Общий вопрос", callback_data="ticket:general")],
        [InlineKeyboardButton(text="💳 Проблемы с оплатой", callback_data="ticket:payment")],
        [InlineKeyboardButton(text="📦 Вопрос по заказу", callback_data="ticket:order")],
        [InlineKeyboardButton(text="🏭 Вопрос по работе фабрики", callback_data="ticket:factory")],
        [InlineKeyboardButton(text="🚨 Жалоба", callback_data="ticket:complaint")],
        [InlineKeyboardButton(text="💡 Предложение", callback_data="ticket:suggestion")]
    ]
    
    if open_tickets and open_tickets[0]['cnt'] > 0:
        buttons.append([
            InlineKeyboardButton(
                text="📋 Мои обращения", 
                callback_data="my_tickets"
            )
        ])
    
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    await msg.answer(support_text, reply_markup=kb)

@router.callback_query(F.data.startswith("ticket:"))
async def create_support_ticket(call: CallbackQuery, state: FSMContext) -> None:
    """Start creating support ticket."""
    category = call.data.split(":", 1)[1]
    
    category_names = {
        'general': 'Общий вопрос',
        'payment': 'Проблемы с оплатой',
        'order': 'Вопрос по заказу',
        'factory': 'Вопрос по работе фабрики',
        'complaint': 'Жалоба',
        'suggestion': 'Предложение'
    }
    
    await state.update_data(ticket_category=category)
    await state.set_state(TicketForm.subject)
    
    await call.message.answer(
        f"<b>Создание обращения</b>\n"
        f"Категория: {category_names.get(category, category)}\n\n"
        f"Введите тему обращения:",
        reply_markup=ReplyKeyboardRemove()
    )
    await call.answer()

@router.message(TicketForm.subject)
async def ticket_subject(msg: Message, state: FSMContext) -> None:
    """Process ticket subject."""
    if not msg.text or len(msg.text) < 5:
        await msg.answer("Введите более подробную тему (минимум 5 символов):")
        return
    
    await state.update_data(subject=msg.text.strip())
    await state.set_state(TicketForm.message)
    await msg.answer("Опишите вашу проблему или вопрос подробно:")

@router.message(TicketForm.message)
async def ticket_message(msg: Message, state: FSMContext) -> None:
    """Process ticket message and create ticket."""
    if not msg.text or len(msg.text) < 20:
        await msg.answer("Пожалуйста, опишите проблему подробнее (минимум 20 символов):")
        return
    
    data = await state.get_data()
    
    # Determine priority based on category
    priority = 'normal'
    if data['ticket_category'] in ['payment', 'complaint']:
        priority = 'high'
    
    # Create ticket
    ticket_id = insert_and_get_id("""
        INSERT INTO tickets (user_id, subject, category, priority, status)
        VALUES (?, ?, ?, ?, 'open')
    """, (msg.from_user.id, data['subject'], data['ticket_category'], priority))
    
    # Create first message
    insert_and_get_id("""
        INSERT INTO ticket_messages (ticket_id, user_id, message)
        VALUES (?, ?, ?)
    """, (ticket_id, msg.from_user.id, msg.text.strip()))
    
    # Get user info
    user = get_or_create_user(msg.from_user)
    
    # Notify admins about new ticket
    priority_emoji = {'high': '🔴', 'normal': '🟡'}.get(priority, '🟢')
    
    await notify_admins(
        'new_ticket',
        f'{priority_emoji} Новый тикет #{ticket_id}',
        f"От: @{msg.from_user.username or user['full_name']}\n"
        f"Категория: {data['ticket_category']}\n"
        f"Тема: {data['subject']}\n\n"
        f"Сообщение:\n{msg.text[:200]}{'...' if len(msg.text) > 200 else ''}",
        {
            'ticket_id': ticket_id,
            'user_id': msg.from_user.id,
            'priority': priority
        },
        [[
            InlineKeyboardButton(text="💬 Ответить", url=f"tg://user?id={msg.from_user.id}")
        ]]
    )
    
    await state.clear()
    await msg.answer(
        f"✅ <b>Обращение #{ticket_id} создано!</b>\n\n"
        f"Мы ответим вам в течение 24 часов.\n"
        f"Вы получите уведомление о нашем ответе.\n\n"
        f"Спасибо за обращение!",
        reply_markup=kb_main(get_user_role(msg.from_user.id))
    )

# ---------------------------------------------------------------------------
#  ДОРАБОТКА: Дополнительные команды и обработчики
# ---------------------------------------------------------------------------

@router.message(F.text.in_(["ℹ️ Как работает", "ℹ Как работает"]))
async def cmd_how_it_works(msg: Message) -> None:
    """Explain how the platform works."""
    await msg.answer(
        "<b>Как работает Mono-Fabrique:</b>\n\n"
        "<b>Для заказчиков:</b>\n"
        "1️⃣ Размещаете заказ (700 ₽)\n"
        "2️⃣ Получаете предложения от фабрик\n"
        "3️⃣ Выбираете лучшее предложение\n"
        "4️⃣ Оплачиваете через безопасный Escrow\n"
        "5️⃣ Контролируете производство\n"
        "6️⃣ Получаете готовый товар\n\n"
        "<b>Для фабрик:</b>\n"
        "1️⃣ Оформляете PRO-подписку (2000 ₽/мес)\n"
        "2️⃣ Получаете подходящие заявки\n"
        "3️⃣ Отправляете предложения\n"
        "4️⃣ Заключаете сделки\n"
        "5️⃣ Производите и отправляете\n"
        "6️⃣ Получаете оплату через Escrow\n\n"
        "💎 <b>Преимущества:</b>\n"
        "• Прямые контакты без посредников\n"
        "• Безопасные сделки\n"
        "• Рейтинги и отзывы\n"
        "• Поддержка на всех этапах",
        reply_markup=kb_main(get_user_role(msg.from_user.id))
    )

@router.message(F.text.in_(["💰 Тарифы", "🧾 Тарифы"]))
async def cmd_tariffs(msg: Message) -> None:
    """Show tariffs."""
    await msg.answer(
        "<b>Тарифы Mono-Fabrique:</b>\n\n"
        "🏭 <b>Для фабрик:</b>\n"
        "• PRO-подписка: 2 000 ₽/месяц\n"
        "• Безлимитные отклики на заявки\n"
        "• Приоритет в поиске\n"
        "• Расширенная аналитика\n"
        "• Поддержка 24/7\n\n"
        "🛍 <b>Для заказчиков:</b>\n"
        "• Размещение заказа: 700 ₽\n"
        "• Неограниченные предложения\n"
        "• Безопасный Escrow\n"
        "• Контроль на всех этапах\n"
        "• Поддержка сделки\n\n"
        "💳 <b>Комиссии:</b>\n"
        "Мы НЕ берем комиссию с суммы сделки!\n"
        "Только фиксированные платежи.",
        reply_markup=kb_main(get_user_role(msg.from_user.id))
    )

# ---------------------------------------------------------------------------
#  ДОРАБОТКА: Редактирование профиля фабрики
# ---------------------------------------------------------------------------

@router.callback_query(F.data == "edit_profile")
async def edit_profile_start(call: CallbackQuery, state: FSMContext) -> None:
    """Start profile editing."""
    user_role = get_user_role(call.from_user.id)
    
    if user_role == UserRole.FACTORY:
        factory = q1("SELECT * FROM factories WHERE tg_id = ?", (call.from_user.id,))
        if not factory:
            await call.answer("Профиль не найден", show_alert=True)
            return
        
        buttons = [
            [InlineKeyboardButton(text="🏢 Название", callback_data="edit_field:name")],
            [InlineKeyboardButton(text="📍 Адрес", callback_data="edit_field:address")],
            [InlineKeyboardButton(text="📦 Категории", callback_data="edit_field:categories")],
            [InlineKeyboardButton(text="📊 Мин. партия", callback_data="edit_field:min_qty")],
            [InlineKeyboardButton(text="📊 Макс. партия", callback_data="edit_field:max_qty")],
            [InlineKeyboardButton(text="💰 Средняя цена", callback_data="edit_field:avg_price")],
            [InlineKeyboardButton(text="📝 Описание", callback_data="edit_field:description")],
            [InlineKeyboardButton(text="🔗 Портфолио", callback_data="edit_field:portfolio")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_edit")]
        ]
        
        kb = InlineKeyboardMarkup(inline_keyboard=buttons)
        
        await call.message.edit_text(
            "<b>Что хотите изменить?</b>\n\n"
            "Выберите пункт для редактирования:",
            reply_markup=kb
        )
    
    elif user_role == UserRole.BUYER:
        buttons = [
            [InlineKeyboardButton(text="👤 Имя", callback_data="edit_field:full_name")],
            [InlineKeyboardButton(text="📱 Телефон", callback_data="edit_field:phone")],
            [InlineKeyboardButton(text="📧 Email", callback_data="edit_field:email")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_edit")]
        ]
        
        kb = InlineKeyboardMarkup(inline_keyboard=buttons)
        
        await call.message.edit_text(
            "<b>Что хотите изменить?</b>\n\n"
            "Выберите пункт для редактирования:",
            reply_markup=kb
        )
    
    await call.answer()

@router.callback_query(F.data.startswith("edit_field:"))
async def edit_field_select(call: CallbackQuery, state: FSMContext) -> None:
    """Select field to edit."""
    field = call.data.split(":", 1)[1]
    
    field_names = {
        'name': 'название фабрики',
        'address': 'адрес производства',
        'categories': 'категории',
        'min_qty': 'минимальную партию',
        'max_qty': 'максимальную партию',
        'avg_price': 'среднюю цену',
        'description': 'описание',
        'portfolio': 'ссылку на портфолио',
        'full_name': 'имя',
        'phone': 'телефон',
        'email': 'email'
    }
    
    await state.update_data(edit_field=field)
    await state.set_state(ProfileEditForm.new_value)
    
    if field == 'categories':
        await call.message.edit_text(
            "Выберите новые категории:",
            reply_markup=kb_categories()
        )
        await state.update_data(selected_categories=[])
    else:
        await call.message.edit_text(
            f"Введите новое значение для поля «{field_names.get(field, field)}»:"
        )
    
    await call.answer()

@router.callback_query(F.data.startswith("cat:"), ProfileEditForm.new_value)
async def edit_category_select(call: CallbackQuery, state: FSMContext) -> None:
    """Handle category selection during profile edit."""
    category = call.data.split(":", 1)[1]
    
    if category == "done":
        data = await state.get_data()
        selected = data.get("selected_categories", [])
        
        if not selected:
            await call.answer("Выберите хотя бы одну категорию!", show_alert=True)
            return
        
        # Update categories
        categories_str = ",".join(selected)
        run("UPDATE factories SET categories = ? WHERE tg_id = ?", 
            (categories_str, call.from_user.id))
        
        await call.message.edit_text(
            f"✅ Категории обновлены!\n\n"
            f"Новые категории: {', '.join([c.capitalize() for c in selected])}"
        )
        
        await state.clear()
    else:
        data = await state.get_data()
        selected: list = data.get("selected_categories", [])
        
        if category in selected:
            selected.remove(category)
            await call.answer(f"❌ {category} удалена")
        else:
            selected.append(category)
            await call.answer(f"✅ {category} добавлена")
        
        await state.update_data(selected_categories=selected)
    
    await call.answer()

@router.message(ProfileEditForm.new_value)
async def edit_field_save(msg: Message, state: FSMContext) -> None:
    """Save edited field value."""
    data = await state.get_data()
    field = data.get('edit_field')
    new_value = msg.text.strip() if msg.text else ""
    
    if not new_value:
        await msg.answer("❌ Введите корректное значение:")
        return
    
    user_role = get_user_role(msg.from_user.id)
    
    try:
        if user_role == UserRole.FACTORY:
            if field in ['min_qty', 'max_qty', 'avg_price']:
                new_value = parse_digits(new_value)
                if not new_value or new_value < 1:
                    await msg.answer("❌ Введите корректное число:")
                    return
            
            run(f"UPDATE factories SET {field} = ? WHERE tg_id = ?", 
                (new_value, msg.from_user.id))
        
        elif user_role == UserRole.BUYER:
            run(f"UPDATE users SET {field} = ? WHERE tg_id = ?", 
                (new_value, msg.from_user.id))
        
        field_names = {
            'name': 'Название фабрики',
            'address': 'Адрес',
            'min_qty': 'Минимальная партия',
            'max_qty': 'Максимальная партия',
            'avg_price': 'Средняя цена',
            'description': 'Описание',
            'portfolio': 'Портфолио',
            'full_name': 'Имя',
            'phone': 'Телефон',
            'email': 'Email'
        }
        
        await msg.answer(
            f"✅ {field_names.get(field, field)} обновлено!",
            reply_markup=kb_factory_menu() if user_role == UserRole.FACTORY else kb_buyer_menu()
        )
        
        await state.clear()
        
    except Exception as e:
        logger.error(f"Error updating profile field {field}: {e}")
        await msg.answer("❌ Ошибка при обновлении данных")

@router.callback_query(F.data == "cancel_edit")
async def cancel_edit(call: CallbackQuery, state: FSMContext) -> None:
    """Cancel profile editing."""
    await state.clear()
    await call.message.edit_text("❌ Редактирование отменено")
    await call.answer()

# ---------------------------------------------------------------------------
#  ДОРАБОТКА: Управление фотографиями фабрики
# ---------------------------------------------------------------------------

@router.callback_query(F.data == "manage_photos")
async def manage_photos_start(call: CallbackQuery, state: FSMContext) -> None:
    """Start photo management."""
    factory = q1("SELECT * FROM factories WHERE tg_id = ?", (call.from_user.id,))
    if not factory:
        await call.answer("Профиль фабрики не найден", show_alert=True)
        return

    photos = q("SELECT * FROM factory_photos WHERE factory_id = ? ORDER BY is_primary DESC, created_at", 
              (call.from_user.id,))

    text = f"<b>Управление фотографиями</b>\n\n"
    if photos:
        text += f"У вас {len(photos)} фото:\n"
        for i, photo in enumerate(photos[:3], 1):
            primary = "👑 " if photo['is_primary'] else ""
            text += f"{primary}{i}. {photo['type'].title()}\n"
    else:
        text += "У вас пока нет фотографий"

    buttons = [
        [InlineKeyboardButton(text="📸 Добавить фото", callback_data="photo_add")],
        [InlineKeyboardButton(text="🗑 Удалить все", callback_data="photo_delete_all")],
        [InlineKeyboardButton(text="❌ Закрыть", callback_data="photo_close")]
    ]
    
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    await call.message.edit_text(text, reply_markup=kb)
    await call.answer()

@router.callback_query(F.data == "photo_add")
async def photo_add_start(call: CallbackQuery, state: FSMContext) -> None:
    """Start adding photos."""
    await state.set_state(PhotoManagementForm.upload)
    
    await call.message.edit_text(
        "📸 <b>Добавление фото</b>\n\n"
        "Отправьте фотографии производства (до 3 штук).\n"
        "Или напишите «готово» когда закончите:"
    )
    await call.answer()

@router.message(PhotoManagementForm.upload, F.photo)
async def photo_upload_process(msg: Message, state: FSMContext) -> None:
    """Process photo upload."""
    # Check current photo count
    current_count = q1("SELECT COUNT(*) as cnt FROM factory_photos WHERE factory_id = ?", 
                      (msg.from_user.id,))['cnt']
    
    if current_count >= 5:
        await msg.answer("❌ Максимум 5 фотографий. Удалите старые, чтобы добавить новые.")
        return
    
    # Add photo
    is_primary = 1 if current_count == 0 else 0
    run("""
        INSERT INTO factory_photos (factory_id, file_id, type, is_primary)
        VALUES (?, ?, 'workshop', ?)
    """, (msg.from_user.id, msg.photo[-1].file_id, is_primary))
    
    await msg.answer(
        f"✅ Фото добавлено! ({current_count + 1}/5)\n"
        f"Отправьте еще или напишите «готово»"
    )

@router.message(PhotoManagementForm.upload, F.text)
async def photo_upload_finish(msg: Message, state: FSMContext) -> None:
    """Finish photo upload."""
    if msg.text and msg.text.lower() in ["готово", "done", "стоп"]:
        await state.clear()
        await msg.answer(
            "✅ Фотографии обновлены!",
            reply_markup=kb_factory_menu()
        )
    else:
        await msg.answer("Отправьте фото или напишите «готово»")

@router.callback_query(F.data == "photo_delete_all")
async def photo_delete_all(call: CallbackQuery) -> None:
    """Delete all photos."""
    run("DELETE FROM factory_photos WHERE factory_id = ?", (call.from_user.id,))
    
    await call.message.edit_text("✅ Все фотографии удалены")
    await call.answer("Фотографии удалены")

@router.callback_query(F.data == "photo_close")
async def photo_close(call: CallbackQuery) -> None:
    """Close photo management."""
    await call.message.edit_text("📸 Управление фотографиями закрыто")
    await call.answer()

# ---------------------------------------------------------------------------
#  ДОРАБОТКА: Дополнительные callback handlers
# ---------------------------------------------------------------------------

@router.callback_query(F.data == "upgrade_pro")
async def upgrade_to_pro(call: CallbackQuery) -> None:
    """Upgrade factory to PRO status."""
    factory = q1("SELECT * FROM factories WHERE tg_id = ?", (call.from_user.id,))
    if not factory:
        await call.answer("Профиль фабрики не найден", show_alert=True)
        return
    
    if factory['is_pro']:
        await call.answer("У вас уже есть PRO статус!", show_alert=True)
        return
    
    # ЗАГЛУШКА для оплаты PRO
    run("""
        UPDATE factories 
        SET is_pro = 1, pro_expires = datetime('now', '+1 month')
        WHERE tg_id = ?
    """, (call.from_user.id,))
    
    # Create payment record
    insert_and_get_id("""
        INSERT INTO payments 
        (user_id, type, amount, status, reference_type, reference_id)
        VALUES (?, 'factory_pro', 2000, 'completed', 'factory', ?)
    """, (call.from_user.id, call.from_user.id))
    
    await call.message.edit_text(
        "✅ <b>PRO статус активирован!</b>\n\n"
        "🎯 Активен на 1 месяц\n"
        "📬 Вы будете получать все подходящие заявки\n"
        "💬 Можете откликаться без ограничений\n\n"
        "Начните получать заказы прямо сейчас!"
    )
    
    await call.answer("PRO статус активирован!")

@router.callback_query(F.data == "view_all_ratings")
async def view_all_ratings(call: CallbackQuery) -> None:
    """View all factory ratings."""
    ratings = q("""
        SELECT r.*, o.title, u.full_name as buyer_name
        FROM ratings r
        JOIN deals d ON r.deal_id = d.id
        JOIN orders o ON d.order_id = o.id
        JOIN users u ON r.buyer_id = u.tg_id
        WHERE r.factory_id = ?
        ORDER BY r.created_at DESC
        LIMIT 10
    """, (call.from_user.id,))
    
    if not ratings:
        await call.message.edit_text("У вас пока нет отзывов.")
        return
    
    ratings_text = f"<b>Все отзывы ({len(ratings)})</b>\n\n"
    
    for rating in ratings:
        stars = "⭐" * rating['rating']
        ratings_text += (
            f"{stars} ({rating['rating']}/5)\n"
            f"📦 {rating['title'][:30]}...\n"
            f"👤 {rating['buyer_name']}\n"
            f"📅 {rating['created_at'][:10]}\n"
        )
        if rating['comment']:
            ratings_text += f"💬 {rating['comment'][:100]}...\n"
        ratings_text += "\n"
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_rating")]
    ])
    
    await call.message.edit_text(ratings_text, reply_markup=kb)
    await call.answer()

@router.callback_query(F.data == "back_to_rating")
async def back_to_rating(call: CallbackQuery) -> None:
    """Go back to rating summary."""
    await call.message.delete()
    await call.answer()

@router.callback_query(F.data == "analytics_detailed")
async def analytics_detailed(call: CallbackQuery) -> None:
    """Show detailed analytics."""
    await call.answer("Детальная аналитика будет добавлена в следующем обновлении", show_alert=True)

@router.callback_query(F.data == "analytics_rating")
async def analytics_rating_comparison(call: CallbackQuery) -> None:
    """Show rating comparison with other factories."""
    factory = q1("SELECT rating, rating_count FROM factories WHERE tg_id = ?", (call.from_user.id,))
    if not factory or factory['rating_count'] == 0:
        await call.answer("Недостаточно данных для сравнения", show_alert=True)
        return
    
    # Get position among all factories
    position = q1("""
        SELECT COUNT(*) + 1 as position
        FROM factories
        WHERE rating > ? AND rating_count > 0
    """, (factory['rating'],))
    
    # Get average rating
    avg_rating = q1("""
        SELECT AVG(rating) as avg_rating, COUNT(*) as total_factories
        FROM factories
        WHERE rating_count > 0
    """)
    
    comparison_text = (
        f"📊 <b>Ваш рейтинг среди фабрик</b>\n\n"
        f"⭐ Ваш рейтинг: {factory['rating']:.1f}/5.0\n"
        f"🏆 Позиция: #{position['position']}\n"
        f"📊 Средний рейтинг: {avg_rating['avg_rating']:.1f}/5.0\n"
        f"🏭 Всего фабрик с рейтингом: {avg_rating['total_factories']}\n\n"
    )
    
    if factory['rating'] > avg_rating['avg_rating']:
        comparison_text += "🎉 Вы выше среднего!"
    else:
        comparison_text += "💪 Есть куда расти!"
    
    await call.message.answer(comparison_text)
    await call.answer()

@router.callback_query(F.data == "payment_history")
async def payment_history(call: CallbackQuery) -> None:
    """Show payment history."""
    payments = q("""
        SELECT * FROM payments 
        WHERE user_id = ? 
        ORDER BY created_at DESC 
        LIMIT 10
    """, (call.from_user.id,))
    
    if not payments:
        await call.message.answer("История платежей пуста")
        return
    
    history_text = "<b>💳 История платежей</b>\n\n"
    
    for payment in payments:
        status_emoji = {"completed": "✅", "pending": "⏳", "failed": "❌"}.get(payment['status'], "❓")
        type_names = {
            "factory_pro": "PRO подписка",
            "order_placement": "Размещение заказа",
            "sample": "Оплата образца"
        }
        
        history_text += (
            f"{status_emoji} {type_names.get(payment['type'], payment['type'])}\n"
            f"💰 {format_price(payment['amount'])} ₽\n"
            f"📅 {payment['created_at'][:16]}\n\n"
        )
    
    await call.message.answer(history_text)
    await call.answer()

@router.callback_query(F.data == "revenue_chart")
async def revenue_chart(call: CallbackQuery) -> None:
    """Show revenue chart (placeholder)."""
    await call.answer("График доходов будет добавлен в следующем обновлении", show_alert=True)

# ---------------------------------------------------------------------------
#  ДОРАБОТКА: Обработчики для редактирования заказов/предложений
# ---------------------------------------------------------------------------

@router.callback_query(F.data == "edit_order")
async def edit_order_from_creation(call: CallbackQuery, state: FSMContext) -> None:
    """Edit order during creation process."""
    await call.answer("Функция редактирования при создании будет добавлена в следующем обновлении", show_alert=True)

@router.callback_query(F.data == "edit_factory")
async def edit_factory_from_creation(call: CallbackQuery, state: FSMContext) -> None:
    """Edit factory data during registration."""
    await call.answer("Функция редактирования при регистрации будет добавлена в следующем обновлении", show_alert=True)

# ---------------------------------------------------------------------------
#  ДОРАБОТКА: Обработчики для просмотра и создания чатов
# ---------------------------------------------------------------------------
async def create_deal_chat(deal_id: int) -> tuple[int | None, str | None]:
    """Create group chat for deal with improved error handling."""
    if not GROUP_CREATOR_AVAILABLE:
        logger.warning("Group creator not available, using fallback notification")
        await send_fallback_chat_notification(deal_id, error="Module not available")
        return None, None
    try:
        deal = q1("""
            SELECT d.*, o.title, f.name as factory_name, u.full_name as buyer_name
            FROM deals d
            JOIN orders o ON d.order_id = o.id
            JOIN factories f ON d.factory_id = f.tg_id
            JOIN users u ON d.buyer_id = u.tg_id
            WHERE d.id = ?
        """, (deal_id,))
        if not deal:
            logger.error(f"Deal {deal_id} not found for chat creation")
            return None, None
        api_id = os.getenv("TELEGRAM_API_ID")
        api_hash = os.getenv("TELEGRAM_API_HASH")
        if not api_id:
            logger.error("TELEGRAM_API_ID not found in environment")
            await send_fallback_chat_notification(deal_id, error="Missing TELEGRAM_API_ID")
            return None, None
        if not api_hash:
            logger.error("TELEGRAM_API_HASH not found in environment")
            await send_fallback_chat_notification(deal_id, error="Missing TELEGRAM_API_HASH")
            return None, None
        logger.info(f"Creating group chat for deal {deal_id}: title={deal['title']}, factory={deal['factory_name']}, buyer={deal['buyer_name']}")
        try:
            chat_id, status_message, invite_link = await create_deal_chat_real(
                deal_id=deal_id,
                deal_title=deal['title'],
                factory_name=deal['factory_name'],
                buyer_name=deal['buyer_name']
            )
        except Exception as e:
            logger.error(f"Exception in create_deal_chat_real: {e}")
            await send_fallback_chat_notification(deal_id, error=str(e))
            return None, None
        if chat_id and isinstance(chat_id, int) and chat_id < 0:
            run("UPDATE deals SET chat_id = ? WHERE id = ?", (chat_id, deal_id))
            logger.info(f"✅ Created REAL group chat {chat_id} for deal {deal_id}")
            await notify_chat_created(deal_id, chat_id, invite_link)
            return chat_id, invite_link
        else:
            error_msg = status_message if status_message else "Unknown error creating group"
            logger.error(f"❌ Failed to create real group for deal {deal_id}: {error_msg}")
            await send_fallback_chat_notification(deal_id, error=error_msg)
            return None, None
    except Exception as e:
        logger.error(f"Unexpected exception in create_deal_chat: {e}")
        await send_fallback_chat_notification(deal_id, error=str(e))
        return None, None

@router.callback_query(F.data.startswith("deal_chat:"))
async def deal_chat_handler(call: CallbackQuery) -> None:
    """Handle deal chat access with improved error handling."""
    deal_id = int(call.data.split(":", 1)[1])
    deal = q1("""
        SELECT d.*, o.title, f.name as factory_name, u.full_name as buyer_name
        FROM deals d
        JOIN orders o ON d.order_id = o.id
        JOIN factories f ON d.factory_id = f.tg_id
        JOIN users u ON d.buyer_id = u.tg_id
        WHERE d.id = ? AND (d.buyer_id = ? OR d.factory_id = ?)
    """, (deal_id, call.from_user.id, call.from_user.id))
    if not deal:
        await call.answer("Сделка не найдена", show_alert=True)
        return
    
    # Проверяем доступность модуля создания групп
    if not GROUP_CREATOR_AVAILABLE:
        chat_info = (
            f"💬 <b>Чат сделки #{deal_id}</b>\n\n"
            f"📦 {deal['title']}\n"
            f"🏭 {deal['factory_name']}\n"
            f"👤 {deal['buyer_name']}\n\n"
            f"⚠️ Групповые чаты временно недоступны.\n"
            f"Обратитесь в поддержку или общайтесь напрямую через профили пользователей."
        )
        await call.message.answer(chat_info)
        await call.answer()
        return
    
    # Check if chat already exists AND is a real chat
    if deal['chat_id'] and deal['chat_id'] < 0:  # Реальные группы имеют отрицательный ID
        try:
            # Проверяем переменные окружения
            api_id = os.getenv("TELEGRAM_API_ID")
            api_hash = os.getenv("TELEGRAM_API_HASH")
            
            if not all([api_id, api_hash]):
                missing = []
                if not api_id: missing.append("TELEGRAM_API_ID")
                if not api_hash: missing.append("TELEGRAM_API_HASH") 
                
                logger.error(f"Missing environment variables: {', '.join(missing)}")
                
                chat_info = (
                    f"❌ <b>Ошибка конфигурации чата</b>\n\n"
                    f"Отсутствуют переменные окружения для работы с чатами.\n"
                    f"Обратитесь к администратору."
                )
                await call.message.answer(chat_info)
                await call.answer()
                return
            
            # Проверяем существование группы
            creator = TelegramGroupCreator(api_id, api_hash)
            group_info = await creator.get_group_info(int(deal['chat_id']))
            
            if group_info:
                # Группа существует
                invite_link = await creator.create_invite_link(int(deal['chat_id']))
                
                chat_info = (
                    f"💬 <b>Чат сделки #{deal_id}</b>\n\n"
                    f"📦 {deal['title']}\n"
                    f"🏭 {deal['factory_name']}\n"
                    f"👤 {deal['buyer_name']}\n\n"
                    f"👥 Участников: {group_info['members_count']}\n"
                    f"📋 Название: {group_info['title']}\n\n"
                    f"✅ Чат активен!"
                )
                
                buttons = []
                if invite_link:
                    buttons.append([
                        InlineKeyboardButton(text="🔗 Войти в чат", url=invite_link)
                    ])
                
                kb = InlineKeyboardMarkup(inline_keyboard=buttons) if buttons else None
                
            else:
                # Группа была удалена - очищаем chat_id
                run("UPDATE deals SET chat_id = NULL WHERE id = ?", (deal_id,))
                
                chat_info = (
                    f"⚠️ <b>Чат был удален</b>\n\n"
                    f"Группа для сделки #{deal_id} была удалена.\n"
                    f"Хотите создать новый чат?"
                )
                
                kb = InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="🔄 Создать новый чат", callback_data=f"recreate_chat:{deal_id}")
                ]])
                
        except Exception as e:
            logger.error(f"Error checking group info for deal {deal_id}: {e}")
            
            # Если ошибка с ID группы - очищаем его
            if "invalid" in str(e).lower() or "not found" in str(e).lower():
                run("UPDATE deals SET chat_id = NULL WHERE id = ?", (deal_id,))
                logger.info(f"Cleared invalid chat_id for deal {deal_id}")
            
            chat_info = (
                f"❌ <b>Ошибка доступа к чату</b>\n\n"
                f"Не удалось получить доступ к чату сделки #{deal_id}.\n"
                f"Попробуйте создать новый чат."
            )
            
            kb = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🔄 Создать новый чат", callback_data=f"recreate_chat:{deal_id}")
            ]])
            
    else:
        # Чата нет или есть фейковый ID - создаем новый
        if deal['chat_id']:
            # Очищаем фейковый chat_id
            run("UPDATE deals SET chat_id = NULL WHERE id = ?", (deal_id,))
            logger.info(f"Cleared fake chat_id {deal['chat_id']} for deal {deal_id}")
        
        chat_id = await create_deal_chat(deal_id])
        
        if chat_id:
            # Получаем ссылку на созданный чат
            try:
                creator = TelegramGroupCreator(os.getenv("TELEGRAM_API_ID"), os.getenv("TELEGRAM_API_HASH"))
                invite_link = await creator.create_invite_link(chat_id)
                
                chat_info = (
                    f"✅ <b>Чат создан!</b>\n\n"
                    f"📦 {deal['title']}\n"
                    f"🏭 {deal['factory_name']}\n"
                    f"👤 {deal['buyer_name']}\n\n"
                    f"Групповой чат для сделки #{deal_id} успешно создан!"
                )
                
                buttons = []
                if invite_link:
                    buttons.append([
                        InlineKeyboardButton(text="💬 Войти в чат", url=invite_link)
                    ])
                
                kb = InlineKeyboardMarkup(inline_keyboard=buttons) if buttons else None
                
            except Exception as e:
                logger.error(f"Error getting invite link for new chat {chat_id}: {e}")
                kb = None
        else:
            chat_info = (
                f"❌ <b>Не удалось создать чат</b>\n\n"
                f"Групповой чат для сделки #{deal_id} не был создан.\n"
                f"Вы можете общаться напрямую или обратиться в поддержку."
            )
            kb = None
    
    await call.message.answer(chat_info, reply_markup=kb)
    await call.answer()

# ---------------------------------------------------------------------------
#  Entry point functions
# ---------------------------------------------------------------------------

async def run_webhook() -> None:
    """Start the bot in webhook mode."""
    if not WEBHOOK_BASE:
        logger.error("Error: WEBHOOK_BASE env var required for webhook mode")
        sys.exit(1)

    logger.info("Starting bot in webhook mode on port %s", PORT)

    # Remove any existing webhook
    await bot.delete_webhook(drop_pending_updates=True)

    # Set the new webhook URL
    webhook_url = f"{WEBHOOK_BASE}/webhook"

    # Create aiohttp app
    app = web.Application()

    # Setup webhook route
    webhook_handler = SimpleRequestHandler(
        dispatcher=dp,
        bot=bot,
    )
    webhook_handler.register(app, path="/webhook")

    # Set the webhook
    await bot.set_webhook(webhook_url)
    logger.info("Webhook set to: %s", webhook_url)

    # Setup startup callback
    dp.startup.register(on_startup)

    # Start web server
    setup_application(app, dp, bot=bot)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host="0.0.0.0", port=PORT)
    await site.start()

    # Run forever
    await asyncio.Event().wait()

async def run_polling() -> None:
    """Start the bot in long-polling mode."""
    logger.info("Starting bot in polling mode")

    # Remove any existing webhook
    await bot.delete_webhook(drop_pending_updates=True)

    # Setup startup callback
    dp.startup.register(on_startup)

    # Start polling
    try:
        await dp.start_polling(bot)
    except KeyboardInterrupt:
        logger.info("Bot stopped by keyboard interrupt")
    finally:
        logger.info("Shutting down...")
        await dp.storage.close()
        await bot.session.close()

async def main() -> None:
    """Main entry point with event loop handling."""
    # Устанавливаем политику event loop
    try:
        if hasattr(asyncio, 'WindowsSelectorEventLoopPolicy'):
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    except:
        pass
    
    try:
        if BOT_MODE == "WEBHOOK":
            await run_webhook()
        else:
            await run_polling()
    except KeyboardInterrupt:
        logger.info("Bot stopped by keyboard interrupt")
    except Exception as e:
        logger.error(f"Unexpected error in main: {e}")
        raise
    finally:
        logger.info("Bot shutdown complete")
        
async def notify_factories(order_row, bot, q, order_caption, send_notification, logger):
    """
    Notify matching factories about new order.
    """
    factories = q("""
        SELECT f.tg_id, f.name, u.notifications 
        FROM factories f
        JOIN users u ON f.tg_id = u.tg_id
        WHERE f.is_pro = 1
          AND f.min_qty <= ?
          AND f.avg_price <= ?
          AND (',' || f.categories || ',') LIKE ('%,' || ? || ',%')
          AND u.is_active = 1
          AND u.is_banned = 0
    """, (order_row['quantity'], order_row['budget'], order_row['category']))
    
    notified_count = 0
    for factory in factories:
        if factory['notifications']:
            try:
                kb = InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="👀 Посмотреть", callback_data=f"view_order:{order_row['id']}"),
                    InlineKeyboardButton(text="💌 Откликнуться", callback_data=f"lead:{order_row['id']}")
                ]])
                
                await bot.send_message(
                    factory['tg_id'],
                    f"🔥 <b>Новая заявка в вашей категории!</b>\n\n" + order_caption(order_row),
                    reply_markup=kb
                )
                notified_count += 1
                
                # Track notification
                await send_notification(
                    factory['tg_id'],
                    'new_order',
                    'Новая заявка',
                    f"Заявка #{order_row['id']} в категории {order_row['category']}",
                    {'order_id': order_row['id']}
                )
            except Exception as e:
                logger.error(f"Failed to notify factory {factory['tg_id']}: {e}")
    
    logger.info(f"Order #{order_row['id']} notified to {notified_count} factories")
    return notified_count

# ---------------------------------------------------------------------------
#  Background tasks and startup
# ---------------------------------------------------------------------------

async def run_background_tasks():
    """Run periodic background tasks."""
    while True:
        try:
            # Clean up old notifications
            run("""
                DELETE FROM notifications 
                WHERE is_sent = 1 
                  AND created_at < datetime('now', '-30 days')
            """)
            
            logger.info("Background cleanup completed")
            
        except Exception as e:
            logger.error(f"Error in background tasks: {e}")
        
        # Run every hour
        await asyncio.sleep(3600)

async def on_startup(bot: Bot) -> None:
    """Run on bot startup."""
    init_db()
    
    # Start background tasks
    asyncio.create_task(run_background_tasks())
    
    # Set bot commands
    await bot.set_my_commands([
        BotCommand(command="start", description="Главное меню"),
        BotCommand(command="help", description="Помощь"),
        BotCommand(command="profile", description="Мой профиль"),
        BotCommand(command="support", description="Поддержка"),
    ])
    
    logger.info("Bot startup complete ✅")

# ---------------------------------------------------------------------------
#  Profile commands
# ---------------------------------------------------------------------------

@router.message(Command("profile"))
@router.message(F.text.in_(["👤 Профиль", "🧾 Профиль"]))
async def cmd_profile(msg: Message) -> None:
    """Show user profile."""
    user = get_or_create_user(msg.from_user)
    role = UserRole(user['role'])
    
    if role == UserRole.FACTORY:
        factory = q1("SELECT * FROM factories WHERE tg_id = ?", (msg.from_user.id,))
        if not factory:
            await msg.answer("Профиль фабрики не найден", reply_markup=kb_main())
            return
        
        # Calculate stats
        active_deals = q1(
            "SELECT COUNT(*) as cnt FROM deals WHERE factory_id = ? AND status NOT IN ('DELIVERED', 'CANCELLED')",
            (msg.from_user.id,)
        )
        
        total_revenue = q1(
            "SELECT SUM(amount) as total FROM deals WHERE factory_id = ? AND status = 'DELIVERED'",
            (msg.from_user.id,)
        )
        
        profile_text = (
            f"<b>Профиль фабрики</b>\n\n"
            f"🏢 {factory['name']}\n"
            f"📍 {factory['address']}\n"
            f"🏷 ИНН: {factory['inn']}\n"
        )
        
        # Categories
        categories = factory['categories'].split(',')
        categories_text = ", ".join([c.capitalize() for c in categories[:5]])
        if len(categories) > 5:
            categories_text += f" +{len(categories) - 5}"
        profile_text += f"📦 Категории: {categories_text}\n"
        
        # Production capacity
        profile_text += f"📊 Партии: {format_price(factory['min_qty'])} - {format_price(factory['max_qty'])} шт.\n"
        profile_text += f"💰 Средняя цена: {format_price(factory['avg_price'])} ₽\n\n"
        
        # Stats
        profile_text += "<b>Статистика:</b>\n"
        if factory['rating_count'] > 0:
            profile_text += f"⭐ Рейтинг: {factory['rating']:.1f}/5.0 ({factory['rating_count']} отзывов)\n"
        else:
            profile_text += "⭐ Рейтинг: нет отзывов\n"
        
        profile_text += f"✅ Выполнено: {factory['completed_orders']} заказов\n"
        profile_text += f"🔄 Активных сделок: {active_deals['cnt']}\n"
        
        if total_revenue and total_revenue['total']:
            profile_text += f"💵 Общий оборот: {format_price(total_revenue['total'])} ₽\n"
        
        # PRO status
        profile_text += f"\n<b>Статус:</b> "
        if factory['is_pro']:
            if factory['pro_expires']:
                profile_text += f"✅ PRO до {factory['pro_expires'][:10]}"
            else:
                profile_text += "✅ PRO (бессрочно)"
        else:
            profile_text += "❌ Базовый (оформите PRO для получения заявок)"
        
        # Action buttons
        buttons = []
        if not factory['is_pro']:
            buttons.append([InlineKeyboardButton(text="💳 Оформить PRO", callback_data="upgrade_pro")])
        
        buttons.append([
            InlineKeyboardButton(text="✏️ Изменить данные", callback_data="edit_profile"),
            InlineKeyboardButton(text="📸 Фото", callback_data="manage_photos")
        ])
        
        kb = InlineKeyboardMarkup(inline_keyboard=buttons) if buttons else None
        
        await msg.answer(profile_text, reply_markup=kb)
        
    elif role == UserRole.BUYER:
        # Buyer profile
        stats = q1("""
            SELECT 
                COUNT(DISTINCT o.id) as total_orders,
                COUNT(DISTINCT d.id) as total_deals,
                SUM(CASE WHEN o.is_active = 1 THEN 1 ELSE 0 END) as active_orders
            FROM orders o
            LEFT JOIN deals d ON o.id = d.order_id
            WHERE o.buyer_id = ?
        """, (msg.from_user.id,))
        
        profile_text = (
            f"<b>Профиль заказчика</b>\n\n"
            f"👤 {user['full_name']}\n"
            f"🆔 ID: {msg.from_user.id}\n"
        )
        
        if user['phone']:
            profile_text += f"📱 Телефон: {user['phone']}\n"
        if user['email']:
            profile_text += f"📧 Email: {user['email']}\n"
        
        profile_text += (
            f"\n<b>Статистика:</b>\n"
            f"📋 Всего заказов: {stats['total_orders']}\n"
            f"✅ Завершено сделок: {stats['total_deals']}\n"
            f"🔄 Активных заказов: {stats['active_orders']}\n"
        )
        
        # Last order
        last_order = q1(
            "SELECT * FROM orders WHERE buyer_id = ? ORDER BY created_at DESC LIMIT 1",
            (msg.from_user.id,)
        )
        
        if last_order:
            profile_text += f"\n📅 Последний заказ: {last_order['created_at'][:10]}"
        
        buttons = [[
            InlineKeyboardButton(text="✏️ Изменить данные", callback_data="edit_profile"),
            InlineKeyboardButton(text="📋 История заказов", callback_data="order_history")
        ]]
        
        kb = InlineKeyboardMarkup(inline_keyboard=buttons)
        await msg.answer(profile_text, reply_markup=kb)
        
    else:
        await msg.answer(
            "У вас пока нет профиля. Выберите, кто вы:",
            reply_markup=kb_main()
        )

# ---------------------------------------------------------------------------
#  Settings
# ---------------------------------------------------------------------------

@router.message(F.text == "⚙️ Настройки")
async def cmd_settings(msg: Message, state: FSMContext) -> None:
    """Show simplified settings menu."""
    await state.clear()
    
    settings_text = (
        "<b>Настройки</b>\n\n"
        "Управление вашим аккаунтом:"
    )
    
    buttons = [
        [
            InlineKeyboardButton(
                text="🗑 Удалить аккаунт", 
                callback_data="settings:delete_account"
            )
        ]
    ]
    
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    await msg.answer(settings_text, reply_markup=kb)

@router.callback_query(F.data == "settings:delete_account")
async def delete_account_confirm(call: CallbackQuery) -> None:
    """Confirm account deletion."""
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="❌ Да, удалить", callback_data="confirm_delete_account"),
            InlineKeyboardButton(text="✅ Отмена", callback_data="cancel_delete_account")
        ]
    ])
    
    await call.message.edit_text(
        "⚠️ <b>Вы уверены, что хотите удалить аккаунт?</b>\n\n"
        "Это действие необратимо. Будут удалены:\n"
        "• Ваш профиль\n"
        "• История заказов/предложений\n"
        "• Все данные\n\n"
        "Активные сделки будут завершены через поддержку.",
        reply_markup=kb
    )
    await call.answer()

@router.callback_query(F.data == "confirm_delete_account")
async def delete_account_execute(call: CallbackQuery) -> None:
    """Execute account deletion."""
    user_id = call.from_user.id
    
    # Check active deals
    active_deals = q1("""
        SELECT COUNT(*) as cnt FROM deals 
        WHERE (buyer_id = ? OR factory_id = ?) 
        AND status NOT IN ('DELIVERED', 'CANCELLED')
    """, (user_id, user_id))
    
    if active_deals and active_deals['cnt'] > 0:
        await call.message.edit_text(
            "❌ <b>Невозможно удалить аккаунт</b>\n\n"
            f"У вас есть {active_deals['cnt']} активных сделок.\n"
            "Завершите все сделки или обратитесь в поддержку."
        )
        await call.answer()
        return
    
    try:
        # Delete all user data
        run("DELETE FROM ratings WHERE buyer_id = ? OR factory_id = ?", (user_id, user_id))
        run("DELETE FROM proposals WHERE factory_id = ?", (user_id,))
        run("DELETE FROM factory_photos WHERE factory_id = ?", (user_id,))
        run("DELETE FROM factories WHERE tg_id = ?", (user_id,))
        run("DELETE FROM orders WHERE buyer_id = ?", (user_id,))
        run("DELETE FROM notifications WHERE user_id = ?", (user_id,))
        run("DELETE FROM ticket_messages WHERE user_id = ?", (user_id,))
        run("DELETE FROM tickets WHERE user_id = ?", (user_id,))
        run("DELETE FROM analytics WHERE user_id = ?", (user_id,))
        run("DELETE FROM users WHERE tg_id = ?", (user_id,))
        
        # Notify admins
        await notify_admins(
            'account_deleted',
            '🗑 Аккаунт удален',
            f"Пользователь {call.from_user.username or call.from_user.full_name} удалил свой аккаунт",
            {'user_id': user_id}
        )
        
        await call.message.edit_text(
            "✅ <b>Аккаунт успешно удален</b>\n\n"
            "Все ваши данные удалены из системы.\n"
            "Спасибо за использование Mono-Fabrique!"
        )
        
    except Exception as e:
        logger.error(f"Error deleting account {user_id}: {e}")
        await call.message.edit_text(
            "❌ Ошибка при удалении аккаунта.\n"
            "Обратитесь в поддержку."
        )
    
    await call.answer()

@router.callback_query(F.data == "cancel_delete_account")
async def cancel_delete_account(call: CallbackQuery) -> None:
    """Cancel account deletion."""
    await call.message.edit_text("✅ Удаление аккаунта отменено")
    await call.answer()

# ---------------------------------------------------------------------------
#  Support system
# ---------------------------------------------------------------------------

@router.message(F.text == "📞 Поддержка")
async def cmd_support(msg: Message, state: FSMContext) -> None:
    """Show support menu."""
    await state.clear()
    
    # Check for open tickets
    open_tickets = q("""
        SELECT COUNT(*) as cnt 
        FROM tickets 
        WHERE user_id = ? AND status = 'open'
    """, (msg.from_user.id,))
    
    support_text = (
        "<b>Поддержка Mono-Fabrique</b>\n\n"
        "Мы готовы помочь вам 24/7!\n\n"
        "📧 Email: support@mono-fabrique.ru\n"
        "📱 Телефон: +7 (800) 123-45-67\n"
        "💬 Telegram: @mono_fabrique_support\n\n"
    )
    
    if open_tickets and open_tickets[0]['cnt'] > 0:
        support_text += f"У вас есть {open_tickets[0]['cnt']} открытых обращений\n\n"
    
    support_text += "Выберите тему обращения:"
    
    buttons = [
        [InlineKeyboardButton(text="❓ Общий вопрос", callback_data="ticket:general")],
        [InlineKeyboardButton(text="💳 Проблемы с оплатой", callback_data="ticket:payment")],
        [InlineKeyboardButton(text="📦 Вопрос по заказу", callback_data="ticket:order")],
        [InlineKeyboardButton(text="🏭 Вопрос по работе фабрики", callback_data="ticket:factory")],
        [InlineKeyboardButton(text="🚨 Жалоба", callback_data="ticket:complaint")],
        [InlineKeyboardButton(text="💡 Предложение", callback_data="ticket:suggestion")]
    ]
    
    if open_tickets and open_tickets[0]['cnt'] > 0:
        buttons.append([
            InlineKeyboardButton(
                text="📋 Мои обращения", 
                callback_data="my_tickets"
            )
        ])
    
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    await msg.answer(support_text, reply_markup=kb)

@router.callback_query(F.data.startswith("ticket:"))
async def create_support_ticket(call: CallbackQuery, state: FSMContext) -> None:
    """Start creating support ticket."""
    category = call.data.split(":", 1)[1]
    
    category_names = {
        'general': 'Общий вопрос',
        'payment': 'Проблемы с оплатой',
        'order': 'Вопрос по заказу',
        'factory': 'Вопрос по работе фабрики',
        'complaint': 'Жалоба',
        'suggestion': 'Предложение'
    }
    
    await state.update_data(ticket_category=category)
    await state.set_state(TicketForm.subject)
    
    await call.message.answer(
        f"<b>Создание обращения</b>\n"
        f"Категория: {category_names.get(category, category)}\n\n"
        f"Введите тему обращения:",
        reply_markup=ReplyKeyboardRemove()
    )
    await call.answer()

@router.message(TicketForm.subject)
async def ticket_subject(msg: Message, state: FSMContext) -> None:
    """Process ticket subject."""
    if not msg.text or len(msg.text) < 5:
        await msg.answer("Введите более подробную тему (минимум 5 символов):")
        return
    
    await state.update_data(subject=msg.text.strip())
    await state.set_state(TicketForm.message)
    await msg.answer("Опишите вашу проблему или вопрос подробно:")

@router.message(TicketForm.message)
async def ticket_message(msg: Message, state: FSMContext) -> None:
    """Process ticket message and create ticket."""
    if not msg.text or len(msg.text) < 20:
        await msg.answer("Пожалуйста, опишите проблему подробнее (минимум 20 символов):")
        return
    
    data = await state.get_data()
    
    # Determine priority based on category
    priority = 'normal'
    if data['ticket_category'] in ['payment', 'complaint']:
        priority = 'high'
    
    # Create ticket
    ticket_id = insert_and_get_id("""
        INSERT INTO tickets (user_id, subject, category, priority, status)
        VALUES (?, ?, ?, ?, 'open')
    """, (msg.from_user.id, data['subject'], data['ticket_category'], priority))
    
    # Create first message
    insert_and_get_id("""
        INSERT INTO ticket_messages (ticket_id, user_id, message)
        VALUES (?, ?, ?)
    """, (ticket_id, msg.from_user.id, msg.text.strip()))
    
    # Get user info
    user = get_or_create_user(msg.from_user)
    
    # Notify admins about new ticket
    priority_emoji = {'high': '🔴', 'normal': '🟡'}.get(priority, '🟢')
    
    await notify_admins(
        'new_ticket',
        f'{priority_emoji} Новый тикет #{ticket_id}',
        f"От: @{msg.from_user.username or user['full_name']}\n"
        f"Категория: {data['ticket_category']}\n"
        f"Тема: {data['subject']}\n\n"
        f"Сообщение:\n{msg.text[:200]}{'...' if len(msg.text) > 200 else ''}",
        {
            'ticket_id': ticket_id,
            'user_id': msg.from_user.id,
            'priority': priority
        },
        [[
            InlineKeyboardButton(text="💬 Ответить", url=f"tg://user?id={msg.from_user.id}")
        ]]
    )
    
    await state.clear()
    await msg.answer(
        f"✅ <b>Обращение #{ticket_id} создано!</b>\n\n"
        f"Мы ответим вам в течение 24 часов.\n"
        f"Вы получите уведомление о нашем ответе.\n\n"
        f"Спасибо за обращение!",
        reply_markup=kb_main(get_user_role(msg.from_user.id))
    )

# ---------------------------------------------------------------------------
#  Entry point functions
# ---------------------------------------------------------------------------

async def run_webhook() -> None:
    """Start the bot in webhook mode."""
    if not WEBHOOK_BASE:
        logger.error("Error: WEBHOOK_BASE env var required for webhook mode")
        return
    
    logger.info("Starting bot in webhook mode on port %s", PORT)
    
    # Remove any existing webhook
    await bot.delete_webhook(drop_pending_updates=True)
    
    # Set the new webhook URL
    webhook_url = f"{WEBHOOK_BASE}/webhook"
    
    # Create aiohttp app
    app = web.Application()
    
    # Setup webhook route
    webhook_handler = SimpleRequestHandler(
        dispatcher=dp,
        bot=bot,
    )
    webhook_handler.register(app, path="/webhook")
    
    # Set the webhook
    await bot.set_webhook(webhook_url)
    logger.info("Webhook set to: %s", webhook_url)
    
    # Setup startup callback
    dp.startup.register(on_startup)
    
    # Start web server
    setup_application(app, dp, bot=bot)
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host="0.0.0.0", port=PORT)
    await site.start()
    
    # Run forever
    await asyncio.Event().wait()

async def run_polling() -> None:
    """Start the bot in long-polling mode."""
    logger.info("Starting bot in polling mode")
    
    # Remove any existing webhook
    await bot.delete_webhook(drop_pending_updates=True)
    
    # Setup startup callback
    dp.startup.register(on_startup)
    
    # Start polling
    try:
        await dp.start_polling(bot)
    except KeyboardInterrupt:
        logger.info("Bot stopped by keyboard interrupt")
    finally:
        logger.info("Shutting down...")
        await dp.storage.close()
        await bot.session.close()

async def main() -> None:
    """Main entry point."""
    if BOT_MODE == "WEBHOOK":
        await run_webhook()
    else:
        await run_polling()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped")
