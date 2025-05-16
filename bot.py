"""Mono‑Fabrique Telegram bot — MVP
=================================================
Telegram bot connecting garment factories («Фабрика») with buyers («Заказчик»).
Single‑file build based on **aiogram 3** ready for Render/Fly deploy.

Key user‑flows ---------------------------------------------------------------
* Factory onboarding ➜ PRO subscription (₽2 000 stub‑payment)
* Buyer request ➜ payment (₽700) ➜ auto‑dispatch to matching factories
* "📂 Заявки" menu for factories with instant respond
* Proposal flow and SQLite persistence

This version fixes `ValidationError: reply_markup` raised on callback
`pay_factory` (aiogram-3 requires InlineKeyboardMarkup for edit_text). Now we
edit the original message **without** keyboard and then send a new chat
message with the factory menu (ReplyKeyboardMarkup).
"""
from __future__ import annotations

import asyncio
import logging
import os
import sqlite3
from datetime import datetime
from typing import Any, Iterable, Sequence

from aiogram import Bot, Dispatcher, F, types
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (CallbackQuery, InlineKeyboardButton,
                           InlineKeyboardMarkup, Message)

# ---------------------------------------------------------------------------
#  Config & bootstrap
# ---------------------------------------------------------------------------
from dotenv import load_dotenv
load_dotenv()
TOKEN = os.getenv("BOT_TOKEN", "").strip()
if not TOKEN:
    raise RuntimeError("Environment variable BOT_TOKEN is missing.")

logging.basicConfig(level=logging.INFO,
                    format="%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger(__name__)

bot = Bot(token=TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher(storage=MemoryStorage())
DB_PATH = "fabrique.db"

# ---------------------------------------------------------------------------
#  DB helpers
# ---------------------------------------------------------------------------

def init_db() -> None:
    """Ensure SQLite schema exists."""
    with sqlite3.connect(DB_PATH) as db:
        db.execute("""
            CREATE TABLE IF NOT EXISTS factories (
                tg_id      INTEGER PRIMARY KEY,
                name       TEXT,
                inn        TEXT,
                categories TEXT,   -- comma‑separated
                min_qty    INTEGER,
                avg_price  INTEGER,
                portfolio  TEXT,
                is_pro     INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        db.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                buyer_id    INTEGER,
                category    TEXT,
                quantity    INTEGER,
                budget      INTEGER,
                destination TEXT,
                lead_time   INTEGER,
                file_id     TEXT,
                paid        INTEGER DEFAULT 0,
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        db.execute("""
            CREATE TABLE IF NOT EXISTS proposals (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id     INTEGER,
                factory_id   INTEGER,
                price        INTEGER,
                lead_time    INTEGER,
                sample_cost  INTEGER,
                created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(order_id)  REFERENCES orders(id),
                FOREIGN KEY(factory_id) REFERENCES factories(tg_id)
            );
        """)
    logger.info("SQLite schema ensured ✔")


# helper wrappers -------------------------------------------------------------

def fetchmany(sql: str, params: Iterable[Any] | None = None) -> list[sqlite3.Row]:
    with sqlite3.connect(DB_PATH) as db:
        db.row_factory = sqlite3.Row
        cur = db.execute(sql, params or [])
        return cur.fetchall()


def fetchone(sql: str, params: Iterable[Any] | None = None) -> sqlite3.Row | None:
    rows = fetchmany(sql, params)
    return rows[0] if rows else None


def execute(sql: str, params: Iterable[Any] | None = None) -> None:
    with sqlite3.connect(DB_PATH) as db:
        db.execute(sql, params or [])
        db.commit()

# ---------------------------------------------------------------------------
#  FSM definitions
# ---------------------------------------------------------------------------

class FactoryForm(StatesGroup):
    inn = State()
    photos = State()
    categories = State()
    min_qty = State()
    avg_price = State()
    portfolio = State()
    confirm_pay = State()


class BuyerForm(StatesGroup):
    category = State()
    quantity = State()
    budget = State()
    destination = State()
    lead_time = State()
    file = State()
    confirm_pay = State()


class ProposalForm(StatesGroup):
    price = State()
    lead_time = State()
    sample_cost = State()
    confirm = State()

# ---------------------------------------------------------------------------
#  Utility helpers
# ---------------------------------------------------------------------------

def build_factory_menu() -> types.ReplyKeyboardMarkup:
    return types.ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[
        [types.KeyboardButton(text="📂 Заявки"), types.KeyboardButton(text="/profile")],
        [types.KeyboardButton(text="/myleads")],
    ])


def send_order_card(factory_tg: int, order_row: sqlite3.Row) -> None:
    """Push single order card with inline buttons to a factory chat."""
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(
        text="Откликнуться",
        callback_data=f"lead:{order_row['id']}")]])
    asyncio.create_task(bot.send_message(
        factory_tg,
        (f"🆕 Заявка #Z‑{order_row['id']}\n"
         f"Категория: {order_row['category']}\n"
         f"Тираж: {order_row['quantity']} шт.\n"
         f"Бюджет: {order_row['budget']} ₽\n"
         f"Срок: {order_row['lead_time']} дней"),
        reply_markup=kb))


def notify_factories(order_row: sqlite3.Row) -> None:
    """Send freshly‑paid order to all matching PRO‑factories."""
    factories = fetchmany(
        """SELECT tg_id FROM factories
            WHERE is_pro = 1
              AND (',' || categories || ',') LIKE ('%,' || ? || ',%')
              AND min_qty <= ?;""",
        (order_row["category"], order_row["quantity"]))
    logger.info("Dispatching lead %s to %d factories", order_row["id"], len(factories))
    for f in factories:
        send_order_card(f["tg_id"], order_row)

# ---------------------------------------------------------------------------
#  /start and main menu
# ---------------------------------------------------------------------------

@dp.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext) -> None:
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[
        [types.KeyboardButton(text="🛠 Я – Фабрика")],
        [types.KeyboardButton(text="🛒 Мне нужна фабрика")],
        [types.KeyboardButton(text="ℹ Как работает"), types.KeyboardButton(text="🧾 Тарифы")],
    ])
    await message.answer("<b>Привет!</b> Кто вы?", reply_markup=kb)

# ---------------------------------------------------------------------------
#  Factory onboarding
# ---------------------------------------------------------------------------

@dp.message(F.text == "🛠 Я – Фабрика")
async def factory_begin(message: Message, state: FSMContext) -> None:
    await message.answer("Введите ИНН / УНП предприятия:")
    await state.set_state(FactoryForm.inn)


@dp.message(FactoryForm.inn)
async def factory_inn(message: Message, state: FSMContext) -> None:
    await state.update_data(inn=message.text.strip())
    await message.answer("Загрузите 1‑3 фото цеха или сертификат ISO (как файл):")
    await state.set_state(FactoryForm.photos)


@dp.message(FactoryForm.photos, F.photo | F.document)
async def factory_photos(message: Message, state: FSMContext) -> None:
    file_ids: Sequence[str] = ([p.file_id for p in message.photo]
                               if message.photo else [message.document.file_id])
    await state.update_data(photos=file_ids)
    cat_kb = types.ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[
        [types.KeyboardButton(text="Трикотаж"), types.KeyboardButton(text="Верхняя одежда")],
        [types.KeyboardButton(text="Домашний текстиль")],
    ])
    await message.answer("Укажите категории производства:", reply_markup=cat_kb)
    await state.set_state(FactoryForm.categories)


@dp.message(FactoryForm.categories)
async def factory_categories(message: Message, state: FSMContext) -> None:
    cats = [c.strip() for c in message.text.split(",")]
    await state.update_data(categories=cats)
    await message.answer("Минимальный тираж (шт.)?")
    await state.set_state(FactoryForm.min_qty)


@dp.message(FactoryForm.min_qty)
async def factory_min_qty(message: Message, state: FSMContext) -> None:
    await state.update_data(min_qty=int(message.text))
    await message.answer("Средняя ставка, ₽ за изделие?")
    await state.set_state(FactoryForm.avg_price)


@dp.message(FactoryForm.avg_price)
async def factory_avg_price(message: Message, state: FSMContext) -> None:
    await state.update_data(avg_price=int(message.text))
    await message.answer("Ссылка на прайс/портфолио? (необязательно)")
    await state.set_state(FactoryForm.portfolio)


@dp.message(FactoryForm.portfolio)
async def factory_portfolio(message: Message, state: FSMContext) -> None:
    await state.update_data(portfolio=message.text.strip())
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(
        text="Оплатить 2 000 ₽", callback_data="pay_factory")]])
    await message.answer(
        "<b>Готово!</b> Витрина будет проверена модератором в течение 1 дня.\n"
        "Пакет “PRO‑фабрика” – 2 000 ₽/мес.",
        reply_markup=kb)
    await state.set_state(FactoryForm.confirm_pay)


@dp.callback_query(F.data == "pay_factory", FactoryForm.confirm_pay)
async def factory_pay(call: CallbackQuery, state: FSMContext) -> None:
    """Mark factory as PRO, update DB, show menu.

    edit_message_text only accepts InlineKeyboardMarkup; we therefore edit the
    existing message without keyboard and send a **new** one with
    ReplyKeyboardMarkup."""
    data = await state
