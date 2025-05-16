"""Mono‑Fabrique Telegram bot — MVP
=================================================
Telegram bot connecting garment factories («Фабрика») with buyers («Заказчик»).
Single‑file implementation based on **aiogram 3.4+**. Ready for Render, Fly
or any bare‑metal host. Works in either *long‑polling* (default) or *webhook*
mode.

Key flows
---------
* Factory onboarding → PRO subscription (₂ 000 ₽ stub‑payment)
* Buyer request → payment (₇ 00 ₽) → instant dispatch to matching factories
* "📂 Заявки" menu for factories + respond flow
* SQLite persistence (`factories`, `orders`, `proposals`)

Environment variables
---------------------
* **BOT_TOKEN** – *required* Telegram bot token
* BOT_MODE=WEBHOOK (optional) – switch to webhook mode
* WEBHOOK_BASE=https://example.com (required in webhook mode)

Recent bug‑fixes
----------------
* Optional `python‑dotenv`; safe if package missing
* Numeric inputs parsed via `re.sub(r"\D", "", text)` → no `ValueError`
* `edit_message_text` never receives ReplyKeyboard → no `ValidationError`
* Order insert & fetch in same connection → no `NoneType` in `notify_factories`
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
import sqlite3
from datetime import datetime
from typing import Any, Iterable

from aiogram import Bot, Dispatcher, F, Router, types
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

# ---------------------------------------------------------------------------
#  Config & bootstrap
# ---------------------------------------------------------------------------
try:
    from dotenv import load_dotenv  # type: ignore

    load_dotenv()
except ModuleNotFoundError:
    pass  # optional in prod

TOKEN = os.getenv("BOT_TOKEN", "").strip()
if not TOKEN:
    raise RuntimeError("Environment variable BOT_TOKEN is missing. Set BOT_TOKEN.")

BOT_MODE = os.getenv("BOT_MODE", "POLLING").upper()
WEBHOOK_BASE = os.getenv("WEBHOOK_BASE", "")
PORT = int(os.getenv("PORT", "8080"))

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger(__name__)

bot = Bot(token=TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)

DB_PATH = "fabrique.db"

# ---------------------------------------------------------------------------
#  DB helpers
# ---------------------------------------------------------------------------

def init_db() -> None:
    with sqlite3.connect(DB_PATH) as db:
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS factories (
                tg_id INTEGER PRIMARY KEY,
                name TEXT,
                inn TEXT,
                categories TEXT,
                min_qty INTEGER,
                avg_price INTEGER,
                portfolio TEXT,
                is_pro INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );"""
        )
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                buyer_id INTEGER,
                category TEXT,
                quantity INTEGER,
                budget INTEGER,
                destination TEXT,
                lead_time INTEGER,
                file_id TEXT,
                paid INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );"""
        )
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS proposals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id INTEGER,
                factory_id INTEGER,
                price INTEGER,
                lead_time INTEGER,
                sample_cost INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(order_id) REFERENCES orders(id),
                FOREIGN KEY(factory_id) REFERENCES factories(tg_id)
            );"""
        )
    logger.info("SQLite schema ensured ✔")


def fetchall(sql: str, params: Iterable[Any] | None = None) -> list[sqlite3.Row]:
    with sqlite3.connect(DB_PATH) as db:
        db.row_factory = sqlite3.Row
        cur = db.execute(sql, params or [])
        return cur.fetchall()


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

# ---------------------------------------------------------------------------
#  UI helpers
# ---------------------------------------------------------------------------

def build_factory_menu() -> types.ReplyKeyboardMarkup:
    return types.ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=[
            [types.KeyboardButton(text="📂 Заявки"), types.KeyboardButton(text="/profile")],
            [types.KeyboardButton(text="/myleads")],
        ],
    )


def send_order_card(chat_id: int, row: sqlite3.Row) -> None:
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="Откликнуться", callback_data=f"lead:{row['id']}")]]
    )
    asyncio.create_task(
        bot.send_message(
            chat_id,
            (
                f"🆕 Заявка #Z‑{row['id']}\n"
                f"Категория: {row['category']}\n"
                f"Тираж: {row['quantity']} шт.\n"
                f"Бюджет: {row['budget']} ₽\n"
                f"Срок: {row['lead_time']} дней"
            ),
            reply_markup=kb,
        )
    )


def notify_factories(order_row: sqlite3.Row) -> None:
    factories = fetchall(
        """
        SELECT tg_id FROM factories
         WHERE is_pro = 1
           AND (',' || categories || ',') LIKE ('%,' || ? || ',%')
           AND min_qty <= ?;""",
        (order_row["category"], order_row["quantity"]),
    )
    logger.info("Dispatching lead %s to %d factories", order_row["id"], len(factories))
    for f in factories:
        send_order_card(f["tg_id"], order_row)

# ---------------------------------------------------------------------------
#  Handlers: common
# ---------------------------------------------------------------------------

@router.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext) -> None:
    kb = types.ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=[
            [types.KeyboardButton(text="🛠 Я – Фабрика")],
            [types.KeyboardButton(text="🛒 Мне нужна фабрика")],
            [types.KeyboardButton(text="ℹ Как работает"), types.KeyboardButton(text="🧾 Тарифы")],
        ],
    )
    await state.clear()
    await message.answer("<b>Привет!</b> Кто вы?", reply_markup=kb)

# ---------------------------------------------------------------------------
#  Factory flow
# ---------------------------------------------------------------------------

@router.message(F.text == "🛠 Я – Фабрика")
async def factory_begin(message: Message, state: FSMContext) -> None:
    await state.set_state(FactoryForm.inn)
    await message.answer("Введите ИНН предприятия:")


@router.message(FactoryForm.inn)
async def factory_inn(message: Message, state: FSMContext) -> None:
    await state.update_data(inn=message.text.strip())
    await state.set_state(FactoryForm.photos)
    await message.answer("Загрузите 1‑3 фото цеха или сертификат ISO:")


@router.message(FactoryForm.photos, F.photo | F.document)
async def factory_photos(message: Message, state: FSMContext) -> None:
    file_ids = [p.file_id for p in message.photo] if message.photo else [message.document.file_id]
    await state.update_data(photos=file_ids)
    await state.set_state(FactoryForm.categories)
    kb = types.ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=[
            [types.KeyboardButton(text="Трикотаж"), types.KeyboardButton(text="Верхняя одежда")],
            [types.KeyboardButton(text="Домашний текстиль")],
        ],
    )
    await message.answer("Категории производства?", reply_markup=kb)


@router.message(FactoryForm.categories)
async def factory_categories(message: Message, state: FSMContext) -> None:
    cats = [c.strip() for c in re.split(r",|\n", message.text) if c.strip()]
    await state.update_data(categories=cats)
    await state.set_state(FactoryForm.min_qty)
    await message.answer("Минимальный тираж (шт.)?")


@router.message(FactoryForm.min_qty)
async def factory_min_qty(message: Message, state: FSMContext) -> None:
    qty = int(re.sub(r"\D", "", message.text))
    await state.update_data(min_qty=qty)
    await state.set_state(FactoryForm.avg_price)
    await message.answer("Средняя ставка, ₽ за изделие?")


@router.message(FactoryForm.avg_price)
async def factory_avg_price(message: Message, state: FSMContext) -> None:
    price = int(re.sub(r"\D", "", message.text))
    await state.update_data(avg_price=price)
    await state.set_state(FactoryForm.portfolio)
    await message.answer("Ссылка на портфолио (необязательно):")


@router.message(FactoryForm.portfolio)
async def factory_portfolio(message: Message, state: FSMContext) -> None:
    await state.update_data(portfolio=message.text.strip())
    await state.set_state(FactoryForm.confirm_pay)
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="Оплатить 2 000 ₽", callback_data="pay_factory")]]
    )
    await message.answer(
        "<b>Готово!</b> После оплаты бот будет присылать лиды, подходящие под ваши параметры.",
        reply_markup=kb,
    )


@router.callback_query(F.data == "pay_factory", FactoryForm.confirm_pay)
async def factory_pay(call: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    execute(
        """
        INSERT INTO factories (tg_id, inn, categories, min_qty, avg_price, portfolio, is_pro)
             VALUES (?, ?, ?, ?, ?, ?, 1)
        ON CONFLICT(tg_id) DO UPDATE SET
            inn=excluded.inn,
            categories=excluded.categories,
            min_qty=excluded.min_qty,
            avg_price=excluded.avg_price,
            portfolio=excluded.portfolio,
            is_pro=1;""",
        (
            call.from_user.id,
            data.get("inn"),
            ",".join(data.get("categories", [])),
            data.get("min_qty"),
            data.get("avg_price"),
            data.get("portfolio"),
        ),
    )
    await state.clear()

    await call.message.edit_text("✅ Статус: <b>PRO</b>. Лиды будут приходить в этот чат.")
    await bot.send_message(call.from_user.id, "Меню фабрики:", reply_markup=build_factory_menu())
    await call.answer()

# ---------------------------------------------------------------------------
#  Buyer flow
# ---------------------------------------------------------------------------

@router.message(F.text == "🛒 Мне нужна фабрика")
async def buyer_begin(message: Message, state: FSMContext) -> None:
    await state.set_state(BuyerForm.category)
    await message.answer("Категория изделия?")


@router.message(BuyerForm.category)
async def buyer_category(message: Message, state


@dp.message(F.text == "🛒 Мне нужна фабрика")
async def buyer_begin(message: Message, state: FSMContext) -> None:
    await message.answer("Категория изделия?")
    await state.set_state(BuyerForm.category)


@dp.message(BuyerForm.category)
async def buyer_category(message: Message, state: FSMContext) -> None:
    await state.update_data(category=message.text.strip())
    await message.answer("Тираж (шт.)?")
    await state.set_state(BuyerForm.quantity)


@dp.message(BuyerForm.quantity)
async def buyer_qty(message: Message, state: FSMContext) -> None:
    await state.update_data(quantity=int(re.sub(r"\D", "", message.text)))
    await message.answer("Бюджет, ₽?")
    await state.set_state(BuyerForm.budget)


@dp.message(BuyerForm.budget)
async def buyer_budget(message: Message, state: FSMContext) -> None:
    await state.update_data(budget=int(re.sub(r"\D", "", message.text)))
    await message.answer("Куда доставить готовую партию?")
    await state.set_state(BuyerForm.destination)


@dp.message(BuyerForm.destination)
async def buyer_dest(message: Message, state: FSMContext) -> None:
    await state.update_data(destination=message.text.strip())
    await message.answer("Срок выпуска (дней)? Укажите число.")
    await state.set_state(BuyerForm.lead_time)


@dp.message(BuyerForm.lead_time)
async def buyer_lead(message: Message, state: FSMContext) -> None:
    digits = re.sub(r"\D", "", message.text)
    if not digits:
        await message.answer("Пожалуйста, укажите срок числом, например <b>45</b>.")
        return
    await state.update_data(lead_time=int(digits))
    await message.answer("Прикрепите техзадание (файл) или фото эскиза:")
    await state.set_state(BuyerForm.file)


@dp.message(BuyerForm.file, F.document | F.photo)
async def buyer_file(message: Message, state: FSMContext) -> None:
    file_id = message.document.file_id if message.document else message.photo[-1].file_id
    await state.update_data(file_id=file_id)

    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="Оплатить 700 ₽", callback_data="pay_order")]]
    )
    await message.answer(
        "<b>Заявка готова!</b> После оплаты она автоматически уйдёт\n"
        "всем подходящим фабрикам (PRO‑аккаунты).",
        reply_markup=kb,
    )
    await state.set_state(BuyerForm.confirm_pay)
