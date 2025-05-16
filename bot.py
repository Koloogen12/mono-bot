"""Mono‑Fabrique Telegram bot — MVP
=================================================
Telegram bot that connects garment factories («Фабрика») with buyers («Заказчик»).
Single‑file implementation (~660 sloc) based on **aiogram 3.4+** with no extra
runtime deps. Works in *long‑polling* (default) or *webhook* mode.

Major flows
-----------
* Factory onboarding → PRO subscription (₂ 000 ₽ stub‑payment)
* Buyer creates order → payment (₇ 00 ₽) → instant dispatch to matching
  PRO‑factories (by category, min_qty, ⩽ budget)
* Factories view «📂 Заявки», send price / lead‑time / sample‑cost; buyer gets
  proposal cards
* Profiles, history, `/help`, SQLite persistence

Environment
-----------
* **BOT_TOKEN** – Telegram bot token (required)
* BOT_MODE – `POLLING` (default) or `WEBHOOK`
* WEBHOOK_BASE – public https URL (required in webhook mode)
* PORT – Render/Fly sets automatically
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
from aiogram.webhook.aiohttp_server import (
    SimpleRequestHandler,
    setup_application,
)
from aiohttp import web

try:
    # optional for local dev
    from dotenv import load_dotenv

    load_dotenv()
except ModuleNotFoundError:
    pass

# ---------------------------------------------------------------------------
#  Config
# ---------------------------------------------------------------------------
TOKEN = os.getenv("BOT_TOKEN", "").strip()
if not TOKEN:
    raise RuntimeError("Environment variable BOT_TOKEN is missing. Set BOT_TOKEN.")

BOT_MODE = os.getenv("BOT_MODE", "POLLING").upper()
WEBHOOK_BASE = os.getenv("WEBHOOK_BASE", "")
PORT = int(os.getenv("PORT", "8080"))

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger("fabrique-bot")

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
        return db.execute(sql, params or []).fetchall()


def fetchone(sql: str, params: Iterable[Any] | None = None) -> sqlite3.Row | None:
    rows = fetchall(sql, params)
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

# ---------------------------------------------------------------------------
#  UI helpers
# ---------------------------------------------------------------------------


def build_factory_menu() -> types.ReplyKeyboardMarkup:
    return types.ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=[
            [
                types.KeyboardButton(text="📂 Заявки"),
                types.KeyboardButton(text="🧾 Профиль"),
            ],
            [types.KeyboardButton(text="/help")],
        ],
    )


def send_order_card(chat_id: int, row: sqlite3.Row) -> None:
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="Откликнуться", callback_data=f"lead:{row['id']}")]]
    )
    caption = (
        f"<b>Заявка #Z‑{row['id']}</b>\n"
        f"Категория: {row['category']}\n"
        f"Тираж: {row['quantity']} шт.\n"
        f"Бюджет: {row['budget']} ₽ за ед.\n"
        f"Срок: {row['lead_time']} дн.\n"
        f"Город: {row['destination']}"
    )
    asyncio.create_task(bot.send_message(chat_id, caption, reply_markup=kb))


# ---------------------------------------------------------------------------
#  Lead dispatching
# ---------------------------------------------------------------------------


def notify_factories(order_row: sqlite3.Row) -> None:
    """Push new order to all suitable PRO‑factories."""
    factories = fetchall(
        """
        SELECT tg_id FROM factories
         WHERE is_pro = 1
           AND min_qty <= ?
           AND avg_price <= ?
           AND (',' || categories || ',') LIKE ('%,' || ? || ',%');""",
        (order_row["quantity"], order_row["budget"], order_row["category"]),
    )
    logger.info("Dispatch lead %s → %d factories", order_row["id"], len(factories))
    for f in factories:
        send_order_card(f["tg_id"], order_row)

# ---------------------------------------------------------------------------
#  Common commands
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


@router.message(F.text == "🧾 Профиль")
async def cmd_profile(message: Message) -> None:
    row = fetchone("SELECT * FROM factories WHERE tg_id = ?", (message.from_user.id,))
    if row:
        await message.answer(
            f"<b>Профиль фабрики</b>\n"
            f"Категории: {row['categories']}\n"
            f"Мин.тираж: {row['min_qty']} шт.\n"
            f"Средняя цена: {row['avg_price']}₽\n"
            f"PRO: {'✅' if row['is_pro'] else '—'}"
        )
    else:
        await message.answer("Профиль не найден. Пройдите онбординг фабрики или оформите заказ.")

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
    file_ids = (
        [p.file_id for p in message.photo] if message.photo else [message.document.file_id]
    )
    await state.update_data(photos=",".join(file_ids))
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
    await state.update_data(categories=",".join(cats))
    await state.set_state(FactoryForm.min_qty)
    await message.answer("Минимальный тираж (шт.)?")


@router.message(FactoryForm.min_qty)
async def factory_min_qty(message: Message, state: FSMContext) -> None:
    digits = re.sub(r"\D", "", message.text)
    if not digits:
        await message.answer("Пожалуйста, введите число.")
        return
    await state.update_data(min_qty=int(digits))
    await state.set_state(FactoryForm.avg_price)
    await message.answer("Средняя цена за единицу (₽)?")


@router.message(FactoryForm.avg_price)
async def factory_avg_price(message: Message, state: FSMContext) -> None:
    digits = re.sub(r"\D", "", message.text)
    if not digits:
        await message.answer("Введите число.")
        return
    await state.update_data(avg_price=int(digits))
    await state.set_state(FactoryForm.portfolio)
    await message.answer("Ссылка на портфолио (Behance/Google Диск)?")


@router.message(FactoryForm.portfolio)
async def factory_portfolio(message: Message, state: FSMContext) -> None:
    await state.update_data(portfolio=message.text.strip())
    data = await state.get_data()
    text = (
        "<b>Проверьте данные</b>\n"
        f"ИНН: {data['inn']}\n"
        f"Категории: {data['categories']}\n"
        f"Мин.тираж: {data['min_qty']}\n"
        f"Цена: {data['avg_price']} ₽"
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="Оплатить 2 000 ₽", callback_data="pay_factory")]]
    )
    await state.set_state(FactoryForm.confirm_pay)
    await message.answer(text, reply_markup=kb)


@router.callback_query(F.data == "pay_factory", FactoryForm.confirm_pay)
async def factory_pay(call: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    execute(
        """INSERT OR REPLACE INTO factories
               (tg_id, name, inn, categories, min_qty, avg_price, portfolio, is_pro)
             VALUES(?, ?, ?, ?, ?, ?, ?, 1);""",
        (
            call.from_user.id,
            call.from_user.full_name,
            data["inn"],
            data["categories"],
            data["min_qty"],
            data["avg_price"],
            data["portfolio"],
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
    await message.answer("Что нужно произвести? Категория:")


@router.message(BuyerForm.category)
async def buyer_category(message: Message, state: FSMContext) -> None:
    await state.update_data(category=message.text.strip())
    await state.set_state(BuyerForm.quantity)
    await message.answer("Тираж (шт.)?")


@router.message(BuyerForm.quantity)
async def buyer_quantity(message: Message, state: FSMContext) -> None:
    digits = re.sub(r"\D", "", message.text)
    if not digits:
        await message.answer("Введите число.")
        return
    await state.update_data(quantity=int(digits))
    await state.set_state(BuyerForm.budget)
    await message.answer("Бюджет (₽)?")


@router.message(BuyerForm.budget)
async def buyer_budget(message: Message, state: FSMContext) -> None:
    digits = re.sub(r"\D", "", message.text)
    if not digits:
        await message.answer("Введите число.")
        return
    await state.update_data(budget=int(digits))
    await state.set_state(BuyerForm.destination)
    await message.answer("Город доставки готовых изделий?")


@router.message(BuyerForm.destination)
async def buyer_destination(message: Message, state: FSMContext) -> None:
    await state.update_data(destination=message.text.strip())
    await state.set_state(BuyerForm.lead_time)
    await message.answer("Срок производства (дней)?")


@router.message(BuyerForm.lead_time)
async def buyer_lead(message: Message, state: FSMContext) -> None:
    digits = re.sub(r"\D", "", message.text)
    if not digits:
        await message.answer("Укажите срок числом, например <b>45</b>.")
        return
    await state.update_data(lead_time=int(digits))
    await state.set_state(BuyerForm.file)
    await message.answer("Добавьте ТЗ или референс (файл/фото), либо напишите «нет»:")


@router.message(BuyerForm.file, F.photo | F.document)
async def buyer_file(message: Message, state: FSMContext) -> None:
    file_id = message.document.file_id if message.document else message.photo[-1].file_id
    await state.update_data(file_id=file_id)
    await state.set_state(BuyerForm.confirm_pay)
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Оплатить 700 ₽", callback_data="pay_order")]])
    await message.answer("Отлично! Оплатите заявку, чтобы она ушла фабрикам.", reply_markup=kb)


@router.message(BuyerForm.file)
async def buyer_file_skip(message: Message, state: FSMContext) -> None:
    await state.update_data(file_id="")
    await state.set_state(BuyerForm.confirm_pay)
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Оплатить 700 ₽", callback_data="pay_order")]])
    await message.answer("Отлично! Оплатите заявку, чтобы она ушла фабрикам.", reply_markup=kb)


@router.callback_query(F.data == "pay_order", BuyerForm.confirm_pay)
async def buyer_pay(call: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    with sqlite3.connect(DB_PATH) as db:
        cur = db.execute(
            """INSERT INTO orders
                   (buyer_id, category, quantity, budget, destination,
                    lead_time, file_id, paid)
                 VALUES(?,?,?,?,?,?,?,1);""",
            (
                call.from_user.id,
                data["category"],
                data["quantity"],
                data["budget"],
                data["destination"],
                data["lead_time"],
                data["file_id"],
            ),
        )
        order_id = cur.lastrowid
        row = db.execute("SELECT * FROM orders WHERE id = ?;", (order_id,)).fetchone()
        db.commit()
    await state.clear()
    await call.message.edit_text("✅ Заявка размещена. Ожидайте предложений от фабрик!")
    await call.answer()
    notify_factories(row)

# ---------------------------------------------------------------------------
#  Factories: заявки и отклики
# ---------------------------------------------------------------------------

@router.message(F.text == "📂 Заявки")
async def factory_orders_list(message: Message) -> None:
    factory = fetchone("SELECT * FROM factories WHERE tg_id = ?", (message.from_user.id,))
    if not factory or not factory["is_pro"]:
        await message.answer("Доступно только PRO-фабрикам.")
        return
    rows = fetchall(
        """
        SELECT o.* FROM orders o
         LEFT JOIN proposals p
           ON p.order_id = o.id AND p.factory_id = ?
         WHERE o.paid = 1
           AND (',' || ? || ',') LIKE ('%,' || o.category || ',%')
           AND o.quantity >= ?
           AND p.id IS NULL
         ORDER BY o.created_at DESC LIMIT 20;""",
        (message.from_user.id, factory["categories"], factory["min_qty"]),
    )
    if not rows:
        await message.answer("Пока подходящих заявок нет.")
        return
    for r in rows:
        send_order_card(message.from_user.id, r)


@router.callback_query(F.data.startswith("lead:"))
async def proposal_begin(call: CallbackQuery, state: FSMContext) -> None:
    order_id = int(call.data.split(":"))[1]
    await state.update_data(order_id=order_id)
    await state.set_state(ProposalForm.price)
    await call.message.answer("Предложите цену за единицу (₽):")
    await call.answer()


@router.message(ProposalForm.price)
async def proposal_price(message: Message, state: FSMContext) -> None:
    digits = re.sub(r"\D", "", message.text)
    if not digits:
        await message.answer("Введите число.")
        return
    await state.update_data(price=int(digits))
    await state.set_state(ProposalForm.lead_time)
    await message.answer("Срок производства (дней):")


@router.message(ProposalForm.lead_time)
async def proposal_lead(message: Message, state: FSMContext) -> None:
    digits = re.sub(r"\D", "", message.text)
    if not digits:
        await message.answer("Введите число.")
        return
    await state.update_data(lead_time=int(digits))
    await state.set_state(ProposalForm.sample_cost)
    await message.answer("Стоимость образца, если требуется (₽) либо 0:")


@router.message(ProposalForm.sample_cost)
async def proposal_finish(message: Message, state: FSMContext) -> None:
    digits = re.sub(r"\D", "", message.text)
    if digits == "":
        await message.answer("Введите число (или 0).")
        return
    await state.update_data(sample_cost=int(digits))
    data = await state.get_data()
    execute(
        """INSERT INTO proposals(order_id, factory_id, price, lead_time, sample_cost)
             VALUES (?,?,?,?,?);""",
        (
            data["order_id"],
            message.from_user.id,
            data["price"],
            data["lead_time"],
            data["sample_cost"],
        ),
    )
    order = fetchone("SELECT * FROM orders WHERE id = ?", (data["order_id"],))
    caption = (
        f"<b>Предложение #P‑{data['order_id']}</b>\n"
        f"Цена: {data['price']}₽\n"
        f"Срок: {data['lead_time']} дн.\n"
        f"Образец: {data['sample_cost']}₽"
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="Связаться", url=f"tg://user?id={message.from_user.id}")]]
    )
    await bot.send_message(order["buyer_id"], caption, reply_markup=kb)
    await state.clear()
    await message.answer("✅ Предложение отправлено покупателю!")

# ---------------------------------------------------------------------------
#  Help & tariffs
# ---------------------------------------------------------------------------

@router.message(F.text == "ℹ Как работает")
async def how_it_works(message: Message) -> None:
    await message.answer(
        "<b>Mono‑Fabrique</b> связывает фабрики и бренды.\n"
        "1. Покупатель оформляет заявку и оплачивает 700 ₽.\n"
        "2. Рассылка идёт по PRO‑фабрикам.\n"
        "3. Фабрики отвечают предложениями, общаются напрямую."
    )


@router.message(F.text == "🧾 Тарифы")
async def tariffs(message: Message) -> None:
    await message.answer("Покупатель: 700 ₽ за заявку.\nФабрика: 2 000 ₽ PRO/мес (MVP – единоразово).")

# ---------------------------------------------------------------------------
#  Startup logic
# ---------------------------------------------------------------------------

async def main() -> None:
    init_db()
    if BOT_MODE == "WEBHOOK":
        await bot.delete_webhook(drop_pending_updates=True)
        webhook_path = f"/tg/{TOKEN}"
        await bot.set_webhook(f"{WEBHOOK_BASE}{webhook_path}")
        app = web.Application()
        SimpleRequestHandler(dispatcher=dp, bot=bot).register(app, path=webhook_path)
        setup_application(app, dp, bot=bot)
        logger.info("Webhook set %s", WEBHOOK_BASE)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", PORT)
        await site.start()
        while True:
            await asyncio.sleep(3600)
    else:
        await bot.delete_webhook(drop_pending_updates=True)
        logger.info("Webhook cleared ✔ – switched to long‑polling mode")
        await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
