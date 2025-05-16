"""Mono‑Fabrique Telegram bot – single‑file MVP (aiogram 3.4+)
================================================================
Connects garment factories («Фабрика») with buyers («Заказчик»).
Implements every mandatory requirement from the technical specification in
≈1200 SLOC, with no runtime dependencies beyond **aiogram** (and optional
python‑dotenv for local development).

Main flows
----------
* Factory onboarding → stub payment (₂ 000 ₽) → PRO → receives leads & "📂 Заявки" menu.
* Buyer creates order → stub payment (₇ 00 ₽) → order stored → automatically
  dispatched to matching PRO‑factories (category, min_qty, avg_price ≤ budget).
* Factories browse «📂 Заявки» or get push‑lead, press «Откликнуться» → send
  price / lead‑time / sample‑cost → Buyer receives proposal.
* Escrow system for secure payments and status tracking.

Runtime
-------
* Works in **long‑polling** (default) or **webhook** mode (`BOT_MODE=WEBHOOK`).
* SQLite persistence (`fabrique.db`) created automatically.
* Graceful shutdown (Ctrl‑C) & readable logging.

Env variables
-------------
* `BOT_TOKEN`    – Telegram token (required)
* `BOT_MODE`     – `POLLING` (default) or `WEBHOOK`
* `WEBHOOK_BASE` – public HTTPS URL when in webhook mode
* `PORT`         – HTTP port for webhook (Render/Fly set automatically)
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
import sqlite3
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

from aiogram import Bot, Dispatcher, F, Router
from aiogram.enums import ParseMode
from aiogram.filters import Command
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

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger("fabrique-bot")

bot = Bot(TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)

DB_PATH = "fabrique.db"

# ---------------------------------------------------------------------------
#  Status & Order Tracking Constants
# ---------------------------------------------------------------------------

ORDER_STATUSES = {
    "DRAFT": "Образец оплачивается. Ожидаем фото QC.",
    "SAMPLE_PASS": "Образец одобрен. Оплатите 30 % предоплаты (Escrow).",
    "PRODUCTION": "Производство. Инспекция в процессе.",
    "READY_TO_SHIP": "Фабрика загрузила B/L. Оплатите остаток 70 %.",
    "IN_TRANSIT": "Товар в пути. Отслеживание активно.",
    "DELIVERED": "Груз получен. Escrow разблокирован. Оцените сделку.",
}

# ---------------------------------------------------------------------------
#  DB helpers
# ---------------------------------------------------------------------------

def init_db() -> None:
    """Ensure SQLite schema."""
    with sqlite3.connect(DB_PATH) as db:
        db.execute(
            """CREATE TABLE IF NOT EXISTS factories (
                    tg_id        INTEGER PRIMARY KEY,
                    name         TEXT,
                    inn          TEXT,
                    categories   TEXT,
                    min_qty      INTEGER,
                    avg_price    INTEGER,
                    portfolio    TEXT,
                    rating       REAL DEFAULT 0,
                    rating_count INTEGER DEFAULT 0,
                    is_pro       INTEGER DEFAULT 0,
                    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );"""
        )
        db.execute(
            """CREATE TABLE IF NOT EXISTS orders (
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
                );"""
        )
        db.execute(
            """CREATE TABLE IF NOT EXISTS proposals (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_id     INTEGER,
                    factory_id   INTEGER,
                    price        INTEGER,
                    lead_time    INTEGER,
                    sample_cost  INTEGER,
                    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(order_id, factory_id)
                );"""
        )
        db.execute(
            """CREATE TABLE IF NOT EXISTS deals (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_id     INTEGER,
                    factory_id   INTEGER,
                    buyer_id     INTEGER,
                    amount       INTEGER,
                    status       TEXT DEFAULT 'DRAFT',
                    deposit_paid INTEGER DEFAULT 0,
                    final_paid   INTEGER DEFAULT 0,
                    tracking_num TEXT,
                    eta          TEXT,
                    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(order_id, factory_id, buyer_id)
                );"""
        )
        db.execute(
            """CREATE TABLE IF NOT EXISTS ratings (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    deal_id      INTEGER,
                    factory_id   INTEGER,
                    buyer_id     INTEGER,
                    rating       INTEGER,
                    comment      TEXT,
                    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(deal_id, factory_id, buyer_id)
                );"""
        )
    logger.info("SQLite schema ensured ✔")


def q(sql: str, params: Iterable[Any] | None = None) -> list[sqlite3.Row]:
    with sqlite3.connect(DB_PATH) as db:
        db.row_factory = sqlite3.Row
        return db.execute(sql, params or []).fetchall()


def q1(sql: str, params: Iterable[Any] | None = None) -> sqlite3.Row | None:
    rows = q(sql, params)
    return rows[0] if rows else None


def run(sql: str, params: Iterable[Any] | None = None) -> None:
    with sqlite3.connect(DB_PATH) as db:
        db.execute(sql, params or [])
        db.commit()


def insert_and_get_id(sql: str, params: Iterable[Any] | None = None) -> int:
    with sqlite3.connect(DB_PATH) as db:
        cursor = db.execute(sql, params or [])
        db.commit()
        return cursor.lastrowid

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


class DealForm(StatesGroup):
    choose_factory = State()
    confirm_sample = State()
    payment_deposit = State()
    payment_final = State()
    confirm_delivery = State()
    rate_factory = State()


class TrackingForm(StatesGroup):
    order_id = State()
    tracking_num = State()
    eta = State()

# ---------------------------------------------------------------------------
#  Keyboards & helpers
# ---------------------------------------------------------------------------


def kb_main() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=[
            [KeyboardButton("🛠 Я – Фабрика"), KeyboardButton("🛒 Мне нужна фабрика")],
            [KeyboardButton("ℹ Как работает"), KeyboardButton("🧾 Тарифы")],
        ],
    )


def kb_factory_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=[
            [KeyboardButton("📂 Заявки"), KeyboardButton("🧾 Профиль")],
            [KeyboardButton("⏱ Статус заказов"), KeyboardButton("⭐ Рейтинг")],
        ]
    )


def kb_buyer_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=[
            [KeyboardButton("📋 Мои заказы"), KeyboardButton("🧾 Профиль")],
            [KeyboardButton("⏱ Статус заказов"), KeyboardButton("🔄 Новый заказ")],
        ]
    )


def parse_digits(text: str) -> int | None:
    digits = re.sub(r"\D", "", text)
    return int(digits) if digits else None


def order_caption(row: sqlite3.Row) -> str:
    return (
        f"<b>Заявка #Z‑{row['id']}</b>\n"
        f"Категория: {row['category']}\n"
        f"Тираж: {row['quantity']} шт.\n"
        f"Бюджет: {row['budget']} ₽\n"
        f"Срок: {row['lead_time']} дн.\n"
        f"Город: {row['destination']}"
    )


def send_order_card(chat_id: int, row: sqlite3.Row) -> None:
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton("Откликнуться", callback_data=f"lead:{row['id']}")]])
    asyncio.create_task(bot.send_message(chat_id, order_caption(row), reply_markup=kb))


def proposal_caption(row: sqlite3.Row, factory_name: str = "") -> str:
    return (
        f"<b>Предложение от фабрики {factory_name}</b>\n"
        f"Цена: {row['price']} ₽\n"
        f"Срок: {row['lead_time']} дн.\n"
        f"Образец: {row['sample_cost']} ₽"
    )


def status_caption(deal: sqlite3.Row) -> str:
    status_text = ORDER_STATUSES.get(deal["status"], "Статус неизвестен")
    factory = q1("SELECT * FROM factories WHERE tg_id=?", (deal["factory_id"],))
    factory_name = factory["name"] if factory else "Неизвестная фабрика"
    order = q1("SELECT * FROM orders WHERE id=?", (deal["order_id"],))
    
    caption = (
        f"<b>Сделка #{deal['id']}</b>\n"
        f"Заказ: #Z-{deal['order_id']}\n"
        f"Фабрика: {factory_name}\n"
        f"Сумма: {deal['amount']} ₽\n"
        f"Статус: {deal['status']}\n"
        f"<i>{status_text}</i>"
    )
    
    if deal["tracking_num"]:
        caption += f"\nТрек-код: {deal['tracking_num']}"
    if deal["eta"]:
        caption += f"\nETA: {deal['eta']}"
    
    return caption

# ---------------------------------------------------------------------------
#  Lead dispatch & listings
# ---------------------------------------------------------------------------


def notify_factories(order_row: sqlite3.Row) -> None:
    factories = q(
        """SELECT tg_id FROM factories
             WHERE is_pro=1
               AND min_qty<=?
               AND avg_price<=?
               AND (','||categories||',') LIKE ('%,'||?||',%');""",
        (order_row["quantity"], order_row["budget"], order_row["category"]),
    )
    logger.info("Lead %s dispatched to %d factories", order_row["id"], len(factories))
    for f in factories:
        send_order_card(f["tg_id"], order_row)

# ---------------------------------------------------------------------------
#  Common info commands
# ---------------------------------------------------------------------------


@router.message(Command("start"))
async def cmd_start(msg: Message, state: FSMContext) -> None:
    await state.clear()
    await msg.answer(
        "<b>Привет!</b> Я соединяю швейные фабрики и заказчиков. Выберите вариант:", reply_markup=kb_main()
    )


@router.message(Command("help"))
async def cmd_help(msg: Message) -> None:
    await msg.answer(
        "<b>Команды бота:</b>\n"
        "/profile — профиль и настройки\n"
        "/myorders — мои заказы (для заказчика)\n"
        "/myleads — мои заявки (для фабрики)\n"
        "/rating — рейтинг и отзывы\n"
        "/start — вернуться в главное меню",
        reply_markup=kb_main(),
    )


@router.message(F.text == "ℹ Как работает")
async def cmd_how(msg: Message) -> None:
    await msg.answer(
        "Заказчик оформляет заявку, оплачивает 700 ₽ →\n"
        "Подходящие PRO‑фабрики получают лид и откликаются →\n"
        "Вы выбираете лучшую фабрику и сотрудничаете через безопасный Escrow.\n\n"
        "Мы берем комиссию только за публикацию и обеспечиваем безопасность сделки.",
        reply_markup=kb_main(),
    )


@router.message(F.text == "🧾 Тарифы")
async def cmd_tariffs(msg: Message) -> None:
    await msg.answer(
        "Для фабрик: 2 000 ₽/мес — статус PRO и доступ ко всем лидам.\n"
        "Для заказчиков: 700 ₽ за публикацию заявки.\n\n"
        "Мы не берем комиссию с итоговой сделки!",
        reply_markup=kb_main(),
    )

# ---------------------------------------------------------------------------
#  Profile & menu
# ---------------------------------------------------------------------------


@router.message(Command("profile"))
@router.message(F.text == "🧾 Профиль")
async def cmd_profile(msg: Message) -> None:
    f = q1("SELECT * FROM factories WHERE tg_id=?", (msg.from_user.id,))
    if f:
        rating_text = f"{f['rating']:.1f}/5.0 ({f['rating_count']})" if f["rating_count"] > 0 else "Нет отзывов"
        await msg.answer(
            f"<b>Профиль фабрики</b>\n"
            f"ИНН: {f['inn']}\nКатегории: {f['categories']}\n"
            f"Мин. тираж: {f['min_qty']} шт.\nСредняя цена: {f['avg_price']}₽\n"
            f"Рейтинг: {rating_text}\n"
            f"PRO: {'✅' if f['is_pro'] else '—'}",
            reply_markup=kb_factory_menu() if f["is_pro"] else None,
        )
    else:
        # Check if user has orders as a buyer
        orders = q("SELECT COUNT(*) as count FROM orders WHERE buyer_id=?", (msg.from_user.id,))
        if orders and orders[0]["count"] > 0:
            await msg.answer(
                f"<b>Профиль заказчика</b>\n"
                f"ID: {msg.from_user.id}\n"
                f"Размещено заказов: {orders[0]['count']}",
                reply_markup=kb_buyer_menu(),
            )
        else:
            await msg.answer(
                "Профиль не найден. Выберите, кто вы:",
                reply_markup=kb_main(),
            )


@router.message(Command("rating"))
@router.message(F.text == "⭐ Рейтинг")
async def cmd_rating(msg: Message) -> None:
    f = q1("SELECT * FROM factories WHERE tg_id=?", (msg.from_user.id,))
    if f:
        if f["rating_count"] > 0:
            # Get recent ratings
            ratings = q(
                """SELECT r.*, o.category 
                   FROM ratings r 
                   JOIN deals d ON r.deal_id = d.id 
                   JOIN orders o ON d.order_id = o.id
                   WHERE r.factory_id = ? 
                   ORDER BY r.created_at DESC LIMIT 5""",
                (msg.from_user.id,),
            )
            
            rating_text = f"<b>Рейтинг фабрики: {f['rating']:.1f}/5.0</b> ({f['rating_count']} отзывов)\n\n"
            rating_text += "Последние отзывы:\n"
            
            for r in ratings:
                stars = "⭐" * r["rating"]
                rating_text += f"{stars} ({r['category']})\n"
                if r["comment"]:
                    rating_text += f"«{r['comment']}»\n"
            
            await msg.answer(rating_text, reply_markup=kb_factory_menu())
        else:
            await msg.answer(
                "У вас пока нет отзывов. Они появятся после выполненных заказов.",
                reply_markup=kb_factory_menu(),
            )
    else:
        # Check if user is a buyer
        ratings = q(
            """SELECT r.*, f.name as factory_name 
               FROM ratings r 
               JOIN factories f ON r.factory_id = f.tg_id
               WHERE r.buyer_id = ? 
               ORDER BY r.created_at DESC LIMIT 5""",
            (msg.from_user.id,),
        )
        
        if ratings:
            rating_text = "<b>Ваши отзывы о фабриках:</b>\n\n"
            for r in ratings:
                stars = "⭐" * r["rating"]
                rating_text += f"{r['factory_name']}: {stars}\n"
                if r["comment"]:
                    rating_text += f"«{r['comment']}»\n"
            
            await msg.answer(rating_text, reply_markup=kb_buyer_menu())
        else:
            await msg.answer(
                "Вы пока не оставляли отзывов о фабриках.",
                reply_markup=kb_buyer_menu() if q1("SELECT 1 FROM orders WHERE buyer_id=?", (msg.from_user.id,)) else kb_main(),
            )

# ---------------------------------------------------------------------------
#  Factory onboarding
# ---------------------------------------------------------------------------


@router.message(F.text == "🛠 Я – Фабрика")
async def factory_start(msg: Message, state: FSMContext) -> None:
    await state.clear()
    await state.set_state(FactoryForm.inn)
    await msg.answer("Введите ИНН вашей фабрики:", reply_markup=ReplyKeyboardRemove())


@router.message(FactoryForm.inn)
async def factory_inn(msg: Message, state: FSMContext) -> None:
    inn_digits = parse_digits(msg.text or "")
    if inn_digits is None or len(str(inn_digits)) not in (10, 12):
        await msg.answer("ИНН должен содержать 10 или 12 цифр. Повторите")
        return
    await state.update_data(inn=str(inn_digits))
    await state.set_state(FactoryForm.photos)
    await msg.answer("Пришлите 1‑2 фото цеха/оборудования (или напишите «skip»):")


@router.message(FactoryForm.photos, F.photo | F.text)
async def factory_photos(msg: Message, state: FSMContext) -> None:
    photos: list[str] = (await state.get_data()).get("photos", [])  # type: ignore
    if msg.text and msg.text.lower().startswith("skip"):
        pass
    elif msg.photo:
        photos.append(msg.photo[-1].file_id)
    await state.update_data(photos=photos)
    if len(photos) < 2 and not (msg.text and msg.text.lower().startswith("skip")):
        await msg.answer("Добавьте ещё фото или напишите «skip»:")
        return
    await state.set_state(FactoryForm.categories)
    await msg.answer("Перечислите через запятую категории (футерки, трикотаж, пековые…):")


@router.message(FactoryForm.categories)
async def factory_categories(msg: Message, state: FSMContext) -> None:
    cats = [c.strip().lower() for c in msg.text.split(",") if c.strip()] if msg.text else []
    if not cats:
        await msg.answer("Введите хотя бы одну категорию:")
        return
    await state.update_data(categories=",".join(cats))
    await state.set_state(FactoryForm.min_qty)
    await msg.answer("Минимальный производственный тираж (число):")


@router.message(FactoryForm.min_qty)
async def factory_min_qty(msg: Message, state: FSMContext) -> None:
    qty = parse_digits(msg.text or "")
    if not qty:
        await msg.answer("Укажите число, например 300:")
        return
    await state.update_data(min_qty=qty)
    await state.set_state(FactoryForm.avg_price)
    await msg.answer("Средняя цена за изделие, ₽:")


@router.message(FactoryForm.avg_price)
async def factory_avg_price(msg: Message, state: FSMContext) -> None:
    price = parse_digits(msg.text or "")
    if not price:
        await msg.answer("Укажите число, например 550:")
        return
    await state.update_data(avg_price=price)
    await state.set_state(FactoryForm.portfolio)
    await msg.answer("Название фабрики и ссылка на портфолио (Instagram/Drive) или «skip»:")


@router.message(FactoryForm.portfolio)
async def factory_portfolio(msg: Message, state: FSMContext) -> None:
    # Extract name and possibly URL from message
    if msg.text and msg.text.lower() != "skip":
        parts = msg.text.split(" ", 1)
        name = parts[0]
        portfolio = parts[1] if len(parts) > 1 else ""
        await state.update_data(name=name, portfolio=portfolio)
    else:
        await state.update_data(name=f"Фабрика_{msg.from_user.id}", portfolio="")
    
    data = await state.get_data()
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton("Оплатить 2 000 ₽", callback_data="pay_factory")]])
    await state.set_state(FactoryForm.confirm_pay)
    await msg.answer(
        "Почти готово! Оплатите PRO‑подписку, чтобы получать заявки:", reply_markup=kb
    )


@router.callback_query(F.data == "pay_factory", FactoryForm.confirm_pay)
async def factory_pay(call: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    run(
        """INSERT OR REPLACE INTO factories
               (tg_id, name, inn, categories, min_qty, avg_price, portfolio, is_pro)
             VALUES(?, ?, ?, ?, ?, ?, ?, 1);""",
        (
            call.from_user.id,
            data.get("name", f"Фабрика_{call.from_user.id}"),
            data["inn"],
            data["categories"],
            data["min_qty"],
            data["avg_price"],
            data.get("portfolio", ""),
        ),
    )
    await state.clear()
    await call.message.edit_text("✅ Статус: <b>PRO</b>. Лиды будут приходить в этот чат.")
    await bot.send_message(call.from_user.id, "Меню фабрики:", reply_markup=kb_factory_menu())
    await call.answer()

# ---------------------------------------------------------------------------
#  Buyer order
# ---------------------------------------------------------------------------


@router.message(F.text == "🛒 Мне нужна фабрика")
@router.message(F.text == "🔄 Новый заказ")
async def buyer_start(msg: Message, state: FSMContext) -> None:
    await state.clear()
    await state.set_state(BuyerForm.category)
    await msg.answer("Категория изделия (например, трикотаж):", reply_markup=ReplyKeyboardRemove())


@router.message(BuyerForm.category)
async def buyer_category(msg: Message, state: FSMContext) -> None:
    if not msg.text:
        await msg.answer("Введите текст:")
        return
    await state.update_data(category=msg.text.strip().lower())
    await state.set_state(BuyerForm.quantity)
    await msg.answer("Тираж (шт.):")


@router.message(BuyerForm.quantity)
async def buyer_qty(msg: Message, state: FSMContext) -> None:
    qty = parse_digits(msg.text or "")
    if not qty:
        await msg.answer("Укажите число, например 500:")
        return
    await state.update_data(quantity=qty)
    await state.set_state(BuyerForm.budget)
    await msg.answer("Бюджет, ₽ за изделие:")


@router.message(BuyerForm.budget)
async def buyer_budget(msg: Message, state: FSMContext) -> None:
    price = parse_digits(msg.text or "")
    if not price:
        await msg.answer("Укажите число:")
        return
    await state.update_data(budget=price)
    await state.set_state(BuyerForm.destination)
    await msg.answer("Город доставки:")


@router.message(BuyerForm.destination)
async def buyer_destination(msg: Message, state: FSMContext) -> None:
    if not msg.text:
        await msg.answer("Введите текст:")
        return
    await state.update_data(destination=msg.text.strip())
    await state.set_state(BuyerForm.lead_time)
    await msg.answer("Желаемый срок производства, дней:")


@router.message(BuyerForm.lead_time)
async def buyer_lead_time(msg: Message, state: FSMContext) -> None:
    days = parse_digits(msg.text or "")
    if not days:
        await msg.answer("Укажите число дней, например 45:")
        return
    await state.update_data(lead_time=days)
    await state.set_state(BuyerForm.file)
    await msg.answer("Прикрепите ТЗ (файл/фото) или напишите «skip»:")


@router.message(BuyerForm.file, F.document | F.photo | F.text)
async def buyer_file(msg: Message, state: FSMContext) -> None:
    if msg.text and msg.text.lower().startswith("skip"):
        await state.update_data(file_id=None)
    elif msg.document:
        await state.update_data(file_id=msg.document.file_id)
    elif msg.photo:
        await state.update_data(file_id=msg.photo[-1].file_id)
    else:
        await msg.answer("Пришлите файл/фото или «skip»:")
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton("Оплатить 700 ₽", callback_data="pay_order")]])
    await state.set_state(BuyerForm.confirm_pay)
    await msg.answer("Оплатите размещение заявки:", reply_markup=kb)


@router.callback_query(F.data == "pay_order", BuyerForm.confirm_pay)
async def buyer_pay(call: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    with sqlite3.connect(DB_PATH) as db:
        # Insert order record
        cursor = db.execute(
            """INSERT INTO orders
            (buyer_id, category, quantity, budget, destination, lead_time, file_id, paid)
            VALUES(?, ?, ?, ?, ?, ?, ?, 1)""",
            (
                call.from_user.id,
                data["category"],
                data["quantity"],
                data["budget"],
                data["destination"],
                data["lead_time"],
                data.get("file_id"),
            ),
        )
        db.commit()
        order_id = cursor.lastrowid
    
    await state.clear()
    await call.message.edit_text(f"✅ Заявка #Z-{order_id} создана! Ожидайте предложения от фабрик.")
    await bot.send_message(
        call.from_user.id, 
        "Меню заказчика:", 
        reply_markup=kb_buyer_menu()
    )
    
    # Fetch the complete order to dispatch
    order_row = q1("SELECT * FROM orders WHERE id=?", (order_id,))
    if order_row:
        # Notify matching factories about the new order
        notify_factories(order_row)
    
    await call.answer()


# ---------------------------------------------------------------------
# Orders for buyers
# ---------------------------------------------------------------------

@router.message(Command("myorders"))
@router.message(F.text == "🛒 Мои заказы")
async def cmd_my_orders(msg: Message) -> None:
    orders = q("SELECT * FROM orders WHERE buyer_id=? ORDER BY created_at DESC", (msg.from_user.id,))
    if not orders:
        await msg.answer(
            "У вас пока нет заказов. Создайте новый:",
            reply_markup=kb_buyer_menu(),
        )
        return

    text = "<b>Ваши заказы:</b>\n\n"
    for o in orders:
        text += f"#Z-{o['id']} ({o['category']}, {o['quantity']} шт.)\n"
        text += f"Статус: {'✅ Оплачено' if o['paid'] else '⏳ Не оплачено'}\n\n"

    await msg.answer(text, reply_markup=kb_buyer_menu())


# ---------------------------------------------------------------------
# Factory leads / proposals
# ---------------------------------------------------------------------

@router.message(Command("myleads"))
@router.message(F.text == "🧩 Заявки")
async def cmd_factory_leads(msg: Message) -> None:
    # Check if factory is PRO
    factory = q1("SELECT * FROM factories WHERE tg_id=? AND is_pro=1", (msg.from_user.id,))
    if not factory:
        await msg.answer(
            "Доступ к заявкам только для PRO-фабрик. Оформите подписку.",
            reply_markup=kb_main(),
        )
        return

    # Get matching orders
    matching_orders = q(
        """SELECT o.* FROM orders o
        WHERE o.paid = 1 
        AND o.quantity >= ? 
        AND o.budget >= ?
        AND (?,'' = '','' OR (',' || o.category || ',') LIKE ('%,' || ? || ',%'))
        ORDER BY o.created_at DESC
        LIMIT 15""",
        (factory["min_qty"], factory["avg_price"], factory["categories"], factory["categories"])
    )

    if not matching_orders:
        await msg.answer(
            "Сейчас нет подходящих заявок. Уведомим, когда появятся!",
            reply_markup=kb_factory_menu(),
        )
        return

    # Check which orders already have proposals from this factory
    existing_proposals = q(
        "SELECT order_id FROM proposals WHERE factory_id = ?", 
        (msg.from_user.id,)
    )
    proposal_ids = {p["order_id"] for p in existing_proposals}

    # Send each matching order
    sent_count = 0
    for order in matching_orders:
        if order["id"] not in proposal_ids:
            kb = InlineKeyboardMarkup(
                inline_keyboard=[[
                    InlineKeyboardButton(
                        text="Откликнуться", 
                        callback_data=f"lead:{order['id']}"
                    )
                ]]
            )
            await msg.answer(order_caption(order), reply_markup=kb)
            sent_count += 1
            if sent_count >= 5:  # Limit to 5 leads at once
                break

    await msg.answer(
        f"Показано {sent_count} заявок из {len(matching_orders)} подходящих.",
        reply_markup=kb_factory_menu(),
    )


@router.callback_query(F.data.startswith("lead:"))
async def process_lead_response(call: CallbackQuery, state: FSMContext) -> None:
    order_id = int(call.data.split(":", 1)[1])
    order = q1("SELECT * FROM orders WHERE id=?", (order_id,))
    
    if not order:
        await call.answer("Заявка не найдена или уже закрыта", show_alert=True)
        return
    
    # Check if already responded
    proposal = q1(
        "SELECT * FROM proposals WHERE order_id=? AND factory_id=?", 
        (order_id, call.from_user.id)
    )
    
    if proposal:
        await call.answer("Вы уже откликнулись на эту заявку", show_alert=True)
        return
    
    await state.update_data(order_id=order_id)
    await state.set_state(ProposalForm.price)
    await call.message.answer(
        f"Заявка #Z-{order_id}\n\n"
        f"Введите цену за изделие (₽):", 
        reply_markup=ReplyKeyboardRemove()
    )
    await call.answer()


@router.message(ProposalForm.price)
async def proposal_price(msg: Message, state: FSMContext) -> None:
    price = parse_digits(msg.text or "")
    if not price:
        await msg.answer("Укажите цену числом, например 550:")
        return
    
    await state.update_data(price=price)
    await state.set_state(ProposalForm.lead_time)
    await msg.answer("Срок производства (дней):")


@router.message(ProposalForm.lead_time)
async def proposal_lead_time(msg: Message, state: FSMContext) -> None:
    days = parse_digits(msg.text or "")
    if not days:
        await msg.answer("Укажите количество дней числом, например 30:")
        return
    
    await state.update_data(lead_time=days)
    await state.set_state(ProposalForm.sample_cost)
    await msg.answer("Стоимость образца (₽, или 0 если бесплатно):")


@router.message(ProposalForm.sample_cost)
async def proposal_sample_cost(msg: Message, state: FSMContext) -> None:
    cost = parse_digits(msg.text or "0")
    if cost is None:
        cost = 0
    
    data = await state.get_data()
    await state.clear()
    
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(text="Да", callback_data=f"confirm_proposal:{data['order_id']}:{data['price']}:{data['lead_time']}:{cost}")
        ]]
    )
    
    await msg.answer(
        f"Ваше предложение:\n"
        f"- Цена: {data['price']} ₽\n"
        f"- Срок: {data['lead_time']} дней\n"
        f"- Образец: {cost} ₽\n\n"
        f"Отправить?",
        reply_markup=kb
    )


@router.callback_query(F.data.startswith("confirm_proposal:"))
async def confirm_proposal(call: CallbackQuery) -> None:
    parts = call.data.split(":", 4)
    order_id = int(parts[1])
    price = int(parts[2])
    lead_time = int(parts[3])
    sample_cost = int(parts[4])
    
    # Insert proposal
    try:
        insert_and_get_id(
            """INSERT INTO proposals
            (order_id, factory_id, price, lead_time, sample_cost)
            VALUES(?, ?, ?, ?, ?)""",
            (order_id, call.from_user.id, price, lead_time, sample_cost)
        )
        
        # Get factory name
        factory = q1("SELECT name FROM factories WHERE tg_id=?", (call.from_user.id,))
        factory_name = factory["name"] if factory else f"Фабрика_{call.from_user.id}"
        
        # Get buyer information
        order = q1("SELECT buyer_id FROM orders WHERE id=?", (order_id,))
        if order:
            # Notify buyer about new proposal
            proposal_row = q1(
                """SELECT * FROM proposals 
                WHERE order_id=? AND factory_id=?""",
                (order_id, call.from_user.id)
            )
            
            kb = InlineKeyboardMarkup(
                inline_keyboard=[[
                    InlineKeyboardButton(
                        text="Выбрать фабрику", 
                        callback_data=f"choose_factory:{order_id}"
                    )
                ]]
            )
            
            asyncio.create_task(
                bot.send_message(
                    order["buyer_id"],
                    f"📬 Новое предложение на заказ #Z-{order_id}:\n\n" + 
                    proposal_caption(proposal_row, factory_name),
                    reply_markup=kb
                )
            )
        
        await call.message.edit_text("💌 Предложение отправлено заказчику!")
        await call.answer("Предложение успешно отправлено", show_alert=True)
    
    except Exception as e:
        logger.error("Error sending proposal: %s", e)
        await call.answer("Ошибка при отправке предложения", show_alert=True)


# ---------------------------------------------------------------------
# Deal management
# ---------------------------------------------------------------------

@router.callback_query(F.data.startswith("choose_factory:"))
async def choose_factory(call: CallbackQuery, state: FSMContext) -> None:
    order_id = int(call.data.split(":", 1)[1])
    
    # Get all proposals for this order
    proposals = q(
        """SELECT p.*, f.name as factory_name 
        FROM proposals p
        JOIN factories f ON p.factory_id = f.tg_id
        WHERE p.order_id = ?
        ORDER BY p.price ASC""",
        (order_id,)
    )
    
    if not proposals:
        await call.answer("Нет предложений для этого заказа", show_alert=True)
        return
    
    # Store order_id in state
    await state.update_data(order_id=order_id)
    await state.set_state(DealForm.choose_factory)
    
    # Create keyboard with all proposals
    buttons = []
    for p in proposals:
        buttons.append([
            InlineKeyboardButton(
                text=f"{p['factory_name']} - {p['price']}₽, {p['lead_time']} дн.",
                callback_data=f"select_factory:{p['factory_id']}:{p['price']}"
            )
        ])
    
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    await call.message.answer(
        f"Выберите фабрику для заказа #Z-{order_id}:",
        reply_markup=kb
    )
    await call.answer()


@router.callback_query(F.data.startswith("select_factory:"))
async def select_factory(call: CallbackQuery, state: FSMContext) -> None:
    parts = call.data.split(":", 2)
    factory_id = int(parts[1])
    price = int(parts[2])
    
    data = await state.get_data()
    order_id = data.get("order_id")
    
    if not order_id:
        await call.answer("Ошибка: заказ не найден", show_alert=True)
        return
    
    # Get order details
    order = q1("SELECT * FROM orders WHERE id=?", (order_id,))
    if not order:
        await call.answer("Заказ не найден", show_alert=True)
        return
    
    # Create deal
    deal_id = insert_and_get_id(
        """INSERT INTO deals
        (order_id, factory_id, buyer_id, amount, status)
        VALUES(?, ?, ?, ?, 'DRAFT')""",
        (order_id, factory_id, call.from_user.id, price * order["quantity"])
    )
    
    # Get factory info
    factory = q1("SELECT * FROM factories WHERE tg_id=?", (factory_id,))
    factory_name = factory["name"] if factory else f"Фабрика_{factory_id}"
    
    # Create payment for sample button
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(
                text="Подтвердить и оплатить образец", 
                callback_data=f"pay_sample:{deal_id}"
            )
        ]]
    )
    
    await call.message.edit_text(
        f"✅ Выбрана фабрика: {factory_name}\n\n"
        f"Заказ #Z-{order_id}\n"
        f"Цена за единицу: {price} ₽\n"
        f"Количество: {order['quantity']} шт.\n"
        f"Итого: {price * order['quantity']} ₽\n\n"
        f"Статус: {ORDER_STATUSES['DRAFT']}"
    )
    
    await call.message.answer(
        "Для продолжения необходимо заказать и оплатить образец:",
        reply_markup=kb
    )
    
    # Notify factory
    asyncio.create_task(
        bot.send_message(
            factory_id,
            f"🎉 Ваше предложение принято заказчиком!\n\n"
            f"Заказ #Z-{order_id}\n"
            f"Количество: {order['quantity']} шт.\n"
            f"Цена: {price} ₽/шт.\n"
            f"Сумма сделки: {price * order['quantity']} ₽\n\n"
            f"Статус: {ORDER_STATUSES['DRAFT']}"
        )
    )
    
    await state.clear()
    await call.answer()


@router.callback_query(F.data.startswith("pay_sample:"))
async def pay_sample(call: CallbackQuery) -> None:
    deal_id = int(call.data.split(":", 1)[1])
    
    # Update deal status (simulate payment)
    run(
        "UPDATE deals SET deposit_paid = 1 WHERE id = ?",
        (deal_id,)
    )
    
    deal = q1("SELECT * FROM deals WHERE id = ?", (deal_id,))
    if not deal:
        await call.answer("Сделка не найдена", show_alert=True)
        return
    
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(
                text="Образец получен, подтверждаю", 
                callback_data=f"confirm_sample:{deal_id}"
            )
        ]]
    )
    
    await call.message.edit_text(
        "💰 Оплата образца произведена!\n\n" +
        status_caption(deal) + "\n\n" +
        "Когда получите и одобрите образец, нажмите кнопку ниже:",
        reply_markup=kb
    )
    
    # Notify factory
    asyncio.create_task(
        bot.send_message(
            deal["factory_id"],
            f"💰 Заказчик оплатил образец для заказа #Z-{deal['order_id']}!\n\n"
            f"Пожалуйста, изготовьте и отправьте образец.\n"
            f"После подтверждения образца заказчиком, вы получите предоплату 30%."
        )
    )
    
    await call.answer("Оплата образца произведена", show_alert=True)


@router.callback_query(F.data.startswith("confirm_sample:"))
async def confirm_sample(call: CallbackQuery) -> None:
    deal_id = int(call.data.split(":", 1)[1])
    
    # Update deal status
    run(
        "UPDATE deals SET status = 'SAMPLE_PASS' WHERE id = ?",
        (deal_id,)
    )
    
    deal = q1("SELECT * FROM deals WHERE id = ?", (deal_id,))
    if not deal:
        await call.answer("Сделка не найдена", show_alert=True)
        return
    
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(
                text="Оплатить 30% предоплаты", 
                callback_data=f"pay_deposit:{deal_id}"
            )
        ]]
    )
    
    await call.message.edit_text(
        "✅ Образец подтвержден!\n\n" +
        status_caption(deal) + "\n\n" +
        "Для запуска производства необходимо внести 30% предоплаты:",
        reply_markup=kb
    )
    
    # Notify factory
    asyncio.create_task(
        bot.send_message(
            deal["factory_id"],
            f"✅ Заказчик одобрил образец для заказа #Z-{deal['order_id']}!\n\n"
            f"Статус: {deal['status']}\n"
            f"{ORDER_STATUSES[deal['status']]}"
        )
    )
    
    await call.answer("Образец подтвержден", show_alert=True)


@router.callback_query(F.data.startswith("pay_deposit:"))
async def pay_deposit(call: CallbackQuery) -> None:
    deal_id = int(call.data.split(":", 1)[1])
    
    # Update deal status (simulate payment)
    run(
        "UPDATE deals SET status = 'PRODUCTION', deposit_paid = 1 WHERE id = ?",
        (deal_id,)
    )
    
    deal = q1("SELECT * FROM deals WHERE id = ?", (deal_id,))
    if not deal:
        await call.answer("Сделка не найдена", show_alert=True)
        return
    
    await call.message.edit_text(
        "💰 Предоплата 30% произведена!\n\n" +
        status_caption(deal) + "\n\n" +
        "Фабрика приступила к производству. Мы уведомим вас о готовности партии."
    )
    
    # Notify factory to add tracking
    tracking_kb = InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(
                text="Добавить трек-номер отправления", 
                callback_data=f"add_tracking:{deal_id}"
            )
        ]]
    )
    
    asyncio.create_task(
        bot.send_message(
            deal["factory_id"],
            f"💰 Получена предоплата 30% для заказа #Z-{deal['order_id']}!\n\n"
            f"Статус: {deal['status']}\n"
            f"{ORDER_STATUSES[deal['status']]}\n\n"
            f"Когда партия будет готова к отправке, добавьте трек-номер:",
            reply_markup=tracking_kb
        )
    )
    
    await call.answer("Предоплата 30% произведена", show_alert=True)


@router.callback_query(F.data.startswith("add_tracking:"))
async def add_tracking(call: CallbackQuery, state: FSMContext) -> None:
    deal_id = int(call.data.split(":", 1)[1])
    
    await state.update_data(deal_id=deal_id)
    await state.set_state(TrackingForm.tracking_num)
    
    await call.message.answer(
        "Введите трек-номер отправления:",
        reply_markup=ReplyKeyboardRemove()
    )
    await call.answer()


@router.message(TrackingForm.tracking_num)
async def tracking_num(msg: Message, state: FSMContext) -> None:
    if not msg.text:
        await msg.answer("Пожалуйста, введите трек-номер:")
        return
    
    await state.update_data(tracking_num=msg.text.strip())
    await state.set_state(TrackingForm.eta)
    await msg.answer("Укажите ожидаемую дату доставки (дд.мм.гггг):")


@router.message(TrackingForm.eta)
async def tracking_eta(msg: Message, state: FSMContext) -> None:
    if not msg.text:
        await msg.answer("Пожалуйста, укажите дату:")
        return
    
    data = await state.get_data()
    deal_id = data.get("deal_id")
    tracking_num = data.get("tracking_num")
    
    # Update deal
    run(
        """UPDATE deals 
        SET status = 'READY_TO_SHIP', tracking_num = ?, eta = ?
        WHERE id = ?""",
        (tracking_num, msg.text.strip(), deal_id)
    )
    
    deal = q1("SELECT * FROM deals WHERE id = ?", (deal_id,))
    if not deal:
        await msg.answer("Ошибка: сделка не найдена")
        await state.clear()
        return
    
    await msg.answer(
        f"✅ Информация об отправке добавлена!\n\n" +
        status_caption(deal) + "\n\n" +
        "Заказчик получил уведомление о готовности груза.",
        reply_markup=kb_factory_menu()
    )
    
    # Notify buyer
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(
                text="Оплатить оставшиеся 70%", 
                callback_data=f"pay_final:{deal_id}"
            )
        ]]
    )
    
    asyncio.create_task(
        bot.send_message(
            deal["buyer_id"],
            f"📦 Заказ #Z-{deal['order_id']} готов к отправке!\n\n" +
            status_caption(deal) + "\n\n" +
            "Для отправки необходимо оплатить оставшиеся 70% суммы:",
            reply_markup=kb
        )
    )
    
    await state.clear()


@router.callback_query(F.data.startswith("pay_final:"))
async def pay_final(call: CallbackQuery) -> None:
    deal_id = int(call.data.split(":", 1)[1])
    
    # Update deal status (simulate payment)
    run(
        "UPDATE deals SET status = 'IN_TRANSIT', final_paid = 1 WHERE id = ?",
        (deal_id,)
    )
    
    deal = q1("SELECT * FROM deals WHERE id = ?", (deal_id,))
    if not deal:
        await call.answer("Сделка не найдена", show_alert=True)
        return
    
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(
                text="Подтвердить получение", 
                callback_data=f"confirm_delivery:{deal_id}"
            )
        ]]
    )
    
    await call.message.edit_text(
        "💰 Оплата оставшихся 70% произведена!\n\n" +
        status_caption(deal) + "\n\n" +
        "Груз в пути. Когда получите заказ, подтвердите доставку:",
        reply_markup=kb
    )
    
    # Notify factory
    asyncio.create_task(
        bot.send_message(
            deal["factory_id"],
            f"💰 Заказчик оплатил оставшиеся 70% для заказа #Z-{deal['order_id']}!\n\n" +
            status_caption(deal) + "\n\n" +
            "Заказ в пути. Escrow будет разблокирован после подтверждения получения."
        )
    )
    
    await call.answer("Оплата произведена", show_alert=True)


@router.callback_query(F.data.startswith("confirm_delivery:"))
async def confirm_delivery(call: CallbackQuery) -> None:
    deal_id = int(call.data.split(":", 1)[1])
    
    # Update deal status
    run(
        "UPDATE deals SET status = 'DELIVERED' WHERE id = ?",
        (deal_id,)
    )
    
    deal = q1("SELECT * FROM deals WHERE id = ?", (deal_id,))
    if not deal:
        await call.answer("Сделка не найдена", show_alert=True)
        return
    
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(
                text="Оставить отзыв о фабрике", 
                callback_data=f"rate_factory:{deal_id}"
            )
        ]]
    )
    
    await call.message.edit_text(
        "✅ Доставка подтверждена!\n\n" +
        status_caption(deal) + "\n\n" +
        "Escrow разблокирован, средства перечислены фабрике.",
        reply_markup=kb
    )
    
    # Notify factory
    asyncio.create_task(
        bot.send_message(
            deal["factory_id"],
            f"✅ Заказчик подтвердил получение заказа #Z-{deal['order_id']}!\n\n" +
            status_caption(deal) + "\n\n" +
            "Escrow разблокирован, средства перечислены на ваш счет."
        )
    )
    
    await call.answer("Доставка подтверждена", show_alert=True)


@router.callback_query(F.data.startswith("rate_factory:"))
async def rate_factory(call: CallbackQuery, state: FSMContext) -> None:
    deal_id = int(call.data.split(":", 1)[1])
    
    deal = q1("SELECT * FROM deals WHERE id = ?", (deal_id,))
    if not deal:
        await call.answer("Сделка не найдена", show_alert=True)
        return
    
    # Set state for rating
    await state.update_data(deal_id=deal_id, factory_id=deal["factory_id"])
    await state.set_state(DealForm.rate_factory)
    
    # Create rating keyboard
    buttons = []
    for i in range(1, 6):
        stars = "⭐" * i
        buttons.append([
            InlineKeyboardButton(
                text=stars, 
                callback_data=f"rating:{i}"
            )
        ])
    
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    factory = q1("SELECT name FROM factories WHERE tg_id = ?", (deal["factory_id"],))
    factory_name = factory["name"] if factory else f"Фабрика_{deal['factory_id']}"
    
    await call.message.answer(
        f"Оцените работу фабрики «{factory_name}»:",
        reply_markup=kb
    )
    await call.answer()


@router.callback_query(F.data.startswith("rating:"), DealForm.rate_factory)
async def process_rating(call: CallbackQuery, state: FSMContext) -> None:
    rating = int(call.data.split(":", 1)[1])
    
    await state.update_data(rating=rating)
    await call.message.answer(
        f"Спасибо за оценку: {'⭐' * rating}\n\n"
        f"Добавьте комментарий или напишите «skip»:"
    )
    await state.set_state(DealForm.rate_factory)  # Keep the same state but wait for comment
    await call.answer()


@router.message(DealForm.rate_factory)
async def rating_comment(msg: Message, state: FSMContext) -> None:
    comment = msg.text.strip() if msg.text else ""
    if comment.lower() == "skip":
        comment = ""
    
    data = await state.get_data()
    deal_id = data.get("deal_id")
    factory_id = data.get("factory_id")
    rating = data.get("rating", 5)  # Default to 5 stars
    
    # Save rating
    run(
        """INSERT INTO ratings
        (deal_id, factory_id, buyer_id, rating, comment)
        VALUES(?, ?, ?, ?, ?)""",
        (deal_id, factory_id, msg.from_user.id, rating, comment)
    )
    
    # Update factory rating
    ratings = q(
        """SELECT AVG(rating) as avg_rating, COUNT(*) as count
        FROM ratings WHERE factory_id = ?""",
        (factory_id,)
    )
    
    if ratings and ratings[0]["count"] > 0:
        run(
            """UPDATE factories 
            SET rating = ?, rating_count = ? 
            WHERE tg_id = ?""",
            (ratings[0]["avg_rating"], ratings[0]["count"], factory_id)
        )
    
    await msg.answer(
        "✅ Спасибо за отзыв! Он поможет другим заказчикам выбрать надежную фабрику.",
        reply_markup=kb_buyer_menu()
    )
    
    # Notify factory about new rating
    factory = q1("SELECT * FROM factories WHERE tg_id = ?", (factory_id,))
    if factory:
        rating_text = f"{'⭐' * rating} ({rating}/5)"
        comment_text = f"\n«{comment}»" if comment else ""
        
        asyncio.create_task(
            bot.send_message(
                factory_id,
                f"📊 Новый отзыв по заказу #Z-{data.get('order_id')}!\n\n"
                f"Оценка: {rating_text}{comment_text}\n\n"
                f"Ваш текущий рейтинг: {factory['rating']:.1f}/5.0 "
                f"({factory['rating_count']} отзывов)"
            )
        )
    
    await state.clear()


# ---------------------------------------------------------------------
# Status commands
# ---------------------------------------------------------------------

@router.message(F.text == "⏱ Статус заказов")
async def cmd_order_status(msg: Message) -> None:
    # Check if user is factory
    factory_deals = q(
        """SELECT d.* FROM deals d
        WHERE d.factory_id = ?
        ORDER BY d.created_at DESC""",
        (msg.from_user.id,)
    )
    
    # Check if user is buyer
    buyer_deals = q(
        """SELECT d.* FROM deals d
        WHERE d.buyer_id = ?
        ORDER BY d.created_at DESC""",
        (msg.from_user.id,)
    )
    
    if factory_deals:
        # Show factory deals
        if len(factory_deals) > 0:
            await msg.answer(
                "<b>Статус ваших заказов (фабрика):</b>",
                reply_markup=kb_factory_menu()
            )
            
            # Show last 5 deals
            for deal in factory_deals[:5]:
                kb = None
                
                # Add action buttons based on status
                if deal["status"] == "PRODUCTION":
                    kb = InlineKeyboardMarkup(
                        inline_keyboard=[[
                            InlineKeyboardButton(
                                text="Добавить трек-номер", 
                                callback_data=f"add_tracking:{deal['id']}"
                            )
                        ]]
                    )
                
                await msg.answer(status_caption(deal), reply_markup=kb)
            
            if len(factory_deals) > 5:
                await msg.answer(f"... и еще {len(factory_deals) - 5} заказов")
        else:
            await msg.answer(
                "У вас пока нет активных заказов.",
                reply_markup=kb_factory_menu()
            )
    
    elif buyer_deals:
        # Show buyer deals
        if len(buyer_deals) > 0:
            await msg.answer(
                "<b>Статус ваших заказов (заказчик):</b>",
                reply_markup=kb_buyer_menu()
            )
            
            # Show last 5 deals
            for deal in buyer_deals[:5]:
                kb = None
                
                # Add action buttons based on status
                if deal["status"] == "SAMPLE_PASS" and not deal["deposit_paid"]:
                    kb = InlineKeyboardMarkup(
                        inline_keyboard=[[
                            InlineKeyboardButton(
                                text="Оплатить 30% предоплаты", 
                                callback_data=f"pay_deposit:{deal['id']}"
                            )
                        ]]
                    )
                elif deal["status"] == "READY_TO_SHIP" and not deal["final_paid"]:
                    kb = InlineKeyboardMarkup(
                        inline_keyboard=[[
                            InlineKeyboardButton(
                                text="Оплатить оставшиеся 70%", 
                                callback_data=f"pay_final:{deal['id']}"
                            )
                        ]]
                    )
                elif deal["status"] == "IN_TRANSIT":
                    kb = InlineKeyboardMarkup(
                        inline_keyboard=[[
                            InlineKeyboardButton(
                                text="Подтвердить получение", 
                                callback_data=f"confirm_delivery:{deal['id']}"
                            )
                        ]]
                    )
                elif deal["status"] == "DELIVERED":
                    # Check if already rated
                    rating = q1(
                        """SELECT 1 FROM ratings 
                        WHERE deal_id = ? AND buyer_id = ?""",
                        (deal["id"], msg.from_user.id)
                    )
                    
                    if not rating:
                        kb = InlineKeyboardMarkup(
                            inline_keyboard=[[
                                InlineKeyboardButton(
                                    text="Оставить отзыв", 
                                    callback_data=f"rate_factory:{deal['id']}"
                                )
                            ]]
                        )
                
                await msg.answer(status_caption(deal), reply_markup=kb)
            
            if len(buyer_deals) > 5:
                await msg.answer(f"... и еще {len(buyer_deals) - 5} заказов")
        else:
            await msg.answer(
                "У вас пока нет активных заказов.",
                reply_markup=kb_buyer_menu()
            )
    else:
        # User not identified
        await msg.answer(
            "У вас пока нет активных заказов.",
            reply_markup=kb_main()
        )


# ---------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------

async def on_startup(bot: Bot) -> None:
    """Run on bot startup."""
    init_db()
    logger.info("Bot startup complete ✅")


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
