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

    # Открываем соединение с БД
    with sqlite3.connect("db.sqlite3") as conn:
        cursor = conn.cursor()

        # Добавляем заказ и получаем ID
        order_id = insert_and_get_id(
            """INSERT INTO orders
                (buyer_id, category, quantity, budget, destination, lead_time, file_id, paid)
               VALUES (?, ?, ?, ?, ?, ?, ?, 1);""",
            (
                call.from_user.id,
                data["category"],
                data["quantity"],
                data["budget"],
                data["destination"],
                data["lead_time"],
                data.get("file_id"),
            ),
            cursor
        )
        conn.commit()

    await state.clear()

    # Получаем заказ для рассылки
    order = q1("SELECT * FROM orders WHERE id=?", (order_id,))

    # Рассылаем фабрикам
    notify_factories(order)

    # Подтверждение заказчику
    await call.message.edit_text(f"👍 Заявка #Z-{order_id} создана! Ожидайте первые предложения в течение 24 ч.")
    await bot.send_message(call.from_user.id, "Меню заказчика:", reply_markup=kb_buyer_menu())
    await call.answer()



# ---------------------------------------------------------------------------
#  Order listings (Factory)
# ---------------------------------------------------------------------------

@router.message(F.text == "📂 Заявки")
async def factory_orders(msg: Message) -> None:
    # Check if factory is PRO
    factory = q1("SELECT * FROM factories WHERE tg_id=? AND is_pro=1", (msg.from_user.id,))
    if not factory:
        await msg.answer("Доступно только для PRO-фабрик", reply_markup=kb_factory_menu())
        return
    
    # Get orders matching factory profile
    orders = q(
        """SELECT * FROM orders 
           WHERE paid = 1 
           AND quantity >= ? 
           AND budget >= ?
           AND (','||?||',') LIKE ('%,'||category||',%')
           AND id NOT IN (SELECT order_id FROM proposals WHERE factory_id=?)
           ORDER BY created_at DESC LIMIT 10""",
        (
            factory["min_qty"], 
            factory["avg_price"], 
            factory["categories"], 
            msg.from_user.id
        )
    )
    
    if not orders:
        await msg.answer("На данный момент нет подходящих заявок", reply_markup=kb_factory_menu())
        return
    
    for order in orders:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton("Откликнуться", callback_data=f"lead:{order['id']}")]
        ])
        
        await msg.answer(order_caption(order), reply_markup=kb)


@router.callback_query(F.data.startswith("lead:"))
async def factory_lead(call: CallbackQuery, state: FSMContext) -> None:
    order_id = int(call.data.split(":", 1)[1])
    order = q1("SELECT * FROM orders WHERE id=?", (order_id,))
    
    if not order:
        await call.answer("Заказ не найден")
        return
    
    # Check if factory already submitted proposal
    proposal = q1(
        "SELECT * FROM proposals WHERE order_id=? AND factory_id=?", 
        (order_id, call.from_user.id)
    )
    
    if proposal:
        await call.answer("Вы уже откликались на эту заявку")
        return
    
    await state.set_state(ProposalForm.price)
    await state.update_data(order_id=order_id)
    await call.message.answer("Введите цену за изделие:")
    await call.answer()


@router.message(ProposalForm.price)
async def proposal_price(msg: Message, state: FSMContext) -> None:
    price = parse_digits(msg.text or "")
    if not price:
        await msg.answer("Укажите число:")
        return
    
    await state.update_data(price=price)
    await state.set_state(ProposalForm.lead_time)
    await msg.answer("Срок производства (дней):")


@router.message(ProposalForm.lead_time)
async def proposal_lead_time(msg: Message, state: FSMContext) -> None:
    days = parse_digits(msg.text or "")
    if not days:
        await msg.answer("Укажите число дней:")
        return
    
    await state.update_data(lead_time=days)
    await state.set_state(ProposalForm.sample_cost)
    await msg.answer("Стоимость образца:")


@router.message(ProposalForm.sample_cost)
async def proposal_sample_cost(msg: Message, state: FSMContext) -> None:
    cost = parse_digits(msg.text or "0") or 0
    await state.update_data(sample_cost=cost)
    
    data = await state.get_data()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton("Да", callback_data=f"confirm_proposal:{data['order_id']}")]
    ])
    await msg.answer(
        f"Предложение:\nЦена: {data['price']} ₽\nСрок: {data['lead_time']} дн.\nОбразец: {data['sample_cost']} ₽\n\n"
        f"Отправить предложение?", 
        reply_markup=kb
    )


@router.callback_query(F.data.startswith("confirm_proposal:"))
async def confirm_proposal(call: CallbackQuery, state: FSMContext) -> None:
    order_id = int(call.data.split(":", 1)[1])
    data = await state.get_data()
    
    # Save proposal
    insert_and_get_id(
        """INSERT INTO proposals 
               (order_id, factory_id, price, lead_time, sample_cost) 
           VALUES(?, ?, ?, ?, ?);""",
        (order_id, call.from_user.id, data["price"], data["lead_time"], data["sample_cost"])
    )
    
    # Get buyer ID to notify
    order = q1("SELECT * FROM orders WHERE id=?", (order_id,))
    if order:
        # Get factory name
        factory = q1("SELECT * FROM factories WHERE tg_id=?", (call.from_user.id,))
        factory_name = factory["name"] if factory else "Фабрика"
        
        # Send notification to buyer
        await bot.send_message(
            order["buyer_id"],
            f"📬 Новое предложение для заказа #Z-{order_id}\n"
            f"От: {factory_name}\n"
            f"Цена: {data['price']} ₽\n"
            f"Срок: {data['lead_time']} дн.\n"
            f"Стоимость образца: {data['sample_cost']} ₽",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton("Выбрать фабрику", callback_data=f"choose_factory:{order_id}")]
            ])
        )
    
    await state.clear()
    await call.message.edit_text("💌 Предложение отправлено заказчику!")
    await call.answer()


# ---------------------------------------------------------------------------
#  Order listings (Buyer)
# ---------------------------------------------------------------------------

@router.message(Command("myorders"))
@router.message(F.text == "📋 Мои заказы")
async def buyer_orders(msg: Message) -> None:
    orders = q(
        "SELECT * FROM orders WHERE buyer_id=? ORDER BY created_at DESC", 
        (msg.from_user.id,)
    )
    
    if not orders:
        await msg.answer("У вас пока нет заказов", reply_markup=kb_buyer_menu())
        return
    
    for order in orders:
        # Count proposals for this order
        proposals = q(
            "SELECT COUNT(*) as count FROM proposals WHERE order_id=?", 
            (order["id"],)
        )
        proposal_count = proposals[0]["count"] if proposals else 0
        
        # Check if there's an active deal
        deal = q1(
            "SELECT * FROM deals WHERE order_id=? AND buyer_id=?", 
            (order["id"], msg.from_user.id)
        )
        
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                f"Просмотреть предложения ({proposal_count})", 
                callback_data=f"view_proposals:{order['id']}"
            )]
        ])
        
        status_text = ""
        if deal:
            status_text = f"\nСтатус: {deal['status']}"
            if deal["status"] in ORDER_STATUSES:
                status_text += f"\n{ORDER_STATUSES[deal['status']]}"
        
        await msg.answer(
            f"<b>Заказ #Z-{order['id']}</b> ({order['created_at'][:10]})\n"
            f"Категория: {order['category']}\n"
            f"Тираж: {order['quantity']} шт.\n"
            f"Бюджет: {order['budget']} ₽{status_text}",
            reply_markup=kb
        )


@router.callback_query(F.data.startswith("view_proposals:"))
async def view_proposals(call: CallbackQuery) -> None:
    order_id = int(call.data.split(":", 1)[1])
    proposals = q(
        """SELECT p.*, f.name as factory_name, f.rating, f.rating_count
           FROM proposals p 
           JOIN factories f ON p.factory_id = f.tg_id
           WHERE p.order_id=?
           ORDER BY p.created_at DESC""",
        (order_id,)
    )
    
    if not proposals:
        await call.answer("Пока нет предложений от фабрик")
        return
    
    # Check if there's an active deal for this order
    deal = q1(
        "SELECT * FROM deals WHERE order_id=? AND buyer_id=?", 
        (order_id, call.from_user.id)
    )
    
    if deal:
        await call.message.answer(
            "У вас уже есть активная сделка по этому заказу. "
            "Текущий статус: " + deal["status"]
        )
        await call.answer()
        return
    
    text = f"<b>Предложения для заказа #Z-{order_id}</b>\n\n"
    
    for i, p in enumerate(proposals, 1):
        rating_text = f"{p['rating']:.1f}/5.0 ({p['rating_count']})" if p["rating_count"] > 0 else "Нет отзывов"
        text += (
            f"{i}. <b>{p['factory_name']}</b>\n"
            f"   Цена: {p['price']} ₽, срок: {p['lead_time']} дн.\n"
            f"   Образец: {p['sample_cost']} ₽\n"
            f"   Рейтинг: {rating_text}\n\n"
        )
        
        # Create inline keyboard for each proposal
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                "Выбрать эту фабрику", 
                callback_data=f"select_factory:{order_id}:{p['factory_id']}"
            )]
        ])
        
        await call.message.answer(
            f"<b>Предложение #{i} - {p['factory_name']}</b>\n"
            f"Цена: {p['price']} ₽\n"
            f"Срок: {p['lead_time']} дн.\n"
            f"Образец: {p['sample_cost']} ₽\n"
            f"Рейтинг: {rating_text}",
            reply_markup=kb
        )
    
    await call.answer()


@router.callback_query(F.data.startswith("select_factory:"))
async def select_factory(call: CallbackQuery, state: FSMContext) -> None:
    parts = call.data.split(":", 2)
    order_id = int(parts[1])
    factory_id = int(parts[2])
    
    # Get proposal details
    proposal = q1(
        "SELECT * FROM proposals WHERE order_id=? AND factory_id=?", 
        (order_id, factory_id)
    )
    
    if not proposal:
        await call.answer("Предложение не найдено")
        return
    
    # Get order details
    order = q1("SELECT * FROM orders WHERE id=?", (order_id,))
    if not order:
        await call.answer("Заказ не найден")
        return
    
    # Get factory details
    factory = q1("SELECT * FROM factories WHERE tg_id=?", (factory_id,))
    if not factory:
        await call.answer("Фабрика не найдена")
        return
    
    # Create deal
    deal_id = insert_and_get_id(
        """INSERT INTO deals
               (order_id, factory_id, buyer_id, amount, status)
           VALUES(?, ?, ?, ?, 'DRAFT');""",
        (order_id, factory_id, call.from_user.id, proposal["price"] * order["quantity"])
    )
    
    # Send notifications to both parties
    await bot.send_message(
        factory_id,
        f"🎉 Ваше предложение по заказу #Z-{order_id} принято!\n"
        f"Сделка #{deal_id} создана. Статус: DRAFT\n"
        f"{ORDER_STATUSES['DRAFT']}"
    )
    
    # Setup sample payment if needed
    payment_text = ""
    if proposal["sample_cost"] > 0:
        payment_text = f"\n\nОплатите образец ({proposal['sample_cost']} ₽):"
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                f"Оплатить образец {proposal['sample_cost']} ₽", 
                callback_data=f"pay_sample:{deal_id}"
            )]
        ])
    else:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                "Подтвердить образец", 
                callback_data=f"approve_sample:{deal_id}"
            )]
        ])
    
    await call.message.edit_text(
        f"✅ Вы выбрали фабрику {factory['name']}!\n"
        f"Сделка #{deal_id} создана. Статус: DRAFT\n"
        f"{ORDER_STATUSES['DRAFT']}{payment_text}",
        reply_markup=kb
    )
    await call.answer()


# ---------------------------------------------------------------------------
#  Escrow system & deal tracking
# ---------------------------------------------------------------------------

@router.message(Command("status"))
@router.message(F.text == "⏱ Статус заказов")
async def show_deals(msg: Message) -> None:
    # Check if user is a factory
    factory_deals = q(
        "SELECT * FROM deals WHERE factory_id=? ORDER BY created_at DESC", 
        (msg.from_user.id,)
    )
    
    # Check if user is a buyer
    buyer_deals = q(
        "SELECT * FROM deals WHERE buyer_id=? ORDER BY created_at DESC", 
        (msg.from_user.id,)
    )
    
    if not factory_deals and not buyer_deals:
        await msg.answer("У вас пока нет активных сделок")
        return
    
    # Show factory deals
    for deal in factory_deals:
        kb = None
        if deal["status"] == "SAMPLE_PASS":
            # Factory is waiting for deposit
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(
                    "Обновить статус производства", 
                    callback_data=f"production_update:{deal['id']}"
                )]
            ])
        elif deal["status"] == "PRODUCTION":
            # Factory should upload tracking
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(
                    "Загрузить трек-номер", 
                    callback_data=f"add_tracking:{deal['id']}"
                )]
            ])
        
        await msg.answer(status_caption(deal), reply_markup=kb)
    
    # Show buyer deals
    for deal in buyer_deals:
        kb = None
        if deal["status"] == "DRAFT" and deal["deposit_paid"] == 0:
            # Buyer needs to pay sample or approve it
            proposal = q1(
                "SELECT * FROM proposals WHERE order_id=? AND factory_id=?", 
                (deal["order_id"], deal["factory_id"])
            )
            if proposal and proposal["sample_cost"] > 0:
                kb = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(
                        f"Оплатить образец {proposal['sample_cost']} ₽", 
                        callback_data=f"pay_sample:{deal['id']}"
                    )]
                ])
            else:
                kb = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(
                        "Подтвердить образец", 
                        callback_data=f"approve_sample:{deal['id']}"
                    )]
                ])
        elif deal["status"] == "SAMPLE_PASS" and deal["deposit_paid"] == 0:
            # Buyer needs to pay deposit
            deposit_amount = int(deal["amount"] * 0.3)
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(
                    f"Оплатить депозит {deposit_amount} ₽ (30%)", 
                    callback_data=f"pay_deposit:{deal['id']}"
                )]
            ])
        elif deal["status"] == "READY_TO_SHIP" and deal["final_paid"] == 0:
            # Buyer needs to pay final amount
            final_amount = int(deal["amount"] * 0.7)
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(
                    f"Оплатить остаток {final_amount} ₽ (70%)", 
                    callback_data=f"pay_final:{deal['id']}"
                )]
            ])
        elif deal["status"] == "DELIVERED" and deal["factory_id"]:
            # Buyer should rate the factory
            factory = q1("SELECT name FROM factories WHERE tg_id=?", (deal["factory_id"],))
            factory_name = factory["name"] if factory else "Фабрика"
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(
                    f"Оценить фабрику {factory_name}", 
                    callback_data=f"rate_factory:{deal['id']}"
                )]
            ])
        
        await msg.answer(status_caption(deal), reply_markup=kb)


@router.callback_query(F.data.startswith("pay_sample:"))
async def pay_sample(call: CallbackQuery) -> None:
    deal_id = int(call.data.split(":", 1)[1])
    
    # Simulate payment success
    await call.message.edit_text(
        f"✅ Образец оплачен! Ожидайте подтверждения и фото QC."
    )
    
    # Get deal info
    deal = q1("SELECT * FROM deals WHERE id=?", (deal_id,))
    if deal:
        # Notify factory
        await bot.send_message(
            deal["factory_id"],
            f"💰 Заказчик оплатил образец по сделке #{deal_id}. "
            f"Пожалуйста, произведите образец и загрузите фото."
        )
    
    await call.answer()


@router.callback_query(F.data.startswith("approve_sample:"))
async def approve_sample(call: CallbackQuery) -> None:
    deal_id = int(call.data.split(":", 1)[1])
    
    # Update deal status
    run(
        "UPDATE deals SET status='SAMPLE_PASS' WHERE id=?", 
        (deal_id,)
    )
    
    # Get deal info
    deal = q1("SELECT * FROM deals WHERE id=?", (deal_id,))
    if not deal:
        await call.answer("Сделка не найдена")
        return
    
    # Calculate deposit amount
    deposit_amount = int(deal["amount"] * 0.3)
    
    # Notify factory
    await bot.send_message(
        deal["factory_id"],
        f"✅ Образец одобрен заказчиком по сделке #{deal_id}!\n"
        f"Статус обновлен: SAMPLE_PASS\n"
        f"Ожидаем оплату депозита 30% ({deposit_amount} ₽)"
    )
    
    # Show payment button to buyer
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            f"Оплатить депозит {deposit_amount} ₽ (30%)", 
            callback_data=f"pay_deposit:{deal_id}"
        )]
    ])
    
    await call.message.edit_text(
        f"✅ Образец одобрен! Статус: SAMPLE_PASS\n"
        f"{ORDER_STATUSES['SAMPLE_PASS']}",
        reply_markup=kb
    )
    await call.answer()


@router.callback_query(F.data.startswith("pay_deposit:"))
async def pay_deposit(call: CallbackQuery) -> None:
    deal_id = int(call.data.split(":", 1)[1])
    
    # Simulate payment and update deal
    run(
        "UPDATE deals SET deposit_paid=1, status='PRODUCTION' WHERE id=?", 
        (deal_id,)
    )
    
    # Get deal info
    deal = q1("SELECT * FROM deals WHERE id=?", (deal_id,))
    if not deal:
        await call.answer("Сделка не найдена")
        return
    
    # Notify factory
    await bot.send_message(
        deal["factory_id"],
        f"💰 Заказчик оплатил депозит 30% по сделке #{deal_id}!\n"
        f"Статус обновлен: PRODUCTION\n"
        f"{ORDER_STATUSES['PRODUCTION']}\n\n"
        f"Пожалуйста, начните производство."
    )
    
    await call.message.edit_text(
        f"✅ Депозит оплачен! Статус: PRODUCTION\n"
        f"{ORDER_STATUSES['PRODUCTION']}"
    )
    await call.answer()


@router.callback_query(F.data.startswith("production_update:"))
async def production_update(call: CallbackQuery) -> None:
    deal_id = int(call.data.split(":", 1)[1])
    
    # Update deal status
    run(
        "UPDATE deals SET status='READY_TO_SHIP' WHERE id=?", 
        (deal_id,)
    )
    
    # Get deal info
    deal = q1("SELECT * FROM deals WHERE id=?", (deal_id,))
    if not deal:
        await call.answer("Сделка не найдена")
        return
    
    # Show tracking form
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            "Загрузить трек-номер", 
            callback_data=f"add_tracking:{deal_id}"
        )]
    ])
    
    await call.message.edit_text(
        f"Статус обновлен: READY_TO_SHIP\n"
        f"{ORDER_STATUSES['READY_TO_SHIP']}",
        reply_markup=kb
    )
    
    # Notify buyer
    final_amount = int(deal["amount"] * 0.7)
    buyer_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            f"Оплатить остаток {final_amount} ₽ (70%)", 
            callback_data=f"pay_final:{deal_id}"
        )]
    ])
    
    await bot.send_message(
        deal["buyer_id"],
        f"📦 Заказ по сделке #{deal_id} готов к отгрузке!\n"
        f"Статус: READY_TO_SHIP\n"
        f"{ORDER_STATUSES['READY_TO_SHIP']}",
        reply_markup=buyer_kb
    )
    
    await call.answer()


@router.callback_query(F.data.startswith("add_tracking:"))
async def add_tracking_cmd(call: CallbackQuery, state: FSMContext) -> None:
    deal_id = int(call.data.split(":", 1)[1])
    await state.update_data(deal_id=deal_id)
    await state.set_state(TrackingForm.tracking_num)
    await call.message.answer("Введите трек-номер отправления:")
    await call.answer()


@router.message(TrackingForm.tracking_num)
async def add_tracking_num(msg: Message, state: FSMContext) -> None:
    if not msg.text:
        await msg.answer("Введите номер отслеживания:")
        return
    
    await state.update_data(tracking_num=msg.text.strip())
    await state.set_state(TrackingForm.eta)
    await msg.answer("Укажите примерную дату доставки (например, 15.07.2025):")


@router.message(TrackingForm.eta)
async def add_tracking_eta(msg: Message, state: FSMContext) -> None:
    if not msg.text:
        await msg.answer("Введите дату:")
        return
    
    data = await state.get_data()
    
    # Update deal with tracking info
    run(
        "UPDATE deals SET tracking_num=?, eta=? WHERE id=?", 
        (data["tracking_num"], msg.text.strip(), data["deal_id"])
    )
    
    # Get deal info
    deal = q1("SELECT * FROM deals WHERE id=?", (data["deal_id"],))
    if not deal:
        await msg.answer("Сделка не найдена")
        await state.clear()
        return
    
    # Notify buyer
    await bot.send_message(
        deal["buyer_id"],
        f"🚚 Фабрика добавила информацию об отгрузке по сделке #{deal['id']}:\n"
        f"Трек-номер: {data['tracking_num']}\n"
        f"Ожидаемая дата доставки: {msg.text.strip()}\n\n"
        f"Пожалуйста, оплатите остаток суммы для завершения сделки."
    )
    
    await msg.answer(f"✅ Информация об отправке добавлена в сделку #{data['deal_id']}")
    await state.clear()


@router.callback_query(F.data.startswith("pay_final:"))
async def pay_final(call: CallbackQuery) -> None:
    deal_id = int(call.data.split(":", 1)[1])
    
    # Simulate payment and update deal
    run(
        "UPDATE deals SET final_paid=1, status='IN_TRANSIT' WHERE id=?", 
        (deal_id,)
    )
    
    # Get deal info
    deal = q1("SELECT * FROM deals WHERE id=?", (deal_id,))
    if not deal:
        await call.answer("Сделка не найдена")
        return
    
    # Notify factory
    await bot.send_message(
        deal["factory_id"],
        f"💰 Заказчик оплатил остаток 70% по сделке #{deal_id}!\n"
        f"Статус обновлен: IN_TRANSIT\n"
        f"{ORDER_STATUSES['IN_TRANSIT']}\n\n"
        f"Escrow разблокирован. Средства поступят на ваш счет."
    )
    
    await call.message.edit_text(
        f"✅ Финальный платеж выполнен! Статус: IN_TRANSIT\n"
        f"{ORDER_STATUSES['IN_TRANSIT']}\n"
        f"Трек-номер: {deal['tracking_num'] or 'Не указан'}\n"
        f"ETA: {deal['eta'] or 'Не указана'}"
    )
    
    # Add button to confirm delivery
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            "Подтвердить получение товара", 
            callback_data=f"confirm_delivery:{deal_id}"
        )]
    ])
    
    await bot.send_message(
        deal["buyer_id"],
        f"Когда вы получите товар, пожалуйста, подтвердите доставку:",
        reply_markup=kb
    )
    
    await call.answer()


@router.callback_query(F.data.startswith("confirm_delivery:"))
async def confirm_delivery(call: CallbackQuery) -> None:
    deal_id = int(call.data.split(":", 1)[1])
    
    # Update deal status
    run(
        "UPDATE deals SET status='DELIVERED' WHERE id=?", 
        (deal_id,)
    )
    
    # Get deal info
    deal = q1("SELECT * FROM deals WHERE id=?", (deal_id,))
    if not deal:
        await call.answer("Сделка не найдена")
        return
    
    # Get factory name
    factory = q1("SELECT name FROM factories WHERE tg_id=?", (deal["factory_id"],))
    factory_name = factory["name"] if factory else "Фабрика"
    
    # Notify factory
    await bot.send_message(
        deal["factory_id"],
        f"🎉 Заказчик подтвердил получение товара по сделке #{deal_id}!\n"
        f"Статус обновлен: DELIVERED\n"
        f"{ORDER_STATUSES['DELIVERED']}\n\n"
        f"Сделка успешно завершена."
    )
    
    # Show rating keyboard to buyer
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            f"Оценить фабрику {factory_name}", 
            callback_data=f"rate_factory:{deal_id}"
        )]
    ])
    
    await call.message.edit_text(
        f"✅ Доставка подтверждена! Статус: DELIVERED\n"
        f"{ORDER_STATUSES['DELIVERED']}",
        reply_markup=kb
    )
    await call.answer()


@router.callback_query(F.data.startswith("rate_factory:"))
async def rate_factory_cmd(call: CallbackQuery, state: FSMContext) -> None:
    deal_id = int(call.data.split(":", 1)[1])
    
    # Get deal info
    deal = q1("SELECT * FROM deals WHERE id=?", (deal_id,))
    if not deal:
        await call.answer("Сделка не найдена")
        return
    
    await state.update_data(deal_id=deal_id, factory_id=deal["factory_id"])
    
    # Create rating keyboard
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton("⭐", callback_data="rate:1"),
            InlineKeyboardButton("⭐⭐", callback_data="rate:2"),
            InlineKeyboardButton("⭐⭐⭐", callback_data="rate:3"),
            InlineKeyboardButton("⭐⭐⭐⭐", callback_data="rate:4"),
            InlineKeyboardButton("⭐⭐⭐⭐⭐", callback_data="rate:5"),
        ]
    ])
    
    await call.message.edit_text(
        "Оцените качество работы фабрики:", 
        reply_markup=kb
    )
    await call.answer()


@router.callback_query(F.data.startswith("rate:"))
async def process_rating(call: CallbackQuery, state: FSMContext) -> None:
    rating = int(call.data.split(":", 1)[1])
    
    await state.update_data(rating=rating)
    await state.set_state(DealForm.rate_factory)
    await call.message.edit_text(
        f"Вы поставили оценку: {'⭐' * rating}\n"
        f"Добавьте комментарий (или напишите «skip»):"
    )
    await call.answer()


@router.message(DealForm.rate_factory)
async def save_rating(msg: Message, state: FSMContext) -> None:
    comment = msg.text if msg.text and not msg.text.lower().startswith("skip") else ""
    
    # Get data from state
    data = await state.get_data()
    
    # Insert rating
    insert_and_get_id(
        """INSERT INTO ratings
               (deal_id, factory_id, buyer_id, rating, comment)
           VALUES(?, ?, ?, ?, ?);""",
        (data["deal_id"], data["factory_id"], msg.from_user.id, data["rating"], comment)
    )
    
    # Update factory rating
    ratings = q(
        "SELECT AVG(rating) as avg_rating, COUNT(*) as count FROM ratings WHERE factory_id=?", 
        (data["factory_id"],)
    )
    
    if ratings and ratings[0]["avg_rating"]:
        run(
            "UPDATE factories SET rating=?, rating_count=? WHERE tg_id=?",
            (ratings[0]["avg_rating"], ratings[0]["count"], data["factory_id"])
        )
    
    # Get factory name
    factory = q1("SELECT name FROM factories WHERE tg_id=?", (data["factory_id"],))
    factory_name = factory["name"] if factory else "Фабрика"
    
    # Notify factory about rating
    await bot.send_message(
        data["factory_id"],
        f"⭐ Заказчик оставил вам оценку по сделке #{data['deal_id']}:\n"
        f"{'⭐' * data['rating']} ({data['rating']}/5)\n"
        + (f"Комментарий: «{comment}»" if comment else "")
    )
    
    await msg.answer(
        f"✅ Спасибо за оценку фабрики {factory_name}!\n"
        f"Сделка #{data['deal_id']} полностью завершена.",
        reply_markup=kb_buyer_menu()
    )
    await state.clear()


# ---------------------------------------------------------------------------
#  Command handlers for quick access
# ---------------------------------------------------------------------------

@router.message(Command("myleads"))
async def cmd_myleads(msg: Message) -> None:
    factory = q1("SELECT * FROM factories WHERE tg_id=?", (msg.from_user.id,))
    if not factory or not factory["is_pro"]:
        await msg.answer("Команда доступна только для PRO-фабрик")
        return
    await factory_orders(msg)


@router.message(Command("myorders"))
async def cmd_myorders(msg: Message) -> None:
    orders = q1("SELECT 1 FROM orders WHERE buyer_id=?", (msg.from_user.id,))
    if not orders:
        await msg.answer("У вас пока нет заказов")
        return
    await buyer_orders(msg)


# ---------------------------------------------------------------------------
#  Main entry point
# ---------------------------------------------------------------------------

async def on_startup(bot: Bot) -> None:
    # Ensure database is initialized
    init_db()

    if BOT_MODE == "WEBHOOK":
        url = f"{WEBHOOK_BASE}/telegram/webhook/{TOKEN}"
        logger.info("Setting webhook URL: %s", url)
        await bot.set_webhook(url=url, drop_pending_updates=True)


async def on_shutdown(bot: Bot) -> None:
    logger.warning("Shutting down bot")
    if BOT_MODE == "WEBHOOK":
        await bot.delete_webhook()
    await bot.session.close()


async def main() -> None:
    # Register startup/shutdown handlers
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)

    # Start the bot
    logger.info("Starting bot in %s mode", BOT_MODE)
    
    if BOT_MODE == "WEBHOOK":
        # Create and configure aiohttp app
        app = web.Application()
        SimpleRequestHandler(dispatcher=dp, bot=bot).register(app, path=f"/telegram/webhook/{TOKEN}")
        
        # Setup the app
        setup_application(app, dp, bot=bot)
        
        # Run the web server
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, host="0.0.0.0", port=PORT)
        await site.start()
        
        # Run forever
        await asyncio.Event().wait()
    else:
        # Start polling
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())


if __name__ == "__main__":
    asyncio.run(main())
