"""Mono‑Fabrique Telegram bot – single‑file MVP (aiogram 3.4+)
================================================================
Connects garment factories («Фабрика») with buyers («Заказчик»).
Implements every mandatory requirement from the technical specification in
≈700 SLOC, with no runtime dependencies beyond **aiogram** (and optional
python‑dotenv for local development).

Main flows
----------
* Factory onboarding → stub payment (₂ 000 ₽) → PRO → receives leads & "📂 Заявки" menu.
* Buyer creates order → stub payment (₇ 00 ₽) → order stored → automatically
  dispatched to matching PRO‑factories (category, min_qty, avg_price ≤ budget).
* Factories browse «📂 Заявки» or get push‑lead, press «Откликнуться» → send
  price / lead‑time / sample‑cost → Buyer receives proposal.

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
from typing import Any, Iterable

from aiogram import Bot, Dispatcher, F, Router
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
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
#  Keyboards & helpers
# ---------------------------------------------------------------------------


def kb_main() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=[
            [
                KeyboardButton(text="🛠 Я – Фабрика"),
                KeyboardButton(text="🛒 Мне нужна фабрика"),
            ],
            [KeyboardButton(text="ℹ Как работает"), KeyboardButton(text="🧾 Тарифы")],
        ],
    )


def kb_factory_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=[[KeyboardButton(text="📂 Заявки"), KeyboardButton(text="🧾 Профиль")]],
    )


def parse_digits(text: str) -> int | None:
    digits = re.sub(r"\D", "", text)
    return int(digits) if digits else None


def order_caption(row: sqlite3.Row) -> str:
    return (
        f"<b>Заявка #Z‑{row['id']}</b>\n"
        f"Категория: {row['category']}\n"
        f"Тираж: {row['quantity']} шт.\n"
        f"Бюджет: {row['budget']} ₽\n"
        f"Срок: {row['lead_time']} дн.\n"
        f"Город: {row['destination']}"
    )


def send_order_card(chat_id: int, row: sqlite3.Row) -> None:
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="Откликнуться", callback_data=f"lead:{row['id']}")]]
    )
    asyncio.create_task(bot.send_message(chat_id, order_caption(row), reply_markup=kb))

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


@router.message(F.text == "ℹ Как работает")
async def cmd_how(msg: Message) -> None:
    await msg.answer(
        "Заказчик оформляет заявку, оплачивает 700 ₽ →\n"
        "Подходящие PRO‑фабрики получают лид и откликаются →\n"
        "Вы выбираете лучшую фабрику и сотрудничаете напрямую.",
        reply_markup=kb_main(),
    )


@router.message(F.text == "🧾 Тарифы")
async def cmd_tariffs(msg: Message) -> None:
    await msg.answer(
        "Для фабрик: 2 000 ₽/мес — статус PRO и доступ ко всем лидам.\n"
        "Для заказчиков: 700 ₽ за публикацию заявки.",
        reply_markup=kb_main(),
    )

# ---------------------------------------------------------------------------
#  Profile & menu
# ---------------------------------------------------------------------------


@router.message(F.text == "🧾 Профиль")
async def cmd_profile(msg: Message) -> None:
    f = q1("SELECT * FROM factories WHERE tg_id=?", (msg.from_user.id,))
    if not f:
        await msg.answer("Профиль не найден.")
        return
    await msg.answer(
        f"<b>Профиль фабрики</b>\n"
        f"ИНН: {f['inn']}\nКатегории: {f['categories']}\n"
        f"Мин. тираж: {f['min_qty']} шт.\nСредняя цена: {f['avg_price']}₽\n"
        f"PRO: {'✅' if f['is_pro'] else '—'}",
        reply_markup=kb_factory_menu() if f["is_pro"] else None,
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
    await msg.answer("Ссылка на портфолио (Instagram/Drive) или «skip»:")


@router.message(FactoryForm.portfolio)
async def factory_portfolio(msg: Message, state: FSMContext) -> None:
    await state.update_data(portfolio=msg.text)
    data = await state.get_data()
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton("Оплатить 2 000 ₽", callback_data="pay_factory")]])
    await state.set_state(FactoryForm.confirm_pay)
    await msg.answer(
        "Почти готово! Оплатите PRO‑подписку, чтобы получать заявки:", reply_markup=kb
    )


@router.callback_query(F.data == "pay_factory", FactoryForm.confirm_pay)
async def factory_pay(call: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    run(
        """INSERT OR REPLACE INTO factories
               (tg_id, inn, categories, min_qty, avg_price, portfolio, is_pro)
             VALUES(?, ?, ?, ?, ?, ?, 1);""",
        (
            call.from_user.id,
            data["inn"],
            data["categories"],
            data["min_qty"],
            data["avg_price"],
            data["portfolio"],
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
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton("Оплатить 700 ₽", callback_data="pay_order")]])
    await state.set_state(BuyerForm.confirm_pay)
    await msg.answer("Оплатите размещение заявки:", reply_markup=kb)


@router.callback_query(F.data == "pay_order", BuyerForm.confirm_pay)
async def buyer_pay(call: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    with sqlite3.connect(DB_PATH) as db:
        cur = db.execute(
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
        )
        order_id = cur.lastrowid
        row = db.execute("SELECT * FROM orders WHERE id=?", (order_id,)).fetchone()
        db.commit()
    await state.clear()
    await call.message.edit_text("✅ Заявка опубликована. Ожидайте отклики фабрик!")
    await call.answer()
    notify_factories(row)  # type: ignore[arg-type]

# ---------------------------------------------------------------------------
#  Factory lead list & proposals
# ---------------------------------------------------------------------------


@router.message(F.text == "📂 Заявки")
async def factory_leads(msg: Message) -> None:
    f = q1("SELECT * FROM factories WHERE tg_id=? AND is_pro=1", (msg.from_user.id,))
    if not f:
        await msg.answer("Раздел доступен только PRO‑фабрикам.")
        return
    leads = q(
        """SELECT o.* FROM orders o
               LEFT JOIN proposals p ON p.order_id=o.id AND p.factory_id=?
             WHERE o.paid=1
               AND o.quantity>=?
               AND o.budget>=?
               AND (','||?||',') LIKE ('%,'||o.category||',%')
               AND p.id IS NULL
             ORDER BY o.created_at DESC LIMIT 20;""",
        (msg.from_user.id, f["min_qty"], f["avg_price"], f["categories"]),
    )
    if not leads:
        await msg.answer("Пока нет подходящих заявок.", reply_markup=kb_factory_menu())
        return
    for l in leads:
        send_order_card(msg.from_user.id, l)


@router.callback_query(F.data.startswith("lead:"))
async def start_proposal(call: CallbackQuery, state: FSMContext) -> None:
    order_id = int(call.data.split(":", 1)[1])
    order = q1("SELECT * FROM orders WHERE id=?", (order_id,))
    if not order:
        await call.answer("Заявка не найдена", show_alert=True)
        return
    # check duplicate proposal
    if q1("SELECT * FROM proposals WHERE order_id=? AND factory_id=?", (order_id, call.from_user.id)):
        await call.answer("Вы уже отправили предложение", show_alert=True)
        return
    await state.clear()
    await state.update_data(order_id=order_id)
    await state.set_state(ProposalForm.price)
    await call.message.answer("Стоимость изделия, ₽:")
    await call.answer()


@router.message(ProposalForm.price)
async def prop_price(msg: Message, state: FSMContext) -> None:
    price = parse_digits(msg.text or "")
    if not price:
        await msg.answer("Укажите число:")
        return
    await state.update_data(price=price)
    await state.set_state(ProposalForm.lead_time)
    await msg.answer("Срок производства, дней:")


@router.message(ProposalForm.lead_time)
async def prop_lead_time(msg: Message, state: FSMContext) -> None:
    days = parse_digits(msg.text or "")
    if not days:
        await msg.answer("Укажите число:")
        return
    await state.update_data(lead_time=days)
    await state.set_state(ProposalForm.sample_cost)
    await msg.answer("Стоимость образца, ₽ (или 0):")


@router.message(ProposalForm.sample_cost)
async def prop_sample(msg: Message, state: FSMContext) -> None:
    cost = parse_digits(msg.text or "")
    if cost is None:
        await msg.answer("Укажите число:")
        return
    data = await state.get_data()
    order = q1("SELECT * FROM orders WHERE id=?", (data["order_id"],))
    if not order:
        await msg.answer("Заявка не найдена.")
        await state.clear()
        return
    run(
        """INSERT INTO proposals (order_id, factory_id, price, lead_time, sample_cost)
             VALUES (?, ?, ?, ?, ?)""",
        (order["id"], msg.from_user.id, data["price"], data["lead_time"], cost),
    )
    await state.clear()
    await msg.answer("✅ Отклик отправлен заказчику.", reply_markup=kb_factory_menu())
    # notify buyer
    caption = (
        f"<b>Отклик от фабрики</b>\n"
        f"Цена: {data['price']}₽\nСрок: {data['lead_time']} дн.\nСтоимость образца: {cost}₽"
    )
    await bot.send_message(order["buyer_id"], caption)

# ---------------------------------------------------------------------------
#  Webhook / polling bootstrap
# ---------------------------------------------------------------------------


async def on_startup() -> None:
    init_db()


async def main() -> None:
    await on_startup()

    if BOT_MODE == "WEBHOOK":
        if not WEBHOOK_BASE:
            raise RuntimeError("WEBHOOK_BASE env var required in webhook mode")
        path = f"/tg/{TOKEN}"
        await bot.set_webhook(url=WEBHOOK_BASE + path, drop_pending_updates=True)
        app = web.Application()
        SimpleRequestHandler(dispatcher=dp, bot=bot).register(app, path)
        setup_application(app, dp, bot=bot)
        logger.info("Webhook set to %s", WEBHOOK_BASE + path)
        web.run_app(app, port=PORT)
    else:
        await bot.delete_webhook(drop_pending_updates=True)
        logger.info("Webhook cleared ✔ – switched to long‑polling mode")
        await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
