"""
Mono‑Fabrique Telegram bot
=========================
MVP implementation that covers the core user‑flows from the technical
specification («ТЗ к боту»):
  • Factory onboarding → PRO subscription (stub payment)
  • Buyer order creation → payment → automated lead dispatch
  • Factory response to a lead (price / lead‑time / sample‑cost)
  • Basic match‑engine, profile & history commands

The code purposefully keeps the architecture extremely light‑weight so the
team can deploy and start testing immediately. Heavy components like real
payment provider hooks, moderation queue or escrow tracker are stubbed with
simple placeholders marked with TODO comments.

Dependencies (all are pure‑python and tiny):
  aiogram==3.1.1   — Telegram framework
  (standard library) sqlite3, logging, asyncio, dataclasses
No extra pip packages are required, so *requirements.txt* stays unchanged.
"""

import asyncio
import logging
import os
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from typing import List, Tuple

from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import CallbackQuery, Message

# ──────────────────────────── CONFIG ──────────────────────────────
TOKEN = os.getenv("BOT_TOKEN")
if not TOKEN:
    raise RuntimeError("BOT_TOKEN env var is missing")

DB_PATH = os.getenv("DB_PATH", "fabrique.db")
PAY_FACTORY_RUB = 2_000   # stub tariff for PRO subscription
PAY_ORDER_RUB = 700       # stub order placement fee

logging.basicConfig(level=logging.INFO)

# ──────────────────────────── DATABASE ────────────────────────────

def _init_db() -> None:
    """Create tables if they don’t exist."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS factories (
                   id          INTEGER PRIMARY KEY AUTOINCREMENT,
                   user_id     INTEGER UNIQUE,
                   inn         TEXT,
                   photos      TEXT,
                   categories  TEXT,
                   min_qty     INTEGER,
                   avg_price   INTEGER,
                   portfolio   TEXT,
                   status      TEXT DEFAULT 'PENDING' -- PENDING | PRO
               )"""
        )
        conn.execute(
            """CREATE TABLE IF NOT EXISTS orders (
                   id          INTEGER PRIMARY KEY AUTOINCREMENT,
                   buyer_id    INTEGER,
                   product     TEXT,
                   qty         INTEGER,
                   budget      INTEGER,
                   delivery    TEXT,
                   lead_time   INTEGER,
                   file_id     TEXT,
                   status      TEXT DEFAULT 'OPEN' -- OPEN | MATCHED | CLOSED
               )"""
        )
        conn.execute(
            """CREATE TABLE IF NOT EXISTS proposals (
                   id          INTEGER PRIMARY KEY AUTOINCREMENT,
                   order_id    INTEGER,
                   factory_id  INTEGER,
                   price       INTEGER,
                   lead_time   INTEGER,
                   sample_cost INTEGER,
                   status      TEXT DEFAULT 'SENT'
               )"""
        )
    logging.info("SQLite schema ensured ✔")

_init_db()

# ──────────────────────────── HELPERS ─────────────────────────────

def db_exec(query: str, params: Tuple = (), *, fetch: bool = False):
    """Small helper for sync sqlite queries (enough for the MVP)."""
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute(query, params)
        conn.commit()
        if fetch:
            return cur.fetchall()
        return None


def match_factories(product: str, qty: int) -> List[int]:
    """Return list of factory.user_id that match order requirements."""
    rows = db_exec(
        """
        SELECT user_id, categories, min_qty
        FROM factories
        WHERE status='PRO'
        """,
        fetch=True,
    )
    suitable: List[int] = []
    for user_id, cats, min_qty in rows:
        if product.lower() in cats.lower() and qty >= int(min_qty or 0):
            suitable.append(user_id)
    return suitable


# ──────────────────────────── FSM STATES ──────────────────────────
class FactoryForm(StatesGroup):
    inn = State()
    photos = State()
    categories = State()
    min_qty = State()
    avg_price = State()
    portfolio = State()


class BuyerForm(StatesGroup):
    product = State()
    qty = State()
    budget = State()
    delivery = State()
    lead_time = State()
    tech_file = State()


class ProposalForm(StatesGroup):
    order_id = State()
    price = State()
    lead_time = State()
    sample_cost = State()


# ──────────────────────────── BOT INIT ────────────────────────────

bot = Bot(TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# ──────────────────────────── KEYBOARDS ───────────────────────────

def main_menu_kb() -> types.ReplyKeyboardMarkup:
    return types.ReplyKeyboardMarkup(
        keyboard=[
            [
                types.KeyboardButton(text="🛠 Я — Фабрика"),
                types.KeyboardButton(text="🛒 Мне нужна фабрика"),
            ],
            [
                types.KeyboardButton(text="ℹ Как работает"),
                types.KeyboardButton(text="🧾 Тарифы"),
            ],
        ],
        resize_keyboard=True,
    )


def pay_button(label: str, payload: str) -> types.InlineKeyboardMarkup:
    return types.InlineKeyboardMarkup(
        inline_keyboard=[[types.InlineKeyboardButton(text=label, callback_data=payload)]]
    )


def lead_action_kb(order_id: int) -> types.InlineKeyboardMarkup:
    return types.InlineKeyboardMarkup(
        inline_keyboard=[
            [
                types.InlineKeyboardButton(
                    text="Откликнуться", callback_data=f"resp:{order_id}"),
                types.InlineKeyboardButton(
                    text="Пропустить", callback_data=f"skip:{order_id}"),
            ]
        ]
    )


# ──────────────────────────── START & MENU ────────────────────────
@dp.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "Привет! Это Marketplace "
        "🧵 *Factory ↔ Buyer*\. Выберите, что вам нужно:",
        reply_markup=main_menu_kb(),
        parse_mode="MarkdownV2",
    )


@dp.message(F.text == "ℹ Как работает")
async def how_it_works(message: Message):
    await message.answer(
        "1\. Фабрики регистрируются и оплачивают PRO\-подписку\\n"
        "2\. Заказчики размещают заказы и оплачивают 700₽\\n"
        "3\. Система матчит заказы с подходящими фабриками\\n"
        "4\. Оплата идёт через Escrow\. Безопасно для обеих сторон\."
    )


@dp.message(F.text == "🧾 Тарифы")
async def tariffs(message: Message):
    await message.answer(
        f"Пакет *PRO‑фабрика* — {PAY_FACTORY_RUB}₽/мес\. "
        f"\nРазмещение заявки заказчика — {PAY_ORDER_RUB}₽",
        parse_mode="MarkdownV2",
    )

# ──────────────────────────── FACTORY FLOW ────────────────────────
@dp.message(F.text == "🛠 Я — Фабрика")
async def factory_begin(message: Message, state: FSMContext):
    await message.answer("Здравствуйте! Подтвердите, что вы представитель производства\. "
                         "\nВведите ИНН / УНП:")
    await state.set_state(FactoryForm.inn)


@dp.message(FactoryForm.inn)
async def fac_inn(message: Message, state: FSMContext):
    await state.update_data(inn=message.text.strip())
    await message.answer("Загрузите 1–3 фото цеха или сертификат ISO")
    await state.set_state(FactoryForm.photos)


@dp.message(FactoryForm.photos, F.photo)
async def fac_photos(message: Message, state: FSMContext):
    photo_ids = [p.file_id for p in message.photo]
    await state.update_data(photos=photo_ids)
    kb = types.ReplyKeyboardMarkup(
        keyboard=[[types.KeyboardButton(text="Трикотаж"), types.KeyboardButton(text="Верхняя одежда")],
                  [types.KeyboardButton(text="Домашний текстиль")]],
        resize_keyboard=True,
    )
    await message.answer("Укажите категории производства (выберите или напишите через запятую)", reply_markup=kb)
    await state.set_state(FactoryForm.categories)


@dp.message(FactoryForm.categories)
async def fac_categories(message: Message, state: FSMContext):
    await state.update_data(categories=message.text.strip())
    await message.answer("Минимальный тираж (шт)?")
    await state.set_state(FactoryForm.min_qty)


@dp.message(FactoryForm.min_qty)
async def fac_min_qty(message: Message, state: FSMContext):
    await state.update_data(min_qty=int(message.text))
    await message.answer("Средняя ставка, ₽ за изделие?")
    await state.set_state(FactoryForm.avg_price)


@dp.message(FactoryForm.avg_price)
async def fac_avg_price(message: Message, state: FSMContext):
    await state.update_data(avg_price=int(message.text))
    await message.answer("Ссылка на прайс/портфолио? (или - если нет)")
    await state.set_state(FactoryForm.portfolio)


@dp.message(FactoryForm.portfolio)
async def fac_portfolio(message: Message, state: FSMContext):
    data = await state.update_data(portfolio=message.text.strip())
    summary = (
        "*Ваш профиль*\n"
        f"ИНН: {data['inn']}\n"
        f"Категории: {data['categories']}\n"
        f"Мин. тираж: {data['min_qty']} шт\n"
        f"Ставка: {data['avg_price']} ₽\n"
        f"Портфолио: {data['portfolio']}\n\n"
        f"Пакет *PRO‑фабрика* — {PAY_FACTORY_RUB} ₽/мес\."
    )
    await message.answer(summary, parse_mode="MarkdownV2",
                         reply_markup=pay_button("Оплатить", "pay_fac"))


# ───────── Factory payment stub
@dp.callback_query(F.data == "pay_fac")
async def fac_paid(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    # TODO: real payment + moderation
    db_exec(
        """INSERT OR REPLACE INTO factories
               (user_id, inn, photos, categories, min_qty, avg_price, portfolio, status)
               VALUES (?, ?, ?, ?, ?, ?, ?, 'PRO')""",
        (
            cb.from_user.id,
            data.get("inn"),
            ",".join(data.get("photos", [])),
            data.get("categories"),
            data.get("min_qty"),
            data.get("avg_price"),
            data.get("portfolio"),
        ),
    )
    await state.clear()
    await cb.message.edit_text("✅ Статус: PRO\. Лиды будут приходить в этот чат\.")
    await cb.answer()

# ──────────────────────────── BUYER FLOW ──────────────────────────
@dp.message(F.text == "🛒 Мне нужна фабрика")
async def buyer_begin(message: Message, state: FSMContext):
    await message.answer("Какой товар ищете? (например: Толстовки/худи)")
    await state.set_state(BuyerForm.product)


@dp.message(BuyerForm.product)
async def buyer_product(message: Message, state: FSMContext):
    await state.update_data(product=message.text.strip())
    await message.answer("Сколько штук в партии?")
    await state.set_state(BuyerForm.qty)


@dp.message(BuyerForm.qty)
async def buyer_qty(message: Message, state: FSMContext):
    await state.update_data(qty=int(message.text))
    await message.answer("Целевой бюджет за изделие, ₽?")
    await state.set_state(BuyerForm.budget)


@dp.message(BuyerForm.budget)
async def buyer_budget(message: Message, state: FSMContext):
    await state.update_data(budget=int(message.text))
    await message.answer("Куда доставить партию? (город)")
    await state.set_state(BuyerForm.delivery)


@dp.message(BuyerForm.delivery)
async def buyer_delivery(message: Message, state: FSMContext):
    await state.update_data(delivery=message.text.strip())
    await message.answer("Срок, когда нужен товар (дней)?")
    await state.set_state(BuyerForm.lead_time)


@dp.message(BuyerForm.lead_time)
async def buyer_lead_time(message: Message, state: FSMContext):
    await state.update_data(lead_time=int(message.text))
    await message.answer("Загрузите техзадание или референсы (jpg/pdf)")
    await state.set_state(BuyerForm.tech_file)


@dp.message(BuyerForm.tech_file, F.document | F.photo)
async def buyer_file(message: Message, state: FSMContext):
    file_id = message.document.file_id if message.document else message.photo[-1].file_id
    data = await state.update_data(file_id=file_id)
    summary = (
        "*Ваша заявка*\n"
        f"Товар: {data['product']}\n"
        f"Тираж: {data['qty']} шт\n"
        f"Бюджет: {data['budget']} ₽\n"
        f"Доставка: {data['delivery']}\n"
        f"Срок: {data['lead_time']} дн\n\n"
        f"Стоимость размещения — {PAY_ORDER_RUB} ₽"
    )
    await message.answer(summary, parse_mode="MarkdownV2",
                         reply_markup=pay_button("Оплатить", "pay_order"))


@dp.callback_query(F.data == "pay_order")
async def order_paid(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    # TODO: hook real payment system
    db_exec(
        """INSERT INTO orders (buyer_id, product, qty, budget, delivery, lead_time, file_id)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            cb.from_user.id,
            data["product"],
            data["qty"],
            data["budget"],
            data["delivery"],
            data["lead_time"],
            data["file_id"],
        ),
    )
    order_id = db_exec("SELECT last_insert_rowid()", fetch=True)[0][0]
    await state.clear()
    await cb.message.edit_text(f"👍 Заявка #Z‑{order_id} создана! Ожидайте предложения в течение 24 ч\.")
    await cb.answer()
    # Notify factories
    recipients = match_factories(data["product"], data["qty"])
    for fac_user_id in recipients:
        try:
            await bot.send_message(
                fac_user_id,
                (
                    "🆕 *Новый запрос* #Z‑{oid}\n"
                    "Категория: {prod}\n"
                    "Тираж: {qty} шт\n"
                    "Бюджет: {budget} ₽\n"
                    "Срок: {lt} дн."
                ).format(oid=order_id, prod=data["product"], qty=data["qty"], budget=data["budget"], lt=data["lead_time"]),
                parse_mode="MarkdownV2",
                reply_markup=lead_action_kb(order_id),
            )
        except Exception as e:
            logging.warning("Cannot notify factory %s: %s", fac_user_id, e)

# ──────────────────────────── LEAD RESPONSE ───────────────────────
@dp.callback_query(lambda c: c.data.startswith("resp:"))
async def lead_respond(cb: CallbackQuery, state: FSMContext):
    order_id = int(cb.data.split(":")[1])
    await state.update_data(order_id=order_id)
    await cb.message.answer("Введите цену за изделие:")
    await state.set_state(ProposalForm.price)
    await cb.answer()


@dp.message(ProposalForm.price)
async def proposal_price(message: Message, state: FSMContext):
    await state.update_data(price=int(message.text))
    await message.answer("Срок производства (дней):")
    await state.set_state(ProposalForm.lead_time)


@dp.message(ProposalForm.lead_time)
async def proposal_lead(message: Message, state: FSMContext):
    await state.update_data(lead_time=int(message.text))
    await message.answer("Стоимость образца:")
    await state.set_state(ProposalForm.sample_cost)


@dp.message(ProposalForm.sample_cost)
async def proposal_sample(message: Message, state: FSMContext):
    data = await state.update_data(sample_cost=int(message.text))
    order_id = data["order_id"]
    buyer_id_row = db_exec("SELECT buyer_id FROM orders WHERE id=?", (order_id,), fetch=True)
    if not buyer_id_row:
        await message.answer("Заказ не найден или закрыт")
        await state.clear()
        return
    buyer_id = buyer_id_row[0][0]
    # Store proposal
    factory_row = db_exec("SELECT id FROM factories WHERE user_id=?", (message.from_user.id,), fetch=True)
    if not factory_row:
        await message.answer("Ваш профиль не найден\, невозможно отправить предложение")
        await state.clear()
        return
    factory_id = factory_row[0][0]
    db_exec(
        """INSERT INTO proposals (order_id, factory_id, price, lead_time, sample_cost)
               VALUES (?, ?, ?, ?, ?)""",
        (
            order_id,
            factory_id,
            data["price"],
            data["lead_time"],
            data["sample_cost"],
        ),
    )
    await state.clear()
    await message.answer("💌 Отправлено заказчику!")
    # Notify buyer
    await bot.send_message(
        buyer_id,
        (
            "📬 Фабрика откликнулась на #Z‑{oid}\n"
            "Цена: {price} ₽\n"
            "Срок: {lead} дн\n"
            "Образец: {sample} ₽"
        ).format(oid=order_id, price=data["price"], lead=data["lead_time"], sample=data["sample_cost"]),
    )

# ──────────────────────────── COMMANDS ────────────────────────────
@dp.message(Command("profile"))
async def cmd_profile(message: Message):
    fac = db_exec("SELECT inn, categories, status FROM factories WHERE user_id=?", (message.from_user.id,), fetch=True)
    if fac:
        inn, cats, status = fac[0]
        await message.answer(f"ИНН: {inn}\nКатегории: {cats}\nСтатус: {status}")
        return
    await message.answer("Профиль не найден\.")


@dp.message(Command("myorders"))
async def cmd_myorders(message: Message):
    orders = db_exec(
        "SELECT id, product, qty, status FROM orders WHERE buyer_id=? ORDER BY id DESC", (message.from_user.id,), fetch=True
    )
    if not orders:
        await message.answer("У вас нет заявок\.")
        return
    lines = [f"#Z‑{oid} · {prod} · {qty} шт · {status}" for oid, prod, qty, status in orders]
    await message.answer("\n".join(lines))


@dp.message(Command("myleads"))
async def cmd_myleads(message: Message):
    fac_row = db_exec("SELECT id FROM factories WHERE user_id=?", (message.from_user.id,), fetch=True)
    if not fac_row:
        await message.answer("Вы ещё не зарегистрированы как фабрика\.")
        return
    fac_id = fac_row[0][0]
    leads = db_exec(
        """SELECT o.id, o.product, o.qty FROM orders o
             WHERE o.status='OPEN'
               AND NOT EXISTS (SELECT 1 FROM proposals p WHERE p.order_id=o.id AND p.factory_id=?)""",
        (fac_id,),
        fetch=True,
    )
    if not leads:
        await message.answer("Нет новых лидов\.")
        return
    for oid, prod, qty in leads:
        await message.answer(
            f"🆕 Запрос #Z‑{oid}\nКатегория: {prod}\nТираж: {qty} шт",
            reply_markup=lead_action_kb(oid),
        )


# ──────────────────────────── MAIN ENTRY ──────────────────────────
async def main() -> None:
    logging.info("Bot starting…")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
