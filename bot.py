"""Mono‑Fabrique Telegram bot — MVP
=================================================
Telegram bot connecting garment factories («Фабрика») with buyers («Заказчик»).
This single‑file version keeps external deps minimal (only **aiogram 3**) and
implements every mandatory step from the technical specification (see
«ТЗ к боту.pdf»).

Implemented flows -------------------------------------------------------------
* **Factory onboarding ➜ PRO‑subscription** (stub payment for 2 000 ₽)
* **Buyer request creation ➜ payment 700 ₽ ➜ automatic lead dispatch**
* **Factory response** (price / lead‑time / sample‑price) with inline FSM
* **Match‑engine** that selects only paid (PRO) factories matching category &
  minimum quantity and pushes them a private lead card.
* Basic commands: `/profile`, `/myleads`, `/myorders`, `/help`.
* SQLite persistence (`factories`, `orders`, `proposals`).
* Logging and graceful DB auto‑initialisation.

The code deliberately keeps the architecture ultra‑lean so it can be copied to
Render or Fly as a single process and started with `python bot.py` (see
`render.yaml`). A prod‑grade version would split DB models, routers and middle‑
wares but for MVP this flat layout is easier to reason about.
"""
from __future__ import annotations

import asyncio
import logging
import os
import sqlite3
from contextlib import closing
from datetime import datetime
from typing import Any, Iterable

from aiogram import Bot, Dispatcher, F, types
from aiogram.enums import ParseMode
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (CallbackQuery, InlineKeyboardButton,
                           InlineKeyboardMarkup, Message)
from aiogram.filters import Command

# ---------------------------------------------------------------------------
#  Config & bootstrap
# ---------------------------------------------------------------------------
TOKEN = os.getenv("BOT_TOKEN") or "TEST_TOKEN"  # put real token in env on prod
if TOKEN == "TEST_TOKEN":
    print("⚠ BOT_TOKEN env var is missing – bot will not connect to Telegram")

logging.basicConfig(level=logging.INFO,
                    format="%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger(__name__)

# Use HTML parse‑mode to avoid MarkdownV2 escaping headaches (e.g. the '!' bug)
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
                categories TEXT,   -- comma separated
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
#  Finite‑state contexts
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
#  Utility: send lead to matching factories
# ---------------------------------------------------------------------------

def notify_factories(order_row: sqlite3.Row) -> None:
    factories = fetchmany(
        """SELECT tg_id, name FROM factories
            WHERE is_pro = 1
            AND (',' || categories || ',') LIKE ('%,' || ? || ',%')
            AND min_qty <= ?;""",
        (order_row["category"], order_row["quantity"]),
    )
    logger.info("Dispatching lead %s to %d factories", order_row["id"], len(factories))
    for f in factories:
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(
            text="Откликнуться",
            callback_data=f"lead:{order_row['id']}"),
            InlineKeyboardButton(text="Пропустить",
                                 callback_data=f"skip:{order_row['id']}")]])
        asyncio.create_task(bot.send_message(
            f["tg_id"],
            (f"🆕 Новый запрос #Z‑{order_row['id']}\n"
             f"Категория: {order_row['category']}\n"
             f"Тираж: {order_row['quantity']} шт.\n"
             f"Бюджет: {order_row['budget']} ₽\n"
             f"Срок: {order_row['lead_time']} дней"),
            reply_markup=kb))

# ---------------------------------------------------------------------------
#  /start and main menu
# ---------------------------------------------------------------------------
@dp.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext) -> None:
    kb = types.ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=[[types.KeyboardButton(text="🛠 Я – Фабрика")],
                  [types.KeyboardButton(text="🛒 Мне нужна фабрика")],
                  [types.KeyboardButton(text="ℹ Как работает")],
                  [types.KeyboardButton(text="🧾 Тарифы")]])
    await message.answer("<b>Привет!</b> Кто вы?", reply_markup=kb)


# ---------------------------------------------------------------------------
#  Factory flow
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
    # In MVP just store file‑id list
    file_ids = [p.file_id for p in message.photo] if message.photo else [message.document.file_id]
    await state.update_data(photos=file_ids)
    # Categories list to pick from
    cat_kb = types.ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[
        [types.KeyboardButton(text="Трикотаж"), types.KeyboardButton(text="Верхняя одежда")],
        [types.KeyboardButton(text="Домашний текстиль")]])
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
    data = await state.get_data()
    text = ("<b>Готово!</b> Витрина будет проверена модератором в течение 1 дня.\n"
            "Пакет “PRO‑фабрика” – 2 000 ₽/мес.")
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(
        text="Оплатить 2 000 ₽",
        callback_data="pay_factory")]])
    await message.answer(text, reply_markup=kb)
    await state.update_data(confirm_time=datetime.utcnow().isoformat())
    await state.set_state(FactoryForm.confirm_pay)


@dp.callback_query(F.data == "pay_factory", FactoryForm.confirm_pay)
async def factory_pay(call: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    execute("""INSERT OR REPLACE INTO factories (tg_id, name, inn, categories,
              min_qty, avg_price, portfolio, is_pro)
              VALUES (?, ?, ?, ?, ?, ?, ?, 1)""",
            (call.from_user.id,
             call.from_user.full_name,
             data["inn"],
             ",".join(data["categories"]),
             data["min_qty"],
             data["avg_price"],
             data["portfolio"]))
    await state.clear()
    await call.message.edit_text("✅ Статус: <b>PRO</b>. Лиды будут приходить в этот чат.")


# ---------------------------------------------------------------------------
#  Buyer flow
# ---------------------------------------------------------------------------
@dp.message(F.text == "🛒 Мне нужна фабрика")
async def buyer_begin(message: Message, state: FSMContext) -> None:
    cat_kb = types.ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[
        [types.KeyboardButton(text="Толстовки / худи")],
        [types.KeyboardButton(text="Футболки"), types.KeyboardButton(text="Платья")]])
    await message.answer("Какой товар ищете?", reply_markup=cat_kb)
    await state.set_state(BuyerForm.category)


@dp.message(BuyerForm.category)
async def buyer_category(message: Message, state: FSMContext) -> None:
    await state.update_data(category=message.text.strip())
    await message.answer("Сколько штук в партии?")
    await state.set_state(BuyerForm.quantity)


@dp.message(BuyerForm.quantity)
async def buyer_quantity(message: Message, state: FSMContext) -> None:
    await state.update_data(quantity=int(message.text))
    await message.answer("Ваш целевой бюджет за изделие, ₽?")
    await state.set_state(BuyerForm.budget)


@dp.message(BuyerForm.budget)
async def buyer_budget(message: Message, state: FSMContext) -> None:
    await state.update_data(budget=int(message.text))
    await message.answer("Куда доставить партию?")
    await state.set_state(BuyerForm.destination)


@dp.message(BuyerForm.destination)
async def buyer_destination(message: Message, state: FSMContext) -> None:
    await state.update_data(destination=message.text.strip())
    await message.answer("Срок, когда нужен товар (дней)?")
    await state.set_state(BuyerForm.lead_time)


@dp.message(BuyerForm.lead_time)
async def buyer_lead_time(message: Message, state: FSMContext) -> None:
    await state.update_data(lead_time=int(message.text))
    await message.answer("Загрузите техзадание или референсы (jpg/pdf):")
    await state.set_state(BuyerForm.file)


@dp.message(BuyerForm.file, F.document | F.photo)
async def buyer_file(message: Message, state: FSMContext) -> None:
    fid = message.document.file_id if message.document else message.photo[-1].file_id
    await state.update_data(file=fid)
    text = ("Проверяем… Размещение заявки – 700 ₽. Оплата включает: рассылку ≥3 фабрикам, "
            "сводное КП, чат с менеджером.")
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(
        text="Оплатить 700 ₽",
        callback_data="pay_order")]])
    await message.answer(text, reply_markup=kb)
    await state.set_state(BuyerForm.confirm_pay)


@dp.callback_query(F.data == "pay_order", BuyerForm.confirm_pay)
async def buyer_pay(call: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    execute("""INSERT INTO orders (buyer_id, category, quantity, budget, destination,
              lead_time, file_id, paid)
              VALUES (?, ?, ?, ?, ?, ?, ?, 1)""",
            (call.from_user.id, data["category"], data["quantity"], data["budget"],
             data["destination"], data["lead_time"], data["file"]))
    order_id = fetchone("SELECT last_insert_rowid() AS id;")["id"]
    await state.clear()
    await call.message.edit_text(f"👍 Заявка #Z‑{order_id} создана! Ожидайте первые предложения в течение 24 ч.")
    notify_factories(fetchone("SELECT * FROM orders WHERE id=?", (order_id,)))


# ---------------------------------------------------------------------------
#  Factory proposal flow triggered by inline button
# ---------------------------------------------------------------------------
@dp.callback_query(lambda c: c.data.startswith("lead:"))
async def lead_open(call: CallbackQuery, state: FSMContext) -> None:
    _, order_id = call.data.split(":", 1)
    order = fetchone("SELECT * FROM orders WHERE id=?", (order_id,))
    if not order:
        await call.answer("Заявка не найдена 🙈", show_alert=True)
        return
    await state.update_data(order_id=order_id)
    await call.message.answer("Введите цену за изделие, ₽:")
    await state.set_state(ProposalForm.price)


@dp.message(ProposalForm.price)
async def proposal_price(message: Message, state: FSMContext) -> None:
    await state.update_data(price=int(message.text))
    await message.answer("Срок производства (дней):")
    await state.set_state(ProposalForm.lead_time)


@dp.message(ProposalForm.lead_time)
async def proposal_time(message: Message, state: FSMContext) -> None:
    await state.update_data(lead_time=int(message.text))
    await message.answer("Стоимость образца, ₽:")
    await state.set_state(ProposalForm.sample_cost)


@dp.message(ProposalForm.sample_cost)
async def proposal_sample(message: Message, state: FSMContext) -> None:
    await state.update_data(sample_cost=int(message.text))
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(
        text="Отправить предложение",
        callback_data="send_proposal")]])
    await message.answer("Отправить предложение заказчику?", reply_markup=kb)
    await state.set_state(ProposalForm.confirm)


@dp.callback_query(F.data == "send_proposal", ProposalForm.confirm)
async def proposal_send(call: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    execute("""INSERT INTO proposals (order_id, factory_id, price, lead_time, sample_cost)
              VALUES (?, ?, ?, ?, ?)""",
            (data["order_id"], call.from_user.id, data["price"], data["lead_time"], data["sample_cost"]))
    buyer_id = fetchone("SELECT buyer_id FROM orders WHERE id=?", (data["order_id"],))["buyer_id"]
    await bot.send_message(
        buyer_id,
        (f"📬 Фабрика {call.from_user.full_name} откликнулась на #Z‑{data['order_id']}\n"
         f"Цена: {data['price']} ₽, срок {data['lead_time']} дн., образец {data['sample_cost']} ₽"))
    await state.clear()
    await call.message.edit_text("💌 Отправлено заказчику!")


# ---------------------------------------------------------------------------
#  Misc commands
# ---------------------------------------------------------------------------
@dp.message(Command("profile"))
async def cmd_profile(message: Message) -> None:
    row = fetchone("SELECT * FROM factories WHERE tg_id=?", (message.from_user.id,))
    if row:
        await message.answer((f"Профиль фабрики “{row['name']}”\n"
                               f"Категории: {row['categories']}\n"
                               f"Мин. тираж: {row['min_qty']}\n"
                               f"Сред. цена: {row['avg_price']} ₽\n"
                               f"Статус: {'PRO' if row['is_pro'] else 'FREE'}"))
    else:
        await message.answer("Ваш профиль не найден. Используйте /start.")


@dp.message(Command("myleads"))
async def cmd_myleads(message: Message) -> None:
    rows = fetchmany("""SELECT p.id, o.id AS oid, p.price, p.lead_time, p.created_at
                         FROM proposals p JOIN orders o ON p.order_id = o.id
                         WHERE p.factory_id=? ORDER BY p.created_at DESC LIMIT 10""",
                     (message.from_user.id,))
    if rows:
        text = "\n".join([f"#{r['oid']} – {r['price']} ₽ / {r['lead_time']} дн." for r in rows])
        await message.answer("Последние предложения:\n" + text)
    else:
        await message.answer("Нет отправленных предложений.")


@dp.message(Command("myorders"))
async def cmd_myorders(message: Message) -> None:
    rows = fetchmany("SELECT id, category, quantity, created_at FROM orders WHERE buyer_id=? ORDER BY created_at DESC LIMIT 10", (message.from_user.id,))
    if rows:
        text = "\n".join([f"#Z‑{r['id']} • {r['category']} • {r['quantity']} шт." for r in rows])
        await message.answer("Ваши последние заявки:\n" + text)
    else:
        await message.answer("У вас пока нет заявок.")


@dp.message(Command("help"))
async def cmd_help(message: Message) -> None:
    await message.answer("Поддержка: hello@mono‑fabrique.io")


# ---------------------------------------------------------------------------
#  Entry‑point
# ---------------------------------------------------------------------------
async def main() -> None:
    init_db()
    logger.info("Bot starting…")
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped")
