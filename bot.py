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
class TicketForm(StatesGroup):
    subject = State()
    message = State()
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import BotCommand
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
#  Admin commands
# ---------------------------------------------------------------------------

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

@router.callback_query(F.data.startswith("admin_view_"))
async def admin_view_entity(call: CallbackQuery) -> None:
    """View specific entity details for admin."""
    if call.from_user.id not in ADMIN_IDS:
        await call.answer("Доступ запрещен", show_alert=True)
        return
    
    entity_type, entity_id = call.data.replace("admin_view_", "").split(":")
    entity_id = int(entity_id)
    
    if entity_type == "user":
        user = q1("SELECT * FROM users WHERE tg_id = ?", (entity_id,))
        if not user:
            await call.answer("Пользователь не найден", show_alert=True)
            return
        
        # Get user's activity
        orders_count = q1("SELECT COUNT(*) as cnt FROM orders WHERE buyer_id = ?", (entity_id,))
        deals_count = q1("SELECT COUNT(*) as cnt FROM deals WHERE buyer_id = ? OR factory_id = ?", (entity_id, entity_id))
        
        text = (
            f"<b>👤 Пользователь</b>\n\n"
            f"ID: {entity_id}\n"
            f"Имя: {user['full_name']}\n"
            f"Username: @{user['username'] or 'нет'}\n"
            f"Роль: {user['role']}\n"
            f"Телефон: {user['phone'] or 'не указан'}\n"
            f"Email: {user['email'] or 'не указан'}\n"
            f"Регистрация: {user['created_at'][:16]}\n"
            f"Последняя активность: {user['last_active'][:16]}\n\n"
            f"Заказов: {orders_count['cnt'] if orders_count else 0}\n"
            f"Сделок: {deals_count['cnt'] if deals_count else 0}\n"
            f"Статус: {'🚫 Заблокирован' if user['is_banned'] else '✅ Активен'}"
        )
        
        buttons = [
            [
                InlineKeyboardButton(text="💬 Написать", url=f"tg://user?id={entity_id}"),
                InlineKeyboardButton(text="🚫 Заблокировать" if not user['is_banned'] else "✅ Разблокировать", 
                                   callback_data=f"admin_toggle_ban:{entity_id}")
            ]
        ]
        
        if user['role'] == 'factory':
            buttons.append([
                InlineKeyboardButton(text="🏭 Профиль фабрики", callback_data=f"admin_view_factory:{entity_id}")
            ])
        
        kb = InlineKeyboardMarkup(inline_keyboard=buttons)
        await call.message.answer(text, reply_markup=kb)
        
    elif entity_type == "order":
        order = q1("""
            SELECT o.*, u.username, u.full_name
            FROM orders o
            JOIN users u ON o.buyer_id = u.tg_id
            WHERE o.id = ?
        """, (entity_id,))
        
        if not order:
            await call.answer("Заказ не найден", show_alert=True)
            return
        
        proposals_count = q1("SELECT COUNT(*) as cnt FROM proposals WHERE order_id = ?", (entity_id,))
        deal = q1("SELECT * FROM deals WHERE order_id = ?", (entity_id,))
        
        text = (
            f"<b>📦 Заказ #Z-{entity_id}</b>\n\n"
            f"Название: {order['title']}\n"
            f"Заказчик: @{order['username'] or order['full_name']}\n"
            f"Категория: {order['category']}\n"
            f"Количество: {format_price(order['quantity'])} шт.\n"
            f"Бюджет: {format_price(order['budget'])} ₽/шт.\n"
            f"Общая сумма: {format_price(order['quantity'] * order['budget'])} ₽\n"
            f"Срок: {order['lead_time']} дней\n"
            f"Город: {order['destination']}\n"
            f"Создан: {order['created_at'][:16]}\n"
            f"Статус: {'✅ Оплачен' if order['paid'] else '❌ Не оплачен'}\n"
            f"Предложений: {proposals_count['cnt'] if proposals_count else 0}\n"
        )
        
        if deal:
            text += f"\n<b>Сделка:</b> #{deal['id']} (статус: {deal['status']})"
        
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="💬 Написать заказчику", url=f"tg://user?id={order['buyer_id']}"),
                InlineKeyboardButton(text="📋 Предложения", callback_data=f"admin_order_proposals:{entity_id}")
            ]
        ])
        
        await call.message.answer(text, reply_markup=kb)
        
    elif entity_type == "deal":
        deal = q1("""
            SELECT d.*, o.title, o.category, f.name as factory_name, 
                   u1.username as buyer_username, u2.username as factory_username
            FROM deals d
            JOIN orders o ON d.order_id = o.id
            JOIN factories f ON d.factory_id = f.tg_id
            JOIN users u1 ON d.buyer_id = u1.tg_id
            JOIN users u2 ON d.factory_id = u2.tg_id
            WHERE d.id = ?
        """, (entity_id,))
        
        if not deal:
            await call.answer("Сделка не найдена", show_alert=True)
            return
        
        buyer_info = f"@{deal['buyer_username']}" if deal['buyer_username'] else f"ID:{deal['buyer_id']}"
        factory_info = f"@{deal['factory_username']}" if deal['factory_username'] else f"ID:{deal['factory_id']}"
        
        text = (
            f"<b>🤝 Сделка #{entity_id}</b>\n\n"
            f"Заказ: #Z-{deal['order_id']} - {deal['title']}\n"
            f"Категория: {deal['category']}\n"
            f"Заказчик: {buyer_info}\n"
            f"Фабрика: {deal['factory_name']} ({factory_info})\n"
            f"Сумма: {format_price(deal['amount'])} ₽\n"
            f"Статус: {deal['status']}\n"
            f"Создана: {deal['created_at'][:16]}\n"
        )
        
        if deal['deposit_paid']:
            text += "\n✅ Предоплата 30% получена"
        if deal['final_paid']:
            text += "\n✅ Финальная оплата получена"
        if deal['tracking_num']:
            text += f"\n🚚 Трек: {deal['tracking_num']}"
        
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="💬 Заказчик", url=f"tg://user?id={deal['buyer_id']}"),
                InlineKeyboardButton(text="💬 Фабрика", url=f"tg://user?id={deal['factory_id']}")
            ],
            [
                InlineKeyboardButton(text="📋 Заказ", callback_data=f"admin_view_order:{deal['order_id']}"),
                InlineKeyboardButton(text="🚨 Открыть диспут", callback_data=f"admin_open_dispute:{entity_id}")
            ]
        ])
        
        await call.message.answer(text, reply_markup=kb)
    
    await call.answer()

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
            InlineKeyboardButton(text="📋 Открыть тикет", callback_data=f"admin_ticket:{ticket_id}"),
            InlineKeyboardButton(text="💬 Ответить", callback_data=f"admin_reply_ticket:{ticket_id}")
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

# Add notification calls to payment and status update functions
@router.callback_query(F.data.startswith("pay_deposit:"))
async def pay_deposit_with_notification(call: CallbackQuery) -> None:
    """Process deposit payment with admin notification."""
    deal_id = int(call.data.split(":", 1)[1])
    
    # Update deal status
    run("UPDATE deals SET status = 'PRODUCTION', deposit_paid = 1 WHERE id = ?", (deal_id,))
    
    deal = q1("""
        SELECT d.*, o.title, f.name as factory_name
        FROM deals d
        JOIN orders o ON d.order_id = o.id
        JOIN factories f ON d.factory_id = f.tg_id
        WHERE d.id = ?
    """, (deal_id,))
    
    if not deal:
        await call.answer("Сделка не найдена", show_alert=True)
        return
    
    # Calculate deposit amount
    deposit_amount = int(deal['amount'] * 0.3)
    
    # Notify admins
    await notify_admins(
        'deposit_paid',
        '💰 Предоплата получена',
        f"Сделка #{deal_id}\n"
        f"Заказ: {deal['title']}\n"
        f"Фабрика: {deal['factory_name']}\n"
        f"Сумма предоплаты: {format_price(deposit_amount)} ₽ (30%)\n"
        f"Общая сумма сделки: {format_price(deal['amount'])} ₽",
        {
            'deal_id': deal_id,
            'order_id': deal['order_id'],
            'amount': deposit_amount
        },
        [[
            InlineKeyboardButton(text="📊 Детали сделки", callback_data=f"admin_view_deal:{deal_id}")
        ]]
    )
    
    # Continue with original logic
    await call.message.edit_text(
        "💰 Предоплата 30% произведена!\n\n" +
        deal_status_caption(deal) + "\n\n" +
        "Фабрика приступила к производству. Мы уведомим вас о готовности партии."
    )
    
    # Notify factory
    tracking_kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="Добавить трек-номер отправления", callback_data=f"add_tracking:{deal_id}")
    ]])
    
    asyncio.create_task(
        bot.send_message(
            deal["factory_id"],
            f"💰 Получена предоплата 30% для заказа #Z-{deal['order_id']}!\n\n"
            f"Статус: {deal['status']}\n"
            f"{ORDER_STATUS_DESCRIPTIONS[OrderStatus(deal['status'])]}\n\n"
            f"Когда партия будет готова к отправке, добавьте трек-номер:",
            reply_markup=tracking_kb
        )
    )
    
    await call.answer("Предоплата 30% произведена", show_alert=True)

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
    
    if proposal.get('message'):
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
#  Background tasks
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

async def check_pro_expiration():
    """Check and notify about PRO subscription expiration."""
    # Get factories with expiring PRO status
    expiring_soon = q("""
        SELECT f.*, u.username 
        FROM factories f
        JOIN users u ON f.tg_id = u.tg_id
        WHERE f.is_pro = 1 
          AND f.pro_expires IS NOT NULL
          AND datetime(f.pro_expires) <= datetime('now', '+3 days')
          AND datetime(f.pro_expires) > datetime('now')
    """)
    
    for factory in expiring_soon:
        await send_notification(
            factory['tg_id'],
            'pro_expiring',
            'PRO статус истекает',
            f"Ваш PRO статус истекает {factory['pro_expires']}. Продлите подписку, чтобы продолжать получать заявки.",
            {'expires_at': factory['pro_expires']}
        )
    
    # Disable expired PRO status
    run("""
        UPDATE factories 
        SET is_pro = 0 
        WHERE is_pro = 1 
          AND pro_expires IS NOT NULL
          AND datetime(pro_expires) <= datetime('now')
    """)

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
#  Factory registration flow
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
    """Process factory payment."""
    data = await state.get_data()
    
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
    
    # Create payment record
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
#  Buyer order flow
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
    """Process special requirements."""
    requirements = ""
    if msg.text and msg.text.lower() not in ["нет", "no", "-"]:
        requirements = msg.text.strip()
    
    await state.update_data(requirements=requirements)
    await state.set_state(BuyerForm.file)
    await msg.answer(
        "Прикрепите техническое задание, эскизы или образцы\n"
        "(документ или фото).\n\n"
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
    
    if requirements:
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
    """Process order payment."""
    data = await state.get_data()
    
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
    
    # Create payment record
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
#  Factory leads browsing
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
#  Buyer - View proposals and choose factory
# ---------------------------------------------------------------------------

@router.message(F.text == "💌 Предложения")
async def cmd_buyer_proposals(msg: Message) -> None:
    """Show all proposals for buyer's orders."""
    # Get orders with proposals
    orders_with_proposals = q("""
        SELECT o.*, COUNT(p.id) as proposal_count
        FROM orders o
        JOIN proposals p ON o.id = p.order_id
        WHERE o.buyer_id = ? AND o.is_active = 1
        GROUP BY o.id
        ORDER BY o.created_at DESC
    """, (msg.from_user.id,))
    
    if not orders_with_proposals:
        await msg.answer(
            "У вас пока нет предложений от фабрик.\n\n"
            "Создайте заказ, и фабрики начнут присылать предложения!",
            reply_markup=kb_buyer_menu()
        )
        return
    
    await msg.answer(
        f"<b>Заказы с предложениями ({len(orders_with_proposals)})</b>",
        reply_markup=kb_buyer_menu()
    )
    
    for order in orders_with_proposals[:5]:
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text=f"👀 Смотреть {order['proposal_count']} предложений", 
                callback_data=f"view_proposals:{order['id']}"
            )
        ]])
        
        await msg.answer(
            order_caption(order) + f"\n\n💌 Предложений: {order['proposal_count']}",
            reply_markup=kb
        )

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
    """Choose factory and create deal."""
    parts = call.data.split(":")
    order_id = int(parts[1])
    factory_id = int(parts[2])
    
    # Verify ownership
    order = q1("SELECT * FROM orders WHERE id = ? AND buyer_id = ?", (order_id, call.from_user.id))
    if not order:
        await call.answer("Заказ не найден", show_alert=True)
        return
    
    # Check if deal already exists
    existing_deal = q1("""
        SELECT * FROM deals 
        WHERE order_id = ? AND status NOT IN ('CANCELLED')
    """, (order_id,))
    
    if existing_deal:
        await call.answer("По этому заказу уже есть активная сделка", show_alert=True)
        return
    
    # Get proposal details
    proposal = q1("""
        SELECT p.*, f.name as factory_name
        FROM proposals p
        JOIN factories f ON p.factory_id = f.tg_id
        WHERE p.order_id = ? AND p.factory_id = ?
    """, (order_id, factory_id))
    
    if not proposal:
        await call.answer("Предложение не найдено", show_alert=True)
        return
    
    # Calculate total amount
    total_amount = proposal['price'] * order['quantity']
    
    # Create deal
    deal_id = insert_and_get_id("""
        INSERT INTO deals
        (order_id, factory_id, buyer_id, amount, status)
        VALUES (?, ?, ?, ?, 'DRAFT')
    """, (order_id, factory_id, call.from_user.id, total_amount))
    
    # Update proposal status
    run("UPDATE proposals SET is_accepted = 1 WHERE order_id = ? AND factory_id = ?", 
        (order_id, factory_id))
    
    # Deactivate order
    run("UPDATE orders SET is_active = 0 WHERE id = ?", (order_id,))
    
    # Track event
    track_event(call.from_user.id, 'deal_created', {
        'deal_id': deal_id,
        'order_id': order_id,
        'factory_id': factory_id,
        'amount': total_amount
    })
    
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
        },
        [[
            InlineKeyboardButton(text="📊 Детали сделки", callback_data=f"admin_view_deal:{deal_id}"),
            InlineKeyboardButton(text="📋 Заказ", callback_data=f"admin_view_order:{order_id}")
        ]]
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
    
    if proposal['sample_cost'] > 0:
        deal_text += f"\n\nСтоимость образца: {format_price(proposal['sample_cost'])} ₽"
    
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="💳 Оплатить образец", callback_data=f"pay_sample:{deal_id}")
    ]])
    
    await call.message.edit_text(deal_text, reply_markup=kb)
    
    # Notify factory
    await send_notification(
        factory_id,
        'deal_created',
        'Ваше предложение выбрано!',
        f'Заказчик выбрал ваше предложение по заказу #Z-{order_id}\n'
        f'Сумма сделки: {format_price(total_amount)} ₽\n\n'
        f'Ожидайте оплату образца.',
        {'deal_id': deal_id, 'order_id': order_id}
    )
    
    # Notify other factories that didn't win
    other_proposals = q("""
        SELECT factory_id FROM proposals 
        WHERE order_id = ? AND factory_id != ?
    """, (order_id, factory_id))
    
    for prop in other_proposals:
        await send_notification(
            prop['factory_id'],
            'proposal_rejected',
            'Предложение не выбрано',
            f'К сожалению, заказчик выбрал другую фабрику для заказа #Z-{order_id}',
            {'order_id': order_id}
        )
    
    await call.answer("✅ Сделка создана!")

# ---------------------------------------------------------------------------
#  Deal management flow
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
    else:
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
    
    # Common actions
    buttons.append([
        InlineKeyboardButton(text="💬 Чат по сделке", callback_data=f"deal_chat:{deal['id']}"),
        InlineKeyboardButton(text="📋 Подробнее", callback_data=f"deal_details:{deal['id']}")
    ])
    
    if status not in [OrderStatus.DELIVERED, OrderStatus.CANCELLED]:
        buttons.append([
            InlineKeyboardButton(text="🚫 Отменить сделку", callback_data=f"cancel_deal:{deal['id']}")
        ])
    
    kb = InlineKeyboardMarkup(inline_keyboard=buttons) if buttons else None
    
    await bot.send_message(user_id, caption, reply_markup=kb)

# ---------------------------------------------------------------------------
#  Settings and support
# ---------------------------------------------------------------------------

@router.message(F.text == "⚙️ Настройки")
async def cmd_settings(msg: Message, state: FSMContext) -> None:
    """Show settings menu."""
    await state.clear()
    
    user = get_or_create_user(msg.from_user)
    
    settings_text = (
        "<b>Настройки</b>\n\n"
        f"🔔 Уведомления: {'✅ Включены' if user['notifications'] else '❌ Выключены'}\n"
        f"🌐 Язык: {user['language'].upper()}\n"
    )
    
    if user['phone']:
        settings_text += f"📱 Телефон: {user['phone']}\n"
    if user['email']:
        settings_text += f"📧 Email: {user['email']}\n"
    
    buttons = [
        [
            InlineKeyboardButton(
                text="🔔 Уведомления", 
                callback_data="settings:notifications"
            ),
            InlineKeyboardButton(
                text="🌐 Язык", 
                callback_data="settings:language"
            )
        ],
        [
            InlineKeyboardButton(
                text="📱 Телефон", 
                callback_data="settings:phone"
            ),
            InlineKeyboardButton(
                text="📧 Email", 
                callback_data="settings:email"
            )
        ],
        [
            InlineKeyboardButton(
                text="🗑 Удалить аккаунт", 
                callback_data="settings:delete_account"
            )
        ]
    ]
    
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    await msg.answer(
        settings_text,
        reply_markup=kb
    )

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

# ---------------------------------------------------------------------------
#  Background tasks runner
# ---------------------------------------------------------------------------

async def run_background_tasks():
    """Run periodic background tasks."""
    last_daily_report = None
    
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
            
        except Exception as e:
            logger.error(f"Error in background tasks: {e}")
        
        # Run every hour
        await asyncio.sleep(3600)

# ---------------------------------------------------------------------------
#  Main entry point
# ---------------------------------------------------------------------------

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
