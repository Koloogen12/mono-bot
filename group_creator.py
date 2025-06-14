"""
Модуль для создания групповых чатов в Telegram
============================================

Требования:
1. pip install pyrogram
2. Получить API_ID и API_HASH на https://my.telegram.org/apps
3. Настроить переменные окружения или передать параметры напрямую

Переменные окружения:
- TELEGRAM_API_ID - 25651355
- TELEGRAM_API_HASH - 216ecff1bbd5b60a8d8734d84013f028
- TELEGRAM_BOT_TOKEN - 7872394424:AAE0sUBNy2p61kXw4XlZTv3JQp9wB8B_fmY
"""

import asyncio
import logging
import os
from typing import List, Optional, Tuple
from pyrogram import Client
from pyrogram.errors import (
    UserPrivacyRestricted, 
    UserNotMutualContact, 
    FloodWait,
    ChatAdminRequired,
    UserAlreadyParticipant
)

logger = logging.getLogger("group_creator")

class TelegramGroupCreator:
    """Класс для создания групповых чатов в Telegram"""
    
    def __init__(self, api_id: str = None, api_hash: str = None, bot_token: str = None):
        """
        Инициализация клиента
        
        Args:
            api_id: API ID от Telegram (или из env TELEGRAM_API_ID)
            api_hash: API Hash от Telegram (или из env TELEGRAM_API_HASH) 
            bot_token: Bot token (или из env TELEGRAM_BOT_TOKEN)
        """
        self.api_id = api_id or os.getenv("TELEGRAM_API_ID")
        self.api_hash = api_hash or os.getenv("TELEGRAM_API_HASH")
        self.bot_token = bot_token or os.getenv("TELEGRAM_BOT_TOKEN")
        
        if not all([self.api_id, self.api_hash, self.bot_token]):
            raise ValueError("Необходимо указать API_ID, API_HASH и BOT_TOKEN")
        
        # Создаем клиент для бота
        self.client = Client(
            "mono_fabrique_bot",
            api_id=int(self.api_id),
            api_hash=self.api_hash,
            bot_token=self.bot_token
        )
        
    async def create_deal_group(
        self, 
        deal_id: int, 
        buyer_id: int, 
        factory_id: int, 
        admin_ids: List[int],
        deal_title: str,
        factory_name: str,
        buyer_name: str
    ) -> Tuple[Optional[int], Optional[str]]:
        """
        Создает групповой чат для сделки
        
        Args:
            deal_id: ID сделки
            buyer_id: Telegram ID покупателя
            factory_id: Telegram ID фабрики
            admin_ids: Список Telegram ID администраторов
            deal_title: Название заказа
            factory_name: Название фабрики
            buyer_name: Имя покупателя
            
        Returns:
            Tuple[chat_id, invite_link] или (None, error_message)
        """
        try:
            async with self.client:
                # Создаем группу
                group_title = f"Сделка #{deal_id} - {deal_title[:20]}..."
                
                # Собираем всех участников
                participants = [buyer_id, factory_id] + admin_ids
                
                # Создаем группу с участниками
                group = await self.client.create_group(
                    title=group_title,
                    users=participants
                )
                
                chat_id = group.id
                
                # Устанавливаем описание группы
                description = (
                    f"Групповой чат по сделке #{deal_id}\n"
                    f"📦 Заказ: {deal_title}\n"
                    f"🏭 Фабрика: {factory_name}\n"
                    f"👤 Заказчик: {buyer_name}\n\n"
                    f"Здесь вы можете обсуждать детали заказа и отслеживать прогресс."
                )
                
                await self.client.set_chat_description(chat_id, description)
                
                # Создаем инвайт-ссылку на случай, если кто-то покинет группу
                invite_link = await self.client.create_chat_invite_link(chat_id)
                
                # Отправляем приветственное сообщение
                welcome_msg = (
                    f"🤝 <b>Добро пожаловать в чат сделки #{deal_id}!</b>\n\n"
                    f"📦 Заказ: {deal_title}\n"
                    f"🏭 Фабрика: {factory_name}\n"
                    f"👤 Заказчик: {buyer_name}\n\n"
                    f"Здесь вы можете:\n"
                    f"• Обсуждать детали заказа\n"
                    f"• Задавать вопросы\n"
                    f"• Отслеживать прогресс\n"
                    f"• Делиться фото и документами\n\n"
                    f"Администрация платформы участвует в чате для решения любых вопросов."
                )
                
                await self.client.send_message(chat_id, welcome_msg)
                
                logger.info(f"Created group {chat_id} for deal {deal_id}")
                return chat_id, invite_link.invite_link
                
        except UserPrivacyRestricted as e:
            error_msg = "Один из участников ограничил добавление в группы"
            logger.error(f"Privacy error creating group for deal {deal_id}: {e}")
            return None, error_msg
            
        except UserNotMutualContact as e:
            error_msg = "Не все участники являются взаимными контактами"
            logger.error(f"Contact error creating group for deal {deal_id}: {e}")
            return None, error_msg
            
        except FloodWait as e:
            error_msg = f"Превышен лимит запросов. Попробуйте через {e.x} секунд"
            logger.error(f"Flood wait creating group for deal {deal_id}: {e}")
            return None, error_msg
            
        except Exception as e:
            error_msg = f"Ошибка создания группы: {str(e)}"
            logger.error(f"Error creating group for deal {deal_id}: {e}")
            return None, error_msg
    
    async def add_user_to_group(self, chat_id: int, user_id: int) -> bool:
        """
        Добавляет пользователя в существующую группу
        
        Args:
            chat_id: ID группы
            user_id: Telegram ID пользователя
            
        Returns:
            True если успешно, False если ошибка
        """
        try:
            async with self.client:
                await self.client.add_chat_members(chat_id, user_id)
                logger.info(f"Added user {user_id} to group {chat_id}")
                return True
                
        except UserAlreadyParticipant:
            logger.info(f"User {user_id} already in group {chat_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error adding user {user_id} to group {chat_id}: {e}")
            return False
    
    async def send_message_to_group(self, chat_id: int, message: str) -> bool:
        """
        Отправляет сообщение в группу
        
        Args:
            chat_id: ID группы
            message: Текст сообщения
            
        Returns:
            True если успешно, False если ошибка
        """
        try:
            async with self.client:
                await self.client.send_message(chat_id, message)
                return True
                
        except Exception as e:
            logger.error(f"Error sending message to group {chat_id}: {e}")
            return False
    
    async def create_invite_link(self, chat_id: int, expire_date: int = None) -> Optional[str]:
        """
        Создает инвайт-ссылку для группы
        
        Args:
            chat_id: ID группы
            expire_date: Unix timestamp истечения ссылки (необязательно)
            
        Returns:
            Инвайт-ссылка или None при ошибке
        """
        try:
            async with self.client:
                invite_link = await self.client.create_chat_invite_link(
                    chat_id, 
                    expire_date=expire_date
                )
                return invite_link.invite_link
                
        except Exception as e:
            logger.error(f"Error creating invite link for group {chat_id}: {e}")
            return None
    
    async def get_group_info(self, chat_id: int) -> Optional[dict]:
        """
        Получает информацию о группе
        
        Args:
            chat_id: ID группы
            
        Returns:
            Словарь с информацией о группе или None при ошибке
        """
        try:
            async with self.client:
                chat = await self.client.get_chat(chat_id)
                
                return {
                    "id": chat.id,
                    "title": chat.title,
                    "description": chat.description,
                    "members_count": chat.members_count,
                    "type": str(chat.type),
                    "username": chat.username
                }
                
        except Exception as e:
            logger.error(f"Error getting group info for {chat_id}: {e}")
            return None

# Функция-обертка для интеграции с основным ботом
async def create_deal_chat_real(
    deal_id: int,
    buyer_id: int, 
    factory_id: int,
    admin_ids: List[int],
    deal_title: str,
    factory_name: str,
    buyer_name: str
) -> Tuple[Optional[int], Optional[str]]:
    """
    Создает реальный групповой чат для сделки
    
    Returns:
        Tuple[chat_id, invite_link] или (None, error_message)
    """
    creator = TelegramGroupCreator()
    
    return await creator.create_deal_group(
        deal_id=deal_id,
        buyer_id=buyer_id,
        factory_id=factory_id,
        admin_ids=admin_ids,
        deal_title=deal_title,
        factory_name=factory_name,
        buyer_name=buyer_name
    )

# Пример использования
async def example_usage():
    """Пример использования модуля"""
    
    # Создание группы для сделки
    chat_id, result = await create_deal_chat_real(
        deal_id=123,
        buyer_id=111111111,  # Telegram ID покупателя
        factory_id=222222222,  # Telegram ID фабрики  
        admin_ids=[333333333],  # Telegram ID админов
        deal_title="Футболки с принтом 500шт",
        factory_name="Текстиль Плюс",
        buyer_name="Светлана"
    )
    
    if chat_id:
        print(f"Группа создана! ID: {chat_id}")
        print(f"Инвайт-ссылка: {result}")
    else:
        print(f"Ошибка: {result}")

if __name__ == "__main__":
    # Запуск примера
    asyncio.run(example_usage())
