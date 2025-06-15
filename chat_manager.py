"""
Улучшенный модуль для создания и управления групповыми чатами сделок
Использует Telethon с retry логикой и лучшей обработкой ошибок
"""
import os
import logging
import asyncio
from typing import List, Optional, Dict, Any
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import (
    UserPrivacyRestrictedError, 
    PeerFloodError, 
    ChatAdminRequiredError,
    SessionPasswordNeededError,
    PhoneCodeInvalidError,
    AuthKeyDuplicatedError,
    UnauthorizedError,
    FloodWaitError
)
from telethon.tl.functions.messages import CreateChatRequest, AddChatUserRequest
from telethon.tl.functions.channels import CreateChannelRequest, InviteToChannelRequest
from telethon.tl.types import InputPeerUser, InputPeerChat

logger = logging.getLogger("group_creator")

class ChatManager:
    """Менеджер для создания и управления чатами сделок с улучшенной обработкой ошибок."""
    
    def __init__(self):
        """Инициализация клиента Telethon."""
        self.api_id = int(os.getenv('API_ID', '25651355'))
        self.api_hash = os.getenv('API_HASH', '216ecff1bbd5b60a8d8734d84013f028')
        self.session_string = os.getenv('SESSION_STRING', '')
        
        if not self.session_string:
            raise ValueError("SESSION_STRING не найден в переменных окружения")
        
        logger.info(f"Initializing ChatManager with API_ID: {self.api_id}")
        
        self.client = TelegramClient(
            StringSession(self.session_string), 
            self.api_id, 
            self.api_hash,
            # Дополнительные параметры для стабильности
            connection_retries=5,
            retry_delay=1,
            timeout=30,
            request_retries=3,
            flood_sleep_threshold=60
        )
        self._initialized = False
    
    async def init(self, max_retries: int = 3):
        """Инициализация клиента с retry логикой."""
        if self._initialized:
            return True
        
        for attempt in range(max_retries):
            try:
                logger.info(f"Attempting to connect to Telegram (attempt {attempt + 1}/{max_retries})")
                
                await self.client.start()
                
                # Проверяем что мы действительно подключены
                me = await self.client.get_me()
                logger.info(f"✅ Telegram client connected successfully: {me.username or me.phone}")
                
                self._initialized = True
                return True
                
            except (ConnectionError, OSError, TimeoutError) as e:
                logger.warning(f"Network error on attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff
                    continue
                else:
                    logger.error(f"Failed to connect after {max_retries} attempts")
                    return False
                    
            except UnauthorizedError as e:
                logger.error(f"Authorization error: {e}")
                logger.error("Session string might be invalid. Try recreating it.")
                return False
                
            except Exception as e:
                logger.error(f"Unexpected error during initialization: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                    continue
                else:
                    return False
        
        return False
    
    async def create_deal_chat(
        self, 
        deal_id: int, 
        buyer_id: int, 
        factory_id: int, 
        admin_ids: List[int],
        order_title: str = ""
    ) -> Optional[Dict[str, Any]]:
        """
        Создать групповой чат для сделки с улучшенной обработкой ошибок.
        """
        logger.info(f"Creating chat for deal {deal_id} with buyer {buyer_id} and factory {factory_id}")
        
        # Инициализируем клиента
        if not await self.init():
            logger.error("Failed to initialize Telegram client")
            return None
        
        try:
            # Получаем информацию об участниках
            participants = []
            participant_names = []
            
            # Добавляем покупателя
            try:
                buyer = await self.client.get_entity(buyer_id)
                participants.append(buyer)
                buyer_name = buyer.first_name or f"Заказчик {buyer_id}"
                participant_names.append(buyer_name)
                logger.info(f"✅ Found buyer: {buyer_name}")
            except Exception as e:
                logger.error(f"❌ Failed to find buyer {buyer_id}: {e}")
                return None
            
            # Добавляем фабрику
            try:
                factory = await self.client.get_entity(factory_id)
                participants.append(factory)
                factory_name = factory.first_name or f"Фабрика {factory_id}"
                participant_names.append(factory_name)
                logger.info(f"✅ Found factory: {factory_name}")
            except Exception as e:
                logger.error(f"❌ Failed to find factory {factory_id}: {e}")
                return None
            
            # Добавляем админов (максимум 3)
            admin_count = 0
            for admin_id in admin_ids[:3]:
                try:
                    admin = await self.client.get_entity(admin_id)
                    participants.append(admin)
                    participant_names.append("Поддержка")
                    admin_count += 1
                    logger.info(f"✅ Added admin: {admin_id}")
                except Exception as e:
                    logger.warning(f"⚠️ Failed to add admin {admin_id}: {e}")
            
            if len(participants) < 2:
                logger.error("❌ Not enough participants for chat creation")
                return None
            
            # Создаем название чата
            title_parts = []
            if order_title:
                title_parts.append(order_title[:20])
            title_parts.append(f"Сделка #{deal_id}")
            chat_title = " - ".join(title_parts)[:255]  # Ограничение Telegram
            
            logger.info(f"Creating chat with title: {chat_title}")
            
            # Создаем обычную группу
            try:
                # Создаем группу с первым участником
                first_user = participants[0]
                
                result = await self.client(CreateChatRequest(
                    users=[first_user],
                    title=chat_title
                ))
                
                if not result.chats:
                    logger.error("❌ No chats returned from CreateChatRequest")
                    return None
                
                chat = result.chats[0]
                chat_id = chat.id
                
                # Делаем chat_id отрицательным (как у настоящих групп)
                if chat_id > 0:
                    chat_id = -chat_id
                
                logger.info(f"✅ Created chat with ID: {chat_id}")
                
                # Добавляем остальных участников с задержками
                for i, participant in enumerate(participants[1:], 1):
                    try:
                        await asyncio.sleep(1)  # Задержка между добавлениями
                        
                        await self.client(AddChatUserRequest(
                            chat_id=abs(chat_id),  # AddChatUserRequest требует положительный ID
                            user_id=participant,
                            fwd_limit=0
                        ))
                        
                        logger.info(f"✅ Added participant {i+1}/{len(participants)}: {participant.id}")
                        
                    except UserPrivacyRestrictedError:
                        logger.warning(f"⚠️ User {participant.id} has privacy restrictions")
                    except PeerFloodError:
                        logger.warning(f"⚠️ Rate limit exceeded, waiting...")
                        await asyncio.sleep(10)
                    except FloodWaitError as e:
                        logger.warning(f"⚠️ Flood wait error: {e.seconds} seconds")
                        await asyncio.sleep(min(e.seconds, 60))
                    except Exception as e:
                        logger.error(f"❌ Failed to add participant {participant.id}: {e}")
                
                # Создаем ссылку на чат
                chat_link = f"https://t.me/c/{abs(chat_id)}"
                
                # Отправляем приветственное сообщение
                welcome_message = (
                    f"🤝 <b>Чат сделки #{deal_id}</b>\n\n"
                    f"Участники:\n"
                    f"👤 Заказчик: {participant_names[0]}\n"
                    f"🏭 Фабрика: {participant_names[1]}\n"
                )
                
                if admin_count > 0:
                    welcome_message += f"👨‍💼 Поддержка: {admin_count} чел.\n"
                
                welcome_message += (
                    f"\n📋 Здесь вы можете обсуждать детали заказа, "
                    f"делиться файлами и решать вопросы по сделке.\n\n"
                    f"⚡ Чат создается автоматически и будет активен до завершения сделки."
                )
                
                try:
                    await self.client.send_message(chat_id, welcome_message, parse_mode='html')
                    logger.info(f"✅ Welcome message sent to chat {chat_id}")
                except Exception as e:
                    logger.warning(f"⚠️ Failed to send welcome message: {e}")
                
                return {
                    'chat_id': chat_id,
                    'chat_title': chat_title,
                    'chat_link': chat_link,
                    'participants': [p.id for p in participants],
                    'participant_names': participant_names
                }
                
            except Exception as e:
                logger.error(f"❌ Failed to create group: {e}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Unexpected error creating group for deal {deal_id}: {e}")
            return None
    
    async def send_message_to_chat(self, chat_id: int, message: str, parse_mode: str = 'html') -> bool:
        """
        Отправить сообщение в чат сделки с retry логикой.
        """
        if not await self.init():
            return False
        
        for attempt in range(3):
            try:
                await self.client.send_message(chat_id, message, parse_mode=parse_mode)
                logger.info(f"✅ Message sent to chat {chat_id}")
                return True
                
            except FloodWaitError as e:
                logger.warning(f"⚠️ Flood wait: {e.seconds}s")
                if attempt < 2:
                    await asyncio.sleep(min(e.seconds, 60))
                    continue
                return False
                
            except Exception as e:
                logger.error(f"❌ Failed to send message to chat {chat_id}: {e}")
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)
                    continue
                return False
        
        return False
    
    async def close(self):
        """Закрыть клиент."""
        if self._initialized:
            try:
                await self.client.disconnect()
                logger.info("Telegram client disconnected")
                self._initialized = False
            except Exception as e:
                logger.error(f"Error disconnecting client: {e}")

# Глобальный экземпляр менеджера чатов
_chat_manager = None

async def get_chat_manager() -> ChatManager:
    """Получить глобальный экземпляр менеджера чатов."""
    global _chat_manager
    if _chat_manager is None:
        _chat_manager = ChatManager()
        success = await _chat_manager.init()
        if not success:
            logger.error("❌ Failed to initialize ChatManager")
            return None
    return _chat_manager

async def create_deal_chat(deal_id: int, buyer_id: int, factory_id: int, admin_ids: List[int], order_title: str = "") -> Optional[Dict[str, Any]]:
    """Удобная функция для создания чата сделки."""
    manager = await get_chat_manager()
    if manager is None:
        logger.error(f"❌ Failed to get chat manager for deal {deal_id}")
        return None
    
    return await manager.create_deal_chat(deal_id, buyer_id, factory_id, admin_ids, order_title)

async def send_deal_message(chat_id: int, message: str) -> bool:
    """Удобная функция для отправки сообщения в чат сделки."""
    manager = await get_chat_manager()
    if manager is None:
        return False
    
    return await manager.send_message_to_chat(chat_id, message)

# Функция для тестирования подключения
async def test_connection() -> bool:
    """Тестовая функция для проверки подключения."""
    try:
        manager = ChatManager()
        success = await manager.init()
        if success:
            await manager.close()
            return True
        return False
    except Exception as e:
        logger.error(f"Connection test failed: {e}")
        return False
