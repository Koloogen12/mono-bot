"""
Модуль для создания и управления групповыми чатами сделок
Использует Telethon для создания групп с участниками сделки
"""
import os
import logging
import asyncio
from typing import List, Optional, Dict, Any
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import UserPrivacyRestrictedError, PeerFloodError, ChatAdminRequiredError
from telethon.tl.functions.messages import CreateChatRequest, AddChatUserRequest
from telethon.tl.functions.channels import CreateChannelRequest, InviteToChannelRequest
from telethon.tl.types import InputPeerUser, InputPeerChat

logger = logging.getLogger(__name__)

class ChatManager:
    """Менеджер для создания и управления чатами сделок."""
    
    def __init__(self):
        """Инициализация клиента Telethon."""
        self.api_id = int(os.getenv('API_ID', '25651355'))
        self.api_hash = os.getenv('API_HASH', '216ecff1bbd5b60a8d8734d84013f028')
        self.session_string = os.getenv('SESSION_STRING', 
            '1ApWapzMBuwJjVnIFToiUTfp8WvLrUZeafrI3zgXmDOHHk4oNUWTH1sOStWPmsebaBDkpj5mmaWu8LMtQ9qGiDjxn6nhWqZ4CaQQdpoF-e_Eg5kXfc01s6moIFITbD9yHSqnpUp20K_smQZMS8pP-5VUHFqh7EITkkie-s_BcnHJMa0tan-QsjsMx99Zsjl9wPIMREjBOAGFR5rFwItDEzi6nExZNW3DcNxoBk1UecJ_kcrbPYg5xD-Bu7uF3lh2vK1y4LgnEzrDwK-6oUT6S7Q24Gq2G5psc-kvWsGXU7UP_diP0jcabVWMZlgR3pvkVO1b9ugi6qUHyIFk1RiinljcZLVid_H4='
        )
        
        if not self.session_string:
            raise ValueError("SESSION_STRING не найден в переменных окружения")
        
        self.client = TelegramClient(
            StringSession(self.session_string), 
            self.api_id, 
            self.api_hash
        )
        self._initialized = False
    
    async def init(self):
        """Инициализация клиента."""
        if not self._initialized:
            try:
                await self.client.start()
                me = await self.client.get_me()
                logger.info(f"Telethon клиент инициализирован: {me.username}")
                self._initialized = True
            except Exception as e:
                logger.error(f"Ошибка инициализации Telethon: {e}")
                raise
    
    async def create_deal_chat(
        self, 
        deal_id: int, 
        buyer_id: int, 
        factory_id: int, 
        admin_ids: List[int],
        order_title: str = ""
    ) -> Optional[Dict[str, Any]]:
        """
        Создать групповой чат для сделки.
        
        Args:
            deal_id: ID сделки
            buyer_id: Telegram ID покупателя
            factory_id: Telegram ID фабрики
            admin_ids: Список Telegram ID админов
            order_title: Название заказа
            
        Returns:
            Словарь с информацией о созданном чате или None
        """
        await self.init()
        
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
            except Exception as e:
                logger.error(f"Не удалось найти покупателя {buyer_id}: {e}")
                return None
            
            # Добавляем фабрику
            try:
                factory = await self.client.get_entity(factory_id)
                participants.append(factory)
                factory_name = factory.first_name or f"Фабрика {factory_id}"
                participant_names.append(factory_name)
            except Exception as e:
                logger.error(f"Не удалось найти фабрику {factory_id}: {e}")
                return None
            
            # Добавляем админов
            for admin_id in admin_ids[:3]:  # Максимум 3 админа
                try:
                    admin = await self.client.get_entity(admin_id)
                    participants.append(admin)
                    participant_names.append("Админ")
                except Exception as e:
                    logger.warning(f"Не удалось найти админа {admin_id}: {e}")
            
            if len(participants) < 2:
                logger.error("Недостаточно участников для создания чата")
                return None
            
            # Создаем название чата
            title_parts = []
            if order_title:
                title_parts.append(order_title[:20])
            title_parts.append(f"Сделка #{deal_id}")
            chat_title = " - ".join(title_parts)
            
            # Создаем обычную группу (работает лучше чем супергруппа)
            try:
                # Берем первого участника (кроме себя) для создания чата
                first_user = participants[0]
                
                result = await self.client(CreateChatRequest(
                    users=[first_user],
                    title=chat_title[:255]  # Ограничение Telegram
                ))
                
                chat_id = result.chats[0].id
                logger.info(f"Создан чат {chat_id} для сделки {deal_id}")
                
                # Добавляем остальных участников
                for participant in participants[1:]:
                    try:
                        await asyncio.sleep(1)  # Задержка между добавлениями
                        await self.client(AddChatUserRequest(
                            chat_id=chat_id,
                            user_id=participant,
                            fwd_limit=0
                        ))
                        logger.info(f"Добавлен участник {participant.id} в чат {chat_id}")
                    except UserPrivacyRestrictedError:
                        logger.warning(f"Пользователь {participant.id} запретил добавление в группы")
                    except PeerFloodError:
                        logger.warning(f"Превышен лимит добавления пользователей")
                        await asyncio.sleep(5)
                    except Exception as e:
                        logger.error(f"Ошибка добавления {participant.id}: {e}")
                
                # Отправляем приветственное сообщение
                welcome_message = (
                    f"🤝 <b>Чат сделки #{deal_id}</b>\n\n"
                    f"Участники:\n"
                    f"👤 Заказчик: {participant_names[0]}\n"
                    f"🏭 Фабрика: {participant_names[1]}\n"
                )
                
                if len(participant_names) > 2:
                    welcome_message += f"👨‍💼 Поддержка: {', '.join(participant_names[2:])}\n"
                
                welcome_message += (
                    f"\n📋 Здесь вы можете обсуждать детали заказа, "
                    f"делиться файлами и решать вопросы по сделке.\n\n"
                    f"⚡ Чат создается автоматически и будет активен до завершения сделки."
                )
                
                await self.client.send_message(chat_id, welcome_message, parse_mode='html')
                
                # Получаем ссылку на чат
                chat_link = f"https://t.me/c/{chat_id}"
                
                return {
                    'chat_id': chat_id,
                    'chat_title': chat_title,
                    'chat_link': chat_link,
                    'participants': [p.id for p in participants],
                    'participant_names': participant_names
                }
                
            except Exception as e:
                logger.error(f"Ошибка создания группы: {e}")
                return None
                
        except Exception as e:
            logger.error(f"Общая ошибка создания чата для сделки {deal_id}: {e}")
            return None
    
    async def send_message_to_chat(self, chat_id: int, message: str, parse_mode: str = 'html') -> bool:
        """
        Отправить сообщение в чат сделки.
        
        Args:
            chat_id: ID чата
            message: Текст сообщения
            parse_mode: Формат разметки
            
        Returns:
            True если отправлено успешно
        """
        await self.init()
        
        try:
            await self.client.send_message(chat_id, message, parse_mode=parse_mode)
            return True
        except Exception as e:
            logger.error(f"Ошибка отправки сообщения в чат {chat_id}: {e}")
            return False
    
    async def add_user_to_chat(self, chat_id: int, user_id: int) -> bool:
        """
        Добавить пользователя в чат.
        
        Args:
            chat_id: ID чата
            user_id: ID пользователя
            
        Returns:
            True если добавлен успешно
        """
        await self.init()
        
        try:
            user = await self.client.get_entity(user_id)
            await self.client(AddChatUserRequest(
                chat_id=chat_id,
                user_id=user,
                fwd_limit=0
            ))
            logger.info(f"Пользователь {user_id} добавлен в чат {chat_id}")
            return True
        except Exception as e:
            logger.error(f"Ошибка добавления пользователя {user_id} в чат {chat_id}: {e}")
            return False
    
    async def close(self):
        """Закрыть клиент."""
        if self._initialized:
            await self.client.disconnect()
            self._initialized = False

# Глобальный экземпляр менеджера чатов
_chat_manager = None

async def get_chat_manager() -> ChatManager:
    """Получить глобальный экземпляр менеджера чатов."""
    global _chat_manager
    if _chat_manager is None:
        _chat_manager = ChatManager()
        await _chat_manager.init()
    return _chat_manager

async def create_deal_chat(deal_id: int, buyer_id: int, factory_id: int, admin_ids: List[int], order_title: str = "") -> Optional[Dict[str, Any]]:
    """Удобная функция для создания чата сделки."""
    manager = await get_chat_manager()
    return await manager.create_deal_chat(deal_id, buyer_id, factory_id, admin_ids, order_title)

async def send_deal_message(chat_id: int, message: str) -> bool:
    """Удобная функция для отправки сообщения в чат сделки."""
    manager = await get_chat_manager()
    return await manager.send_message_to_chat(chat_id, message)
