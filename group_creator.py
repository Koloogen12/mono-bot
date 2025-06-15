import asyncio
import logging
import os
from typing import Optional, Tuple, Dict, Any
from contextlib import asynccontextmanager

try:
    from telethon import TelegramClient, errors
    from telethon.tl.functions.messages import CreateChatRequest, ExportChatInviteRequest
    TELETHON_AVAILABLE = True
except ImportError:
    TELETHON_AVAILABLE = False

logger = logging.getLogger("group_creator")

class TelegramGroupCreator:
    """
    Creates and manages Telegram group chats for deals.
    Uses TELETHON USER SESSION, NOT BOT TOKEN!
    """
    def __init__(self, api_id: str, api_hash: str, session_name: str = "fabrique"):
        if not TELETHON_AVAILABLE:
            raise ImportError("telethon is required for group creation. Install with: pip install telethon")
        self.api_id = int(api_id)
        self.api_hash = api_hash
        self.session_name = session_name

    @asynccontextmanager
    async def get_client(self):
        loop = None
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        client = TelegramClient(self.session_name, self.api_id, self.api_hash, loop=loop)
        try:
            await client.start()
            logger.info("Telegram client (user) started successfully")
            yield client
        except Exception as e:
            logger.error(f"Failed to start Telegram client: {e}")
            raise
        finally:
            try:
                if client.is_connected():
                    await client.disconnect()
                    logger.info("Telegram client disconnected")
            except Exception as e:
                logger.warning(f"Error disconnecting client: {e}")

    async def create_deal_group(
        self, 
        deal_id: int, 
        deal_title: str,
        factory_name: str,
        buyer_name: str
    ) -> Tuple[Optional[int], str, Optional[str]]:
        """
        Create a group chat for a deal and generate an invite link.
        Returns:
            Tuple[Optional[int], status_message, Optional[invite_link]]
        """
        if not TELETHON_AVAILABLE:
            return None, "Telethon not available", None

        group_title = f"🤝 Сделка #{deal_id} - {deal_title[:20]}..."
        try:
            async with self.get_client() as client:
                me = await client.get_me()
                result = await client(CreateChatRequest(
                    users=[me],    # Создаём только с собой
                    title=group_title
                ))
                if hasattr(result, 'chats') and result.chats:
                    chat = result.chats[0]
                    chat_id = chat.id
                    logger.info(f"Created group chat with ID: {chat_id}")
                else:
                    return None, "Failed to get chat ID from create result", None

                # Отправляем приветственное сообщение
                try:
                    welcome_message = (
                        f"🤝 <b>Групповой чат сделки #{deal_id}</b>\n\n"
                        f"📦 <b>Заказ:</b> {deal_title}\n"
                        f"🏭 <b>Фабрика:</b> {factory_name}\n"
                        f"👤 <b>Заказчик:</b> {buyer_name}\n\n"
                        f"💬 Здесь вы можете обсуждать детали заказа, делиться файлами и отслеживать прогресс.\n\n"
                        f"ℹ️ Все сообщения сохраняются для безопасности сделки."
                    )
                    await client.send_message(chat_id, welcome_message, parse_mode='html')
                    logger.info(f"Sent welcome message to group {chat_id}")
                except Exception as e:
                    logger.warning(f"Failed to send welcome message: {e}")

                # Получаем инвайт-ссылку
                try:
                    invite = await client(ExportChatInviteRequest(chat_id))
                    logger.info(f"Generated invite link: {invite.link}")
                except Exception as e:
                    logger.error(f"Failed to create invite link: {e}")
                    return chat_id, "Group created but failed to get invite link", None

                return chat_id, f"Group created successfully with title: {group_title}", invite.link

        except Exception as e:
            logger.error(f"Unexpected error creating group: {e}")
            return None, f"Unexpected error: {e}", None

# Main function to create deal chat
async def create_deal_chat_real(
    deal_id: int,
    deal_title: str,
    factory_name: str,
    buyer_name: str
) -> Tuple[Optional[int], str, Optional[str]]:
    """
    Create a real group chat for a deal and return invite link.
    Returns:
        Tuple[Optional[int], status_message, Optional[invite_link]]
    """
    api_id = os.getenv("TELEGRAM_API_ID")
    api_hash = os.getenv("TELEGRAM_API_HASH")

    if not api_id:
        return None, "TELEGRAM_API_ID not set", None
    if not api_hash:
        return None, "TELEGRAM_API_HASH not set", None

    try:
        creator = TelegramGroupCreator(api_id, api_hash)
        return await creator.create_deal_group(
            deal_id=deal_id,
            deal_title=deal_title,
            factory_name=factory_name,
            buyer_name=buyer_name
        )
    except Exception as e:
        logger.error(f"Error in create_deal_chat_real: {e}")
        return None, str(e), None

# Test function
async def test_group_creation():
    api_id = os.getenv("TELEGRAM_API_ID")
    api_hash = os.getenv("TELEGRAM_API_HASH")

    if not all([api_id, api_hash]):
        print("Missing environment variables")
        return

    creator = TelegramGroupCreator(api_id, api_hash, session_name="fabrique")

    chat_id, result, invite_link = await creator.create_deal_group(
        deal_id=999,
        deal_title="Test Deal",
        factory_name="Test Factory",
        buyer_name="Test Buyer"
    )

    print(f"Result: {result}")
    print(f"Chat ID: {chat_id}")
    print(f"Invite Link: {invite_link}")

if __name__ == "__main__":
    asyncio.run(test_group_creation())
