"""
Group creator module for creating Telegram group chats for deals.
Handles real group creation using Telegram Client API (user session).
"""
import asyncio
import logging
import os
from typing import Optional, Tuple, Dict, Any, List
from contextlib import asynccontextmanager

try:
    import telethon
    TELETHON_AVAILABLE = True
except ImportError:
    TELETHON_AVAILABLE = False

if TELETHON_AVAILABLE:
    from telethon import TelegramClient, errors
    from telethon.tl.functions.messages import CreateChatRequest, AddChatUserRequest, ExportChatInviteRequest
    from telethon.tl.types import InputPeerUser

logger = logging.getLogger("group_creator")

class TelegramGroupCreator:
    """
    Creates and manages Telegram group chats for deals.
    Uses TELETHON USER SESSION, NOT BOT TOKEN!
    """
    def __init__(self, api_id: str, api_hash: str, session_name: str = "fabrique_group_creator"):
        if not TELETHON_AVAILABLE:
            raise ImportError("telethon is required for group creation. Install with: pip install telethon")
        self.api_id = int(api_id)
        self.api_hash = api_hash
        self.session_name = session_name

    @asynccontextmanager
    async def get_client(self):
        """
        Context manager for Telegram client with event loop handling.
        Uses user session (will prompt for phone & code on first run).
        """
        loop = None
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        client = TelegramClient(self.session_name, self.api_id, self.api_hash, loop=loop)
        try:
            await client.start()  # USER SESSION! Will ask phone/code on first run
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
        buyer_id: int, 
        factory_id: int, 
        admin_ids: List[int],
        deal_title: str,
        factory_name: str,
        buyer_name: str
    ) -> Tuple[Optional[int], str]:
        """
        Create a group chat for a deal.
        Returns:
            Tuple[Optional[int], str]: (chat_id, status_message)
        """
        if not TELETHON_AVAILABLE:
            return None, "Telethon not available"

        group_title = f"🤝 Сделка #{deal_id} - {deal_title[:20]}..."
        try:
            async with self.get_client() as client:
                # Get user entities
                user_entities = {}
                all_user_ids = [buyer_id, factory_id] + admin_ids
                for user_id in all_user_ids:
                    try:
                        entity = await client.get_entity(user_id)
                        user_entities[user_id] = entity
                        logger.info(f"Got entity for user {user_id}: {getattr(entity, 'username', 'no username')}")
                    except Exception as e:
                        logger.warning(f"Could not get entity for user {user_id}: {e}")
                        continue

                # For group creation need at least 2 unique users
                unique_users = []
                if buyer_id in user_entities:
                    unique_users.append(user_entities[buyer_id])
                if factory_id in user_entities and factory_id != buyer_id:
                    unique_users.append(user_entities[factory_id])
                # Optional: add first admin if not already present
                for admin_id in admin_ids:
                    if admin_id in user_entities and admin_id not in [buyer_id, factory_id]:
                        unique_users.append(user_entities[admin_id])
                        break  # Only add one admin for initial creation

                if len(unique_users) < 2:
                    return None, f"Could not get entities for enough unique users to create a group. Got {len(unique_users)}."

                # Create the group
                try:
                    result = await client(CreateChatRequest(
                        users=unique_users,
                        title=group_title
                    ))
                    if hasattr(result, 'chats') and result.chats:
                        chat = result.chats[0]
                        chat_id = chat.id
                        logger.info(f"Created group chat with ID: {chat_id}")
                    else:
                        return None, "Failed to get chat ID from create result"
                except Exception as e:
                    logger.error(f"Failed to create group chat: {e}")
                    return None, f"Failed to create group: {e}"

                # Add any remaining admins not in the initial group
                initial_ids = [getattr(user, 'id', None) for user in unique_users]
                for user_id in admin_ids:
                    if user_id in user_entities and user_id not in initial_ids:
                        try:
                            await client(AddChatUserRequest(
                                chat_id=chat.id,
                                user_id=user_entities[user_id],
                                fwd_limit=0
                            ))
                            logger.info(f"Added admin {user_id} to group")
                        except Exception as e:
                            logger.warning(f"Failed to add admin {user_id}: {e}")
                            continue

                # Send welcome message
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

                # Wait a bit for group to fully initialize
                await asyncio.sleep(1)
                # Verify group exists by getting its info
                try:
                    chat_info = await client.get_entity(chat_id)
                    logger.info(f"Verified group exists: {chat_info.title}")
                    return chat_id, f"Group created successfully with title: {chat_info.title}"
                except Exception as e:
                    logger.error(f"Failed to verify created group: {e}")
                    return None, f"Group created but verification failed: {e}"

        except Exception as e:
            logger.error(f"Unexpected error creating group: {e}")
            return None, f"Unexpected error: {e}"

    async def get_group_info(self, chat_id: int) -> Optional[Dict[str, Any]]:
        """Get information about a group chat."""
        if not TELETHON_AVAILABLE:
            return None
        try:
            async with self.get_client() as client:
                try:
                    chat = await client.get_entity(chat_id)
                    # Get participants count
                    try:
                        participants = await client.get_participants(chat)
                        members_count = len(participants)
                    except Exception:
                        members_count = 0
                    return {
                        'id': chat_id,
                        'title': getattr(chat, 'title', 'Unknown'),
                        'members_count': members_count,
                        'type': 'group'
                    }
                except errors.PeerIdInvalidError:
                    logger.warning(f"Group {chat_id} not found or invalid")
                    return None
                except Exception as e:
                    logger.error(f"Error getting group info for {chat_id}: {e}")
                    return None
        except Exception as e:
            logger.error(f"Client error getting group info: {e}")
            return None

    async def create_invite_link(self, chat_id: int) -> Optional[str]:
        """Create an invite link for the group."""
        if not TELETHON_AVAILABLE:
            return None
        try:
            async with self.get_client() as client:
                try:
                    result = await client(ExportChatInviteRequest(
                        peer=chat_id
                    ))
                    return result.link
                except Exception as e:
                    logger.error(f"Failed to create invite link for {chat_id}: {e}")
                    return None
        except Exception as e:
            logger.error(f"Client error creating invite link: {e}")
            return None

    async def send_message_to_group(self, chat_id: int, message: str) -> bool:
        """Send a message to the group."""
        if not TELETHON_AVAILABLE:
            return False
        try:
            async with self.get_client() as client:
                await client.send_message(chat_id, message, parse_mode='html')
                return True
        except Exception as e:
            logger.error(f"Failed to send message to group {chat_id}: {e}")
            return False

# Main function to create deal chat
async def create_deal_chat_real(
    deal_id: int,
    buyer_id: int, 
    factory_id: int,
    admin_ids: List[int],
    deal_title: str,
    factory_name: str,
    buyer_name: str
) -> Tuple[Optional[int], str]:
    """
    Create a real group chat for a deal.
    Returns:
        Tuple[Optional[int], str]: (chat_id, status_message)
    """
    # Get environment variables
    api_id = os.getenv("TELEGRAM_API_ID")
    api_hash = os.getenv("TELEGRAM_API_HASH")

    # Validate environment variables
    if not api_id:
        return None, "TELEGRAM_API_ID not set"
    if not api_hash:
        return None, "TELEGRAM_API_HASH not set"

    try:
        creator = TelegramGroupCreator(api_id, api_hash)
        return await creator.create_deal_group(
            deal_id=deal_id,
            buyer_id=buyer_id,
            factory_id=factory_id,
            admin_ids=admin_ids,
            deal_title=deal_title,
            factory_name=factory_name,
            buyer_name=buyer_name
        )
    except Exception as e:
        logger.error(f"Error in create_deal_chat_real: {e}")
        return None, str(e)

# Test function
async def test_group_creation():
    """Test function for group creation."""
    api_id = os.getenv("TELEGRAM_API_ID")
    api_hash = os.getenv("TELEGRAM_API_HASH")

    if not all([api_id, api_hash]):
        print("Missing environment variables")
        return

    creator = TelegramGroupCreator(api_id, api_hash)

    # Test with some dummy data
    chat_id, result = await creator.create_deal_group(
        deal_id=999,
        buyer_id=123456789,  # Replace with real user ID for testing
        factory_id=987654321,  # Replace with real user ID for testing  
        admin_ids=[],
        deal_title="Test Deal",
        factory_name="Test Factory",
        buyer_name="Test Buyer"
    )

    print(f"Result: {result}")
    print(f"Chat ID: {chat_id}")

if __name__ == "__main__":
    asyncio.run(test_group_creation())
