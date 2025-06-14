"""
Group creator module for creating Telegram group chats for deals.
Handles real group creation using Telegram Client API.
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
    from telethon.tl.functions.messages import CreateChatRequest, AddChatUserRequest
    from telethon.tl.functions.channels import CreateChannelRequest, InviteToChannelRequest
    from telethon.tl.functions.channels import ExportChatInviteRequest
    from telethon.tl.types import PeerChannel, PeerChat, InputPeerUser

logger = logging.getLogger("group_creator")

class TelegramGroupCreator:
    """Creates and manages Telegram group chats for deals."""
    
    def __init__(self, api_id: str, api_hash: str, bot_token: str):
        if not TELETHON_AVAILABLE:
            raise ImportError("telethon is required for group creation. Install with: pip install telethon")
        
        self.api_id = int(api_id)
        self.api_hash = api_hash
        self.bot_token = bot_token
        self.session_name = f"mono_fabrique_bot_{api_id}"
        
    @asynccontextmanager
    async def get_client(self):
        """Context manager for Telegram client with event loop handling."""
        # Используем текущий event loop
        loop = None
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        client = TelegramClient(self.session_name, self.api_id, self.api_hash, loop=loop)
        try:
            await client.start(bot_token=self.bot_token)
            logger.info("Telegram client started successfully")
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
                # Get bot entity
                try:
                    bot_entity = await client.get_me()
                    logger.info(f"Bot entity: {bot_entity.username}")
                except Exception as e:
                    logger.error(f"Failed to get bot entity: {e}")
                    return None, f"Failed to get bot entity: {e}"
                
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
                        # Don't fail completely, just skip this user
                        continue
                
                if len(user_entities) < 2:
                    return None, f"Could not get entities for enough users. Got {len(user_entities)} out of {len(all_user_ids)}"
                
                # Create group chat (legacy groups work better than supergroups for small chats)
                try:
                    # Start with buyer and factory
                    initial_users = []
                    if buyer_id in user_entities:
                        initial_users.append(user_entities[buyer_id])
                    if factory_id in user_entities and factory_id != buyer_id:
                        initial_users.append(user_entities[factory_id])
                    
                    if not initial_users:
                        return None, "No valid users to create group with"
                    
                    # Create the group
                    result = await client(CreateChatRequest(
                        users=initial_users,
                        title=group_title
                    ))
                    
                    # Extract chat ID
                    if hasattr(result, 'chats') and result.chats:
                        chat = result.chats[0]
                        chat_id = -chat.id  # Make it negative for group chats
                        logger.info(f"Created group chat with ID: {chat_id}")
                    else:
                        return None, "Failed to get chat ID from create result"
                    
                except Exception as e:
                    logger.error(f"Failed to create group chat: {e}")
                    return None, f"Failed to create group: {e}"
                
                # Add remaining users (admins)
                added_users = [buyer_id, factory_id]
                for user_id in admin_ids:
                    if user_id in user_entities and user_id not in added_users:
                        try:
                            await client(AddChatUserRequest(
                                chat_id=chat.id,  # Use positive ID for adding users
                                user_id=user_entities[user_id],
                                fwd_limit=0
                            ))
                            logger.info(f"Added admin {user_id} to group")
                            added_users.append(user_id)
                        except Exception as e:
                            logger.warning(f"Failed to add admin {user_id}: {e}")
                            # Don't fail completely
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
                    # Group is created, so don't fail
                
                # Wait a bit for group to fully initialize
                await asyncio.sleep(1)
                
                # Verify group exists by getting its info
                try:
                    chat_info = await client.get_entity(chat_id)
                    logger.info(f"Verified group exists: {chat_info.title}")
                    return chat_id, f"Group created successfully with {len(added_users)} members"
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
                    except:
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
                    # For regular groups, export chat invite
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
    bot_token = os.getenv("BOT_TOKEN")
    
    # Validate environment variables
    if not api_id:
        return None, "TELEGRAM_API_ID not set"
    if not api_hash:
        return None, "TELEGRAM_API_HASH not set"
    if not bot_token:
        return None, "BOT_TOKEN not set"
    
    try:
        creator = TelegramGroupCreator(api_id, api_hash, bot_token)
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
    bot_token = os.getenv("BOT_TOKEN")
    
    if not all([api_id, api_hash, bot_token]):
        print("Missing environment variables")
        return
    
    creator = TelegramGroupCreator(api_id, api_hash, bot_token)
    
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
