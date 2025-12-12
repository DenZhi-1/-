import aiohttp
import asyncio
import logging
from typing import Dict, List, Optional, Any
import re

from config import config

logger = logging.getLogger(__name__)

class VKAPIClient:
    def __init__(self):
        self.base_url = "https://api.vk.com/method/"
        self.session = None
        
    async def _get_session(self):
        if not self.session:
            self.session = aiohttp.ClientSession()
        return self.session
    
    def extract_group_id(self, group_link: str) -> Optional[str]:
        """Извлекает ID группы из ссылки (короткое имя или числовой ID)"""
        if not group_link:
            return None
            
        patterns = [
            r'(?:https?://)?(?:www\.)?vk\.com/(?:club|public)(\d+)',
            r'(?:https?://)?(?:www\.)?vk\.com/([a-zA-Z0-9_.]+)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, group_link)
            if match:
                # Возвращаем либо числовой ID, либо короткое имя
                return match.group(1)
        return None
    
    async def make_request(self, method: str, params: Dict) -> Optional[Dict[str, Any]]:
        """Безопасный запрос к VK API с логированием ошибок"""
        try:
            session = await self._get_session()
            params.update({
                'access_token': config.VK_SERVICE_TOKEN,
                'v': config.VK_API_VERSION
            })
            
            logger.debug(f"VK API Request: {method}, params: { {k: v for k, v in params.items() if k != 'access_token'} }")
            
            async with session.get(f"{self.base_url}{method}", params=params, timeout=30) as response:
                data = await response.json()
                
                if 'error' in data:
                    error = data['error']
                    logger.error(f"VK API Error {error.get('error_code', 'unknown')}: {error.get('error_msg', 'No message')}")
                    return None
                    
                logger.debug(f"VK API Response for {method}: {data.get('response', {})}")
                return data.get('response')
                
        except aiohttp.ClientError as e:
            logger.error(f"Network error: {e}")
            return None
        except asyncio.TimeoutError:
            logger.error("VK API request timeout")
            return None
        except Exception as e:
            logger.error(f"Unexpected error in make_request: {e}")
            return None
    
    async def get_group_info(self, group_link: str) -> Optional[Dict[str, Any]]:
        """Получает информацию о группе с безопасной обработкой ответа"""
        group_id = self.extract_group_id(group_link)
        if not group_id:
            logger.error(f"Could not extract group ID from link: {group_link}")
            return None
            
        params = {
            'group_id': group_id,
            'fields': 'members_count,description,activity,status'
        }
        
        response = await self.make_request('groups.getById', params)
        
        # Безопасная обработка ответа
        if not response:
            logger.error(f"No response from VK API for group {group_id}")
            return None
            
        # Ответ может быть списком или словарем в зависимости от ситуации
        if isinstance(response, list) and len(response) > 0:
            group = response[0]
        elif isinstance(response, dict):
            group = response
        else:
            logger.error(f"Unexpected response format: {type(response)} - {response}")
            return None
        
        # Проверяем обязательные поля
        if 'id' not in group or 'name' not in group:
            logger.error(f"Incomplete group data: {group}")
            return None
            
        return {
            'id': group.get('id'),
            'name': group.get('name'),
            'screen_name': group.get('screen_name', ''),
            'members_count': group.get('members_count', 0),
            'description': group.get('description', ''),
            'activity': group.get('activity', ''),
            'is_closed': group.get('is_closed', 1)  # 0 - открытая, 1 - закрытая, 2 - частная
        }
    
    async def get_group_members(self, group_id: str, limit: int = 1000) -> List[Dict[str, Any]]:
        """Получает участников группы с безопасной обработкой"""
        members = []
        offset = 0
        count = 1000  # Максимум за запрос в VK API
        
        # Для приватных групп возвращаем пустой список
        group_info = await self.get_group_info(f"vk.com/{group_id}")
        if group_info and group_info.get('is_closed', 1) != 0:
            logger.warning(f"Group {group_id} is closed or private. Cannot get members.")
            return []
        
        while len(members) < limit:
            params = {
                'group_id': group_id,
                'offset': offset,
                'count': min(count, limit - len(members)),
                'fields': 'sex,bdate,city,country,interests,activities,books,music,movies,games'
            }
            
            response = await self.make_request('groups.getMembers', params)
            if not response:
                break
                
            # Безопасная обработка ответа
            items = []
            if isinstance(response, dict):
                items = response.get('items', [])
            elif isinstance(response, list):
                items = response
            
            if not items:
                break
                
            members.extend(items)
            offset += len(items)
            
            if len(items) < count or len(members) >= limit:
                break
                
            # Соблюдаем лимиты VK API
            await asyncio.sleep(config.REQUEST_DELAY)
        
        logger.info(f"Retrieved {len(members)} members for group {group_id}")
        return members
    
    async def close(self):
        """Корректное закрытие сессии"""
        if self.session:
            await self.session.close()
            logger.info("VK API session closed")

# Глобальный экземпляр
vk_client = VKAPIClient()
