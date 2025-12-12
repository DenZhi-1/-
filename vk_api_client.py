import aiohttp
import asyncio
import logging
from typing import Dict, List, Optional
from urllib.parse import urlparse
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
        """Извлекает ID группы из ссылки"""
        patterns = [
            r'vk\.com/(club|public)(\d+)',
            r'vk\.com/([a-zA-Z0-9_]+)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, group_link)
            if match:
                return match.group(2) if 'club' in pattern or 'public' in pattern else match.group(1)
        return None
    
    async def make_request(self, method: str, params: Dict) -> Optional[Dict]:
        """Выполняет запрос к VK API"""
        try:
            session = await self._get_session()
            params.update({
                'access_token': config.VK_SERVICE_TOKEN,
                'v': config.VK_API_VERSION
            })
            
            async with session.get(f"{self.base_url}{method}", params=params) as response:
                data = await response.json()
                if 'error' in data:
                    logger.error(f"VK API error: {data['error']}")
                    return None
                return data.get('response')
        except Exception as e:
            logger.error(f"Request error: {e}")
            return None
    
    async def get_group_info(self, group_link: str) -> Optional[Dict]:
        """Получает информацию о группе"""
        group_id = self.extract_group_id(group_link)
        if not group_id:
            return None
            
        params = {
            'group_id': group_id,
            'fields': 'members_count,description,activity'
        }
        
        response = await self.make_request('groups.getById', params)
        if response and len(response) > 0:
            group = response[0]
            return {
                'id': group.get('id'),
                'name': group.get('name'),
                'screen_name': group.get('screen_name'),
                'members_count': group.get('members_count', 0),
                'description': group.get('description', ''),
                'activity': group.get('activity', '')
            }
        return None
    
    async def get_group_members(self, group_id: str, limit: int = 1000) -> List[Dict]:
        """Получает участников группы с их профилями"""
        members = []
        offset = 0
        count = 1000  # Максимум за запрос
        
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
                
            users = response.get('items', [])
            if not users:
                break
                
            members.extend(users)
            offset += len(users)
            
            if len(users) < count or len(members) >= limit:
                break
                
            await asyncio.sleep(config.REQUEST_DELAY)
        
        return members
    
    async def close(self):
        """Закрывает сессию"""
        if self.session:
            await self.session.close()

# Синглтон экземпляр
vk_client = VKAPIClient()
