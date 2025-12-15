import asyncio
import logging
import aiohttp
from urllib.parse import urlparse
from typing import Dict, List, Optional, Any

from config import config

logger = logging.getLogger(__name__)


class VKAPIClient:
    """Клиент для работы с VK API"""
    
    def __init__(self):
        self.base_url = "https://api.vk.com/method/"
        self.api_version = config.VK_API_VERSION
        self.access_token = config.VK_SERVICE_TOKEN
        self.request_delay = config.REQUEST_DELAY
        self.session: Optional[aiohttp.ClientSession] = None
        self.request_timeout = aiohttp.ClientTimeout(total=30)
        
    async def __aenter__(self):
        await self.init_session()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
    
    async def init_session(self):
        """Инициализация HTTP сессии"""
        if not self.session or self.session.closed:
            connector = aiohttp.TCPConnector(limit=10, ssl=False)
            self.session = aiohttp.ClientSession(
                connector=connector,
                timeout=self.request_timeout
            )
    
    async def close(self):
        """Закрытие HTTP сессии"""
        if self.session and not self.session.closed:
            await self.session.close()
    
    def extract_group_id(self, group_link: str) -> Optional[str]:
        """
        Извлекает идентификатор группы из ссылки
        
        Поддерживаемые форматы:
        - https://vk.com/public123456
        - https://vk.com/club123456
        - https://vk.com/group_name
        - vk.com/public123
        - @group_name
        """
        try:
            # Удаляем пробелы и символы @
            link = group_link.strip().lstrip('@')
            
            # Если ссылка уже является числовым ID
            if link.isdigit():
                return link
            
            # Добавляем https:// если нет протокола
            if not link.startswith(('http://', 'https://')):
                link = 'https://' + link
            
            parsed = urlparse(link)
            path = parsed.path.strip('/')
            
            # Извлекаем последнюю часть пути
            if path:
                parts = path.split('/')
                if parts:
                    identifier = parts[-1]
                    
                    # Если это числовой ID в формате public123 или club123
                    if identifier.startswith(('public', 'club', 'event')):
                        # Извлекаем цифры
                        import re
                        numbers = re.findall(r'\d+', identifier)
                        if numbers:
                            return numbers[0]
                    else:
                        # Возвращаем короткое имя группы
                        return identifier
            
            return None
            
        except Exception as e:
            logger.error(f"Ошибка извлечения ID из ссылки {group_link}: {e}")
            return None
    
    async def make_request(self, method: str, params: Dict) -> Optional[Dict]:
        """Выполняет запрос к VK API"""
        try:
            await asyncio.sleep(self.request_delay)
            
            # Добавляем обязательные параметры
            params.update({
                'v': self.api_version,
                'access_token': self.access_token
            })
            
            if not self.session or self.session.closed:
                await self.init_session()
            
            logger.debug(f"VK API запрос: {method} с параметрами {params}")
            
            async with self.session.post(
                f"{self.base_url}{method}",
                params=params,
                timeout=self.request_timeout
            ) as response:
                response_text = await response.text()
                data = await response.json()
                
                if response.status != 200:
                    logger.error(f"HTTP ошибка {response.status}: {response_text}")
                    return None
                
                if 'error' in data:
                    error = data['error']
                    error_msg = error.get('error_msg', 'Неизвестная ошибка')
                    logger.error(f"VK API ошибка: {error_msg}")
                    
                    # Не прерываем выполнение для некоторых ошибок
                    if error.get('error_code') == 15:  # Доступ запрещен
                        return {'error': 'group_closed', 'message': 'Группа закрыта'}
                    elif error.get('error_code') == 18:  # Страница удалена
                        return {'error': 'group_deleted', 'message': 'Группа удалена'}
                    elif error.get('error_code') == 100:  # Неверный параметр
                        return {'error': 'invalid_param', 'message': 'Неверный ID группы'}
                    
                    return None
                
                logger.debug(f"VK API успешный ответ: {method}")
                return data.get('response')
                
        except asyncio.TimeoutError:
            logger.error(f"Таймаут запроса к VK API: {method}")
            return None
        except aiohttp.ClientError as e:
            logger.error(f"Ошибка сети при запросе к VK API: {e}")
            return None
        except Exception as e:
            logger.error(f"Неожиданная ошибка при запросе к VK API: {e}")
            return None
    
    async def get_group_info(self, group_link: str) -> Optional[Dict]:
        """
        Получает информацию о группе ВК
        
        Returns:
            Dict с информацией о группе или None в случае ошибки
        """
        try:
            group_id = self.extract_group_id(group_link)
            if not group_id:
                logger.error(f"Не удалось извлечь ID группы из ссылки: {group_link}")
                return None
            
            logger.info(f"Запрос информации о группе: {group_link} (ID: {group_id})")
            
            # Определяем тип идентификатора
            params = {}
            if group_id.isdigit():
                params['group_id'] = group_id
            else:
                params['group_ids'] = group_id
            
            params['fields'] = 'description,members_count,activity,status'
            
            response = await self.make_request('groups.getById', params)
            if not response:
                return None
            
            # Обработка ответа
            if isinstance(response, list) and len(response) > 0:
                group_info = response[0]
                
                # Проверяем обязательные поля
                required_fields = ['id', 'name']
                for field in required_fields:
                    if field not in group_info:
                        logger.error(f"В информации о группе отсутствует поле {field}: {group_info}")
                        return None
                
                # Добавляем числовой ID если был передан screen_name
                if isinstance(group_info['id'], str) and group_info['id'].isdigit():
                    group_info['id'] = int(group_info['id'])
                
                # Нормализуем поля
                group_info['is_closed'] = group_info.get('is_closed', 1)
                group_info['members_count'] = group_info.get('members_count', 0)
                group_info['screen_name'] = group_info.get('screen_name', f"club{group_info['id']}")
                
                logger.info(f"Получена информация о группе {group_info['name']} (ID: {group_info['id']})")
                return group_info
            
            logger.error(f"Неверный формат ответа для группы {group_id}: {response}")
            return None
            
        except Exception as e:
            logger.error(f"Ошибка при получении информации о группе {group_link}: {e}")
            return None
    
    async def get_group_members(self, group_id: int, limit: int = 1000) -> List[Dict]:
        """
        Получает список участников группы
        
        Args:
            group_id: ID группы
            limit: Максимальное количество участников
            
        Returns:
            Список участников или пустой список в случае ошибки
        """
        try:
            logger.info(f"Запрос участников группы {group_id} (лимит: {limit})")
            
            members = []
            offset = 0
            count = min(limit, 1000)  # Максимум 1000 за один запрос
            
            while len(members) < limit:
                params = {
                    'group_id': group_id,
                    'offset': offset,
                    'count': count,
                    'fields': 'sex,bdate,city,country,interests,activities',
                    'sort': 'id_asc'
                }
                
                response = await self.make_request('groups.getMembers', params)
                if not response:
                    break
                
                # Проверяем структуру ответа
                if not isinstance(response, dict) or 'items' not in response:
                    logger.error(f"Неверная структура ответа members: {response}")
                    break
                
                batch = response['items']
                if not batch:
                    break
                
                members.extend(batch)
                offset += len(batch)
                
                # Если получено меньше, чем запрошено, значит больше нет
                if len(batch) < count:
                    break
                
                # Ограничиваем общее количество
                if len(members) >= limit:
                    members = members[:limit]
                    break
            
            logger.info(f"Получено {len(members)} участников группы {group_id}")
            return members
            
        except Exception as e:
            logger.error(f"Ошибка при получении участников группы {group_id}: {e}")
            return []
    
    async def get_users_info(self, user_ids: List[int]) -> List[Dict]:
        """
        Получает информацию о пользователях по их ID
        
        Args:
            user_ids: Список ID пользователей
            
        Returns:
            Список с информацией о пользователях
        """
        try:
            if not user_ids:
                return []
            
            # Разбиваем на батчи по 100 (лимит VK API)
            batch_size = 100
            all_users = []
            
            for i in range(0, len(user_ids), batch_size):
                batch_ids = user_ids[i:i + batch_size]
                
                params = {
                    'user_ids': ','.join(map(str, batch_ids)),
                    'fields': 'sex,bdate,city,country,interests,activities,last_seen',
                    'name_case': 'nom'
                }
                
                response = await self.make_request('users.get', params)
                if response and isinstance(response, list):
                    all_users.extend(response)
                
                await asyncio.sleep(0.5)  # Задержка между запросами
            
            return all_users
            
        except Exception as e:
            logger.error(f"Ошибка при получении информации о пользователях: {e}")
            return []
    
    async def test_connection(self) -> Dict[str, Any]:
        """
        Тестирование подключения к VK API
        
        Returns:
            Результат тестирования
        """
        results = {
            'success': False,
            'message': '',
            'details': []
        }
        
        try:
            # Тест 1: Базовый запрос к API (проверка токена)
            logger.info("Тест 1: Базовый запрос к API (проверка токена)")
            params = {'fields': 'screen_name'}
            response = await self.make_request('users.get', params)
            
            if response:
                results['details'].append({
                    'test': 'Проверка токена',
                    'success': True,
                    'message': 'Токен действителен'
                })
            else:
                results['details'].append({
                    'test': 'Проверка токена',
                    'success': False,
                    'message': 'Токен недействителен или отсутствует'
                })
                results['message'] = 'Проблема с токеном доступа'
                return results
            
            # Тест 2: Запрос известной группы
            logger.info("Тест 2: Запрос группы по ID (public1)")
            group_info = await self.get_group_info('vk.com/public1')
            
            if group_info:
                results['details'].append({
                    'test': 'Запрос группы',
                    'success': True,
                    'message': f"Группа: {group_info.get('name', 'Unknown')} (ID: {group_info.get('id')})"
                })
            else:
                results['details'].append({
                    'test': 'Запрос группы',
                    'success': False,
                    'message': 'Не удалось получить информацию о группе'
                })
            
            # Тест 3: Запрос группы по короткому имени
            logger.info("Тест 3: Запрос группы по короткому имени")
            test_groups = ['vk.com/durov', 'vk.com/club1', 'vk.com/hobby_universe']
            
            for group_link in test_groups:
                group_info = await self.get_group_info(group_link)
                if group_info:
                    results['details'].append({
                        'test': f'Запрос {group_link}',
                        'success': True,
                        'message': f"Найдена: {group_info.get('name', 'Unknown')}"
                    })
                else:
                    results['details'].append({
                        'test': f'Запрос {group_link}',
                        'success': False,
                        'message': 'Группа не найдена или ошибка доступа'
                    })
            
            # Проверяем общий результат
            success_tests = sum(1 for d in results['details'] if d['success'])
            total_tests = len(results['details'])
            
            if success_tests >= total_tests * 0.5:  # Если пройдено хотя бы 50% тестов
                results['success'] = True
                results['message'] = f'✅ Подключение к VK API работает ({success_tests}/{total_tests} тестов пройдено)'
            else:
                results['message'] = f'❌ Проблемы с подключением к VK API ({success_tests}/{total_tests} тестов пройдено)'
            
            return results
            
        except Exception as e:
            logger.error(f"Ошибка при тестировании подключения: {e}")
            results['message'] = f'Критическая ошибка тестирования: {str(e)[:200]}'
            return results


# Глобальный экземпляр клиента
vk_client = VKAPIClient()
