import aiohttp
import asyncio
import logging
from typing import Dict, List, Optional, Any, Union
import re
from urllib.parse import urlparse

from config import config

logger = logging.getLogger(__name__)

class VKAPIClient:
    """Клиент для работы с VK API с безопасной обработкой ошибок"""
    
    def __init__(self):
        self.base_url = "https://api.vk.com/method/"
        self.session = None
        self.request_counter = 0
        
    async def _get_session(self) -> aiohttp.ClientSession:
        """Создание или получение сессии"""
        if not self.session or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=30)
            self.session = aiohttp.ClientSession(timeout=timeout)
        return self.session
    
    def _extract_group_id(self, group_link: str) -> Optional[str]:
        """
        Извлекает ID группы из ссылки.
        Поддерживает форматы: 
        - https://vk.com/club123456
        - https://vk.com/public123456  
        - https://vk.com/groupname
        - vk.com/groupname
        - @groupname
        """
        if not group_link:
            return None
            
        # Очищаем ссылку от пробелов
        group_link = group_link.strip()
        
        patterns = [
            # Для числовых ID: club123456, public123456
            r'(?:https?://)?(?:www\.)?(?:m\.)?vk\.com/(?:club|public|event)(\d+)',
            # Для коротких имен: vk.com/groupname
            r'(?:https?://)?(?:www\.)?(?:m\.)?vk\.com/([a-zA-Z0-9_.]+[a-zA-Z0-9_])',
            # Для упоминаний: @groupname
            r'@([a-zA-Z0-9_.]+[a-zA-Z0-9_])'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, group_link, re.IGNORECASE)
            if match:
                extracted_id = match.group(1)
                logger.debug(f"Извлечен ID группы из '{group_link}': {extracted_id}")
                return extracted_id
        
        # Если ничего не нашли, проверяем, не является ли это уже ID
        if re.match(r'^[a-zA-Z0-9_.]+$', group_link):
            return group_link
            
        logger.warning(f"Не удалось извлечь ID группы из ссылки: {group_link}")
        return None
    
    async def _make_request(self, method: str, params: Dict) -> Optional[Union[Dict, List]]:
        """Безопасный запрос к VK API с обработкой ошибок и логированием"""
        self.request_counter += 1
        request_id = self.request_counter
        
        try:
            session = await self._get_session()
            
            # Подготавливаем параметры
            request_params = params.copy()
            request_params.update({
                'access_token': config.VK_SERVICE_TOKEN,
                'v': config.VK_API_VERSION,
                'lang': 'ru'
            })
            
            # Логируем запрос (без токена)
            safe_params = {k: v for k, v in request_params.items() if k != 'access_token'}
            logger.debug(f"[Request #{request_id}] VK API: {method}, params: {safe_params}")
            
            # Отправляем запрос
            async with session.get(
                f"{self.base_url}{method}", 
                params=request_params,
                headers={'User-Agent': 'VKAnalyzerBot/1.0'}
            ) as response:
                
                # Проверяем HTTP статус
                if response.status != 200:
                    logger.error(f"[Request #{request_id}] HTTP ошибка: {response.status}")
                    return None
                
                # Парсим JSON ответ
                try:
                    data = await response.json()
                except Exception as e:
                    logger.error(f"[Request #{request_id}] Ошибка парсинга JSON: {e}")
                    return None
                
                # Логируем полный ответ в debug режиме
                if config.DEBUG:
                    logger.debug(f"[Request #{request_id}] Полный ответ: {data}")
                
                # Обработка ошибок VK API
                if 'error' in data:
                    error = data['error']
                    error_code = error.get('error_code', 'unknown')
                    error_msg = error.get('error_msg', 'Нет описания')
                    
                    logger.error(f"[Request #{request_id}] VK API ошибка {error_code}: {error_msg}")
                    
                    # Специальная обработка частых ошибок
                    if error_code == 5:
                        logger.critical("❌ НЕВЕРНЫЙ ТОКЕН VK! Проверьте VK_SERVICE_TOKEN в .env")
                    elif error_code == 15:
                        logger.warning("Группа недоступна (закрытая или удаленная)")
                    elif error_code == 18:
                        logger.warning("Группа удалена или забанена")
                    elif error_code == 100:
                        logger.warning("Неверный идентификатор группы")
                    elif error_code == 203:
                        logger.warning("Доступ к группе запрещен")
                    
                    return None
                
                # Возвращаем успешный ответ
                response_data = data.get('response')
                logger.debug(f"[Request #{request_id}] Успешный ответ, тип: {type(response_data)}")
                return response_data
                
        except aiohttp.ClientError as e:
            logger.error(f"[Request #{request_id}] Ошибка сети: {e}")
            return None
        except asyncio.TimeoutError:
            logger.error(f"[Request #{request_id}] Таймаут запроса")
            return None
        except Exception as e:
            logger.error(f"[Request #{request_id}] Неожиданная ошибка: {e}", exc_info=True)
            return None
        finally:
            # Соблюдаем лимиты VK API
            await asyncio.sleep(config.REQUEST_DELAY)
    
    async def get_group_info(self, group_link: str) -> Optional[Dict[str, Any]]:
        """
        Получает информацию о группе.
        Возвращает словарь с информацией или None в случае ошибки.
        """
        logger.info(f"Запрос информации о группе: {group_link}")
        
        # Извлекаем ID группы
        group_id = self._extract_group_id(group_link)
        if not group_id:
            logger.error(f"Неверный формат ссылки: {group_link}")
            return None
        
        logger.debug(f"Используется group_id: {group_id}")
        
        # Подготавливаем параметры запроса
        params = {
            'group_id': group_id,
            'fields': 'members_count,description,activity,status,is_closed,type'
        }
        
        # Делаем запрос
        response = await self._make_request('groups.getById', params)
        
        # Безопасная обработка ответа
        if response is None:
            logger.error(f"Нет ответа от VK API для группы {group_id}")
            return None
        
        # Проверяем формат ответа
        if isinstance(response, list):
            if len(response) == 0:
                logger.error(f"Пустой список в ответе для группы {group_id}")
                return None
            group_data = response[0]
        elif isinstance(response, dict):
            group_data = response
        else:
            logger.error(f"Неизвестный формат ответа: {type(response)} - {response}")
            return None
        
        # Проверяем наличие обязательных полей
        if 'id' not in group_data or 'name' not in group_data:
            logger.error(f"Отсутствуют обязательные поля в ответе: {group_data}")
            return None
        
        # Формируем результат
        result = {
            'id': group_data.get('id'),
            'name': group_data.get('name'),
            'screen_name': group_data.get('screen_name', group_id),
            'members_count': group_data.get('members_count', 0),
            'description': group_data.get('description', ''),
            'activity': group_data.get('activity', ''),
            'status': group_data.get('status', ''),
            'is_closed': group_data.get('is_closed', 1),  # 0 - открытая, 1 - закрытая, 2 - частная
            'type': group_data.get('type', 'group')
        }
        
        logger.info(f"Получена информация о группе: {result['name']} (ID: {result['id']}), участников: {result['members_count']}")
        return result
    
    async def get_group_members(self, group_id: str, limit: int = 1000) -> List[Dict[str, Any]]:
        """Получает участников группы с пагинацией"""
        logger.info(f"Запрос участников группы {group_id}, лимит: {limit}")
        
        members = []
        offset = 0
        batch_size = 1000  # Максимум за один запрос в VK API
        
        # Проверяем, доступна ли группа
        group_info = await self.get_group_info(f"vk.com/{group_id}")
        if not group_info:
            logger.error(f"Не удалось получить информацию о группе {group_id}")
            return []
        
        # Проверяем тип группы
        if group_info.get('is_closed', 1) != 0:
            logger.warning(f"Группа {group_id} закрытая или приватная. Участники недоступны.")
            return []
        
        # Проверяем количество участников
        total_members = group_info.get('members_count', 0)
        if total_members == 0:
            logger.warning(f"В группе {group_id} нет участников или данные скрыты")
            return []
        
        # Ограничиваем лимит разумными значениями
        limit = min(limit, total_members, 10000)  # Не более 10к для производительности
        
        logger.debug(f"Начинаем сбор участников. Всего в группе: {total_members}, будем собирать: {limit}")
        
        while len(members) < limit:
            current_batch = min(batch_size, limit - len(members))
            
            params = {
                'group_id': group_id,
                'offset': offset,
                'count': current_batch,
                'fields': 'sex,bdate,city,country,interests,activities,books,music,movies,games,occupation,relation,last_seen,online'
            }
            
            response = await self._make_request('groups.getMembers', params)
            
            # Обработка ответа
            if not response:
                logger.warning(f"Пустой ответ при запросе участников, offset={offset}")
                break
            
            items = []
            if isinstance(response, dict):
                items = response.get('items', [])
                total_in_response = response.get('count', 0)
                logger.debug(f"Получено {len(items)} участников из {total_in_response}")
            elif isinstance(response, list):
                items = response
            
            if not items:
                logger.debug(f"Больше нет участников для загрузки")
                break
            
            # Добавляем участников
            members.extend(items)
            offset += len(items)
            
            logger.debug(f"Собрано участников: {len(members)}/{limit}")
            
            # Проверяем условия завершения
            if len(items) < current_batch or len(members) >= limit:
                break
        
        logger.info(f"Сбор участников завершен. Всего собрано: {len(members)}")
        return members
    
    async def test_connection(self) -> Dict[str, Any]:
        """Тестирование подключения к VK API"""
        logger.info("Тестирование подключения к VK API...")
        
        test_params = {
            'user_ids': '1',  # Запрос информации о пользователе с ID=1 (Павел Дуров)
            'fields': ''
        }
        
        response = await self._make_request('users.get', test_params)
        
        if response is None:
            return {
                'success': False,
                'message': 'Нет ответа от VK API',
                'details': 'Проверьте токен и интернет-соединение'
            }
        
        if isinstance(response, list) and len(response) > 0:
            user = response[0]
            return {
                'success': True,
                'message': f'✅ Подключение успешно! Токен работает. Привет, {user.get("first_name", "Пользователь")}!',
                'user_info': user
            }
        else:
            return {
                'success': False,
                'message': 'Неожиданный ответ от VK API',
                'response': response
            }
    
    async def close(self):
        """Корректное закрытие сессии"""
        if self.session and not self.session.closed:
            await self.session.close()
            logger.info("Сессия VK API закрыта")

# Глобальный экземпляр
vk_client = VKAPIClient()
