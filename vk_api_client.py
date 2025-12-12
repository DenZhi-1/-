import aiohttp
import asyncio
import logging
from typing import Dict, List, Optional, Any, Union
import re
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from config import config

logger = logging.getLogger(__name__)

class VKAPIClient:
    def __init__(self):
        self.base_url = "https://api.vk.com/method/"
        self.session = None
        self.request_counter = 0
        
    async def _get_session(self) -> aiohttp.ClientSession:
        """Создание сессии с правильными таймаутами для Railway"""
        if not self.session or self.session.closed:
            # Увеличиваем таймауты для работы через Railway
            timeout = aiohttp.ClientTimeout(
                total=60,      # Общий таймаут 60 секунд
                connect=30,    # Таймаут на подключение 30 секунд
                sock_read=30   # Таймаут на чтение 30 секунд
            )
            connector = aiohttp.TCPConnector(
                limit=100,     # Увеличиваем лимит соединений
                force_close=True,
                enable_cleanup_closed=True
            )
            self.session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
                headers={
                    'User-Agent': 'VKAnalyzerBot/1.0 (Railway)',
                    'Accept': 'application/json',
                    'Accept-Encoding': 'gzip, deflate'
                }
            )
        return self.session
    
    def _extract_group_id(self, group_link: str) -> Optional[str]:
        if not group_link:
            return None
            
        group_link = group_link.strip()
        
        patterns = [
            r'(?:https?://)?(?:www\.)?(?:m\.)?vk\.com/(?:club|public|event)(\d+)',
            r'(?:https?://)?(?:www\.)?(?:m\.)?vk\.com/([a-zA-Z0-9_.]+[a-zA-Z0-9_])',
            r'@([a-zA-Z0-9_.]+[a-zA-Z0-9_])'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, group_link, re.IGNORECASE)
            if match:
                extracted = match.group(1)
                logger.debug(f"Извлечен ID: {extracted} из {group_link}")
                return extracted
        
        if re.match(r'^[a-zA-Z0-9_.]+$', group_link):
            return group_link
            
        logger.warning(f"Не удалось извлечь ID из {group_link}")
        return None
    
    @retry(
        stop=stop_after_attempt(3),  # 3 попытки
        wait=wait_exponential(multiplier=1, min=2, max=10),  # Экспоненциальная задержка
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
        reraise=True
    )
    async def _make_request(self, method: str, params: Dict) -> Optional[Union[Dict, List]]:
        """Запрос с повторными попытками при ошибках"""
        self.request_counter += 1
        request_id = self.request_counter
        
        try:
            session = await self._get_session()
            
            request_params = params.copy()
            request_params.update({
                'access_token': config.VK_SERVICE_TOKEN,
                'v': config.VK_API_VERSION,
                'lang': 'ru'
            })
            
            # Логируем без токена
            safe_params = {k: v for k, v in request_params.items() if k != 'access_token'}
            logger.debug(f"[Req #{request_id}] {method} params: {safe_params}")
            
            start_time = asyncio.get_event_loop().time()
            
            async with session.get(
                f"{self.base_url}{method}", 
                params=request_params,
                ssl=False  # Отключаем SSL проверку для Railway
            ) as response:
                
                response_time = asyncio.get_event_loop().time() - start_time
                
                # Логируем время ответа
                logger.debug(f"[Req #{request_id}] Response time: {response_time:.2f}s, Status: {response.status}")
                
                if response.status != 200:
                    error_text = await response.text()
                    logger.error(f"[Req #{request_id}] HTTP {response.status}: {error_text[:200]}")
                    return None
                
                # Читаем ответ
                try:
                    data = await response.json()
                except Exception as e:
                    error_text = await response.text()
                    logger.error(f"[Req #{request_id}] JSON parse error: {e}, Response: {error_text[:500]}")
                    return None
                
                # Проверяем ошибки VK API
                if 'error' in data:
                    error = data['error']
                    error_code = error.get('error_code', 'unknown')
                    error_msg = error.get('error_msg', 'Нет описания')
                    
                    logger.error(f"[Req #{request_id}] VK API error {error_code}: {error_msg}")
                    
                    # Специальная обработка ошибок
                    if error_code in [5, 28, 29]:  # Auth failed, rate limit
                        raise aiohttp.ClientError(f"VK API error {error_code}: {error_msg}")
                    
                    return None
                
                # Успешный ответ
                response_data = data.get('response')
                logger.debug(f"[Req #{request_id}] Success, response type: {type(response_data)}")
                return response_data
                
        except asyncio.TimeoutError:
            logger.error(f"[Req #{request_id}] Timeout after 60s")
            raise  # Пробрасываем для retry
        except aiohttp.ClientError as e:
            logger.error(f"[Req #{request_id}] Client error: {e}")
            raise  # Пробрасываем для retry
        except Exception as e:
            logger.error(f"[Req #{request_id}] Unexpected error: {str(e)}", exc_info=True)
            return None
        finally:
            # Базовое ожидание между запросами для соблюдения лимитов
            await asyncio.sleep(config.REQUEST_DELAY)
    
    async def get_group_info(self, group_link: str) -> Optional[Dict[str, Any]]:
        """Получает информацию о группе с улучшенной обработкой ошибок"""
        logger.info(f"Запрос информации о группе: {group_link}")
        
        group_id = self._extract_group_id(group_link)
        if not group_id:
            await self._send_debug_message(f"❌ Не удалось извлечь ID из ссылки: {group_link}")
            return None
        
        logger.info(f"Извлечен group_id: {group_id}")
        
        # Пробуем сначала как числовой ID
        if group_id.isdigit():
            params = {'group_id': group_id}
        else:
            # Если не число, пробуем как screen_name
            params = {'group_ids': group_id}
        
        params['fields'] = 'members_count,description,activity,status,is_closed,type,verified'
        
        try:
            response = await self._make_request('groups.getById', params)
            
            if response is None:
                await self._send_debug_message(f"❌ Нет ответа от VK API для {group_id}")
                return None
            
            # Обработка разных форматов ответа
            group_data = None
            
            if isinstance(response, list):
                if len(response) > 0:
                    group_data = response[0]
                else:
                    await self._send_debug_message(f"❌ Пустой список в ответе для {group_id}")
                    return None
            elif isinstance(response, dict):
                if 'items' in response:
                    items = response.get('items', [])
                    if items:
                        group_data = items[0]
                else:
                    group_data = response
            else:
                await self._send_debug_message(f"❌ Неизвестный формат ответа для {group_id}")
                return None
            
            if not group_data:
                await self._send_debug_message(f"❌ Не удалось получить данные группы {group_id}")
                return None
            
            if 'id' not in group_data or 'name' not in group_data:
                await self._send_debug_message(f"❌ Неполные данные группы {group_id}: {group_data}")
                return None
            
            result = {
                'id': abs(int(group_data.get('id'))),  # VK возвращает отрицательные ID для групп
                'name': group_data.get('name'),
                'screen_name': group_data.get('screen_name', group_id),
                'members_count': group_data.get('members_count', 0),
                'description': group_data.get('description', '')[:500],  # Ограничиваем длину
                'activity': group_data.get('activity', ''),
                'status': group_data.get('status', ''),
                'is_closed': group_data.get('is_closed', 1),
                'type': group_data.get('type', 'group'),
                'verified': group_data.get('verified', 0)
            }
            
            logger.info(f"✅ Получена информация о группе: {result['name']} (ID: {result['id']}), участников: {result['members_count']}")
            await self._send_debug_message(f"✅ Группа найдена: {result['name']} ({result['members_count']} участников)")
            
            return result
            
        except Exception as e:
            logger.error(f"Ошибка в get_group_info для {group_id}: {e}", exc_info=True)
            await self._send_debug_message(f"❌ Ошибка при запросе группы {group_id}: {str(e)}")
            return None
    
    async def get_group_members(self, group_id: str, limit: int = 500) -> List[Dict[str, Any]]:
        """Получает участников группы с ограничениями"""
        logger.info(f"Запрос участников группы {group_id}, лимит: {limit}")
        
        # Проверяем, что group_id - число (VK API требует числовой ID для members)
        if not str(group_id).isdigit():
            logger.error(f"Group ID должен быть числом, получено: {group_id}")
            return []
        
        # Для безопасности ограничиваем лимит
        limit = min(limit, 1000)
        
        try:
            # Сначала проверяем доступность группы
            group_info = await self.get_group_info(f"vk.com/{group_id}")
            if not group_info:
                logger.error(f"Группа {group_id} не найдена или недоступна")
                return []
            
            # Проверяем, что группа открыта
            if group_info.get('is_closed', 1) != 0:
                logger.warning(f"Группа {group_id} закрыта. Участники недоступны.")
                return []
            
            # Проверяем количество участников
            total_members = group_info.get('members_count', 0)
            if total_members == 0:
                logger.warning(f"У группы {group_id} нет участников")
                return []
            
            # Ограничиваем реальный лимит
            real_limit = min(limit, total_members)
            
            logger.info(f"Начинаем сбор {real_limit} участников из {total_members}")
            
            members = []
            offset = 0
            batch_size = 250  # Уменьшаем batch для надежности
            
            while len(members) < real_limit:
                current_batch = min(batch_size, real_limit - len(members))
                
                params = {
                    'group_id': abs(int(group_id)),  # Берем абсолютное значение
                    'offset': offset,
                    'count': current_batch,
                    'fields': 'sex,bdate,city,country'
                }
                
                response = await self._make_request('groups.getMembers', params)
                
                if not response:
                    logger.warning(f"Пустой ответ при offset={offset}")
                    break
                
                items = []
                if isinstance(response, dict):
                    items = response.get('items', [])
                    logger.debug(f"Получено {len(items)} участников")
                elif isinstance(response, list):
                    items = response
                
                if not items:
                    logger.debug("Больше нет участников")
                    break
                
                members.extend(items)
                offset += len(items)
                
                # Прогресс
                progress = len(members) / real_limit * 100
                logger.debug(f"Прогресс: {len(members)}/{real_limit} ({progress:.1f}%)")
                
                # Условие выхода
                if len(items) < current_batch or len(members) >= real_limit:
                    break
            
            logger.info(f"✅ Собрано {len(members)} участников")
            return members
            
        except Exception as e:
            logger.error(f"Ошибка в get_group_members для {group_id}: {e}", exc_info=True)
            return []
    
    async def test_connection(self) -> Dict[str, Any]:
        """Тестирование подключения с подробной диагностикой"""
        logger.info("Запуск теста подключения к VK API...")
        
        test_cases = [
            {
                'method': 'users.get',
                'params': {'user_ids': '1', 'fields': 'first_name,last_name'},
                'description': 'Базовый запрос к API'
            },
            {
                'method': 'groups.getById',
                'params': {'group_ids': 'durov', 'fields': 'name'},
                'description': 'Запрос информации о группе'
            }
        ]
        
        results = []
        
        for i, test in enumerate(test_cases, 1):
            try:
                logger.info(f"Тест {i}: {test['description']}")
                response = await self._make_request(test['method'], test['params'])
                
                if response:
                    results.append({
                        'test': test['description'],
                        'success': True,
                        'message': f"✅ Успешно, получен ответ"
                    })
                else:
                    results.append({
                        'test': test['description'],
                        'success': False,
                        'message': f"❌ Нет ответа"
                    })
                    
                # Пауза между тестами
                await asyncio.sleep(1)
                
            except Exception as e:
                results.append({
                    'test': test['description'],
                    'success': False,
                    'message': f"❌ Ошибка: {str(e)}"
                })
        
        # Анализ результатов
        success_count = sum(1 for r in results if r['success'])
        
        if success_count == len(test_cases):
            return {
                'success': True,
                'message': '✅ Все тесты пройдены успешно! VK API доступен.',
                'details': results
            }
        elif success_count > 0:
            return {
                'success': True,
                'message': f'⚠️ Частичная доступность: {success_count}/{len(test_cases)} тестов пройдены',
                'details': results
            }
        else:
            return {
                'success': False,
                'message': '❌ Все тесты не пройдены. VK API недоступен.',
                'details': results
            }
    
    async def _send_debug_message(self, message: str):
        """Отправка отладочных сообщений в лог"""
        if config.DEBUG:
            logger.info(f"[DEBUG] {message}")
    
    async def close(self):
        """Корректное закрытие сессии"""
        if self.session and not self.session.closed:
            await self.session.close()
            logger.info("Сессия VK API закрыта")

# Глобальный экземпляр
vk_client = VKAPIClient()
