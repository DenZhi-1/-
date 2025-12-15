import logging
import re
import asyncio
from typing import Dict, List, Any, Optional
from collections import Counter

from vk_api_client import vk_client

logger = logging.getLogger(__name__)


class CompetitorAnalyzer:
    """Анализатор конкурентов для поиска и анализа похожих групп"""
    
    def __init__(self):
        self.min_similarity_score = 0.3
        self.max_competitors = 10
        
        # Категории для классификации групп
        self.categories = {
            'технологии': ['it', 'программирование', 'разработка', 'технологии', 'гаджеты', 'софт'],
            'образование': ['образование', 'курсы', 'обучение', 'школа', 'университет', 'студент'],
            'бизнес': ['бизнес', 'стартап', 'предпринимательство', 'маркетинг', 'продажи'],
            'развлечения': ['развлечения', 'юмор', 'приколы', 'мемы', 'смешно'],
            'спорт': ['спорт', 'фитнес', 'тренировки', 'футбол', 'хоккей'],
            'красота': ['красота', 'мода', 'стиль', 'косметика', 'уход'],
            'здоровье': ['здоровье', 'медицина', 'диета', 'спорт', 'фитнес'],
            'путешествия': ['путешествия', 'туризм', 'отдых', 'страны', 'города'],
            'еда': ['еда', 'рецепты', 'кулинария', 'готовка', 'рестораны'],
            'авто': ['авто', 'машины', 'автомобили', 'водитель', 'дорога']
        }
    
    def extract_keywords(self, text: str) -> List[str]:
        """Извлекает ключевые слова из текста"""
        if not text:
            return []
        
        # Приводим к нижнему регистру
        text_lower = text.lower()
        
        # Удаляем спецсимволы, оставляем только слова
        words = re.findall(r'\b[а-яa-z]{3,}\b', text_lower)
        
        # Удаляем стоп-слова
        stop_words = {'это', 'также', 'очень', 'можно', 'будет', 'есть', 'который', 
                     'которые', 'чтобы', 'как', 'для', 'или', 'и', 'в', 'на', 'с'}
        filtered_words = [word for word in words if word not in stop_words]
        
        # Подсчитываем частоту
        word_freq = Counter(filtered_words)
        
        # Возвращаем самые частые слова
        return [word for word, _ in word_freq.most_common(20)]
    
    def calculate_similarity(self, text1: str, text2: str) -> float:
        """Вычисляет схожесть двух текстов"""
        if not text1 or not text2:
            return 0.0
        
        keywords1 = set(self.extract_keywords(text1))
        keywords2 = set(self.extract_keywords(text2))
        
        if not keywords1 or not keywords2:
            return 0.0
        
        # Вычисляем коэффициент Жаккара
        intersection = len(keywords1.intersection(keywords2))
        union = len(keywords1.union(keywords2))
        
        return intersection / union if union > 0 else 0.0
    
    async def search_similar_groups(self, query: str, limit: int = 20) -> List[Dict]:
        """Ищет группы по запросу"""
        try:
            # Используем поиск VK API
            params = {
                'q': query,
                'type': 'group',
                'count': limit,
                'fields': 'description,members_count,activity'
            }
            
            response = await vk_client.make_request('groups.search', params)
            if not response or 'items' not in response:
                return []
            
            groups = []
            for item in response['items']:
                # Фильтруем закрытые группы
                if item.get('is_closed', 1) == 0:
                    groups.append({
                        'id': item['id'],
                        'name': item['name'],
                        'screen_name': item.get('screen_name', f"club{item['id']}"),
                        'description': item.get('description', ''),
                        'members_count': item.get('members_count', 0),
                        'activity': item.get('activity', ''),
                        'type': item.get('type', 'group')
                    })
            
            return groups
            
        except Exception as e:
            logger.error(f"Ошибка поиска групп: {e}")
            return []
    
    def categorize_group(self, name: str, description: str) -> List[str]:
        """Определяет категории группы"""
        text = f"{name} {description}".lower()
        categories_found = []
        
        for category, keywords in self.categories.items():
            for keyword in keywords:
                if keyword in text:
                    categories_found.append(category)
                    break
        
        return list(set(categories_found))
    
    async def find_similar_groups(self, group_name: str, group_description: str, 
                                limit: int = 10) -> List[Dict]:
        """Находит похожие группы"""
        logger.info(f"Поиск похожих групп для: {group_name}")
        
        # Извлекаем ключевые слова
        keywords = self.extract_keywords(f"{group_name} {group_description}")
        
        if not keywords:
            logger.warning("Не удалось извлечь ключевые слова")
            return []
        
        # Определяем категории
        categories = self.categorize_group(group_name, group_description)
        
        # Формируем поисковые запросы
        search_queries = []
        
        # Используем название группы
        search_queries.append(group_name)
        
        # Используем ключевые слова
        search_queries.extend(keywords[:3])
        
        # Используем категории
        search_queries.extend(categories[:2])
        
        # Удаляем дубликаты и None
        search_queries = list(set([q for q in search_queries if q]))
        
        # Ищем группы по всем запросам
        all_groups = {}
        
        for query in search_queries:
            try:
                groups = await self.search_similar_groups(query, limit=15)
                
                for group in groups:
                    group_id = group['id']
                    
                    # Пропускаем если уже есть
                    if group_id in all_groups:
                        continue
                    
                    # Вычисляем схожесть
                    similarity = self.calculate_similarity(
                        f"{group_name} {group_description}",
                        f"{group['name']} {group['description']}"
                    )
                    
                    # Добавляем только если схожесть выше порога
                    if similarity >= self.min_similarity_score:
                        group['similarity_score'] = similarity
                        group['categories'] = self.categorize_group(
                            group['name'], group['description']
                        )
                        all_groups[group_id] = group
                        
                        logger.debug(f"Найдена похожая группа: {group['name']} "
                                   f"(схожесть: {similarity:.2f})")
                
                await asyncio.sleep(0.5)  # Задержка между запросами
                
            except Exception as e:
                logger.error(f"Ошибка поиска по запросу '{query}': {e}")
                continue
        
        # Сортируем по схожести
        similar_groups = sorted(
            all_groups.values(),
            key=lambda x: x.get('similarity_score', 0),
            reverse=True
        )
        
        # Ограничиваем количество
        result = similar_groups[:self.max_competitors]
        
        logger.info(f"Найдено {len(result)} похожих групп")
        return result
    
    async def analyze_competitor(self, competitor: Dict) -> Dict:
        """Анализирует одного конкурента"""
        try:
            logger.info(f"Анализ конкурента: {competitor.get('name')}")
            
            # Получаем участников (ограничиваем для скорости)
            members_limit = min(200, competitor.get('members_count', 0))
            if members_limit <= 0:
                return competitor
            
            members = await vk_client.get_group_members(competitor['id'], limit=members_limit)
            
            if not members:
                return competitor
            
            # Здесь можно добавить анализ аудитории конкурента
            # Для простоты пока возвращаем как есть
            competitor['analyzed_members'] = len(members)
            competitor['analysis_available'] = True
            
            return competitor
            
        except Exception as e:
            logger.error(f"Ошибка анализа конкурента {competitor.get('name')}: {e}")
            competitor['analysis_available'] = False
            return competitor
    
    async def compare_with_competitors(self, target_group: Dict, target_analysis: Dict, 
                                     competitors: List[Dict]) -> Dict[str, Any]:
        """Сравнивает целевую группу с конкурентами"""
        if not competitors:
            return {}
        
        comparison = {
            'target_group': target_group['name'],
            'total_competitors': len(competitors),
            'competitors_analyzed': 0,
            'strengths': [],
            'weaknesses': [],
            'recommendations': []
        }
        
        # Собираем метрики конкурентов
        competitor_metrics = []
        
        for competitor in competitors:
            if 'analysis' not in competitor:
                continue
            
            analysis = competitor['analysis']
            
            metrics = {
                'name': competitor['name'],
                'quality_score': analysis.get('audience_quality_score', 0),
                'members_count': competitor.get('members_count', 0),
                'similarity': competitor.get('similarity_score', 0)
            }
            
            # Добавляем демографические данные если есть
            if 'gender' in analysis:
                metrics['male_percentage'] = analysis['gender'].get('male', 0)
                metrics['female_percentage'] = analysis['gender'].get('female', 0)
            
            competitor_metrics.append(metrics)
        
        comparison['competitors_analyzed'] = len(competitor_metrics)
        
        if not competitor_metrics:
            return comparison
        
        # Сравниваем по качеству аудитории
        target_quality = target_analysis.get('audience_quality_score', 0)
        competitor_qualities = [c['quality_score'] for c in competitor_metrics]
        
        avg_competitor_quality = sum(competitor_qualities) / len(competitor_qualities)
        
        # Определяем позицию в рейтинге
        all_qualities = [target_quality] + competitor_qualities
        sorted_qualities = sorted(all_qualities, reverse=True)
        rank = sorted_qualities.index(target_quality) + 1
        
        comparison['rank'] = rank
        comparison['target_quality'] = target_quality
        comparison['avg_competitor_quality'] = round(avg_competitor_quality, 1)
        
        # Определяем сильные и слабые стороны
        if target_quality > avg_competitor_quality:
            comparison['strengths'].append(
                f"Качество вашей аудитории выше среднего на "
                f"{target_quality - avg_competitor_quality:.1f} баллов"
            )
        else:
            comparison['weaknesses'].append(
                f"Качество вашей аудитории ниже среднего на "
                f"{avg_competitor_quality - target_quality:.1f} баллов"
            )
        
        # Сравниваем по размеру аудитории
        target_size = target_group.get('members_count', 0)
        competitor_sizes = [c.get('members_count', 0) for c in competitors]
        
        if competitor_sizes:
            avg_competitor_size = sum(competitor_sizes) / len(competitor_sizes)
            
            if target_size > avg_competitor_size * 1.5:
                comparison['strengths'].append(
                    f"Ваша аудитория больше средней у конкурентов в "
                    f"{target_size / avg_competitor_size:.1f} раз"
                )
            elif target_size < avg_competitor_size * 0.7:
                comparison['weaknesses'].append(
                    f"Ваша аудитория меньше средней у конкурентов в "
                    f"{avg_competitor_size / target_size:.1f} раз"
                )
        
        # Формируем рекомендации
        if rank == 1:
            comparison['recommendations'].append(
                "🎉 Ваша группа лучшая среди конкурентов! Продолжайте в том же духе."
            )
        elif rank <= 3:
            comparison['recommendations'].append(
                f"🏆 Вы в топ-{rank}! Сфокусируйтесь на улучшении качества контента."
            )
        else:
            comparison['recommendations'].append(
                f"📈 Вы на {rank} месте. Изучите лидеров рынка и внедрите их лучшие практики."
            )
        
        # Рекомендации по улучшению
        if target_quality < 60:
            comparison['recommendations'].append(
                "⚡ Работайте над улучшением качества аудитории через более качественный контент."
            )
        
        # Рекомендации по тематике
        if 'categories' in competitors[0]:
            competitor_categories = set()
            for comp in competitors:
                if 'categories' in comp:
                    competitor_categories.update(comp['categories'])
            
            if competitor_categories:
                comparison['recommendations'].append(
                    f"🎯 Основные темы конкурентов: {', '.join(list(competitor_categories)[:3])}"
                )
        
        return comparison
    
    def generate_competitor_report(self, target_group: Dict, competitors: List[Dict]) -> str:
        """Генерирует текстовый отчет по конкурентам"""
        report_lines = [
            f"📊 АНАЛИЗ КОНКУРЕНТОВ: {target_group['name']}",
            "=" * 50,
            f"Целевая группа: {target_group['name']}",
            f"Участников: {target_group.get('members_count', 0):,}",
            f"Найдено конкурентов: {len(competitors)}",
            "",
            "ТОП-5 КОНКУРЕНТОВ:",
            ""
        ]
        
        for i, competitor in enumerate(competitors[:5], 1):
            report_lines.append(
                f"{i}. {competitor['name']} "
                f"({competitor.get('members_count', 0):,} участников) - "
                f"схожесть: {competitor.get('similarity_score', 0):.2f}"
            )
            
            if competitor.get('description'):
                desc = competitor['description'][:100] + "..." if len(competitor['description']) > 100 else competitor['description']
                report_lines.append(f"   Описание: {desc}")
            
            if competitor.get('categories'):
                report_lines.append(f"   Категории: {', '.join(competitor['categories'][:3])}")
            
            report_lines.append(f"   Ссылка: vk.com/{competitor.get('screen_name', '')}")
            report_lines.append("")
        
        # Рекомендации
        report_lines.append("💡 РЕКОМЕНДАЦИИ:")
        report_lines.append("1. Изучите контент топ-3 конкурентов")
        report_lines.append("2. Проанализируйте их активность и вовлеченность")
        report_lines.append("3. Определите свои уникальные преимущества")
        report_lines.append("4. Разработайте стратегию отстройки от конкурентов")
        
        return "\n".join(report_lines)
