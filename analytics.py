import logging
import re
import asyncio
from collections import Counter
from typing import Dict, List, Any, Tuple, Optional
from datetime import datetime, date

logger = logging.getLogger(__name__)


class AudienceAnalyzer:
    """Анализатор аудитории ВКонтакте с расширенной аналитикой"""
    
    def __init__(self):
        # Категории интересов для классификации
        self.interest_categories = {
            'технологии': ['программирование', 'it', 'код', 'python', 'java', 'javascript', 'разработка',
                          'компьютер', 'айти', 'гаджеты', 'технологии', 'техника', 'смартфон', 'ноутбук'],
            'образование': ['учеба', 'образование', 'курсы', 'школа', 'университет', 'вуз', 'студент',
                           'обучение', 'знания', 'наука', 'исследование', 'лаборатория'],
            'спорт': ['спорт', 'футбол', 'хоккей', 'баскетбол', 'тренировка', 'фитнес', 'зал', 'бег',
                     'йога', 'плавание', 'кроссфит', 'бокс', 'единоборства'],
            'искусство': ['искусство', 'живопись', 'музыка', 'кино', 'театр', 'танцы', 'фотография',
                         'дизайн', 'арт', 'творчество', 'рисование', 'пение'],
            'бизнес': ['бизнес', 'стартап', 'предпринимательство', 'инвестиции', 'финансы', 'маркетинг',
                      'продажи', 'управление', 'компания', 'проект', 'деньги', 'экономика'],
            'путешествия': ['путешествия', 'туризм', 'отдых', 'отпуск', 'страны', 'города', 'поездки',
                           'авиабилеты', 'отели', 'курорты', 'пляж', 'горы'],
            'мода': ['мода', 'стиль', 'одежда', 'обувь', 'косметика', 'красота', 'прическа', 'макияж',
                    'шопинг', 'бренды', 'тренды', 'луки'],
            'авто': ['авто', 'машина', 'автомобиль', 'водитель', 'дорога', 'тюнинг', 'мотоцикл',
                    'бензин', 'ремонт', 'запчасти'],
            'кулинария': ['кулинария', 'готовка', 'рецепты', 'еда', 'кухня', 'повар', 'блюда', 'десерты',
                         'рестораны', 'напитки', 'кофе', 'чай'],
            'здоровье': ['здоровье', 'медицина', 'врач', 'больница', 'лечение', 'диета', 'витамины',
                        'тренировка', 'йога', 'медитация', 'психология'],
            'гейминг': ['игры', 'гейминг', 'игрок', 'консоль', 'ps', 'xbox', 'steam', 'киберспорт',
                       'стрим', 'twitch', 'дота', 'кспоп', 'майнкрафт'],
            'книги': ['книги', 'чтение', 'литература', 'роман', 'фэнтези', 'детектив', 'поэзия',
                     'писатель', 'библиотека', 'аудиокнига'],
            'сериалы': ['сериалы', 'фильмы', 'кино', 'нетфликс', 'hdrezka', 'тв', 'актеры', 'режиссер',
                       'премьера', 'обзор'],
            'музыка': ['музыка', 'плейлист', 'концерт', 'альбом', 'исполнитель', 'группа', 'рок',
                      'поп', 'хип-хоп', 'джаз', 'классика'],
            'хобби': ['хобби', 'рукоделие', 'коллекционирование', 'садоводство', 'рыбалка', 'охота',
                     'вышивка', 'вязание', 'моделирование', 'пазлы'],
        }
        
        # Города России и СНГ для классификации
        self.russian_cities = [
            'москва', 'санкт-петербург', 'новосибирск', 'екатеринбург', 'нижний новгород',
            'казань', 'челябинск', 'омск', 'самара', 'ростов-на-дону', 'уфа', 'красноярск',
            'пермь', 'воронеж', 'волгоград', 'краснодар', 'саратов', 'тюмень', 'тольятти',
            'ижевск', 'барнаул', 'ульяновск', 'иркутск', 'хабаровск', 'ярославль', 'владивосток',
            'махачкала', 'томск', 'оренбург', 'кемерово', 'новокузнецк', 'рязань', 'астрахань',
            'пенза', 'липецк', 'киров', 'чебоксары', 'калининград', 'тула', 'ставрополь',
            'курск', 'сочи', 'тверь', 'магнитогорск', 'сургут', 'волжский', 'салават'
        ]
        
        # Возрастные группы
        self.age_groups = {
            'до 18': (0, 18),
            '18-24': (18, 25),
            '25-34': (25, 35),
            '35-44': (35, 45),
            '45-54': (45, 55),
            '55+': (55, 200)
        }

    def _calculate_age(self, bdate: str) -> Optional[int]:
        """Вычисляет возраст по дате рождения"""
        try:
            if not bdate or len(bdate.split('.')) < 2:
                return None
            
            parts = bdate.split('.')
            day = int(parts[0])
            month = int(parts[1])
            year = int(parts[2]) if len(parts) > 2 else None
            
            if not year:
                return None
            
            today = date.today()
            age = today.year - year - ((today.month, today.day) < (month, day))
            return max(0, age)  # Возраст не может быть отрицательным
            
        except (ValueError, IndexError):
            return None

    def _categorize_interests(self, interests_text: str) -> List[str]:
        """Категоризирует интересы по предопределенным категориям"""
        if not interests_text:
            return []
        
        text_lower = interests_text.lower()
        categories = []
        
        for category, keywords in self.interest_categories.items():
            for keyword in keywords:
                if keyword in text_lower:
                    categories.append(category)
                    break  # Не добавляем категорию дважды
        
        return list(set(categories))  # Убираем дубликаты

    def _analyze_gender(self, members: List[Dict]) -> Dict[str, float]:
        """Анализ гендерного распределения"""
        gender_counter = Counter()
        
        for member in members:
            gender = member.get('sex', 0)
            if gender == 1:  # женский
                gender_counter['female'] += 1
            elif gender == 2:  # мужской
                gender_counter['male'] += 1
            else:
                gender_counter['unknown'] += 1
        
        total = len(members)
        if total == 0:
            return {'male': 0, 'female': 0, 'unknown': 0}
        
        return {
            'male': round((gender_counter['male'] / total) * 100, 1),
            'female': round((gender_counter['female'] / total) * 100, 1),
            'unknown': round((gender_counter['unknown'] / total) * 100, 1)
        }

    def _analyze_age(self, members: List[Dict]) -> Dict[str, float]:
        """Анализ возрастного распределения"""
        age_counter = Counter()
        ages = []
        unknown_ages = 0
        
        for member in members:
            bdate = member.get('bdate')
            if bdate:
                age = self._calculate_age(bdate)
                if age is not None:
                    ages.append(age)
                    # Распределяем по возрастным группам
                    for group_name, (min_age, max_age) in self.age_groups.items():
                        if min_age <= age < max_age:
                            age_counter[group_name] += 1
                            break
                else:
                    unknown_ages += 1
            else:
                unknown_ages += 1
        
        total_known = len(ages)
        total_members = len(members)
        
        result = {}
        if total_members > 0:
            for group_name in self.age_groups.keys():
                result[group_name] = round((age_counter[group_name] / total_members) * 100, 1)
            
            # Средний возраст
            if ages:
                result['average_age'] = round(sum(ages) / len(ages), 1)
            else:
                result['average_age'] = 0
            
            # Доля неизвестных возрастов
            result['unknown_percentage'] = round((unknown_ages / total_members) * 100, 1)
        
        return result

    def _analyze_geography(self, members: List[Dict]) -> Dict[str, Any]:
        """Анализ географического распределения"""
        cities_counter = Counter()
        countries_counter = Counter()
        unknown_location = 0
        
        for member in members:
            city_info = member.get('city')
            country_info = member.get('country')
            
            if city_info and 'title' in city_info:
                city_name = city_info['title'].lower()
                cities_counter[city_name] += 1
            else:
                unknown_location += 1
            
            if country_info and 'title' in country_info:
                country_name = country_info['title']
                countries_counter[country_name] += 1
        
        total = len(members)
        
        # Топ-10 городов
        top_cities = {}
        for city, count in cities_counter.most_common(10):
            percentage = round((count / total) * 100, 1)
            top_cities[city.title()] = percentage
        
        # Распределение по странам
        countries_distribution = {}
        for country, count in countries_counter.most_common(5):
            percentage = round((count / total) * 100, 1)
            countries_distribution[country] = percentage
        
        # Классификация городов
        city_types = {
            'столицы': 0,
            'миллионники': 0,
            'крупные_города': 0,
            'средние_города': 0,
            'малые_города': 0
        }
        
        for city, count in cities_counter.items():
            city_lower = city.lower()
            if city_lower in ['москва', 'санкт-петербург', 'минск', 'киев', 'астана']:
                city_types['столицы'] += count
            elif city_lower in self.russian_cities[:15]:  # Первые 15 - миллионники
                city_types['миллионники'] += count
            elif count >= 100:  # Крупные города
                city_types['крупные_города'] += count
            elif count >= 30:  # Средние города
                city_types['средние_города'] += count
            else:
                city_types['малые_города'] += count
        
        # Проценты по типам городов
        city_types_percentage = {}
        for city_type, count in city_types.items():
            if total > 0:
                city_types_percentage[city_type] = round((count / total) * 100, 1)
        
        return {
            'top_cities': top_cities,
            'countries': countries_distribution,
            'city_types': city_types_percentage,
            'unknown_location_percentage': round((unknown_location / total) * 100, 1) if total > 0 else 0
        }

    def _analyze_interests(self, members: List[Dict]) -> Dict[str, Any]:
        """Анализ интересов и активностей"""
        interests_counter = Counter()
        activities_counter = Counter()
        categories_counter = Counter()
        
        for member in members:
            # Анализ интересов
            interests = member.get('interests', '')
            if interests:
                categories = self._categorize_interests(interests)
                for category in categories:
                    categories_counter[category] += 1
            
            # Анализ активностей
            activities = member.get('activities', '')
            if activities:
                categories = self._categorize_interests(activities)
                for category in categories:
                    categories_counter[category] += 1
        
        total_with_interests = sum(categories_counter.values())
        
        # Популярные категории интересов
        popular_categories = {}
        for category, count in categories_counter.most_common(10):
            if total_with_interests > 0:
                popular_categories[category] = round((count / total_with_interests) * 100, 1)
        
        # Степень заполненности профилей
        filled_profiles = sum(1 for m in members if m.get('interests') or m.get('activities'))
        profile_fill_rate = round((filled_profiles / len(members)) * 100, 1) if members else 0
        
        return {
            'popular_categories': popular_categories,
            'profile_fill_rate': profile_fill_rate,
            'total_categories_found': len(categories_counter)
        }

    def _analyze_social_activity(self, members: List[Dict]) -> Dict[str, Any]:
        """Анализ социальной активности"""
        # Время последнего посещения
        last_seen_distribution = {
            'менее_дня': 0,
            '1-7_дней': 0,
            '1-4_недели': 0,
            '1-3_месяца': 0,
            'более_3_месяцев': 0,
            'никогда': 0
        }
        
        now_timestamp = datetime.now().timestamp()
        
        for member in members:
            last_seen = member.get('last_seen')
            if last_seen and 'time' in last_seen:
                time_diff = now_timestamp - last_seen['time']
                days_diff = time_diff / (24 * 3600)
                
                if days_diff < 1:
                    last_seen_distribution['менее_дня'] += 1
                elif days_diff < 7:
                    last_seen_distribution['1-7_дней'] += 1
                elif days_diff < 30:
                    last_seen_distribution['1-4_недели'] += 1
                elif days_diff < 90:
                    last_seen_distribution['1-3_месяца'] += 1
                else:
                    last_seen_distribution['более_3_месяцев'] += 1
            else:
                last_seen_distribution['никогда'] += 1
        
        total = len(members)
        last_seen_percentage = {}
        for period, count in last_seen_distribution.items():
            if total > 0:
                last_seen_percentage[period] = round((count / total) * 100, 1)
        
        return {
            'last_seen_distribution': last_seen_percentage,
            'active_users_percentage': round(
                (last_seen_distribution['менее_дня'] + last_seen_distribution['1-7_дней']) / total * 100, 1
            ) if total > 0 else 0
        }

    def _analyze_profile_completeness(self, members: List[Dict]) -> Dict[str, float]:
        """Анализ полноты профилей"""
        completeness_scores = []
        
        for member in members:
            score = 0
            total_fields = 0
            
            # Проверяем заполнение основных полей
            fields_to_check = [
                ('sex', 10),
                ('bdate', 15),
                ('city', 15),
                ('country', 10),
                ('interests', 15),
                ('activities', 15),
                ('last_seen', 20)
            ]
            
            for field, weight in fields_to_check:
                total_fields += weight
                if member.get(field):
                    score += weight
            
            if total_fields > 0:
                completeness_scores.append((score / total_fields) * 100)
        
        if not completeness_scores:
            return {
                'average_completeness': 0,
                'high_completeness_percentage': 0,
                'low_completeness_percentage': 0
            }
        
        avg_completeness = round(sum(completeness_scores) / len(completeness_scores), 1)
        
        # Процент профилей с высокой заполненностью (>70%)
        high_completeness = sum(1 for score in completeness_scores if score > 70)
        high_percentage = round((high_completeness / len(completeness_scores)) * 100, 1)
        
        # Процент профилей с низкой заполненностью (<30%)
        low_completeness = sum(1 for score in completeness_scores if score < 30)
        low_percentage = round((low_completeness / len(completeness_scores)) * 100, 1)
        
        return {
            'average_completeness': avg_completeness,
            'high_completeness_percentage': high_percentage,
            'low_completeness_percentage': low_percentage
        }

    def _generate_recommendations(self, analysis: Dict[str, Any]) -> List[str]:
        """Генерация рекомендаций для таргетинга"""
        recommendations = []
        
        # Гендерные рекомендации
        gender = analysis.get('gender', {})
        if gender.get('male', 0) > 70:
            recommendations.append("✅ <b>Аудитория преимущественно мужская</b> - используйте мужские темы в рекламе")
        elif gender.get('female', 0) > 70:
            recommendations.append("✅ <b>Аудитория преимущественно женская</b> - акцент на женские интересы")
        
        # Возрастные рекомендации
        age_groups = analysis.get('age_groups', {})
        if age_groups.get('18-24', 0) > 40:
            recommendations.append("🎓 <b>Молодая аудитория 18-24 года</b> - эффективны трендовый контент и соцсети")
        elif age_groups.get('35-44', 0) > 40:
            recommendations.append("💼 <b>Аудитория 35-44 года</b> - делайте акцент на стабильность и качество")
        
        # Географические рекомендации
        geography = analysis.get('geography', {})
        city_types = geography.get('city_types', {})
        if city_types.get('столицы', 0) > 50:
            recommendations.append("🏙️ <b>Преобладают столичные жители</b> - можно предлагать премиум-товары")
        elif city_types.get('малые_города', 0) > 50:
            recommendations.append("🏡 <b>Много жителей малых городов</b> - важны доступность и доставка")
        
        # Активность
        social = analysis.get('social_activity', {})
        if social.get('active_users_percentage', 0) > 70:
            recommendations.append("📱 <b>Высокая активность аудитории</b> - подходят частые публикации и интерактив")
        else:
            recommendations.append("⏰ <b>Аудитория не очень активна</b> - делайте публикации реже, но качественнее")
        
        # Интересы
        interests = analysis.get('interests', {})
        popular_categories = interests.get('popular_categories', {})
        for category, percentage in list(popular_categories.items())[:3]:
            if percentage > 20:
                recommendations.append(f"🎯 <b>Популярная тема: {category}</b> - используйте в контенте")
        
        # Полнота профилей
        completeness = analysis.get('profile_completeness', {})
        if completeness.get('high_completeness_percentage', 0) > 60:
            recommendations.append("📋 <b>Профили хорошо заполнены</b> - можно использовать сложный таргетинг")
        
        # Добавляем общие рекомендации
        recommendations.append("💡 <b>Используйте Lookalike-аудитории</b> для поиска похожих пользователей")
        recommendations.append("📊 <b>Тестируйте разные форматы рекламы</b> - картинки, видео, карусели")
        recommendations.append("⏳ <b>Публикуйте контент в пиковые часы</b> - 9-11 утра и 19-22 вечера")
        
        return recommendations[:10]  # Ограничиваем 10 рекомендациями

    def _calculate_audience_quality_score(self, analysis: Dict[str, Any]) -> float:
        """Оценка качества аудитории (0-100 баллов)"""
        score = 50  # Базовый балл
        
        # Полнота профилей (+20 макс)
        completeness = analysis.get('profile_completeness', {})
        score += (completeness.get('average_completeness', 0) / 100) * 20
        
        # Активность пользователей (+20 макс)
        social = analysis.get('social_activity', {})
        activity_percentage = social.get('active_users_percentage', 0)
        score += (activity_percentage / 100) * 20
        
        # Разнообразие интересов (+10 макс)
        interests = analysis.get('interests', {})
        if interests.get('total_categories_found', 0) > 5:
            score += 10
        
        # Сбалансированность по полу (+10 макс)
        gender = analysis.get('gender', {})
        gender_diff = abs(gender.get('male', 0) - gender.get('female', 0))
        if gender_diff < 20:  # Более сбалансированная аудитория
            score += 10
        
        return round(min(100, max(0, score)), 1)  # Ограничиваем 0-100

    async def analyze_audience(self, members: List[Dict]) -> Dict[str, Any]:
        """Основной метод анализа аудитории"""
        if not members:
            return {}
        
        logger.info(f"Начинаем анализ {len(members)} участников")
        
        # Параллельный анализ разных аспектов
        tasks = []
        tasks.append(asyncio.to_thread(self._analyze_gender, members))
        tasks.append(asyncio.to_thread(self._analyze_age, members))
        tasks.append(asyncio.to_thread(self._analyze_geography, members))
        tasks.append(asyncio.to_thread(self._analyze_interests, members))
        tasks.append(asyncio.to_thread(self._analyze_social_activity, members))
        tasks.append(asyncio.to_thread(self._analyze_profile_completeness, members))
        
        results = await asyncio.gather(*tasks)
        
        analysis = {
            'gender': results[0],
            'age_groups': results[1],
            'geography': results[2],
            'interests': results[3],
            'social_activity': results[4],
            'profile_completeness': results[5],
            'total_members_analyzed': len(members)
        }
        
        # Генерация рекомендаций
        analysis['recommendations'] = self._generate_recommendations(analysis)
        
        # Оценка качества аудитории
        analysis['audience_quality_score'] = self._calculate_audience_quality_score(analysis)
        
        # Интерпретация оценки
        score = analysis['audience_quality_score']
        if score >= 80:
            analysis['quality_interpretation'] = "Отличная аудитория! Высокая вовлеченность и качество"
        elif score >= 60:
            analysis['quality_interpretation'] = "Хорошая аудитория. Есть потенциал для роста"
        elif score >= 40:
            analysis['quality_interpretation'] = "Средняя аудитория. Рекомендуется работа над вовлечением"
        else:
            analysis['quality_interpretation'] = "Слабая аудитория. Нужна стратегия по улучшению"
        
        logger.info(f"Анализ завершен. Оценка качества: {score}/100")
        return analysis

    async def compare_audiences(self, analysis1: Dict[str, Any], analysis2: Dict[str, Any]) -> Dict[str, Any]:
        """Сравнение двух аудиторий"""
        
        def calculate_similarity_percentage(dict1: Dict, dict2: Dict) -> float:
            """Вычисляет процент схожести двух словарей"""
            if not dict1 or not dict2:
                return 0
            
            total_keys = set(dict1.keys()) | set(dict2.keys())
            if not total_keys:
                return 0
            
            similarity_sum = 0
            for key in total_keys:
                val1 = dict1.get(key, 0)
                val2 = dict2.get(key, 0)
                similarity_sum += 100 - abs(val1 - val2)
            
            return round(similarity_sum / len(total_keys), 1)
        
        # Сравнение по разным аспектам
        gender_similarity = calculate_similarity_percentage(
            analysis1.get('gender', {}),
            analysis2.get('gender', {})
        )
        
        age_similarity = calculate_similarity_percentage(
            analysis1.get('age_groups', {}),
            analysis2.get('age_groups', {})
        )
        
        # Общий процент схожести
        total_similarity = round((gender_similarity + age_similarity) / 2, 1)
        
        # Поиск общих характеристик
        common_characteristics = []
        
        # Сравнение преобладающего пола
        gender1 = analysis1.get('gender', {})
        gender2 = analysis2.get('gender', {})
        if gender1.get('male', 0) > 50 and gender2.get('male', 0) > 50:
            common_characteristics.append("Преобладает мужская аудитория")
        elif gender1.get('female', 0) > 50 and gender2.get('female', 0) > 50:
            common_characteristics.append("Преобладает женская аудитория")
        
        # Сравнение основной возрастной группы
        age1 = analysis1.get('age_groups', {})
        age2 = analysis2.get('age_groups', {})
        main_age1 = max(age1.items(), key=lambda x: x[1])[0] if age1 else None
        main_age2 = max(age2.items(), key=lambda x: x[1])[0] if age2 else None
        
        if main_age1 and main_age2 and main_age1 == main_age2:
            common_characteristics.append(f"Основная возрастная группа: {main_age1}")
        
        # Сравнение качества аудитории
        score1 = analysis1.get('audience_quality_score', 0)
        score2 = analysis2.get('audience_quality_score', 0)
        
        if abs(score1 - score2) < 10:
            common_characteristics.append("Схожее качество аудитории")
        elif score1 > score2 + 10:
            common_characteristics.append("Первая аудитория качественнее")
        elif score2 > score1 + 10:
            common_characteristics.append("Вторая аудитория качественнее")
        
        return {
            'similarity_score': total_similarity,
            'gender_similarity': gender_similarity,
            'age_similarity': age_similarity,
            'common_characteristics': common_characteristics,
            'audience1_quality': score1,
            'audience2_quality': score2,
            'quality_difference': round(abs(score1 - score2), 1)
        }
