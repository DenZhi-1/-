from typing import Dict, List, Any, Optional
from collections import Counter
from datetime import datetime
import re

class AudienceAnalyzer:
    def __init__(self):
        self.age_groups = {
            '12-17': (12, 17),
            '18-24': (18, 24),
            '25-34': (25, 34),
            '35-44': (35, 44),
            '45+': (45, 120)
        }
    
    def extract_age(self, bdate: str) -> Optional[int]:
        """Извлекает возраст из даты рождения"""
        if not bdate:
            return None
            
        try:
            parts = bdate.split('.')
            if len(parts) >= 2:
                day, month = int(parts[0]), int(parts[1])
                year = int(parts[2]) if len(parts) == 3 else None
                
                if year:
                    current_year = datetime.now().year
                    age = current_year - year
                    if datetime.now().month < month or (datetime.now().month == month and datetime.now().day < day):
                        age -= 1
                    return age if 12 <= age <= 120 else None
        except:
            return None
        return None
    
    def categorize_interests(self, user: Dict) -> List[str]:
        """Категоризирует интересы пользователя"""
        interests = []
        fields = ['interests', 'activities', 'books', 'music', 'movies', 'games']
        
        for field in fields:
            value = user.get(field)
            if value and isinstance(value, str):
                interests.extend([interest.strip().lower() for interest in value.split(',') if interest.strip()])
        
        return list(set(interests))
    
    async def analyze_audience(self, members: List[Dict]) -> Dict[str, Any]:
        """Анализирует аудиторию группы"""
        if not members:
            return {}
        
        total = len(members)
        analysis = {
            'total_members': total,
            'gender': {'male': 0, 'female': 0, 'unknown': 0},
            'age_groups': {group: 0 for group in self.age_groups},
            'cities': Counter(),
            'interests': Counter(),
            'recommendations': []
        }
        
        # Сбор данных
        for member in members:
            # Гендер
            sex = member.get('sex', 0)
            if sex == 2:
                analysis['gender']['male'] += 1
            elif sex == 1:
                analysis['gender']['female'] += 1
            else:
                analysis['gender']['unknown'] += 1
            
            # Возраст
            age = self.extract_age(member.get('bdate', ''))
            if age:
                for group, (min_age, max_age) in self.age_groups.items():
                    if min_age <= age <= max_age:
                        analysis['age_groups'][group] += 1
                        break
            
            # Город
            city = member.get('city', {}).get('title')
            if city:
                analysis['cities'][city] += 1
            
            # Интересы
            interests = self.categorize_interests(member)
            for interest in interests:
                analysis['interests'][interest] += 1
        
        # Преобразование в проценты
        if total > 0:
            for key in analysis['gender']:
                analysis['gender'][key] = round(analysis['gender'][key] / total * 100, 1)
            
            for group in analysis['age_groups']:
                analysis['age_groups'][group] = round(analysis['age_groups'][group] / total * 100, 1)
            
            # Топ 10 городов
            top_cities = dict(analysis['cities'].most_common(10))
            analysis['cities'] = {city: round(count / total * 100, 1) for city, count in top_cities.items()}
            
            # Топ 20 интересов
            top_interests = dict(analysis['interests'].most_common(20))
            analysis['interests'] = top_interests
        
        # Генерация рекомендаций
        analysis['recommendations'] = self._generate_recommendations(analysis)
        
        return analysis
    
    def _generate_recommendations(self, analysis: Dict) -> List[str]:
        """Генерирует рекомендации для таргетированной рекламы"""
        recommendations = []
        
        # Рекомендации по полу
        if analysis['gender']['male'] > 70:
            recommendations.append("Аудитория преимущественно мужская - используйте мужские образы в рекламе")
        elif analysis['gender']['female'] > 70:
            recommendations.append("Аудитория преимущественно женская - адаптируйте контент под женскую аудиторию")
        
        # Рекомендации по возрасту
        main_age_group = max(analysis['age_groups'].items(), key=lambda x: x[1])
        if main_age_group[1] > 40:
            recommendations.append(f"Основная возрастная группа: {main_age_group[0]} - используйте соответствующий язык и референсы")
        
        # Рекомендации по географии
        if analysis['cities']:
            top_city = list(analysis['cities'].keys())[0]
            if analysis['cities'][top_city] > 30:
                recommendations.append(f"Аудитория сконцентрирована в {top_city} - используйте геотаргетинг")
        
        # Рекомендации по интересам
        if analysis['interests']:
            top_interests = list(analysis['interests'].keys())[:3]
            recommendations.append(f"Популярные интересы: {', '.join(top_interests)} - обыгрывайте их в креативах")
        
        return recommendations[:5]  # Ограничиваем 5 рекомендациями
    
    async def compare_audiences(self, analysis1: Dict, analysis2: Dict) -> Dict:
        """Сравнивает две аудитории"""
        similarity_score = 0
        common_characteristics = []
        
        # Сравнение демографии
        gender_diff = abs(analysis1['gender']['male'] - analysis2['gender']['male'])
        if gender_diff < 10:
            similarity_score += 25
            common_characteristics.append("Схожее гендерное распределение")
        
        # Сравнение возрастных групп
        age_similarity = 0
        for group in analysis1['age_groups']:
            diff = abs(analysis1['age_groups'][group] - analysis2['age_groups'][group])
            if diff < 5:
                age_similarity += 1
        
        if age_similarity >= 3:
            similarity_score += 25
            common_characteristics.append("Схожие возрастные группы")
        
        # Сравнение городов
        cities1 = set(analysis1['cities'].keys())
        cities2 = set(analysis2['cities'].keys())
        common_cities = cities1.intersection(cities2)
        if len(common_cities) >= 2:
            similarity_score += 25
            common_characteristics.append(f"Общие города: {', '.join(list(common_cities)[:3])}")
        
        # Сравнение интересов
        interests1 = set(analysis1['interests'].keys())
        interests2 = set(analysis2['interests'].keys())
        common_interests = interests1.intersection(interests2)
        if len(common_interests) >= 5:
            similarity_score += 25
            common_characteristics.append("Схожие интересы аудитории")
        
        return {
            'similarity_score': min(similarity_score, 100),
            'common_characteristics': common_characteristics
        }
        }
