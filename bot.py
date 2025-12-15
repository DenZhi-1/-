import asyncio
import logging
import json
from datetime import datetime
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command, CommandObject
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.enums import ParseMode

from config import config
from vk_api_client import vk_client
from analytics import AudienceAnalyzer
from text_analyzer import TextAnalyzer
from database import Database
from competitor_analysis import CompetitorAnalyzer

# Настройка логирования
log_level = getattr(logging, config.LOG_LEVEL.upper(), logging.INFO)
logging.basicConfig(
    level=log_level,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Уменьшаем логирование внешних библиотек
logging.getLogger('aiogram').setLevel(logging.WARNING)
logging.getLogger('aiohttp').setLevel(logging.WARNING)
logging.getLogger('asyncio').setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

# Валидация конфигурации при запуске
try:
    config.validate()
    logger.info("Конфигурация проверена успешно")
except ValueError as e:
    logger.error(f"Ошибка конфигурации: {e}")
    raise

# Инициализация компонентов бота
bot = Bot(
    token=config.TELEGRAM_BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
dp = Dispatcher()
db = Database()
analyzer = AudienceAnalyzer()
text_analyzer = TextAnalyzer()
competitor_analyzer = CompetitorAnalyzer()

# Словарь для хранения временных данных пользователей
user_sessions = {}

# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====================

def create_back_button(callback_data: str = "back_to_report") -> InlineKeyboardMarkup:
    """Создает кнопку 'Назад'"""
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data=callback_data)]
        ]
    )
    return keyboard

def create_main_menu_keyboard() -> InlineKeyboardMarkup:
    """Создает клавиатуру главного меню"""
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔍 Анализ группы", callback_data="analyze_group")],
            [InlineKeyboardButton(text="🥊 Анализ конкурентов", callback_data="competitors_help")],
            [InlineKeyboardButton(text="🧠 AI-анализ текста", callback_data="text_analysis_help")],
            [
                InlineKeyboardButton(text="📊 Статистика", callback_data="user_stats"),
                InlineKeyboardButton(text="📚 Помощь", callback_data="full_help")
            ]
        ]
    )
    return keyboard

def format_number(num: int) -> str:
    """Форматирует число с разделителями тысяч"""
    return f"{num:,}".replace(",", " ")

def get_quality_stars(score: float) -> str:
    """Возвращает звезды для оценки качества"""
    stars_count = min(5, max(1, int(score / 20)))
    return "⭐" * stars_count + "☆" * (5 - stars_count)

def create_competitor_keyboard() -> InlineKeyboardMarkup:
    """Создает клавиатуру для анализа конкурентов"""
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🔍 Найти конкурентов", callback_data="find_competitors"),
                InlineKeyboardButton(text="📊 Сравнить всех", callback_data="compare_all_competitors")
            ],
            [
                InlineKeyboardButton(text="📈 ТОП-5 конкурентов", callback_data="top_competitors"),
                InlineKeyboardButton(text="💡 Рекомендации", callback_data="competitor_recommendations")
            ],
            [
                InlineKeyboardButton(text="📤 Экспорт данных", callback_data="export_competitor_data"),
                InlineKeyboardButton(text="🔙 В главное меню", callback_data="main_menu")
            ]
        ]
    )
    return keyboard

def create_text_analysis_keyboard() -> InlineKeyboardMarkup:
    """Создает клавиатуру для AI-анализа текста"""
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="📊 Тональность", callback_data="text_sentiment"),
                InlineKeyboardButton(text="🔑 Ключевые слова", callback_data="text_keywords")
            ],
            [
                InlineKeyboardButton(text="📚 Темы", callback_data="text_topics"),
                InlineKeyboardButton(text="😊 Эмоции", callback_data="text_emotions")
            ],
            [
                InlineKeyboardButton(text="💡 Рекомендации", callback_data="text_recommendations"),
                InlineKeyboardButton(text="🔙 В главное меню", callback_data="main_menu")
            ]
        ]
    )
    return keyboard

# ==================== ОСНОВНЫЕ КОМАНДЫ БОТА ====================

@dp.message(Command("start"))
async def cmd_start(message: Message):
    """Приветственное сообщение и список команд"""
    welcome_text = """
👋 <b>Привет! Я бот для глубокого анализа аудитории ВКонтакте.</b>

🚀 <b>НОВЫЕ ВОЗМОЖНОСТИ:</b>
• 🥊 <b>Анализ конкурентов</b> - автоматический поиск и анализ похожих групп
• 🧠 <b>AI-анализ текста</b> - определение тональности и тематик
• 📊 <b>Расширенная аналитика</b> - еще больше метрик и рекомендаций

🎯 <b>Основные команды:</b>
• /analyze [ссылка] — полный анализ аудитории
• /competitors [ссылка] — найти и проанализировать конкурентов
• /text_analysis [ссылка] — AI-анализ текстового контента
• /compare [ссылка1] [ссылка2] — сравнить две группы
• /quick [ссылка] — быстрый анализ
• /stats — ваша статистика
• /help — подробная справка

📝 <b>Примеры:</b>
<code>/analyze https://vk.com/vk</code>
<code>/competitors vk.com/public1</code>
<code>/text_analysis vk.com/groupname</code>

💡 <b>Совет:</b> Используйте команду /competitors для поиска и анализа похожих групп!
"""
    
    await message.answer(welcome_text, reply_markup=create_main_menu_keyboard())

@dp.message(Command("help"))
async def cmd_help(message: Message):
    """Подробная справка по использованию бота"""
    help_text = """
<b>📚 ПОЛНАЯ СПРАВКА ПО ИСПОЛЬЗОВАНИЮ БОТА</b>

<b>Основные команды:</b>

<code>/analyze ссылка_на_группу</code>
<b>Полный анализ аудитории</b>
• Глубокий анализ всех метрик
• Оценка качества аудитории
• Детальные рекомендации

<code>/competitors ссылка_на_группу</code>
<b>Анализ конкурентов (НОВОЕ!)</b>
• Автоматический поиск похожих групп
• Сравнение с конкурентами
• Определение конкурентных преимуществ
• Рекомендации по развитию

<code>/text_analysis ссылка_на_группу</code>
<b>AI-анализ текста (НОВОЕ!)</b>
• Анализ тональности контента
• Определение основных тематик
• Анализ ключевых слов
• Оценка эмоциональной окраски

<code>/quick ссылка_на_группу</code>
<b>Быстрый анализ</b>
• Основные метрики за 1 минуту
• Быстрая оценка аудитории

<code>/compare ссылка1 ссылка2</code>
<b>Сравнение двух групп</b>
• Сравнение демографии
• Сравнение интересов
• Оценка схожести

<code>/stats</code>
<b>Ваша статистика</b>
• Количество анализов
• История запросов
• Сохраненные отчеты

<code>/export [id]</code>
<b>Экспорт данных</b>
• Экспорт анализа в текстовый формат
• Полный отчет с детализацией

<b>🥊 АНАЛИЗ КОНКУРЕНТОВ:</b>
Бот автоматически найдет похожие группы по тематике и проведет их анализ:
1. Поиск конкурентов по ключевым словам
2. Анализ их аудитории
3. Сравнение с вашей группой
4. Выявление сильных и слабых сторон
5. Рекомендации по улучшению

<b>🧠 AI-АНАЛИЗ ТЕКСТА:</b>
Анализ текстового контента группы:
• Тональность (позитивная/негативная/нейтральная)
• Основные темы и категории
• Ключевые слова и фразы
• Эмоциональная окраска
• Рекомендации по контенту

<b>📋 ПОДДЕРЖИВАЕМЫЕ ФОРМАТЫ ССЫЛОК:</b>
• Полная ссылка: <code>https://vk.com/public123456</code>
• Сокращенная: <code>vk.com/club123456</code>
• Короткое имя: <code>https://vk.com/durov</code>
• Упоминание: <code>@durov</code>
• ID группы: <code>public1</code>

<b>⚠️ ОГРАНИЧЕНИЯ:</b>
• Только открытые группы ВК
• Максимум 1000 участников за анализ
• Лимиты VK API
• Анализ может занять 3-5 минут

<b>💡 СОВЕТЫ:</b>
1. Используйте /competitors для исследования рынка
2. Анализируйте текст с помощью /text_analysis
3. Сохраняйте интересные отчеты через /export
4. Сравнивайте группы через /compare
"""
    
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🥊 Анализ конкурентов", callback_data="start_competitors"),
                InlineKeyboardButton(text="🧠 AI-анализ текста", callback_data="start_text_analysis")
            ],
            [
                InlineKeyboardButton(text="🔍 Начать анализ", callback_data="start_analysis"),
                InlineKeyboardButton(text="🔙 В начало", callback_data="back_to_start")
            ]
        ]
    )
    
    await message.answer(help_text, reply_markup=keyboard, disable_web_page_preview=True)

@dp.message(Command("analyze"))
async def cmd_analyze(message: Message, command: CommandObject = None):
    """Полный анализ аудитории группы ВК"""
    try:
        args = message.text.split()[1:] if command is None else command.args.split()
        if not args:
            await message.answer(
                "❌ <b>Укажите ссылку на группу ВК</b>\n\n"
                "Пример: <code>/analyze https://vk.com/public123</code>\n"
                "Или: <code>/analyze vk.com/groupname</code>\n\n"
                "Для быстрого анализа используйте: <code>/quick ссылка</code>"
            )
            return
        
        group_link = args[0].strip()
        user_id = message.from_user.id
        
        # Проверяем, не выполняется ли уже анализ для этого пользователя
        if user_id in user_sessions and user_sessions[user_id].get('status') == 'analyzing':
            await message.answer(
                "⏳ <b>У вас уже выполняется анализ</b>\n\n"
                "Пожалуйста, дождитесь завершения текущего анализа."
            )
            return
        
        # Начинаем анализ
        user_sessions[user_id] = {
            'status': 'analyzing',
            'group_link': group_link,
            'current_step': 'получение_информации'
        }
        
        await message.answer("⏳ <b>Начинаю полный анализ аудитории...</b>")
        logger.info(f"Пользователь {user_id} запросил полный анализ {group_link}")
        
        # Получаем информацию о группе
        await message.answer("🔍 <b>Шаг 1 из 5:</b> Получаю информацию о группе...")
        group_info = await vk_client.get_group_info(group_link)
        
        if not group_info:
            del user_sessions[user_id]
            await message.answer(
                "❌ <b>Не удалось получить информацию о группе</b>\n\n"
                "Возможные причины:\n"
                "• Группа не существует или удалена\n"
                "• Группа заблокирована (banned) в ВК\n"
                "• Группа приватная или закрытая\n"
                "• Неверный формат ссылки\n\n"
                "Попробуйте:\n"
                "1. Проверить правильность ссылки\n"
                "2. Убедиться, что группа открыта и активна\n"
                "3. Использовать другую группу для анализа"
            )
            return
        
        # Проверяем, что группа открыта
        if group_info.get('is_closed', 1) != 0:
            del user_sessions[user_id]
            await message.answer(
                f"⚠️ <b>Группа '{group_info['name']}' закрытая или приватная</b>\n\n"
                "Анализ участников недоступен для закрытых групп ВК."
            )
            return
        
        # Проверяем наличие участников
        if group_info.get('members_count', 0) == 0:
            del user_sessions[user_id]
            await message.answer(
                f"⚠️ <b>В группе '{group_info['name']}' нет участников</b>\n\n"
                "Либо группа пустая, либо данные скрыты."
            )
            return
        
        # Обновляем сессию
        user_sessions[user_id].update({
            'group_info': group_info,
            'current_step': 'сбор_участников'
        })
        
        # Информируем о начале сбора данных
        info_message = await message.answer(
            f"📊 <b>Группа:</b> {group_info['name']}\n"
            f"👥 <b>Участников:</b> {format_number(group_info['members_count'])}\n"
            f"🔍 <b>Статус:</b> {'Открытая' if group_info.get('is_closed') == 0 else 'Закрытая'}\n\n"
            "⏳ <b>Шаг 2 из 5:</b> Собираю данные об участниках..."
        )
        
        # Получаем участников группы
        members_limit = min(1000, group_info['members_count'])
        members = await vk_client.get_group_members(group_info['id'], limit=members_limit)
        
        if not members:
            del user_sessions[user_id]
            await message.answer(
                "❌ <b>Не удалось получить информацию об участниках</b>\n\n"
                "Возможно:\n"
                "• Группа стала приватной во время анализа\n"
                "• Превышены лимиты VK API\n"
                "• Проблемы с сетью\n\n"
                "Попробуйте позже или выберите другую группу."
            )
            return
        
        user_sessions[user_id].update({
            'members': members,
            'current_step': 'анализ_демографии'
        })
        
        await info_message.edit_text(
            f"📊 <b>Группа:</b> {group_info['name']}\n"
            f"👥 <b>Участников:</b> {format_number(group_info['members_count'])}\n"
            f"📈 <b>Проанализировано:</b> {format_number(len(members))} "
            f"({min(100, (len(members) * 100) // group_info['members_count'])}%)\n\n"
            "⏳ <b>Шаг 3 из 5:</b> Анализирую демографию и географию..."
        )
        
        # Анализируем аудиторию
        analysis = await analyzer.analyze_audience(members)
        
        user_sessions[user_id].update({
            'analysis': analysis,
            'current_step': 'генерация_отчета'
        })
        
        await info_message.edit_text(
            f"📊 <b>Группа:</b> {group_info['name']}\n"
            f"👥 <b>Участников:</b> {format_number(group_info['members_count'])}\n"
            f"📈 <b>Проанализировано:</b> {format_number(len(members))}\n\n"
            "⏳ <b>Шаг 4 из 5:</b> Формирую детальный отчет..."
        )
        
        # Сохраняем результаты в базу данных
        saved = await db.save_analysis(
            user_id=user_id,
            group_id=group_info['id'],
            group_name=group_info['name'],
            analysis=analysis
        )
        
        if saved:
            logger.info(f"Анализ группы {group_info['name']} сохранен в БД")
        
        user_sessions[user_id].update({
            'current_step': 'отправка_результатов',
            'report_saved': saved
        })
        
        # Формируем и отправляем отчет
        await send_comprehensive_report(message, group_info, analysis, len(members))
        
        # Завершаем сессию
        user_sessions[user_id]['status'] = 'completed'
        
    except KeyError as e:
        logger.error(f"KeyError при анализе группы: {e}", exc_info=True)
        if message.from_user.id in user_sessions:
            del user_sessions[message.from_user.id]
        await message.answer(
            "❌ <b>Ошибка обработки данных от ВКонтакте</b>\n\n"
            "Техническая информация отправлена в лог.\n"
            "Попробуйте другую группу или повторите позже."
        )
    except Exception as e:
        logger.error(f"Непредвиденная ошибка в /analyze: {e}", exc_info=True)
        if message.from_user.id in user_sessions:
            del user_sessions[message.from_user.id]
        await message.answer(
            "❌ <b>Внутренняя ошибка при анализе</b>\n\n"
            "Пожалуйста, попробуйте позже.\n"
            "Если ошибка повторяется, сообщите администратору."
        )

async def send_comprehensive_report(message: Message, group_info: dict, analysis: dict, analyzed_count: int):
    """Отправляет комплексный отчет по анализу"""
    total_members = group_info['members_count']
    analyzed_percentage = min(100, (analyzed_count * 100) // total_members)
    
    # Клавиатура для навигации по отчету
    report_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="📊 Демография", callback_data="report_demography"),
                InlineKeyboardButton(text="🎯 Интересы", callback_data="report_interests")
            ],
            [
                InlineKeyboardButton(text="📱 Активность", callback_data="report_activity"),
                InlineKeyboardButton(text="🏙️ География", callback_data="report_geography")
            ],
            [
                InlineKeyboardButton(text="⭐ Качество", callback_data="report_quality"),
                InlineKeyboardButton(text="💡 Рекомендации", callback_data="report_recommendations")
            ],
            [
                InlineKeyboardButton(text="💾 Сохранить отчет", callback_data="save_report"),
                InlineKeyboardButton(text="📤 Экспорт", callback_data="export_report")
            ]
        ]
    )
    
    # Основное сообщение с сводкой
    summary_report = f"""
📊 <b>ПОЛНЫЙ АНАЛИЗ АУДИТОРИИ: {group_info['name']}</b>

<b>📋 ОБЩАЯ ИНФОРМАЦИЯ:</b>
👥 Всего участников: <b>{format_number(total_members)}</b>
📈 Проанализировано: <b>{format_number(analyzed_count)}</b> ({analyzed_percentage}%)
🔗 Ссылка: vk.com/{group_info.get('screen_name', '')}

<b>⭐ ОЦЕНКА КАЧЕСТВА АУДИТОРИИ:</b>
{get_quality_stars(analysis.get('audience_quality_score', 0))} <b>{analysis.get('audience_quality_score', 0)}/100</b>
<i>{analysis.get('quality_interpretation', '')}</i>

<b>👫 ОСНОВНЫЕ МЕТРИКИ:</b>
"""
    
    # Добавляем основные метрики
    gender = analysis.get('gender', {})
    if gender:
        main_gender = "👨 Мужчины" if gender.get('male', 0) > gender.get('female', 0) else "👩 Женщины"
        main_percentage = max(gender.get('male', 0), gender.get('female', 0))
        summary_report += f"• {main_gender}: <b>{main_percentage}%</b>\n"
    
    age_groups = analysis.get('age_groups', {})
    if age_groups:
        main_age = max(age_groups.items(), key=lambda x: x[1])[0] if age_groups else 'не определено'
        summary_report += f"• Основная возрастная группа: <b>{main_age}</b>\n"
    
    if 'average_age' in age_groups:
        summary_report += f"• Средний возраст: <b>{age_groups.get('average_age', 0)} лет</b>\n"
    
    geography = analysis.get('geography', {})
    if geography:
        top_cities = geography.get('top_cities', {})
        if top_cities:
            first_city = list(top_cities.keys())[0] if top_cities else 'не определен'
            summary_report += f"• Основной город: <b>{first_city}</b>\n"
    
    social = analysis.get('social_activity', {})
    if social:
        active_percentage = social.get('active_users_percentage', 0)
        summary_report += f"• Активные пользователи: <b>{active_percentage}%</b>\n"
    
    summary_report += f"\n<b>💡 ИСПОЛЬЗУЙТЕ КНОПКИ НИЖЕ</b> для детального просмотра каждого раздела анализа."
    
    await message.answer(summary_report, reply_markup=report_keyboard)
    
    # Сохраняем данные для callback
    user_id = message.from_user.id
    if user_id in user_sessions:
        user_sessions[user_id]['report_data'] = {
            'group_info': group_info,
            'analysis': analysis,
            'analyzed_count': analyzed_count
        }

@dp.callback_query(F.data.startswith("report_"))
async def handle_report_callback(callback: CallbackQuery):
    """Обработка callback для детальных разделов отчета"""
    user_id = callback.from_user.id
    
    if user_id not in user_sessions or 'report_data' not in user_sessions[user_id]:
        await callback.answer("Данные отчета устарели. Пожалуйста, выполните анализ заново.")
        return
    
    report_data = user_sessions[user_id]['report_data']
    group_info = report_data['group_info']
    analysis = report_data['analysis']
    
    report_type = callback.data.replace("report_", "")
    
    if report_type == "demography":
        await send_demography_report(callback.message, analysis)
    elif report_type == "interests":
        await send_interests_report(callback.message, analysis)
    elif report_type == "activity":
        await send_activity_report(callback.message, analysis)
    elif report_type == "geography":
        await send_geography_report(callback.message, analysis)
    elif report_type == "quality":
        await send_quality_report(callback.message, analysis)
    elif report_type == "recommendations":
        await send_recommendations_report(callback.message, analysis)
    
    await callback.answer()

async def send_demography_report(message: Message, analysis: dict):
    """Отправляет отчет по демографии"""
    gender = analysis.get('gender', {})
    age_groups = analysis.get('age_groups', {})
    
    report = "<b>📊 ДЕТАЛЬНЫЙ АНАЛИЗ ДЕМОГРАФИИ</b>\n\n"
    
    report += "<b>👫 ГЕНДЕРНОЕ РАСПРЕДЕЛЕНИЕ:</b>\n"
    if gender:
        # Прогресс-бары для наглядности
        male_bars = "█" * max(1, int(gender.get('male', 0) / 3))
        female_bars = "█" * max(1, int(gender.get('female', 0) / 3))
        unknown_bars = "█" * max(1, int(gender.get('unknown', 0) / 3))
        
        report += f"👨 Мужчины: <b>{gender.get('male', 0)}%</b> {male_bars}\n"
        report += f"👩 Женщины: <b>{gender.get('female', 0)}%</b> {female_bars}\n"
        if gender.get('unknown', 0) > 0:
            report += f"❓ Не указано: <b>{gender.get('unknown', 0)}%</b> {unknown_bars}\n"
    else:
        report += "Нет данных о поле участников\n"
    
    report += "\n<b>📅 ВОЗРАСТНЫЕ ГРУППЫ:</b>\n"
    if age_groups:
        for age_group, percentage in sorted(age_groups.items()):
            if 'average' not in age_group and 'unknown' not in age_group and percentage > 0:
                bars = "█" * max(1, int(percentage / 5))
                report += f"• {age_group}: <b>{percentage}%</b> {bars}\n"
        
        if 'average_age' in age_groups:
            report += f"\n<b>Средний возраст:</b> {age_groups['average_age']} лет\n"
        
        if 'unknown_percentage' in age_groups and age_groups['unknown_percentage'] > 0:
            report += f"<i>Возраст не указали: {age_groups['unknown_percentage']}% участников</i>\n"
    else:
        report += "Нет данных о возрасте участников\n"
    
    # Анализ распределения
    report += "\n<b>📈 АНАЛИЗ РАСПРЕДЕЛЕНИЯ:</b>\n"
    if gender and age_groups:
        if gender.get('male', 0) > 70:
            report += "• Преобладает мужская аудитория\n"
        elif gender.get('female', 0) > 70:
            report += "• Преобладает женская аудитория\n"
        else:
            report += "• Сбалансированная аудитория по полу\n"
        
        # Определяем основную возрастную группу
        if age_groups:
            main_age_group = max(
                [(k, v) for k, v in age_groups.items() if 'average' not in k and 'unknown' not in k],
                key=lambda x: x[1],
                default=(None, 0)
            )
            if main_age_group[1] > 30:
                report += f"• Основная возрастная группа: {main_age_group[0]}\n"
    
    await message.answer(report, reply_markup=create_back_button())

async def send_interests_report(message: Message, analysis: dict):
    """Отправляет отчет по интересам"""
    interests = analysis.get('interests', {})
    popular_categories = interests.get('popular_categories', {})
    
    report = "<b>🎯 АНАЛИЗ ИНТЕРЕСОВ И АКТИВНОСТИ</b>\n\n"
    
    if popular_categories:
        report += "<b>🔥 ПОПУЛЯРНЫЕ КАТЕГОРИИ ИНТЕРЕСОВ:</b>\n"
        for category, percentage in sorted(popular_categories.items(), key=lambda x: x[1], reverse=True)[:8]:
            emoji_map = {
                'технологии': '💻', 'образование': '🎓', 'спорт': '⚽', 
                'искусство': '🎨', 'бизнес': '💼', 'путешествия': '✈️',
                'мода': '👗', 'авто': '🚗', 'кулинария': '🍳',
                'здоровье': '🏥', 'гейминг': '🎮', 'книги': '📚',
                'сериалы': '🎬', 'музыка': '🎵', 'хобби': '🎨'
            }
            emoji = emoji_map.get(category, '•')
            bars = "█" * max(1, int(percentage / 5))
            report += f"{emoji} {category.title()}: <b>{percentage}%</b> {bars}\n"
    else:
        report += "Не удалось определить популярные категории интересов\n"
    
    report += f"\n<b>📝 ЗАПОЛНЕННОСТЬ ПРОФИЛЕЙ:</b>\n"
    report += f"• Заполнено профилей: <b>{interests.get('profile_fill_rate', 0)}%</b>\n"
    report += f"• Категорий найдено: <b>{interests.get('total_categories_found', 0)}</b>\n"
    
    report += "\n<b>💡 ИНТЕРПРЕТАЦИЯ:</b>\n"
    if popular_categories:
        top_3 = list(popular_categories.keys())[:3]
        if top_3:
            report += f"Основные интересы аудитории: {', '.join(top_3)}\n"
        
        # Анализ по сочетаниям интересов
        if 'технологии' in popular_categories and 'образование' in popular_categories:
            report += "• Аудитория технически подкована и стремится к обучению\n"
        if 'спорт' in popular_categories and 'здоровье' in popular_categories:
            report += "• Аудитория заботится о здоровье и физической форме\n"
        if 'искусство' in popular_categories and 'музыка' in popular_categories:
            report += "• Аудитория творческая, интересуется искусством\n"
    
    await message.answer(report, reply_markup=create_back_button())

async def send_activity_report(message: Message, analysis: dict):
    """Отправляет отчет по активности"""
    social = analysis.get('social_activity', {})
    completeness = analysis.get('profile_completeness', {})
    last_seen = social.get('last_seen_distribution', {})
    
    report = "<b>📱 АНАЛИЗ АКТИВНОСТИ И ПОЛНОТЫ ПРОФИЛЕЙ</b>\n\n"
    
    report += "<b>⏰ ВРЕМЯ ПОСЛЕДНЕЙ АКТИВНОСТИ:</b>\n"
    if last_seen:
        # Сортируем по порядку
        order = ['менее_дня', '1-7_дней', '1-4_недели', '1-3_месяца', 'более_3_месяцев', 'никогда']
        for period in order:
            if period in last_seen and last_seen[period] > 0:
                period_name = {
                    'менее_дня': 'Сегодня',
                    '1-7_дней': 'За последнюю неделю',
                    '1-4_недели': '1-4 недели назад',
                    '1-3_месяца': '1-3 месяца назад',
                    'более_3_месяцев': 'Более 3 месяцев назад',
                    'никогда': 'Никогда не заходили'
                }.get(period, period)
                
                bars = "█" * max(1, int(last_seen[period] / 5))
                report += f"• {period_name}: <b>{last_seen[period]}%</b> {bars}\n"
    else:
        report += "Нет данных о времени активности\n"
    
    report += f"\n<b>📊 УРОВЕНЬ АКТИВНОСТИ:</b>\n"
    active_percentage = social.get('active_users_percentage', 0)
    if active_percentage >= 70:
        report += f"• <b>Высокая активность</b> ({active_percentage}% активных пользователей)\n"
        report += "  <i>Аудитория регулярно посещает ВК</i>\n"
    elif active_percentage >= 40:
        report += f"• <b>Средняя активность</b> ({active_percentage}% активных пользователей)\n"
        report += "  <i>Аудитория умеренно активна</i>\n"
    else:
        report += f"• <b>Низкая активность</b> ({active_percentage}% активных пользователей)\n"
        report += "  <i>Аудитория редко посещает ВК</i>\n"
    
    report += "\n<b>📋 ПОЛНОТА ЗАПОЛНЕНИЯ ПРОФИЛЕЙ:</b>\n"
    if completeness:
        avg_completeness = completeness.get('average_completeness', 0)
        high_percentage = completeness.get('high_completeness_percentage', 0)
        low_percentage = completeness.get('low_completeness_percentage', 0)
        
        report += f"• Средняя заполненность: <b>{avg_completeness}%</b>\n"
        report += f"• Хорошо заполнены (>70%): <b>{high_percentage}%</b>\n"
        report += f"• Плохо заполнены (<30%): <b>{low_percentage}%</b>\n"
        
        if avg_completeness > 70:
            report += "  <i>Профили хорошо заполнены, можно использовать сложный таргетинг</i>\n"
        elif avg_completeness < 30:
            report += "  <i>Профили заполнены слабо, упрощайте таргетинг</i>\n"
    else:
        report += "Нет данных о полноте профилей\n"
    
    await message.answer(report, reply_markup=create_back_button())

async def send_geography_report(message: Message, analysis: dict):
    """Отправляет отчет по географии"""
    geography = analysis.get('geography', {})
    top_cities = geography.get('top_cities', {})
    countries = geography.get('countries', {})
    city_types = geography.get('city_types', {})
    
    report = "<b>🏙️ АНАЛИЗ ГЕОГРАФИЧЕСКОГО РАСПРЕДЕЛЕНИЯ</b>\n\n"
    
    if top_cities:
        report += "<b>🗺️ ТОП-10 ГОРОДОВ УЧАСТНИКОВ:</b>\n"
        for i, (city, percentage) in enumerate(list(top_cities.items())[:10], 1):
            flag = "🇷🇺" if city.lower() in ['москва', 'санкт-петербург'] else "🏙️"
            bars = "█" * max(1, int(percentage / 5))
            report += f"{i}. {flag} {city}: <b>{percentage}%</b> {bars}\n"
    else:
        report += "Нет данных о городах участников\n"
    
    if countries:
        report += "\n<b>🌍 РАСПРЕДЕЛЕНИЕ ПО СТРАНАМ:</b>\n"
        for country, percentage in sorted(countries.items(), key=lambda x: x[1], reverse=True)[:5]:
            flag = "🇷🇺" if "россия" in country.lower() else "🌐"
            report += f"{flag} {country}: <b>{percentage}%</b>\n"
    
    if city_types:
        report += "\n<b>📊 РАСПРЕДЕЛЕНИЕ ПО ТИПАМ ГОРОДОВ:</b>\n"
        
        # Переименовываем ключи для читаемости
        type_names = {
            'столицы': 'Столицы и крупнейшие города',
            'миллионники': 'Города-миллионники',
            'крупные_города': 'Крупные города (100к+)',
            'средние_города': 'Средние города (30-100к)',
            'малые_города': 'Малые города (до 30к)'
        }
        
        for city_type, percentage in city_types.items():
            if percentage > 0:
                readable_name = type_names.get(city_type, city_type.replace('_', ' ').title())
                bars = "█" * max(1, int(percentage / 5))
                report += f"• {readable_name}: <b>{percentage}%</b> {bars}\n"
        
        # Анализ распределения
        if city_types.get('столицы', 0) > 50:
            report += "\n<i>🎯 Аудитория преимущественно столичная</i>\n"
            report += "  • Подходят премиум-товары и услуги\n"
            report += "  • Высокая покупательная способность\n"
            report += "  • Быстрая реакция на тренды\n"
        elif city_types.get('малые_города', 0) > 50:
            report += "\n<i>🎯 Аудитория из малых городов</i>\n"
            report += "  • Важны доступные цены и доставка\n"
            report += "  • Меньшая конкуренция\n"
            report += "  • Лояльность к брендам\n"
    
    unknown_percentage = geography.get('unknown_location_percentage', 0)
    if unknown_percentage > 0:
        report += f"\n<i>📍 Географию не указали: {unknown_percentage}% участников</i>\n"
    
    await message.answer(report, reply_markup=create_back_button())

async def send_quality_report(message: Message, analysis: dict):
    """Отправляет отчет по качеству аудитории"""
    quality_score = analysis.get('audience_quality_score', 0)
    quality_interpretation = analysis.get('quality_interpretation', '')
    completeness = analysis.get('profile_completeness', {})
    social = analysis.get('social_activity', {})
    interests = analysis.get('interests', {})
    
    report = f"<b>⭐ ОЦЕНКА КАЧЕСТВА АУДИТОРИИ: {quality_score}/100</b>\n\n"
    
    # Звезды для наглядности
    stars = get_quality_stars(quality_score)
    report += f"{stars}\n\n"
    
    report += f"<i>{quality_interpretation}</i>\n\n"
    
    report += "<b>📊 ФАКТОРЫ, ВЛИЯЮЩИЕ НА ОЦЕНКУ:</b>\n\n"
    
    # Полнота профилей (макс 20 баллов)
    avg_completeness = completeness.get('average_completeness', 0)
    completeness_score = (avg_completeness / 100) * 20
    report += f"<b>📋 Полнота профилей:</b> {completeness_score:.1f}/20 баллов\n"
    report += f"   Средняя заполненность: {avg_completeness}%\n"
    if avg_completeness > 70:
        report += "   ✅ Высокий показатель\n"
    elif avg_completeness > 40:
        report += "   ⚠️ Средний показатель\n"
    else:
        report += "   ❌ Низкий показатель\n"
    
    report += "\n"
    
    # Активность пользователей (макс 20 баллов)
    active_percentage = social.get('active_users_percentage', 0)
    activity_score = (active_percentage / 100) * 20
    report += f"<b>📱 Активность пользователей:</b> {activity_score:.1f}/20 баллов\n"
    report += f"   Активных пользователей: {active_percentage}%\n"
    if active_percentage > 70:
        report += "   ✅ Высокая активность\n"
    elif active_percentage > 40:
        report += "   ⚠️ Средняя активность\n"
    else:
        report += "   ❌ Низкая активность\n"
    
    report += "\n"
    
    # Разнообразие интересов (макс 10 баллов)
    total_categories = interests.get('total_categories_found', 0)
    interests_score = min(10, total_categories * 2)
    report += f"<b>🎯 Разнообразие интересов:</b> {interests_score:.1f}/10 баллов\n"
    report += f"   Категорий интересов: {total_categories}\n"
    if total_categories > 5:
        report += "   ✅ Широкий спектр интересов\n"
    elif total_categories > 2:
        report += "   ⚠️ Умеренное разнообразие\n"
    else:
        report += "   ❌ Ограниченные интересы\n"
    
    report += "\n"
    
    # Сбалансированность по полу (макс 10 баллов)
    gender = analysis.get('gender', {})
    gender_diff = abs(gender.get('male', 0) - gender.get('female', 0))
    gender_score = max(0, 10 - (gender_diff / 10))
    report += f"<b>⚖️ Сбалансированность по полу:</b> {gender_score:.1f}/10 баллов\n"
    report += f"   Разница мужчин/женщин: {gender_diff}%\n"
    if gender_diff < 20:
        report += "   ✅ Сбалансированная аудитория\n"
    elif gender_diff < 40:
        report += "   ⚠️ Умеренный перекос\n"
    else:
        report += "   ❌ Сильный перекос\n"
    
    report += "\n<b>📈 РЕКОМЕНДАЦИИ ПО УЛУЧШЕНИЮ:</b>\n"
    
    if avg_completeness < 50:
        report += "• Работайте над полнотой профилей участников\n"
    if active_percentage < 50:
        report += "• Повышайте активность через контент и взаимодействие\n"
    if total_categories < 3:
        report += "• Расширяйте тематику контента для привлечения разнообразной аудитории\n"
    if gender_diff > 40:
        report += "• Попробуйте привлечь аудиторию противоположного пола\n"
    
    if quality_score >= 80:
        report += "\n✅ <b>Ваша аудитория уже высокого качества!</b> Фокусируйтесь на удержании и монетизации."
    elif quality_score >= 60:
        report += "\n⚠️ <b>Аудитория хорошего качества.</b> Работайте над улучшением слабых сторон."
    else:
        report += "\n❌ <b>Аудитория требует улучшений.</b> Сфокусируйтесь на рекомендациях выше."
    
    await message.answer(report, reply_markup=create_back_button())

async def send_recommendations_report(message: Message, analysis: dict):
    """Отправляет отчет с рекомендациями"""
    recommendations = analysis.get('recommendations', [])
    gender = analysis.get('gender', {})
    age_groups = analysis.get('age_groups', {})
    geography = analysis.get('geography', {})
    social = analysis.get('social_activity', {})
    
    report = "<b>💡 РЕКОМЕНДАЦИИ ДЛЯ ТАРГЕТИРОВАННОЙ РЕКЛАМЫ</b>\n\n"
    
    if recommendations:
        for i, rec in enumerate(recommendations[:12], 1):
            # Определяем эмодзи для типа рекомендации
            if "аудитория" in rec.lower() or "преобладает" in rec.lower():
                emoji = "👥"
            elif "возраст" in rec.lower():
                emoji = "📅"
            elif "город" in rec.lower() or "гео" in rec.lower():
                emoji = "🏙️"
            elif "активность" in rec.lower():
                emoji = "📱"
            elif "интересы" in rec.lower() or "тема" in rec.lower():
                emoji = "🎯"
            elif "качество" in rec.lower() or "профиль" in rec.lower():
                emoji = "📋"
            elif "таргетинг" in rec.lower() or "реклам" in rec.lower():
                emoji = "🎯"
            else:
                emoji = "💡"
            
            report += f"{emoji} <b>{i}.</b> {rec}\n"
    else:
        report += "Нет сгенерированных рекомендаций\n"
    
    report += "\n<b>🎯 КОНКРЕТНЫЕ СТРАТЕГИИ ТАРГЕТИНГА:</b>\n\n"
    
    # Гендерный таргетинг
    if gender.get('male', 0) > 60:
        report += "<b>👨 Для мужской аудитории:</b>\n"
        report += "• Технологии, гаджеты, авто\n"
        report += "• Спорт, фитнес, здоровье\n"
        report += "• Бизнес, финансы, карьера\n"
        report += "• Юмор, игры, развлечения\n\n"
    elif gender.get('female', 0) > 60:
        report += "<b>👩 Для женской аудитории:</b>\n"
        report += "• Мода, красота, стиль\n"
        report += "• Здоровье, диеты, уход\n"
        report += "• Семья, дети, отношения\n"
        report += "• Творчество, хобби, рукоделие\n\n"
    
    # Возрастной таргетинг
    main_age_group = max(
        [(k, v) for k, v in age_groups.items() if 'average' not in k and 'unknown' not in k],
        key=lambda x: x[1],
        default=(None, 0)
    )[0]
    
    if main_age_group:
        report += f"<b>📅 Для возрастной группы {main_age_group}:</b>\n"
        if main_age_group == 'до 18':
            report += "• Образование, курсы, учеба\n"
            report += "• Мода, музыка, сериалы\n"
            report += "• Игры, развлечения\n\n"
        elif main_age_group == '18-24':
            report += "• Образование, карьера, стартапы\n"
            report += "• Путешествия, активный отдых\n"
            report += "• Технологии, гаджеты\n\n"
        elif main_age_group == '25-34':
            report += "• Карьера, бизнес, инвестиции\n"
            report += "• Недвижимость, автомобили\n"
            report += "• Семья, дети, здоровье\n\n"
        elif main_age_group == '35-44':
            report += "• Карьера, бизнес, управление\n"
            report += "• Недвижимость, инвестиции\n"
            report += "• Здоровье, путешествия\n\n"
        elif main_age_group == '45+':
            report += "• Здоровье, медицина\n"
            report += "• Отдых, хобби, дача\n"
            report += "• Финансы, недвижимость\n\n"
    
    # Географический таргетинг
    city_types = geography.get('city_types', {})
    if city_types.get('столицы', 0) > 50:
        report += "<b>🏙️ Для столичной аудитории:</b>\n"
        report += "• Премиум-товары и услуги\n"
        report += "• Образование, курсы повышения квалификации\n"
        report += "• Рестораны, развлечения, события\n\n"
    elif city_types.get('малые_города', 0) > 50:
        report += "<b>🏡 Для аудитории из малых городов:</b>\n"
        report += "• Товары с доставкой по всей России\n"
        report += "• Образовательные курсы онлайн\n"
        report += "• Услуги для дома и семьи\n\n"
    
    # Рекомендации по времени публикаций
    active_percentage = social.get('active_users_percentage', 0)
    if active_percentage > 70:
        report += "<b>⏰ Рекомендуемое время публикаций:</b>\n"
        report += "• Утро (9-11): образовательный контент\n"
        report += "• Обед (13-15): развлекательный контент\n"
        report += "• Вечер (19-22): основные публикации\n"
        report += "• Можно публиковать чаще (3-5 раз в день)\n"
    else:
        report += "<b>⏰ Рекомендуемое время публикаций:</b>\n"
        report += "• Утро (10-11): основные публикации\n"
        report += "• Вечер (20-21): повтор важного контент\n"
        report += "• Публикуйте реже, но качественнее (1-2 раза в день)\n"
    
    report += "\n<b>🎯 КЛЮЧЕВОЙ СОВЕТ:</b>\n"
    report += "Тестируйте разные подходы, анализируйте результаты и оптимизируйте стратегию на основе данных.\n"
    
    await message.answer(report, reply_markup=create_back_button())

@dp.callback_query(F.data == "back_to_report")
async def back_to_report(callback: CallbackQuery):
    """Возвращает к основному отчету"""
    user_id = callback.from_user.id
    
    if user_id not in user_sessions or 'report_data' not in user_sessions[user_id]:
        await callback.answer("Данные отчета устарели")
        return
    
    report_data = user_sessions[user_id]['report_data']
    group_info = report_data['group_info']
    analysis = report_data['analysis']
    analyzed_count = report_data['analyzed_count']
    
    await send_comprehensive_report(callback.message, group_info, analysis, analyzed_count)
    await callback.answer()

@dp.message(Command("competitors"))
async def cmd_competitors(message: Message, command: CommandObject = None):
    """Анализ конкурентов группы"""
    try:
        args = message.text.split()[1:] if command is None else command.args.split()
        if not args:
            await message.answer(
                "🥊 <b>Анализ конкурентов</b>\n\n"
                "Эта команда найдет и проанализирует похожие группы.\n\n"
                "<b>Пример:</b>\n"
                "<code>/competitors https://vk.com/public123</code>\n"
                "<code>/competitors vk.com/groupname</code>\n\n"
                "<i>Бот найдет до 10 похожих групп и проведет их анализ</i>"
            )
            return
        
        group_link = args[0].strip()
        user_id = message.from_user.id
        
        # Проверяем, не выполняется ли уже анализ для этого пользователя
        if user_id in user_sessions and user_sessions[user_id].get('status') == 'analyzing_competitors':
            await message.answer(
                "⏳ <b>У вас уже выполняется анализ конкурентов</b>\n\n"
                "Пожалуйста, дождитесь завершения текущего анализа."
            )
            return
        
        # Начинаем анализ
        user_sessions[user_id] = {
            'status': 'analyzing_competitors',
            'group_link': group_link,
            'current_step': 'получение_информации'
        }
        
        await message.answer("🥊 <b>Начинаю анализ конкурентов...</b>")
        logger.info(f"Пользователь {user_id} запросил анализ конкурентов для {group_link}")
        
        # Получаем информацию о целевой группе
        await message.answer("🔍 <b>Шаг 1 из 4:</b> Анализирую целевую группу...")
        group_info = await vk_client.get_group_info(group_link)
        
        if not group_info:
            del user_sessions[user_id]
            await message.answer(
                "❌ <b>Не удалось получить информацию о группе</b>\n\n"
                "Проверьте ссылку и убедитесь, что группа открыта."
            )
            return
        
        if group_info.get('is_closed', 1) != 0:
            del user_sessions[user_id]
            await message.answer(f"⚠️ <b>Группа '{group_info['name']}' закрытая или приватная</b>")
            return
        
        user_sessions[user_id].update({
            'group_info': group_info,
            'current_step': 'поиск_конкурентов'
        })
        
        # Поиск конкурентов
        info_msg = await message.answer(
            f"🎯 <b>Целевая группа:</b> {group_info['name']}\n"
            f"👥 <b>Участников:</b> {format_number(group_info.get('members_count', 0))}\n\n"
            "🔍 <b>Шаг 2 из 4:</b> Ищу похожие группы..."
        )
        
        competitors = await competitor_analyzer.find_similar_groups(
            group_info['name'],
            group_info.get('description', ''),
            limit=8
        )
        
        if not competitors:
            del user_sessions[user_id]
            await info_msg.edit_text(
                f"🎯 <b>Целевая группа:</b> {group_info['name']}\n\n"
                "❌ <b>Не удалось найти похожие группы</b>\n\n"
                "Попробуйте:\n"
                "• Указать группу с более четкой тематикой\n"
                "• Проверить, что группа имеет описание\n"
                "• Попробовать другую группу"
            )
            return
        
        user_sessions[user_id].update({
            'competitors': competitors,
            'current_step': 'анализ_конкурентов'
        })
        
        await info_msg.edit_text(
            f"🎯 <b>Целевая группа:</b> {group_info['name']}\n"
            f"🥊 <b>Найдено конкурентов:</b> {len(competitors)}\n\n"
            "📊 <b>Шаг 3 из 4:</b> Анализирую аудиторию конкурентов..."
        )
        
        # Анализируем аудиторию конкурентов
        analyzed_competitors = []
        for i, competitor in enumerate(competitors[:5], 1):  # Ограничиваем 5 конкурентами
            try:
                await info_msg.edit_text(
                    f"🎯 <b>Целевая группа:</b> {group_info['name']}\n"
                    f"🥊 <b>Анализирую конкурента {i} из 5...</b>\n\n"
                    f"Группа: {competitor.get('name', 'Без названия')}"
                )
                
                # Получаем участников конкурента (ограничиваем для скорости)
                members = await vk_client.get_group_members(
                    competitor['id'],
                    limit=min(300, competitor.get('members_count', 0))
                )
                
                if members:
                    analysis = await analyzer.analyze_audience(members)
                    competitor['analysis'] = analysis
                    analyzed_competitors.append(competitor)
                    
                    logger.info(f"Проанализирован конкурент {competitor.get('name')}")
                
                await asyncio.sleep(1)  # Задержка между запросами
                
            except Exception as e:
                logger.error(f"Ошибка анализа конкурента {competitor.get('name')}: {e}")
                continue
        
        if not analyzed_competitors:
            del user_sessions[user_id]
            await message.answer(
                "❌ <b>Не удалось проанализировать конкурентов</b>\n\n"
                "Возможно, у конкурентов закрытые группы или нет участников."
            )
            return
        
        user_sessions[user_id].update({
            'analyzed_competitors': analyzed_competitors,
            'current_step': 'формирование_отчета'
        })
        
        await info_msg.edit_text(
            f"🎯 <b>Целевая группа:</b> {group_info['name']}\n"
            f"🥊 <b>Проанализировано:</b> {len(analyzed_competitors)} конкурентов\n\n"
            "📋 <b>Шаг 4 из 4:</b> Формирую отчет по конкурентам..."
        )
        
        # Анализируем аудиторию целевой группы для сравнения
        target_members = await vk_client.get_group_members(
            group_info['id'],
            limit=min(500, group_info.get('members_count', 0))
        )
        
        target_analysis = None
        if target_members:
            target_analysis = await analyzer.analyze_audience(target_members)
        
        # Формируем и отправляем отчет
        await send_competitor_report(
            message,
            group_info,
            target_analysis,
            analyzed_competitors
        )
        
        # Сохраняем результаты
        user_sessions[user_id]['status'] = 'completed'
        user_sessions[user_id]['report_data'] = {
            'group_info': group_info,
            'target_analysis': target_analysis,
            'competitors': analyzed_competitors
        }
        
    except Exception as e:
        logger.error(f"Ошибка в команде /competitors: {e}", exc_info=True)
        if message.from_user.id in user_sessions:
            del user_sessions[message.from_user.id]
        await message.answer(
            "❌ <b>Ошибка при анализе конкурентов</b>\n\n"
            "Попробуйте позже или выберите другую группу."
        )

async def send_competitor_report(message: Message, target_group: dict, 
                               target_analysis: dict, competitors: list):
    """Отправляет отчет по анализу конкурентов"""
    
    # Основная информация
    report = f"""
🥊 <b>АНАЛИЗ КОНКУРЕНТОВ: {target_group['name']}</b>

<b>🎯 ЦЕЛЕВАЯ ГРУППА:</b>
• Название: {target_group['name']}
• Участников: {format_number(target_group.get('members_count', 0))}
• Описание: {target_group.get('description', 'Нет описания')[:100]}...

<b>🥊 НАЙДЕНО КОНКУРЕНТОВ:</b> {len(competitors)}
"""
    
    if target_analysis:
        report += f"""• Качество аудитории: {target_analysis.get('audience_quality_score', 0)}/100
• Основной пол: {'Мужчины' if target_analysis.get('gender', {}).get('male', 0) > 50 else 'Женщины'}
• Основной возраст: {max(target_analysis.get('age_groups', {}).items(), key=lambda x: x[1])[0] if target_analysis.get('age_groups') else 'Не определен'}
"""
    
    await message.answer(report, reply_markup=create_competitor_keyboard())
    
    # Детальная информация о конкурентах
    details = "<b>📊 ПОДРОБНЫЙ АНАЛИЗ КОНКУРЕНТОВ:</b>\n\n"
    
    for i, competitor in enumerate(competitors[:5], 1):
        details += f"<b>{i}. {competitor.get('name', 'Без названия')}</b>\n"
        details += f"• Участников: {format_number(competitor.get('members_count', 0))}\n"
        
        if 'analysis' in competitor:
            analysis = competitor['analysis']
            details += f"• Качество: {analysis.get('audience_quality_score', 0)}/100\n"
            
            gender = analysis.get('gender', {})
            if gender.get('male', 0) > gender.get('female', 0):
                details += f"• Преобладающий пол: 👨 Мужчины ({gender.get('male', 0)}%)\n"
            else:
                details += f"• Преобладающий пол: 👩 Женщины ({gender.get('female', 0)}%)\n"
        
        details += f"• Ссылка: vk.com/{competitor.get('screen_name', '')}\n\n"
    
    await message.answer(details)
    
    # Сравнительная таблица
    if target_analysis and len(competitors) > 0:
        comparison = await competitor_analyzer.compare_with_competitors(
            target_group, target_analysis, competitors
        )
        
        if comparison:
            await send_comparison_report(message, comparison)

async def send_comparison_report(message: Message, comparison: dict):
    """Отправляет отчет сравнения с конкурентами"""
    report = "<b>📈 СРАВНИТЕЛЬНЫЙ АНАЛИЗ</b>\n\n"
    
    # Позиция в рейтинге
    if 'rank' in comparison:
        report += f"<b>🏆 Ваша позиция среди конкурентов:</b> {comparison['rank']} место\n\n"
    
    # Сильные стороны
    if comparison.get('strengths'):
        report += "<b>✅ ВАШИ СИЛЬНЫЕ СТОРОНЫ:</b>\n"
        for strength in comparison['strengths'][:3]:
            report += f"• {strength}\n"
        report += "\n"
    
    # Слабые стороны
    if comparison.get('weaknesses'):
        report += "<b>⚠️ ВАШИ СЛАБЫЕ СТОРОНЫ:</b>\n"
        for weakness in comparison['weaknesses'][:3]:
            report += f"• {weakness}\n"
        report += "\n"
    
    # Рекомендации
    if comparison.get('recommendations'):
        report += "<b>💡 РЕКОМЕНДАЦИИ:</b>\n"
        for i, rec in enumerate(comparison['recommendations'][:5], 1):
            report += f"{i}. {rec}\n"
    
    await message.answer(report)

@dp.callback_query(F.data == "top_competitors")
async def top_competitors_callback(callback: CallbackQuery):
    """Показывает ТОП-5 конкурентов"""
    user_id = callback.from_user.id
    
    if user_id not in user_sessions or 'report_data' not in user_sessions[user_id]:
        await callback.answer("Данные устарели. Выполните анализ заново.")
        return
    
    report_data = user_sessions[user_id]['report_data']
    competitors = report_data.get('competitors', [])
    
    if not competitors:
        await callback.answer("Нет данных о конкурентах")
        return
    
    # Сортируем по качеству аудитории
    sorted_competitors = sorted(
        competitors,
        key=lambda x: x.get('analysis', {}).get('audience_quality_score', 0),
        reverse=True
    )
    
    report = "<b>🏆 ТОП-5 КОНКУРЕНТОВ ПО КАЧЕСТВУ АУДИТОРИИ</b>\n\n"
    
    for i, competitor in enumerate(sorted_competitors[:5], 1):
        score = competitor.get('analysis', {}).get('audience_quality_score', 0)
        stars = "⭐" * min(5, int(score / 20))
        
        report += f"<b>{i}. {competitor.get('name', 'Без названия')}</b>\n"
        report += f"• Качество: {score}/100 {stars}\n"
        report += f"• Участников: {format_number(competitor.get('members_count', 0))}\n"
        
        gender = competitor.get('analysis', {}).get('gender', {})
        if gender:
            main_gender = "👨 М" if gender.get('male', 0) > gender.get('female', 0) else "👩 Ж"
            report += f"• Преобладающий пол: {main_gender}\n"
        
        report += f"• Ссылка: vk.com/{competitor.get('screen_name', '')}\n\n"
    
    await callback.message.answer(report, reply_markup=create_back_button("back_to_competitors"))
    await callback.answer()

@dp.callback_query(F.data == "back_to_competitors")
async def back_to_competitors(callback: CallbackQuery):
    """Возвращает к отчету по конкурентам"""
    user_id = callback.from_user.id
    
    if user_id not in user_sessions or 'report_data' not in user_sessions[user_id]:
        await callback.answer("Данные устарели")
        return
    
    report_data = user_sessions[user_id]['report_data']
    group_info = report_data['group_info']
    target_analysis = report_data['target_analysis']
    competitors = report_data['competitors']
    
    await send_competitor_report(callback.message, group_info, target_analysis, competitors)
    await callback.answer()

@dp.message(Command("text_analysis"))
async def cmd_text_analysis(message: Message, command: CommandObject = None):
    """AI-анализ текстового контента группы"""
    try:
        args = message.text.split()[1:] if command is None else command.args.split()
        if not args:
            await message.answer(
                "🧠 <b>AI-анализ текстового контента</b>\n\n"
                "Эта команда проанализирует текстовый контент группы:\n"
                "• Тональность (позитивная/негативная/нейтральная)\n"
                "• Основные темы и категории\n"
                "• Ключевые слова и фразы\n"
                "• Эмоциональная окраска\n\n"
                "<b>Пример:</b>\n"
                "<code>/text_analysis https://vk.com/public123</code>\n"
                "<code>/text_analysis vk.com/groupname</code>"
            )
            return
        
        group_link = args[0].strip()
        user_id = message.from_user.id
        
        await message.answer("🧠 <b>Начинаю AI-анализ текста...</b>")
        logger.info(f"Пользователь {user_id} запросил текстовый анализ {group_link}")
        
        # Получаем информацию о группе
        group_info = await vk_client.get_group_info(group_link)
        if not group_info:
            await message.answer("❌ <b>Не удалось получить информацию о группе</b>")
            return
        
        # Получаем текстовый контент
        text_content = await get_group_text_content(group_info['id'])
        
        if not text_content:
            await message.answer(
                f"❌ <b>Не удалось получить текстовый контент группы {group_info['name']}</b>\n\n"
                "Возможно:\n"
                "• У группы нет описания и постов\n"
                "• Группа закрытая\n"
                "• Ограничения VK API"
            )
            return
        
        # Анализируем текст
        analysis = await text_analyzer.analyze_text(text_content)
        
        # Формируем отчет
        await send_text_analysis_report(message, group_info, analysis)
        
        # Сохраняем результаты
        user_sessions[user_id] = {
            'text_analysis_data': {
                'group_info': group_info,
                'analysis': analysis,
                'text_content': text_content[:1000]  # Сохраняем первые 1000 символов
            }
        }
        
    except Exception as e:
        logger.error(f"Ошибка в команде /text_analysis: {e}", exc_info=True)
        await message.answer(
            "❌ <b>Ошибка при анализе текста</b>\n\n"
            "Попробуйте позже или выберите другую группу."
        )

async def get_group_text_content(group_id: int) -> str:
    """Получает текстовый контент группы"""
    try:
        # Получаем описание группы
        group_info = await vk_client.get_group_info(f"club{group_id}")
        text_content = group_info.get('description', '') if group_info else ''
        
        # Добавляем название группы
        if group_info and 'name' in group_info:
            text_content = f"{group_info['name']}. {text_content}"
        
        # Пытаемся получить несколько последних постов
        try:
            # Это упрощенный запрос - в реальности нужно использовать wall.get
            # Но для демонстрации используем то, что есть
            members = await vk_client.get_group_members(group_id, limit=10)
            if members:
                # Добавляем интересы участников
                interests = []
                for member in members[:20]:  # Ограничиваем 20 участниками
                    if 'interests' in member and member['interests']:
                        interests.append(member['interests'])
                
                if interests:
                    text_content += " " + " ".join(interests)
        except:
            pass
        
        return text_content if text_content.strip() else None
        
    except Exception as e:
        logger.error(f"Ошибка получения текстового контента: {e}")
        return None

async def send_text_analysis_report(message: Message, group_info: dict, analysis: dict):
    """Отправляет отчет по анализу текста"""
    report = f"""
🧠 <b>AI-АНАЛИЗ ТЕКСТА: {group_info['name']}</b>

<b>📝 ОБЩАЯ ИНФОРМАЦИЯ:</b>
• Проанализировано символов: {analysis.get('text_length', 0):,}
• Уникальных слов: {analysis.get('unique_words', 0)}
• Средняя длина предложения: {analysis.get('avg_sentence_length', 0):.1f} слов
"""
    
    # Тональность
    sentiment = analysis.get('sentiment', {})
    if sentiment:
        sentiment_score = sentiment.get('score', 0)
        sentiment_label = sentiment.get('label', 'нейтральная')
        
        if sentiment_label == 'positive':
            sentiment_emoji = "😊"
            sentiment_desc = "Позитивная"
        elif sentiment_label == 'negative':
            sentiment_emoji = "😔"
            sentiment_desc = "Негативная"
        else:
            sentiment_emoji = "😐"
            sentiment_desc = "Нейтральная"
        
        report += f"\n<b>🎭 ТОНАЛЬНОСТЬ:</b> {sentiment_desc} {sentiment_emoji}\n"
        report += f"• Оценка: {sentiment_score:.2f} (от -1 до 1)\n"
        report += f"• Уверенность: {sentiment.get('confidence', 0):.1%}\n"
    
    # Основные темы
    topics = analysis.get('topics', [])
    if topics:
        report += "\n<b>📚 ОСНОВНЫЕ ТЕМЫ:</b>\n"
        for i, topic in enumerate(topics[:5], 1):
            report += f"{i}. {topic['name']}: {topic['score']:.1%}\n"
    
    # Ключевые слова
    keywords = analysis.get('keywords', [])
    if keywords:
        report += "\n<b>🔑 КЛЮЧЕВЫЕ СЛОВА:</b>\n"
        for i, keyword in enumerate(keywords[:10], 1):
            report += f"• {keyword['word']} ({keyword['count']})\n"
    
    # Эмоции
    emotions = analysis.get('emotions', {})
    if emotions:
        report += "\n<b>😊 ЭМОЦИОНАЛЬНАЯ ОКРАСКА:</b>\n"
        for emotion, score in emotions.items():
            if score > 0.1:  # Показываем только значимые эмоции
                bars = "█" * int(score * 10)
                report += f"• {emotion}: {score:.1%} {bars}\n"
    
    # Рекомендации
    recommendations = analysis.get('recommendations', [])
    if recommendations:
        report += "\n<b>💡 РЕКОМЕНДАЦИИ ПО КОНТЕНТУ:</b>\n"
        for i, rec in enumerate(recommendations[:5], 1):
            report += f"{i}. {rec}\n"
    
    await message.answer(report, reply_markup=create_text_analysis_keyboard())
    
    # Дополнительная информация
    if 'readability_score' in analysis:
        readability = analysis['readability_score']
        additional = f"\n<b>📖 ЧИТАЕМОСТЬ ТЕКСТА:</b> {readability}/100\n"
        
        if readability >= 80:
            additional += "✅ Отличная читаемость! Текст понятен и доступен.\n"
        elif readability >= 60:
            additional += "⚠️ Средняя читаемость. Можно упростить некоторые предложения.\n"
        else:
            additional += "❌ Низкая читаемость. Рекомендуется упростить текст.\n"
        
        await message.answer(additional)

@dp.callback_query(F.data == "text_sentiment")
async def text_sentiment_callback(callback: CallbackQuery):
    """Показывает детальный анализ тональности"""
    user_id = callback.from_user.id
    
    if user_id not in user_sessions or 'text_analysis_data' not in user_sessions[user_id]:
        await callback.answer("Данные устарели. Выполните анализ заново.")
        return
    
    text_data = user_sessions[user_id]['text_analysis_data']
    analysis = text_data['analysis']
    sentiment = analysis.get('sentiment', {})
    
    report = "<b>🎭 ДЕТАЛЬНЫЙ АНАЛИЗ ТОНАЛЬНОСТИ</b>\n\n"
    
    if sentiment:
        score = sentiment.get('score', 0)
        label = sentiment.get('label', 'neutral')
        confidence = sentiment.get('confidence', 0)
        
        # Визуализация тональности
        if score > 0.3:
            visual = "😊 " + "🟢" * int(score * 10) + "⚪" * int((1 - score) * 10)
            interpretation = "Сильно позитивный текст"
        elif score > 0.1:
            visual = "🙂 " + "🟡" * int(score * 10) + "⚪" * int((1 - score) * 10)
            interpretation = "Умеренно позитивный текст"
        elif score > -0.1:
            visual = "😐 " + "⚪" * 10
            interpretation = "Нейтральный текст"
        elif score > -0.3:
            visual = "🙁 " + "🟠" * int(abs(score) * 10) + "⚪" * int((1 - abs(score)) * 10)
            interpretation = "Умеренно негативный текст"
        else:
            visual = "😔 " + "🔴" * int(abs(score) * 10) + "⚪" * int((1 - abs(score)) * 10)
            interpretation = "Сильно негативный текст"
        
        report += f"<b>Оценка тональности:</b> {score:.3f}\n"
        report += f"<b>Визуализация:</b> {visual}\n"
        report += f"<b>Интерпретация:</b> {interpretation}\n"
        report += f"<b>Уверенность анализа:</b> {confidence:.1%}\n\n"
        
        # Статистика по словам
        report += f"<b>📊 СТАТИСТИКА:</b>\n"
        report += f"• Позитивных слов: {sentiment.get('positive_words', 0)}\n"
        report += f"• Негативных слов: {sentiment.get('negative_words', 0)}\n"
        report += f"• Всего слов: {sentiment.get('total_words', 0)}\n\n"
        
        # Рекомендации по тональности
        report += "<b>💡 РЕКОМЕНДАЦИИ:</b>\n"
        if score < -0.2:
            report += "1. Добавьте больше позитивных формулировок\n"
            report += "2. Избегайте резкой критики\n"
            report += "3. Используйте конструктивные предложения\n"
        elif score < 0:
            report += "1. Сбалансируйте негативные и позитивные высказывания\n"
            report += "2. Добавьте примеры успешных решений\n"
            report += "3. Предложите пути улучшения\n"
        elif score < 0.2:
            report += "1. Текст хорошо сбалансирован\n"
            report += "2. Можно добавить немного эмоциональности\n"
            report += "3. Используйте больше конкретных примеров\n"
        else:
            report += "1. Отличная позитивная тональность!\n"
            report += "2. Такие тексты хорошо воспринимаются аудиторией\n"
            report += "3. Поддерживайте этот стиль\n"
    
    await callback.message.answer(report, reply_markup=create_back_button("back_to_text_analysis"))
    await callback.answer()

@dp.callback_query(F.data == "back_to_text_analysis")
async def back_to_text_analysis(callback: CallbackQuery):
    """Возвращает к отчету по текстовому анализу"""
    user_id = callback.from_user.id
    
    if user_id not in user_sessions or 'text_analysis_data' not in user_sessions[user_id]:
        await callback.answer("Данные устарели")
        return
    
    text_data = user_sessions[user_id]['text_analysis_data']
    group_info = text_data['group_info']
    analysis = text_data['analysis']
    
    await send_text_analysis_report(callback.message, group_info, analysis)
    await callback.answer()

@dp.message(Command("quick"))
async def cmd_quick(message: Message, command: CommandObject = None):
    """Быстрый анализ аудитории"""
    try:
        args = message.text.split()[1:] if command is None else command.args.split()
        if not args:
            await message.answer(
                "⚡ <b>Быстрый анализ аудитории</b>\n\n"
                "Пример: <code>/quick https://vk.com/public123</code>\n"
                "Или: <code>/quick vk.com/groupname</code>\n\n"
                "<i>Быстрый анализ показывает основные метрики за 1-2 минуты</i>"
            )
            return
        
        group_link = args[0].strip()
        
        await message.answer("⚡ <b>Запускаю быстрый анализ...</b>")
        logger.info(f"Пользователь {message.from_user.id} запросил быстрый анализ {group_link}")
        
        # Получаем информацию о группе
        group_info = await vk_client.get_group_info(group_link)
        if not group_info:
            await message.answer(
                "❌ <b>Не удалось получить информацию о группе</b>\n\n"
                "Проверьте ссылку и убедитесь, что группа открыта."
            )
            return
        
        if group_info.get('is_closed', 1) != 0:
            await message.answer(f"⚠️ <b>Группа '{group_info['name']}' закрытая</b>")
            return
        
        if group_info.get('members_count', 0) == 0:
            await message.answer(f"⚠️ <b>В группе '{group_info['name']}' нет участников</b>")
            return
        
        # Быстрый сбор участников (ограничиваем 200 для скорости)
        quick_limit = min(200, group_info['members_count'])
        members = await vk_client.get_group_members(group_info['id'], limit=quick_limit)
        
        if not members:
            await message.answer("❌ <b>Не удалось получить участников для анализа</b>")
            return
        
        # Быстрый анализ (только основные метрики)
        quick_analyzer = AudienceAnalyzer()
        
        # Анализируем только основные аспекты
        gender = await asyncio.to_thread(quick_analyzer._analyze_gender, members)
        age_groups = await asyncio.to_thread(quick_analyzer._analyze_age, members)
        geography = await asyncio.to_thread(quick_analyzer._analyze_geography, members)
        
        # Формируем быстрый отчет
        report = f"⚡ <b>БЫСТРЫЙ АНАЛИЗ: {group_info['name']}</b>\n\n"
        report += f"👥 Участников: {format_number(group_info['members_count'])}\n"
        report += f"📊 Проанализировано: {format_number(len(members))}\n\n"
        
        report += "<b>👫 ОСНОВНЫЕ МЕТРИКИ:</b>\n"
        
        # Гендер
        if gender:
            main_gender = "👨 М" if gender.get('male', 0) > gender.get('female', 0) else "👩 Ж"
            main_percentage = max(gender.get('male', 0), gender.get('female', 0))
            report += f"• {main_gender}: {main_percentage}%\n"
        
        # Возраст
        if age_groups:
            main_age = max(
                [(k, v) for k, v in age_groups.items() if 'average' not in k and 'unknown' not in k],
                key=lambda x: x[1],
                default=(None, 0)
            )[0]
            if main_age:
                report += f"• Возраст: {main_age}\n"
        
        # География
        if geography and geography.get('top_cities'):
            top_city = list(geography['top_cities'].keys())[0]
            top_percentage = geography['top_cities'][top_city]
            report += f"• Город: {top_city} ({top_percentage}%)\n"
        
        # Быстрые рекомендации
        report += "\n<b>💡 БЫСТРЫЕ РЕКОМЕНДАЦИИ:</b>\n"
        
        if gender.get('male', 0) > 70:
            report += "• Фокус на мужскую аудиторию\n"
        elif gender.get('female', 0) > 70:
            report += "• Фокус на женскую аудиторию\n"
        
        if age_groups.get('18-24', 0) > 40:
            report += "• Контент для молодежи\n"
        elif age_groups.get('35-44', 0) > 40:
            report += "• Контент для взрослой аудитории\n"
        
        report += "\n<i>Для детального анализа используйте команду /analyze</i>"
        
        await message.answer(report)
        
    except Exception as e:
        logger.error(f"Ошибка в команде /quick: {e}", exc_info=True)
        await message.answer("❌ <b>Ошибка быстрого анализа.</b> Попробуйте позже.")

@dp.message(Command("compare"))
async def cmd_compare(message: Message):
    """Сравнение аудиторий двух групп"""
    try:
        args = message.text.split()[1:]
        if len(args) < 2:
            await message.answer(
                "🔄 <b>Сравнение двух групп</b>\n\n"
                "Пример: <code>/compare https://vk.com/group1 https://vk.com/group2</code>\n\n"
                "<i>Сравнивает демографию, интересы и качество аудитории</i>"
            )
            return
        
        group1_link, group2_link = args[0].strip(), args[1].strip()
        
        await message.answer("🔄 <b>Начинаю сравнение аудиторий...</b>")
        logger.info(f"Пользователь {message.from_user.id} сравнивает {group1_link} и {group2_link}")
        
        groups_data = []
        successful_groups = []
        
        # Анализируем обе группы
        for i, link in enumerate([group1_link, group2_link], 1):
            status_msg = await message.answer(f"🔍 <b>Анализирую группу {i}...</b>")
            
            group_info = await vk_client.get_group_info(link)
            if not group_info:
                await status_msg.edit_text(f"❌ <b>Не удалось получить группу {i}</b>: {link}")
                continue
            
            if group_info.get('is_closed', 1) != 0:
                await status_msg.edit_text(f"⚠️ <b>Группа {i} закрытая:</b> {group_info['name']}")
                continue
            
            # Быстрый анализ (200 участников для скорости)
            members = await vk_client.get_group_members(group_info['id'], limit=200)
            if not members:
                await status_msg.edit_text(f"⚠️ <b>Нет данных об участниках:</b> {group_info['name']}")
                continue
            
            analysis = await analyzer.analyze_audience(members)
            groups_data.append({
                'info': group_info,
                'analysis': analysis
            })
            successful_groups.append(group_info['name'])
            
            await status_msg.edit_text(f"✅ <b>Группа {i} проанализирована:</b> {group_info['name']}")
        
        # Проверяем, что получили данные обеих групп
        if len(groups_data) < 2:
            await message.answer(
                "❌ <b>Не удалось получить данные для сравнения</b>\n\n"
                f"Успешно проанализировано: {len(groups_data)} из 2 групп\n"
                f"Группы: {', '.join(successful_groups) if successful_groups else 'нет'}"
            )
            return
        
        # Сравниваем аудитории
        comparison = await analyzer.compare_audiences(
            groups_data[0]['analysis'],
            groups_data[1]['analysis']
        )
        
        # Формируем отчет сравнения
        report = f"🔄 <b>СРАВНЕНИЕ АУДИТОРИЙ</b>\n\n"
        report += f"1️⃣ <b>{groups_data[0]['info']['name']}</b>\n"
        report += f"2️⃣ <b>{groups_data[1]['info']['name']}</b>\n\n"
        
        # Индикатор сходства
        similarity = comparison['similarity_score']
        if similarity >= 80:
            similarity_emoji = "🔴"
            similarity_text = "ОЧЕНЬ ВЫСОКОЕ"
        elif similarity >= 60:
            similarity_emoji = "🟠"
            similarity_text = "ВЫСОКОЕ"
        elif similarity >= 40:
            similarity_emoji = "🟡"
            similarity_text = "СРЕДНЕЕ"
        elif similarity >= 20:
            similarity_emoji = "🟢"
            similarity_text = "НИЗКОЕ"
        else:
            similarity_emoji = "🔵"
            similarity_text = "ОЧЕНЬ НИЗКОЕ"
        
        report += f"📈 <b>СХОДСТВО АУДИТОРИЙ: {similarity}%</b> {similarity_emoji}\n"
        report += f"<i>({similarity_text} сходство)</i>\n\n"
        
        # Общие характеристики
        if comparison['common_characteristics']:
            report += "<b>🔗 ОБЩИЕ ХАРАКТЕРИСТИКИ:</b>\n"
            for char in comparison['common_characteristics']:
                report += f"• {char}\n"
        else:
            report += "<i>⚠️ Значительных общих характеристик не обнаружено</i>\n"
        
        report += "\n<b>📊 КАЧЕСТВО АУДИТОРИЙ:</b>\n"
        report += f"• Группа 1: {comparison['audience1_quality']}/100\n"
        report += f"• Группа 2: {comparison['audience2_quality']}/100\n"
        report += f"• Разница: {comparison['quality_difference']} баллов\n"
        
        # Рекомендации по результатам сравнения
        report += "\n<b>💡 РЕКОМЕНДАЦИИ:</b>\n"
        if similarity > 70:
            report += "• Аудитории очень похожи - можно использовать схожие стратегии\n"
            report += "• Подойдут одинаковые темы контента и таргетинг\n"
        elif similarity > 40:
            report += "• Аудитории имеют сходства, но и различия\n"
            report += "• Адаптируйте контент под особенности каждой группы\n"
        else:
            report += "• Аудитории сильно отличаются\n"
            report += "• Используйте разные подходы для каждой группы\n"
        
        if comparison['audience1_quality'] > comparison['audience2_quality'] + 15:
            report += f"• Аудитория группы 1 качественнее на {comparison['quality_difference']} баллов\n"
        elif comparison['audience2_quality'] > comparison['audience1_quality'] + 15:
            report += f"• Аудитория группы 2 качественнее на {comparison['quality_difference']} баллов\n"
        
        await message.answer(report)
        
    except Exception as e:
        logger.error(f"Ошибка в команде /compare: {e}", exc_info=True)
        await message.answer(
            "❌ <b>Ошибка при сравнении групп</b>\n\n"
            "Попробуйте позже или проверьте правильность ссылок."
        )

@dp.message(Command("stats"))
async def cmd_stats(message: Message):
    """Показать статистику пользователя"""
    try:
        stats = await db.get_user_stats(message.from_user.id)
        
        report = f"📈 <b>ВАША СТАТИСТИКА</b>\n\n"
        report += f"👤 <b>Ваш ID:</b> {message.from_user.id}\n"
        report += f"📊 <b>Проанализировано групп:</b> {stats.get('total_analyses', 0)}\n"
        report += f"💾 <b>Сохранено отчетов:</b> {stats.get('saved_reports', 0)}\n"
        
        if stats.get('last_analyses'):
            report += "\n<b>📅 ПОСЛЕДНИЕ АНАЛИЗЫ:</b>\n"
            for i, analysis in enumerate(stats['last_analyses'][:5], 1):
                report += f"{i}. {analysis['group_name']} — {analysis['created_at']}\n"
        else:
            report += "\n<i>У вас пока нет сохраненных анализов.</i>\n"
            report += "<i>Используйте команду /analyze для первого анализа!</i>"
        
        # Добавляем кнопки действий
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="📊 Новый анализ", callback_data="start_analysis")],
                [InlineKeyboardButton(text="📤 Экспорт истории", callback_data="export_history")],
                [InlineKeyboardButton(text="🔙 В главное меню", callback_data="main_menu")]
            ]
        )
        
        await message.answer(report, reply_markup=keyboard)
        
    except Exception as e:
        logger.error(f"Ошибка в команде /stats: {e}", exc_info=True)
        await message.answer("❌ <b>Ошибка при получении статистики.</b> Попробуйте позже.")

@dp.message(Command("test_vk"))
async def cmd_test_vk(message: Message):
    """Тестирование подключения к VK API (только для администраторов)"""
    # Проверка прав администратора
    if message.from_user.id not in config.ADMIN_IDS:
        await message.answer(
            "❌ <b>Эта команда доступна только администраторам</b>\n\n"
            f"Ваш ID: {message.from_user.id}\n"
            f"Администраторы: {', '.join(map(str, config.ADMIN_IDS))}"
        )
        return
    
    await message.answer("🔍 <b>Запускаю тестирование подключения к VK API...</b>")
    
    try:
        result = await vk_client.test_connection()
        
        if result['success']:
            report = "✅ <b>ТЕСТИРОВАНИЕ ПРОЙДЕНО УСПЕШНО</b>\n\n"
            report += f"{result['message']}\n\n"
            
            if 'details' in result:
                report += "<b>Детали тестов:</b>\n"
                for detail in result['details']:
                    status = "✅" if detail['success'] else "❌"
                    message_text = detail['message'].replace('\n', ' ')
                    report += f"{status} <b>{detail['test']}:</b> {message_text}\n"
            
            report += f"\n<b>Конфигурация VK API:</b>\n"
            report += f"• Версия API: {config.VK_API_VERSION}\n"
            report += f"• Задержка между запросами: {config.REQUEST_DELAY:.2f}с\n"
            report += f"• Токен: {'✅ Установлен' if config.VK_SERVICE_TOKEN else '❌ Отсутствует'}\n"
            
            await message.answer(report)
            
        else:
            report = "❌ <b>ПРОБЛЕМЫ С ПОДКЛЮЧЕНИЕМ К VK API</b>\n\n"
            report += f"{result['message']}\n\n"
            
            if 'details' in result:
                report += "<b>Результаты тестов:</b>\n"
                for detail in result['details']:
                    status = "✅" if detail['success'] else "❌"
                    message_text = detail['message'].replace('\n', ' ')
                    report += f"{status} <b>{detail['test']}:</b> {message_text}\n"
            
            report += "\n<b>Возможные причины:</b>\n"
            report += "1. Неверный или просроченный VK_SERVICE_TOKEN\n"
            report += "2. Группа заблокирована (banned) в ВК\n"
            report += "3. Ограничения приложения VK\n"
            report += "4. Проблемы с сетью или блокировки\n"
            report += "5. Превышение лимитов API\n\n"
            report += "<b>Рекомендации:</b>\n"
            report += "1. Проверьте токен в настройках Railway\n"
            report += "2. Убедитесь, что приложение VK активно\n"
            report += "3. Проверьте права доступа приложения\n"
            report += "4. Попробуйте создать новый сервисный ключ\n"
            
            await message.answer(report)
            
    except Exception as e:
        logger.error(f"Ошибка тестирования VK: {e}", exc_info=True)
        await message.answer(
            f"❌ <b>Критическая ошибка тестирования:</b>\n\n"
            f"{str(e)[:200]}\n\n"
            "<i>Проверьте логи бота для подробной информации.</i>"
        )

@dp.callback_query(F.data == "analyze_group")
async def analyze_group_callback(callback: CallbackQuery):
    """Обработчик кнопки анализа группы"""
    await callback.message.answer(
        "🔍 <b>Анализ группы ВКонтакте</b>\n\n"
        "Отправьте ссылку на группу:\n"
        "<code>https://vk.com/public123</code>\n"
        "Или: <code>vk.com/groupname</code>\n\n"
        "Для полного анализа: /analyze ссылка\n"
        "Для быстрого анализа: /quick ссылка"
    )
    await callback.answer()

@dp.callback_query(F.data == "competitors_help")
async def competitors_help_callback(callback: CallbackQuery):
    """Обработчик кнопки помощи по конкурентам"""
    await callback.message.answer(
        "🥊 <b>Анализ конкурентов</b>\n\n"
        "Эта функция найдет и проанализирует похожие группы.\n\n"
        "<b>Пример команды:</b>\n"
        "<code>/competitors https://vk.com/public123</code>\n\n"
        "<b>Что делает бот:</b>\n"
        "1. Находит похожие группы по тематике\n"
        "2. Анализирует их аудиторию\n"
        "3. Сравнивает с вашей группой\n"
        "4. Дает рекомендации по улучшению\n\n"
        "<i>Анализ может занять 3-5 минут</i>"
    )
    await callback.answer()

@dp.callback_query(F.data == "text_analysis_help")
async def text_analysis_help_callback(callback: CallbackQuery):
    """Обработчик кнопки помощи по AI-анализу текста"""
    await callback.message.answer(
        "🧠 <b>AI-анализ текста</b>\n\n"
        "Эта функция анализирует текстовый контент группы.\n\n"
        "<b>Пример команды:</b>\n"
        "<code>/text_analysis https://vk.com/public123</code>\n\n"
        "<b>Что анализирует бот:</b>\n"
        "• Тональность (позитивная/негативная/нейтральная)\n"
        "• Основные темы и категории\n"
        "• Ключевые слова и фразы\n"
        "• Эмоциональную окраску\n"
        "• Читаемость текста\n\n"
        "<i>Анализ использует NLP-алгоритмы</i>"
    )
    await callback.answer()

@dp.callback_query(F.data == "full_help")
async def full_help_callback(callback: CallbackQuery):
    """Обработчик кнопки полной помощи"""
    await cmd_help(callback.message)
    await callback.answer()

@dp.callback_query(F.data == "start_analysis")
async def start_analysis_callback(callback: CallbackQuery):
    """Обработчик кнопки начала анализа"""
    await callback.message.answer(
        "🎯 <b>Начать анализ группы</b>\n\n"
        "Отправьте ссылку на группу ВК:\n"
        "<code>https://vk.com/public123</code>\n"
        "Или: <code>vk.com/groupname</code>\n\n"
        "Для полного анализа: /analyze ссылка\n"
        "Для быстрого анализа: /quick ссылка"
    )
    await callback.answer()

@dp.callback_query(F.data == "start_competitors")
async def start_competitors_callback(callback: CallbackQuery):
    """Обработчик кнопки начала анализа конкурентов"""
    await callback.message.answer(
        "🥊 <b>Анализ конкурентов</b>\n\n"
        "Отправьте ссылку на вашу группу:\n"
        "<code>/competitors https://vk.com/public123</code>\n\n"
        "Бот найдет похожие группы и проанализирует их."
    )
    await callback.answer()

@dp.callback_query(F.data == "start_text_analysis")
async def start_text_analysis_callback(callback: CallbackQuery):
    """Обработчик кнопки начала AI-анализа текста"""
    await callback.message.answer(
        "🧠 <b>AI-анализ текста</b>\n\n"
        "Отправьте ссылку на группу:\n"
        "<code>/text_analysis https://vk.com/public123</code>\n\n"
        "Бот проанализирует текстовый контент группы."
    )
    await callback.answer()

@dp.callback_query(F.data == "user_stats")
async def user_stats_callback(callback: CallbackQuery):
    """Обработчик кнопки статистики"""
    await cmd_stats(callback.message)
    await callback.answer()

@dp.callback_query(F.data == "main_menu")
async def main_menu_callback(callback: CallbackQuery):
    """Обработчик кнопки главного меню"""
    await cmd_start(callback.message)
    await callback.answer()

@dp.callback_query(F.data == "back_to_start")
async def back_to_start_callback(callback: CallbackQuery):
    """Обработчик кнопки возврата в начало"""
    await cmd_start(callback.message)
    await callback.answer()

# ==================== ОСНОВНАЯ ФУНКЦИЯ ====================

async def main():
    """Основная функция запуска бота"""
    logger.info("=" * 60)
    logger.info("🚀 ЗАПУСК ТЕЛЕГРАМ БОТА С AI-АНАЛИЗОМ И АНАЛИЗОМ КОНКУРЕНТОВ")
    logger.info("=" * 60)
    
    try:
        # Инициализация базы данных
        logger.info("Инициализация базы данных...")
        db_success = await db.init_db()
        
        if db_success:
            logger.info("✅ База данных подключена успешно")
        else:
            logger.warning("⚠️  Бот запущен с временной SQLite базой")
        
        # Получение информации о боте
        bot_info = await bot.get_me()
        logger.info(f"🤖 Бот: @{bot_info.username} (ID: {bot_info.id})")
        logger.info(f"👥 Администраторы: {config.ADMIN_IDS}")
        logger.info(f"🌐 VK API Версия: {config.VK_API_VERSION}")
        logger.info(f"🧠 AI-анализ: {'✅ Включен' if config.ENABLE_AI_ANALYSIS else '❌ Выключен'}")
        logger.info(f"🥊 Анализ конкурентов: {'✅ Включен' if config.ENABLE_COMPETITOR_ANALYSIS else '❌ Выключен'}")
        
        # Сбрасываем вебхук
        try:
            await bot.delete_webhook(drop_pending_updates=True)
            logger.info("✅ Вебхук сброшен, старые обновления удалены")
        except Exception as e:
            logger.warning(f"При сбросе вебхука: {e}")
        
        # Ждем 2 секунды для очистки состояния
        await asyncio.sleep(2)
        
        logger.info("✅ Бот готов к работе! Ожидание команд...")
        logger.info("-" * 60)
        
        # Запуск бота
        await dp.start_polling(bot, skip_updates=True)
        
    except KeyboardInterrupt:
        logger.info("Получен сигнал прерывания (Ctrl+C)")
    except Exception as e:
        logger.critical(f"КРИТИЧЕСКАЯ ОШИБКА ПРИ ЗАПУСКЕ БОТА: {e}", exc_info=True)
        raise
    finally:
        # Корректное завершение работы
        logger.info("Завершение работы бота...")
        
        try:
            await db.close()
            logger.info("✅ Соединения с базой данных закрыты")
        except Exception as e:
            logger.error(f"Ошибка при закрытии БД: {e}")
        
        try:
            await vk_client.close()
            logger.info("✅ Сессия VK API закрыта")
        except Exception as e:
            logger.error(f"Ошибка при закрытии VK клиента: {e}")
        
        logger.info("Бот остановлен")
        logger.info("=" * 60)

if __name__ == "__main__":
    asyncio.run(main())
