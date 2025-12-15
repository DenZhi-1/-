import asyncio
import logging
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command, CommandObject
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.enums import ParseMode

from config import config
from vk_api_client import vk_client
from analytics import AudienceAnalyzer
from database import Database

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

# Словарь для хранения временных данных пользователей
user_sessions = {}

# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====================

def create_back_button() -> InlineKeyboardMarkup:
    """Создает кнопку 'Назад'"""
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад к отчету", callback_data="back_to_report")]
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

# ==================== КОМАНДЫ БОТА ====================

@dp.message(Command("start"))
async def cmd_start(message: Message):
    """Приветственное сообщение и список команд"""
    welcome_text = """
👋 <b>Привет! Я бот для глубокого анализа аудитории ВКонтакте.</b>

📊 <b>Я умею анализировать:</b>
• 👫 Демографию (пол, возраст, города)
• 🎯 Интересы и активность пользователей
• 📱 Социальную активность и вовлеченность
• 📊 Качество аудитории и полноту профилей
• 🏙️ Географическое распределение по типам городов

🚀 <b>Доступные команды:</b>
• /analyze [ссылка] — полный анализ аудитории группы
• /quick [ссылка] — быстрый анализ (основные метрики)
• /compare [ссылка1] [ссылка2] — сравнить две аудитории
• /stats — ваша статистика анализов
• /export — экспорт данных анализа
• /test_vk — тест подключения к VK API (админы)
• /help — подробная справка

🎯 <b>Пример использования:</b>
<code>/analyze https://vk.com/vk</code>
<code>/quick vk.com/public1</code>

⚠️ <i>Для анализа доступны только открытые группы ВК (до 1000 участников за анализ)</i>
"""
    
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📚 Справка", callback_data="help")],
            [InlineKeyboardButton(text="⚡ Быстрый анализ", callback_data="quick_help")],
            [InlineKeyboardButton(text="📊 Пример отчета", callback_data="example_report")]
        ]
    )
    
    await message.answer(welcome_text, reply_markup=keyboard)

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
• Сохранение в историю

<code>/quick ссылка_на_группу</code>
<b>Быстрый анализ</b>
• Основные метрики за 1 минуту
• Быстрая оценка аудитории
• Основные рекомендации

<code>/compare ссылка1 ссылка2</code>
<b>Сравнение двух групп</b>
• Сравнение демографии
• Сравнение интересов
• Оценка схожести аудиторий

<code>/stats</code>
<b>Ваша статистика</b>
• Количество анализов
• История запросов
• Сохраненные отчеты

<code>/export [id]</code>
<b>Экспорт данных</b>
• Экспорт анализа в текстовый формат
• Полный отчет с детализацией
• Данные для дальнейшей обработки

<code>/test_vk</code>
<b>Тест подключения к VK API</b>
• Проверка токена
• Тест запросов к API
• Только для администраторов

<b>📋 ПОДДЕРЖИВАЕМЫЕ ФОРМАТЫ ССЫЛОК:</b>
• Полная ссылка: <code>https://vk.com/public123456</code>
• Сокращенная: <code>vk.com/club123456</code>
• Короткое имя: <code>https://vk.com/durov</code>
• Упоминание: <code>@durov</code>
• ID группы: <code>public1</code>

<b>🎯 КАКИЕ ДАННЫЕ МЫ АНАЛИЗИРУЕМ:</b>

<u>Демография:</u>
• Пол и возраст пользователей
• Возрастные группы (до 18, 18-24, 25-34, 35-44, 45-54, 55+)
• Средний возраст аудитории

<u>География:</u>
• Топ-10 городов участников
• Распределение по типам городов (столицы, миллионники, малые города)
• Распределение по странам

<u>Интересы и активность:</u>
• Популярные категории интересов (технологии, спорт, искусство и др.)
• Социальная активность (когда были онлайн)
• Полнота заполнения профилей

<u>Качество аудитории:</u>
• Оценка качества от 0 до 100 баллов
• Анализ вовлеченности пользователей
• Рекомендации по улучшению

<b>⚠️ ОГРАНИЧЕНИЯ И ВАЖНАЯ ИНФОРМАЦИЯ:</b>
• Только открытые группы ВК
• Максимум 1000 участников за один анализ
• Лимиты VK API (~3 запроса в секунду)
• Данные обновляются в реальном времени
• Анализ может занять от 1 до 5 минут

<b>💡 СОВЕТЫ ПО ИСПОЛЬЗОВАНИЮ:</b>
1. Для быстрой проверки используйте /quick
2. Для детального анализа — /analyze
3. Сохраняйте интересные отчеты через /export
4. Сравнивайте похожие группы через /compare
5. Проверяйте статистику через /stats

<b>📞 ПОДДЕРЖКА:</b>
Если возникли проблемы или вопросы:
1. Проверьте правильность ссылки
2. Убедитесь, что группа открыта
3. Используйте /test_vk для проверки подключения
4. Обратитесь к администратору
"""
    
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⚡ Начать анализ", callback_data="start_analysis")],
            [InlineKeyboardButton(text="🔙 В начало", callback_data="back_to_start")]
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
        report += "• Вечер (20-21): повтор важного контента\n"
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
        from analytics import AudienceAnalyzer
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
                [InlineKeyboardButton(text="🧹 Очистить историю", callback_data="clear_history")]
            ]
        )
        
        await message.answer(report, reply_markup=keyboard)
        
    except Exception as e:
        logger.error(f"Ошибка в команде /stats: {e}", exc_info=True)
        await message.answer("❌ <b>Ошибка при получении статистики.</b> Попробуйте позже.")

@dp.message(Command("export"))
async def cmd_export(message: Message, command: CommandObject = None):
    """Экспорт данных анализа"""
    try:
        args = command.args.split() if command and command.args else []
        
        if not args:
            # Показываем список доступных для экспорта анализов
            user_analyses = await db.get_user_analyses(message.from_user.id, limit=10)
            
            if not user_analyses:
                await message.answer(
                    "📤 <b>Экспорт данных</b>\n\n"
                    "У вас пока нет сохраненных анализов для экспорта.\n\n"
                    "Сначала выполните анализ группы: <code>/analyze ссылка_на_группу</code>"
                )
                return
            
            keyboard = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(
                        text=f"{i+1}. {analysis['group_name']} ({analysis['created_at'][:10]})",
                        callback_data=f"export_{analysis['id']}"
                    )] for i, analysis in enumerate(user_analyses)
                ] + [[InlineKeyboardButton(text="📋 Экспорт всей истории", callback_data="export_all")]]
            )
            
            await message.answer(
                "📤 <b>Выберите анализ для экспорта:</b>\n\n"
                "Нажмите на кнопку ниже, чтобы экспортировать конкретный анализ.",
                reply_markup=keyboard
            )
            return
        
        # Экспорт конкретного анализа по ID
        analysis_id = args[0]
        analysis_data = await db.get_analysis_by_id(analysis_id, message.from_user.id)
        
        if not analysis_data:
            await message.answer("❌ <b>Анализ не найден или у вас нет доступа к нему.</b>")
            return
        
        # Формируем текстовый экспорт
        export_text = await format_export_text(analysis_data)
        
        # Отправляем как документ (или разбиваем на части если слишком большой)
        if len(export_text) > 4000:
            # Разбиваем на части
            parts = [export_text[i:i+4000] for i in range(0, len(export_text), 4000)]
            for i, part in enumerate(parts, 1):
                await message.answer(f"<b>Часть {i} из {len(parts)}:</b>\n\n<code>{part}</code>")
        else:
            await message.answer(f"<b>📤 Экспорт анализа:</b>\n\n<code>{export_text}</code>")
        
        await message.answer("✅ <b>Экспорт завершен!</b>\nВы можете скопировать данные или сохранить в файл.")
        
    except Exception as e:
        logger.error(f"Ошибка в команде /export: {e}", exc_info=True)
        await message.answer(
            "❌ <b>Ошибка при экспорте данных.</b>\n\n"
            "Используйте: <code>/export</code> для выбора анализа или <code>/export id</code>"
        )

async def format_export_text(analysis_data: dict) -> str:
    """Форматирует данные анализа для экспорта"""
    analysis = analysis_data.get('analysis', {})
    group_name = analysis_data.get('group_name', 'Неизвестная группа')
    created_at = analysis_data.get('created_at', 'Неизвестная дата')
    
    export_lines = [
        "=" * 60,
        f"ЭКСПОРТ АНАЛИЗА АУДИТОРИИ",
        f"Группа: {group_name}",
        f"Дата анализа: {created_at}",
        "=" * 60,
        ""
    ]
    
    # Общая информация
    export_lines.append("[ОБЩАЯ ИНФОРМАЦИЯ]")
    export_lines.append(f"Всего участников: {analysis.get('total_members_analyzed', 0)}")
    export_lines.append(f"Оценка качества: {analysis.get('audience_quality_score', 0)}/100")
    export_lines.append("")
    
    # Демография
    if 'gender' in analysis:
        export_lines.append("[ДЕМОГРАФИЯ]")
        gender = analysis['gender']
        export_lines.append(f"Мужчины: {gender.get('male', 0)}%")
        export_lines.append(f"Женщины: {gender.get('female', 0)}%")
        export_lines.append(f"Не указано: {gender.get('unknown', 0)}%")
        export_lines.append("")
    
    # Возрастные группы
    if 'age_groups' in analysis:
        export_lines.append("[ВОЗРАСТНЫЕ ГРУППЫ]")
        age_groups = analysis['age_groups']
        for age_group, percentage in age_groups.items():
            if 'average' not in age_group and 'unknown' not in age_group:
                export_lines.append(f"{age_group}: {percentage}%")
        if 'average_age' in age_groups:
            export_lines.append(f"Средний возраст: {age_groups['average_age']} лет")
        export_lines.append("")
    
    # География
    if 'geography' in analysis:
        geography = analysis['geography']
        export_lines.append("[ГЕОГРАФИЯ]")
        
        if geography.get('top_cities'):
            export_lines.append("Топ городов:")
            for city, percentage in geography['top_cities'].items():
                export_lines.append(f"  {city}: {percentage}%")
        
        if geography.get('city_types'):
            export_lines.append("Типы городов:")
            for city_type, percentage in geography['city_types'].items():
                export_lines.append(f"  {city_type}: {percentage}%")
        
        export_lines.append("")
    
    # Интересы
    if 'interests' in analysis:
        interests = analysis['interests']
        export_lines.append("[ИНТЕРЕСЫ]")
        export_lines.append(f"Заполненность профилей: {interests.get('profile_fill_rate', 0)}%")
        
        if interests.get('popular_categories'):
            export_lines.append("Популярные категории:")
            for category, percentage in interests['popular_categories'].items():
                export_lines.append(f"  {category}: {percentage}%")
        
        export_lines.append("")
    
    # Активность
    if 'social_activity' in analysis:
        social = analysis['social_activity']
        export_lines.append("[АКТИВНОСТЬ]")
        export_lines.append(f"Активных пользователей: {social.get('active_users_percentage', 0)}%")
        
        if social.get('last_seen_distribution'):
            export_lines.append("Время последней активности:")
            for period, percentage in social['last_seen_distribution'].items():
                period_name = period.replace('_', ' ')
                export_lines.append(f"  {period_name}: {percentage}%")
        
        export_lines.append("")
    
    # Рекомендации
    if 'recommendations' in analysis:
        export_lines.append("[РЕКОМЕНДАЦИИ]")
        for i, rec in enumerate(analysis['recommendations'][:10], 1):
            # Убираем HTML теги для чистого текста
            clean_rec = rec.replace('<b>', '').replace('</b>', '').replace('<i>', '').replace('</i>', '')
            export_lines.append(f"{i}. {clean_rec}")
    
    export_lines.append("")
    export_lines.append("=" * 60)
    export_lines.append(f"Экспортировано: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    export_lines.append("=" * 60)
    
    return "\n".join(export_lines)

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

@dp.callback_query(F.data == "start_analysis")
async def start_analysis_callback(callback: CallbackQuery):
    """Обработка callback для начала анализа"""
    await callback.message.answer(
        "🎯 <b>Начать анализ группы</b>\n\n"
        "Отправьте ссылку на группу ВК:\n"
        "<code>https://vk.com/public123</code>\n"
        "Или: <code>vk.com/groupname</code>\n\n"
        "Для полного анализа: /analyze ссылка\n"
        "Для быстрого анализа: /quick ссылка"
    )
    await callback.answer()

@dp.callback_query(F.data == "example_report")
async def example_report_callback(callback: CallbackQuery):
    """Показывает пример отчета"""
    example = """
📊 <b>ПРИМЕР ОТЧЕТА АНАЛИЗА</b>

<b>Группа:</b> ВКонтакте API
<b>Участников:</b> 4 914
<b>Проанализировано:</b> 1 000 (20%)

<b>👫 ГЕНДЕРНОЕ РАСПРЕДЕЛЕНИЕ:</b>
👨 Мужчины: 75% ████████████
👩 Женщины: 22% ████
❓ Не указано: 3% █

<b>📅 ВОЗРАСТНЫЕ ГРУППЫ:</b>
• 18-24: 45% █████████
• 25-34: 35% ███████
• 35-44: 15% ███
• 45-54: 3% █
• 55+: 2% █

<b>⭐ ОЦЕНКА КАЧЕСТВА:</b> 82/100 ⭐⭐⭐⭐⭐

<b>💡 ОСНОВНЫЕ РЕКОМЕНДАЦИИ:</b>
1. ✅ Аудитория преимущественно мужская - используйте мужские темы
2. 🎓 Основная группа 18-24 года - эффективны трендовый контент
3. 💻 Популярная тема: технологии - используйте в контенте
"""
    
    await callback.message.answer(example)
    await callback.answer("Пример отчета показан")

# ==================== ОСНОВНАЯ ФУНКЦИЯ ====================

async def main():
    """Основная функция запуска бота"""
    logger.info("=" * 60)
    logger.info("🚀 ЗАПУСК ТЕЛЕГРАМ БОТА ДЛЯ АНАЛИЗА АУДИТОРИИ ВК")
    logger.info("=" * 60)
    
    try:
        # Инициализация базы данных
        logger.info("Инициализация базы данных...")
        db_success = await db.init_db()
        
        if db_success:
            logger.info("✅ База данных подключена успешно")
        else:
            logger.warning("⚠️  Бот запущен с временной SQLite базой. Данные могут быть не сохранены!")
        
        # Получение информации о боте
        bot_info = await bot.get_me()
        logger.info(f"🤖 Бот: @{bot_info.username} (ID: {bot_info.id})")
        logger.info(f"👥 Администраторы: {config.ADMIN_IDS}")
        logger.info(f"🌐 VK API Версия: {config.VK_API_VERSION}")
        
        # Сбрасываем вебхук на случай остаточных состояний
        try:
            await bot.delete_webhook(drop_pending_updates=True)
            logger.info("✅ Вебхук сброшен, старые обновления удалены")
        except Exception as e:
            logger.warning(f"При сбросе вебхука: {e}")
        
        # Ждем 2 секунды для очистки состояния
        await asyncio.sleep(2)
        
        logger.info("✅ Бот готов к работе! Ожидание команд...")
        logger.info("-" * 60)
        
        # Запуск бота с пропуском старых обновлений
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
