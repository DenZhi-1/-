import asyncio
import logging
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.enums import ParseMode

from config import config
from vk_api_client import vk_client
from analytics import AudienceAnalyzer
from database import Database

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Диагностика: вывод полученного DATABASE_URL (первые 60 символов)
logger.info(f"DATABASE_URL из конфига: {config.DATABASE_URL[:60]}...")

# Валидация конфигурации
try:
    config.validate()
except ValueError as e:
    logger.error(str(e))
    raise

# Инициализация бота с новым синтаксисом aiogram 3.7.0+
bot = Bot(
    token=config.TELEGRAM_BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
dp = Dispatcher()
db = Database()
analyzer = AudienceAnalyzer()

@dp.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer(
        "👋 Привет! Я бот для анализа аудитории ВКонтакте.\n\n"
        "Доступные команды:\n"
        "/analyze [ссылка] - проанализировать аудиторию группы\n"
        "/compare [ссылка1] [ссылка2] - сравнить две аудитории\n"
        "/stats - моя статистика\n"
        "/help - справка по использованию\n\n"
        "⚠️ <i>Внимание: Для анализа доступны только открытые группы ВК.</i>"
    )

@dp.message(Command("analyze"))
async def cmd_analyze(message: Message):
    try:
        args = message.text.split()[1:]
        if not args:
            await message.answer("❌ Укажите ссылку на группу ВК\nНапример: <code>/analyze https://vk.com/public123</code>")
            return
        
        group_link = args[0].strip()
        await message.answer("⏳ Начинаю анализ аудитории...")
        
        logger.info(f"Пользователь {message.from_user.id} запросил анализ {group_link}")
        
        # Получаем данные из ВК
        group_info = await vk_client.get_group_info(group_link)
        if not group_info:
            await message.answer("❌ Не удалось получить информацию о группе. Проверьте ссылку и доступность группы.")
            return
        
        # Проверяем, что группа не приватная
        if group_info.get('members_count', 0) == 0:
            await message.answer("⚠️ Группа приватная или недоступна для анализа участников.")
            return
        
        await message.answer(f"📊 Группа: <b>{group_info['name']}</b>\n👥 Участников: {group_info['members_count']:,}\n\n⌛️ Собираю данные...")
        
        # Получаем участников (ограничиваем для скорости)
        members_limit = min(1000, group_info['members_count'])
        members = await vk_client.get_group_members(group_info['id'], limit=members_limit)
        
        if not members:
            await message.answer("❌ Не удалось получить информацию об участниках группы.")
            return
        
        # Анализируем аудиторию
        analysis = await analyzer.analyze_audience(members)
        
        # Сохраняем результаты
        saved = await db.save_analysis(
            user_id=message.from_user.id,
            group_id=group_info['id'],
            group_name=group_info['name'],
            analysis=analysis
        )
        
        if saved:
            logger.info(f"Анализ группы {group_info['name']} сохранен в БД")
        
        # Формируем отчет
        report = f"📊 <b>Анализ аудитории: {group_info['name']}</b>\n\n"
        report += f"👥 Всего участников: {group_info['members_count']:,}\n"
        report += f"📈 Проанализировано: {len(members):,} ({min(100, len(members)*100//group_info['members_count'])}%)\n\n"
        
        if 'gender' in analysis:
            male = analysis['gender'].get('male', 0)
            female = analysis['gender'].get('female', 0)
            unknown = analysis['gender'].get('unknown', 0)
            report += f"<b>Гендерное распределение:</b>\n"
            report += f"👨 Мужчины: {male}%\n"
            report += f"👩 Женщины: {female}%\n"
            if unknown > 0:
                report += f"❓ Не указано: {unknown}%\n"
            report += "\n"
        
        if 'age_groups' in analysis:
            report += "<b>Возрастные группы:</b>\n"
            for age, perc in analysis['age_groups'].items():
                if perc > 0:
                    report += f"• {age}: {perc}%\n"
            report += "\n"
        
        if 'cities' in analysis and analysis['cities']:
            report += "<b>Топ городов:</b>\n"
            for i, (city, count) in enumerate(list(analysis['cities'].items())[:5], 1):
                report += f"{i}. {city}: {count}%\n"
            report += "\n"
        
        await message.answer(report)
        
        # Рекомендации для таргета
        if analysis.get('recommendations'):
            rec_text = "<b>🎯 Рекомендации для таргета:</b>\n"
            for i, rec in enumerate(analysis['recommendations'][:3], 1):
                rec_text += f"{i}. {rec}\n"
            await message.answer(rec_text)
            
    except Exception as e:
        logger.error(f"Ошибка в команде /analyze: {e}", exc_info=True)
        await message.answer("❌ Произошла внутренняя ошибка при анализе. Попробуйте позже.")

@dp.message(Command("compare"))
async def cmd_compare(message: Message):
    try:
        args = message.text.split()[1:]
        if len(args) < 2:
            await message.answer("❌ Укажите две ссылки на группы для сравнения\nНапример: <code>/compare https://vk.com/group1 https://vk.com/group2</code>")
            return
        
        await message.answer("⏳ Сравниваю аудитории...")
        
        groups_data = []
        for i, link in enumerate(args[:2], 1):
            group_info = await vk_client.get_group_info(link.strip())
            if group_info:
                members = await vk_client.get_group_members(group_info['id'], limit=500)
                analysis = await analyzer.analyze_audience(members)
                groups_data.append({
                    'info': group_info,
                    'members': members,
                    'analysis': analysis
                })
                await message.answer(f"✅ Группа {i}: <b>{group_info['name']}</b> ({len(members)} участников)")
            else:
                await message.answer(f"❌ Не удалось получить данные группы {i}: {link}")
        
        if len(groups_data) < 2:
            await message.answer("❌ Не удалось получить данные одной из групп")
            return
        
        # Сравниваем аудитории
        comparison = await analyzer.compare_audiences(
            groups_data[0]['analysis'],
            groups_data[1]['analysis']
        )
        
        report = f"📊 <b>Сравнение аудиторий</b>\n\n"
        report += f"1️⃣ {groups_data[0]['info']['name']}\n"
        report += f"2️⃣ {groups_data[1]['info']['name']}\n\n"
        report += f"📈 <b>Сходство аудиторий: {comparison['similarity_score']}%</b>\n\n"
        
        if comparison['common_characteristics']:
            report += "<b>Общие характеристики:</b>\n"
            for char in comparison['common_characteristics']:
                report += f"• {char}\n"
        else:
            report += "⚠️ <i>Значительных общих характеристик не обнаружено</i>"
        
        await message.answer(report)
        
    except Exception as e:
        logger.error(f"Ошибка в команде /compare: {e}", exc_info=True)
        await message.answer("❌ Ошибка при сравнении групп. Попробуйте позже.")

@dp.message(Command("stats"))
async def cmd_stats(message: Message):
    try:
        stats = await db.get_user_stats(message.from_user.id)
        
        report = f"📈 <b>Ваша статистика</b>\n\n"
        report += f"👤 Ваш ID: {message.from_user.id}\n"
        report += f"🔍 Проанализировано групп: {stats.get('total_analyses', 0)}\n"
        report += f"💾 Сохранено отчетов: {stats.get('saved_reports', 0)}\n"
        
        if stats.get('last_analyses'):
            report += "\n<b>Последние анализы:</b>\n"
            for analysis in stats['last_analyses']:
                report += f"• {analysis['group_name']} - {analysis['created_at']}\n"
        else:
            report += "\n<i>У вас пока нет сохраненных анализов</i>"
        
        await message.answer(report)
        
    except Exception as e:
        logger.error(f"Ошибка в команде /stats: {e}")
        await message.answer("❌ Ошибка при получении статистики.")

@dp.message(Command("help"))
async def cmd_help(message: Message):
    help_text = """
<b>📚 Справка по использованию бота</b>

<b>Основные команды:</b>
<code>/analyze https://vk.com/groupname</code> - проанализировать аудиторию группы ВК

<code>/compare ссылка1 ссылка2</code> - сравнить аудитории двух групп

<code>/stats</code> - посмотреть вашу статистику

<code>/help</code> - показать эту справку

<b>Примеры использования:</b>
• Анализ группы: <code>/analyze https://vk.com/public123</code>
• Сравнение групп: <code>/compare https://vk.com/group1 https://vk.com/group2</code>

<b>Что анализирует бот:</b>
✅ Демография (пол, возраст)
✅ География (города)
✅ Интересы и активность
✅ Рекомендации для таргетированной рекламы

<b>Ограничения:</b>
⚠️ Доступны только открытые группы
⚠️ Анализируется до 1000 участников
⚠️ Данные предоставляются "как есть"
"""
    await message.answer(help_text)

async def main():
    """Основная функция запуска бота"""
    logger.info("Запуск Telegram бота для анализа аудитории ВК...")
    
    try:
        # Инициализируем базу данных
        logger.info("Инициализация базы данных...")
        db_success = await db.init_db()
        
        if db_success:
            logger.info("Бот готов к работе!")
        else:
            logger.warning("Бот запущен с временной SQLite базой. Данные не будут сохранены после перезапуска!")
        
        # Запускаем бота
        logger.info(f"Бот @{(await bot.get_me()).username} запущен")
        await dp.start_polling(bot)
        
    except Exception as e:
        logger.critical(f"Критическая ошибка при запуске бота: {e}", exc_info=True)
        raise
        
    finally:
        # Корректное завершение работы
        logger.info("Завершение работы бота...")
        await db.close()
        await vk_client.close()
        logger.info("Бот остановлен")

if __name__ == "__main__":
    asyncio.run(main())
