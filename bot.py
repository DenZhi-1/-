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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
        "/help - справка по использованию"
    )

@dp.message(Command("analyze"))
async def cmd_analyze(message: Message):
    try:
        args = message.text.split()[1:]
        if not args:
            await message.answer("Укажите ссылку на группу ВК")
            return
        
        group_link = args[0]
        await message.answer("⏳ Начинаю анализ аудитории...")
        
        # Получаем данные из ВК
        group_info = await vk_client.get_group_info(group_link)
        if not group_info:
            await message.answer("❌ Не удалось получить информацию о группе")
            return
        
        members = await vk_client.get_group_members(group_info['id'], limit=1000)
        
        # Анализируем аудиторию
        analysis = await analyzer.analyze_audience(members)
        
        # Сохраняем результаты
        await db.save_analysis(
            user_id=message.from_user.id,
            group_id=group_info['id'],
            group_name=group_info['name'],
            analysis=analysis
        )
        
        # Формируем отчет
        report = f"📊 <b>Анализ аудитории: {group_info['name']}</b>\n\n"
        report += f"👥 Всего участников: {group_info['members_count']:,}\n"
        report += f"📈 Проанализировано: {len(members):,}\n\n"
        
        if 'gender' in analysis:
            male = analysis['gender'].get('male', 0)
            female = analysis['gender'].get('female', 0)
            report += f"👨 Мужчины: {male}%\n"
            report += f"👩 Женщины: {female}%\n\n"
        
        if 'age_groups' in analysis:
            report += "<b>Возрастные группы:</b>\n"
            for age, perc in analysis['age_groups'].items():
                report += f"{age}: {perc}%\n"
        
        if 'cities' in analysis and analysis['cities']:
            report += f"\n<b>Топ городов:</b>\n"
            for i, (city, count) in enumerate(list(analysis['cities'].items())[:5], 1):
                report += f"{i}. {city}: {count}%\n"
        
        await message.answer(report)
        
        # Рекомендации для таргета
        if analysis.get('recommendations'):
            rec_text = "\n<b>🎯 Рекомендации для таргета:</b>\n"
            for rec in analysis['recommendations'][:3]:
                rec_text += f"• {rec}\n"
            await message.answer(rec_text)
            
    except Exception as e:
        logger.error(f"Ошибка анализа: {e}")
        await message.answer("❌ Произошла ошибка при анализе")

@dp.message(Command("compare"))
async def cmd_compare(message: Message):
    try:
        args = message.text.split()[1:]
        if len(args) < 2:
            await message.answer("Укажите две ссылки на группы для сравнения")
            return
        
        await message.answer("⏳ Сравниваю аудитории...")
        
        groups_data = []
        for link in args[:2]:
            group_info = await vk_client.get_group_info(link)
            if group_info:
                members = await vk_client.get_group_members(group_info['id'], limit=500)
                groups_data.append({
                    'info': group_info,
                    'members': members,
                    'analysis': await analyzer.analyze_audience(members)
                })
        
        if len(groups_data) < 2:
            await message.answer("❌ Не удалось получить данные одной из групп")
            return
        
        # Сравниваем аудитории
        comparison = await analyzer.compare_audiences(
            groups_data[0]['analysis'],
            groups_data[1]['analysis']
        )
        
        report = f"📊 <b>Сравнение аудиторий:</b>\n\n"
        report += f"1. {groups_data[0]['info']['name']}\n"
        report += f"2. {groups_data[1]['info']['name']}\n\n"
        report += f"📈 Сходство аудиторий: {comparison['similarity_score']}%\n\n"
        
        if comparison['common_characteristics']:
            report += "<b>Общие характеристики:</b>\n"
            for char in comparison['common_characteristics'][:3]:
                report += f"• {char}\n"
        
        await message.answer(report)
        
    except Exception as e:
        logger.error(f"Ошибка сравнения: {e}")
        await message.answer("❌ Ошибка при сравнении")

@dp.message(Command("stats"))
async def cmd_stats(message: Message):
    try:
        stats = await db.get_user_stats(message.from_user.id)
        
        report = f"📈 <b>Ваша статистика</b>\n\n"
        report += f"🔍 Проанализировано групп: {stats.get('total_analyses', 0)}\n"
        report += f"💾 Сохранено отчетов: {stats.get('saved_reports', 0)}\n"
        
        if stats.get('last_analyses'):
            report += "\n<b>Последние анализы:</b>\n"
            for analysis in stats['last_analyses'][:3]:
                report += f"• {analysis['group_name']} - {analysis['created_at'].strftime('%d.%m.%Y')}\n"
        
        await message.answer(report)
    except Exception as e:
        logger.error(f"Ошибка статистики: {e}")
        await message.answer("❌ Ошибка при получении статистики")

@dp.message(Command("help"))
async def cmd_help(message: Message):
    help_text = """
<b>📚 Справка по использованию бота</b>

<code>/analyze https://vk.com/groupname</code> - проанализировать аудиторию группы

<code>/compare ссылка1 ссылка2</code> - сравнить две аудитории

<code>/stats</code> - ваша статистика

<b>Примеры использования:</b>
• Анализ конкурента: <code>/analyze https://vk.com/competitor</code>
• Сравнение с целевой аудиторией: <code>/compare https://vk.com/mygroup https://vk.com/targetgroup</code>

<b>Что анализирует бот:</b>
• Демография (пол, возраст)
• География (города)
• Интересы и активность
• Рекомендации для таргета
"""
    await message.answer(help_text)

async def main():
    try:
        # Инициализируем базу данных
        await db.init_db()
        
        # Запускаем бота
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
    finally:
        # Закрываем сессию VK клиента при завершении
        await vk_client.close()

if __name__ == "__main__":
    asyncio.run(main())
