#!/usr/bin/env python3
import os
import sys
import asyncpg
import logging
from urllib.parse import urlparse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def fix_database_structure():
    """Исправляет структуру базы данных PostgreSQL"""
    
    # Получаем DATABASE_URL из переменных окружения
    DATABASE_URL = os.getenv("DATABASE_URL")
    if not DATABASE_URL:
        logger.error("❌ DATABASE_URL не найден")
        return False
    
    logger.info(f"Подключаемся к PostgreSQL...")
    
    try:
        # Парсим URL и создаем DSN для asyncpg
        parsed = urlparse(DATABASE_URL)
        
        # Собираем параметры подключения
        conn_params = {
            'user': parsed.username,
            'password': parsed.password,
            'host': parsed.hostname,
            'port': parsed.port or 5432,
            'database': parsed.path[1:],  # Убираем первый слеш
            'ssl': 'require' if 'railway' in DATABASE_URL else None
        }
        
        # Подключаемся к PostgreSQL
        conn = await asyncpg.connect(**conn_params)
        
        logger.info("✅ Подключение к PostgreSQL установлено")
        
        # 1. Проверяем и исправляем таблицу analyses
        await conn.execute("""
            DO $$ 
            BEGIN
                -- Создаем таблицу, если не существует
                CREATE TABLE IF NOT EXISTS analyses (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    group_id VARCHAR(255) NOT NULL,
                    group_name VARCHAR(255) NOT NULL,
                    analysis_data JSONB NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                -- Если столбец group_id существует как INTEGER, меняем тип
                IF EXISTS (
                    SELECT 1 
                    FROM information_schema.columns 
                    WHERE table_name = 'analyses' 
                    AND column_name = 'group_id'
                    AND data_type = 'integer'
                ) THEN
                    -- Создаем временную таблицу
                    CREATE TABLE analyses_new (
                        id SERIAL PRIMARY KEY,
                        user_id INTEGER NOT NULL,
                        group_id VARCHAR(255) NOT NULL,
                        group_name VARCHAR(255) NOT NULL,
                        analysis_data JSONB NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                    
                    -- Копируем данные с преобразованием
                    INSERT INTO analyses_new (id, user_id, group_id, group_name, analysis_data, created_at)
                    SELECT id, user_id, group_id::VARCHAR, group_name, analysis_data, created_at
                    FROM analyses;
                    
                    -- Удаляем старую таблицу
                    DROP TABLE analyses CASCADE;
                    
                    -- Переименовываем новую таблицу
                    ALTER TABLE analyses_new RENAME TO analyses;
                    
                    RAISE NOTICE 'Тип столбца group_id изменен с INTEGER на VARCHAR';
                END IF;
            END $$;
        """)
        
        # 2. Создаем индексы, если их нет
        await conn.execute("""
            DO $$ 
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_indexes 
                    WHERE tablename = 'analyses' AND indexname = 'idx_analyses_user_id'
                ) THEN
                    CREATE INDEX idx_analyses_user_id ON analyses(user_id);
                END IF;
                
                IF NOT EXISTS (
                    SELECT 1 FROM pg_indexes 
                    WHERE tablename = 'analyses' AND indexname = 'idx_analyses_group_id'
                ) THEN
                    CREATE INDEX idx_analyses_group_id ON analyses(group_id);
                END IF;
                
                IF NOT EXISTS (
                    SELECT 1 FROM pg_indexes 
                    WHERE tablename = 'analyses' AND indexname = 'idx_analyses_created_at'
                ) THEN
                    CREATE INDEX idx_analyses_created_at ON analyses(created_at);
                END IF;
            END $$;
        """)
        
        # 3. Создаем таблицу user_stats, если не существует
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS user_stats (
                user_id INTEGER PRIMARY KEY,
                total_analyses INTEGER DEFAULT 0,
                saved_reports INTEGER DEFAULT 0,
                last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        logger.info("✅ Структура базы данных проверена и исправлена")
        
        # 4. Проверяем текущую структуру
        structure = await conn.fetch("""
            SELECT 
                table_name,
                column_name,
                data_type,
                character_maximum_length
            FROM information_schema.columns 
            WHERE table_name IN ('analyses', 'user_stats')
            ORDER BY table_name, ordinal_position;
        """)
        
        logger.info("\n📊 Текущая структура таблиц:")
        for row in structure:
            logger.info(f"  {row['table_name']}.{row['column_name']}: {row['data_type']}")
        
        await conn.close()
        logger.info("\n🎯 База данных готова к работе!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    import asyncio
    success = asyncio.run(fix_database_structure())
    sys.exit(0 if success else 1)
