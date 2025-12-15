import logging
import json
import os
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union
from urllib.parse import urlparse

import asyncpg
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, Integer, String, JSON, DateTime, select, text, func, Index
from sqlalchemy.exc import SQLAlchemyError
import sqlalchemy

from config import config

logger = logging.getLogger(__name__)

# SQLAlchemy Base для моделей
Base = declarative_base()

class Analysis(Base):
    """Модель для хранения результатов анализа"""
    __tablename__ = 'analyses'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, nullable=False, index=True)
    group_id = Column(String(255), nullable=False, index=True)  # VARCHAR для VK ID
    group_name = Column(String(255), nullable=False)
    analysis_data = Column(JSON, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    # Индексы для оптимизации запросов
    __table_args__ = (
        Index('idx_user_created', 'user_id', 'created_at'),
        Index('idx_group_created', 'group_id', 'created_at'),
    )
    
    def __repr__(self):
        return f"<Analysis(user_id={self.user_id}, group_id={self.group_id}, created_at={self.created_at})>"
    
    def to_dict(self) -> Dict[str, Any]:
        """Преобразует объект в словарь"""
        return {
            'id': self.id,
            'user_id': self.user_id,
            'group_id': self.group_id,
            'group_name': self.group_name,
            'analysis_data': self.analysis_data,
            'created_at': self.created_at.isoformat() if self.created_at else None
        }

class UserStats(Base):
    """Модель для хранения статистики пользователей"""
    __tablename__ = 'user_stats'
    
    user_id = Column(Integer, primary_key=True)
    total_analyses = Column(Integer, default=0, nullable=False)
    saved_reports = Column(Integer, default=0, nullable=False)
    last_activity = Column(DateTime, default=datetime.utcnow, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    
    def __repr__(self):
        return f"<UserStats(user_id={self.user_id}, total_analyses={self.total_analyses})>"

class Database:
    """Класс для работы с базой данных с поддержкой PostgreSQL и SQLite"""
    
    def __init__(self):
        self.engine = None
        self.async_session = None
        self.pool = None  # Пул соединений asyncpg для PostgreSQL
        self.db_type = None  # Тип БД: 'postgresql' или 'sqlite'
        self.db_url = None
        
    async def init_db(self) -> bool:
        """Инициализация подключения к базе данных"""
        try:
            self.db_url = config.DATABASE_URL
            logger.info(f"Инициализация БД с URL: {self.db_url[:50] if self.db_url else 'Нет URL'}...")
            
            # Если нет DATABASE_URL, используем SQLite
            if not self.db_url:
                logger.warning("DATABASE_URL не указан, используется SQLite")
                return await self._init_sqlite()
            
            # Определяем тип базы данных
            if "postgres" in self.db_url.lower():
                self.db_type = 'postgresql'
                # Пробуем инициализировать PostgreSQL
                pg_success = await self._init_postgresql()
                if pg_success:
                    return True
                else:
                    logger.warning("Не удалось инициализировать PostgreSQL, пробуем SQLite...")
                    return await self._init_sqlite()
            else:
                # SQLite или другая база
                self.db_type = 'sqlite'
                return await self._init_sqlite(self.db_url)
                
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации базы данных: {e}")
            # Fallback на in-memory SQLite
            return await self._init_sqlite("sqlite+aiosqlite:///:memory:")
    
    async def _init_postgresql(self) -> bool:
        """Инициализация PostgreSQL через asyncpg"""
        try:
            # Парсим URL для получения параметров подключения
            parsed = urlparse(self.db_url.replace("postgresql+asyncpg://", "postgresql://"))
            
            # Собираем параметры подключения для asyncpg
            pg_params = {
                'user': parsed.username,
                'password': parsed.password,
                'host': parsed.hostname,
                'port': parsed.port or 5432,
                'database': parsed.path[1:],  # Убираем первый слеш
                'ssl': 'require' if 'railway' in self.db_url.lower() else None
            }
            
            logger.info(f"Подключение к PostgreSQL: {parsed.hostname}:{parsed.port}/{parsed.path[1:]}")
            
            # Создаем пул соединений asyncpg
            self.pool = await asyncpg.create_pool(
                **{k: v for k, v in pg_params.items() if v is not None},
                min_size=1,
                max_size=10,
                command_timeout=60,
                max_inactive_connection_lifetime=300
            )
            
            # Проверяем соединение
            async with self.pool.acquire() as conn:
                # Проверяем существование таблиц и исправляем структуру если нужно
                await self._ensure_postgresql_structure(conn)
            
            # Также инициализируем SQLAlchemy для совместимости
            self.engine = create_async_engine(
                self.db_url,
                echo=False,
                pool_size=5,
                max_overflow=10,
                pool_pre_ping=True,
                connect_args={"server_settings": {"jit": "off"}}
            )
            
            self.async_session = async_sessionmaker(
                self.engine,
                class_=AsyncSession,
                expire_on_commit=False
            )
            
            # Создаем таблицы через SQLAlchemy (если не созданы через asyncpg)
            async with self.engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            
            logger.info("✅ PostgreSQL успешно инициализирован")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации PostgreSQL: {e}")
            if self.pool:
                await self.pool.close()
            return False
    
    async def _ensure_postgresql_structure(self, conn: asyncpg.Connection) -> None:
        """Проверяет и исправляет структуру таблиц в PostgreSQL"""
        try:
            # Проверяем существование таблицы analyses
            table_exists = await conn.fetchval(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'analyses')"
            )
            
            if table_exists:
                # Проверяем тип столбца group_id
                column_info = await conn.fetchrow("""
                    SELECT data_type 
                    FROM information_schema.columns 
                    WHERE table_name = 'analyses' AND column_name = 'group_id'
                """)
                
                if column_info and column_info['data_type'] == 'integer':
                    logger.warning("⚠️  Обнаружен неправильный тип INTEGER для group_id. Исправляем...")
                    
                    # Создаем временную таблицу
                    await conn.execute("""
                        CREATE TABLE analyses_new (
                            id SERIAL PRIMARY KEY,
                            user_id INTEGER NOT NULL,
                            group_id VARCHAR(255) NOT NULL,
                            group_name VARCHAR(255) NOT NULL,
                            analysis_data JSONB NOT NULL,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                    
                    # Копируем данные с преобразованием
                    await conn.execute("""
                        INSERT INTO analyses_new (user_id, group_id, group_name, analysis_data, created_at)
                        SELECT user_id, group_id::VARCHAR, group_name, analysis_data, created_at
                        FROM analyses
                    """)
                    
                    # Удаляем старую и переименовываем новую
                    await conn.execute("DROP TABLE analyses CASCADE")
                    await conn.execute("ALTER TABLE analyses_new RENAME TO analyses")
                    
                    # Создаем индексы
                    await conn.execute("CREATE INDEX IF NOT EXISTS idx_analyses_user_id ON analyses(user_id)")
                    await conn.execute("CREATE INDEX IF NOT EXISTS idx_analyses_group_id ON analyses(group_id)")
                    await conn.execute("CREATE INDEX IF NOT EXISTS idx_analyses_created_at ON analyses(created_at)")
                    
                    logger.info("✅ Структура таблицы analyses исправлена")
            
            # Создаем таблицу user_stats если не существует
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS user_stats (
                    user_id INTEGER PRIMARY KEY,
                    total_analyses INTEGER DEFAULT 0 NOT NULL,
                    saved_reports INTEGER DEFAULT 0 NOT NULL,
                    last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
                )
            """)
            
        except Exception as e:
            logger.error(f"Ошибка проверки структуры PostgreSQL: {e}")
    
    async def _init_sqlite(self, db_url: str = None) -> bool:
        """Инициализация SQLite"""
        try:
            if not db_url:
                db_url = "sqlite+aiosqlite:///database.db"
            
            logger.info(f"Инициализация SQLite: {db_url}")
            
            self.db_type = 'sqlite'
            self.engine = create_async_engine(
                db_url,
                echo=False,
                connect_args={"check_same_thread": False}
            )
            
            self.async_session = async_sessionmaker(
                self.engine,
                class_=AsyncSession,
                expire_on_commit=False
            )
            
            # Создаем таблицы
            async with self.engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            
            logger.info("✅ SQLite успешно инициализирован")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации SQLite: {e}")
            return False
    
    async def save_analysis(self, user_id: int, group_id: int, 
                           group_name: str, analysis: Dict[str, Any]) -> bool:
        """
        Сохраняет результат анализа в базу данных
        
        Args:
            user_id: ID пользователя Telegram
            group_id: ID группы ВК (будет преобразован в строку)
            group_name: Название группы
            analysis: Данные анализа
            
        Returns:
            bool: Успешно ли сохранено
        """
        try:
            # Преобразуем group_id в строку
            group_id_str = str(group_id)
            
            # Для PostgreSQL используем asyncpg для надежности
            if self.db_type == 'postgresql' and self.pool:
                return await self._save_analysis_postgresql(user_id, group_id_str, group_name, analysis)
            else:
                # Для SQLite используем SQLAlchemy
                return await self._save_analysis_sqlalchemy(user_id, group_id_str, group_name, analysis)
                
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения анализа: {e}", exc_info=True)
            return False
    
    async def _save_analysis_postgresql(self, user_id: int, group_id: str, 
                                       group_name: str, analysis: Dict[str, Any]) -> bool:
        """Сохранение анализа в PostgreSQL через asyncpg"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    # Сохраняем анализ
                    await conn.execute("""
                        INSERT INTO analyses (user_id, group_id, group_name, analysis_data, created_at)
                        VALUES ($1, $2, $3, $4, $5)
                    """, user_id, group_id, group_name[:250], json.dumps(analysis, ensure_ascii=False), datetime.utcnow())
                    
                    # Обновляем статистику пользователя
                    await conn.execute("""
                        INSERT INTO user_stats (user_id, total_analyses, last_activity, created_at, updated_at)
                        VALUES ($1, 1, $2, $2, $2)
                        ON CONFLICT (user_id) DO UPDATE SET
                        total_analyses = user_stats.total_analyses + 1,
                        last_activity = $2,
                        updated_at = $2
                    """, user_id, datetime.utcnow())
            
            logger.info(f"✅ Анализ сохранен (PostgreSQL): user_id={user_id}, group_id={group_id}")
            return True
            
        except asyncpg.exceptions.DataError as e:
            logger.error(f"❌ Ошибка типа данных PostgreSQL: {e}")
            logger.error(f"Параметры: user_id={user_id}, group_id={group_id} (тип: {type(group_id)})")
            return False
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения в PostgreSQL: {e}")
            return False
    
    async def _save_analysis_sqlalchemy(self, user_id: int, group_id: str, 
                                       group_name: str, analysis: Dict[str, Any]) -> bool:
        """Сохранение анализа через SQLAlchemy (для SQLite)"""
        try:
            async with self.async_session() as session:
                # Создаем запись анализа
                analysis_record = Analysis(
                    user_id=user_id,
                    group_id=group_id,
                    group_name=group_name[:250],
                    analysis_data=analysis,
                    created_at=datetime.utcnow()
                )
                session.add(analysis_record)
                
                # Обновляем статистику пользователя
                stats = await session.get(UserStats, user_id)
                if not stats:
                    stats = UserStats(
                        user_id=user_id,
                        total_analyses=1,
                        last_activity=datetime.utcnow(),
                        created_at=datetime.utcnow(),
                        updated_at=datetime.utcnow()
                    )
                    session.add(stats)
                else:
                    stats.total_analyses += 1
                    stats.last_activity = datetime.utcnow()
                    stats.updated_at = datetime.utcnow()
                
                await session.commit()
                logger.info(f"✅ Анализ сохранен (SQLAlchemy): user_id={user_id}, group_id={group_id}")
                return True
                
        except SQLAlchemyError as e:
            logger.error(f"❌ Ошибка SQLAlchemy при сохранении: {e}")
            return False
    
    async def get_user_stats(self, user_id: int) -> Dict[str, Any]:
        """Получает статистику пользователя"""
        try:
            if self.db_type == 'postgresql' and self.pool:
                return await self._get_user_stats_postgresql(user_id)
            else:
                return await self._get_user_stats_sqlalchemy(user_id)
                
        except Exception as e:
            logger.error(f"❌ Ошибка получения статистики: {e}")
            return {
                'total_analyses': 0,
                'saved_reports': 0,
                'last_analyses': []
            }
    
    async def _get_user_stats_postgresql(self, user_id: int) -> Dict[str, Any]:
        """Получение статистики из PostgreSQL"""
        try:
            async with self.pool.acquire() as conn:
                # Получаем статистику пользователя
                stats = await conn.fetchrow("""
                    SELECT total_analyses, saved_reports
                    FROM user_stats
                    WHERE user_id = $1
                """, user_id)
                
                if not stats:
                    return {
                        'total_analyses': 0,
                        'saved_reports': 0,
                        'last_analyses': []
                    }
                
                # Получаем последние анализы
                analyses = await conn.fetch("""
                    SELECT group_name, created_at, group_id
                    FROM analyses
                    WHERE user_id = $1
                    ORDER BY created_at DESC
                    LIMIT 5
                """, user_id)
                
                return {
                    'total_analyses': stats['total_analyses'],
                    'saved_reports': stats['saved_reports'],
                    'last_analyses': [
                        {
                            'group_name': a['group_name'],
                            'created_at': a['created_at'].strftime('%d.%m.%Y %H:%M'),
                            'group_id': a['group_id']
                        } for a in analyses
                    ]
                }
                
        except Exception as e:
            logger.error(f"Ошибка получения статистики PostgreSQL: {e}")
            return {
                'total_analyses': 0,
                'saved_reports': 0,
                'last_analyses': []
            }
    
    async def _get_user_stats_sqlalchemy(self, user_id: int) -> Dict[str, Any]:
        """Получение статистики через SQLAlchemy"""
        try:
            async with self.async_session() as session:
                # Получаем статистику
                stats = await session.get(UserStats, user_id)
                
                if not stats:
                    return {
                        'total_analyses': 0,
                        'saved_reports': 0,
                        'last_analyses': []
                    }
                
                # Получаем последние анализы
                query = select(Analysis).where(
                    Analysis.user_id == user_id
                ).order_by(
                    Analysis.created_at.desc()
                ).limit(5)
                
                result = await session.execute(query)
                analyses = result.scalars().all()
                
                return {
                    'total_analyses': stats.total_analyses,
                    'saved_reports': stats.saved_reports,
                    'last_analyses': [
                        {
                            'group_name': a.group_name,
                            'created_at': a.created_at.strftime('%d.%m.%Y %H:%M'),
                            'group_id': a.group_id
                        } for a in analyses
                    ]
                }
                
        except Exception as e:
            logger.error(f"Ошибка получения статистики SQLAlchemy: {e}")
            return {
                'total_analyses': 0,
                'saved_reports': 0,
                'last_analyses': []
            }
    
    async def get_analyses_count(self, user_id: int) -> int:
        """Получает количество анализов пользователя"""
        try:
            if self.db_type == 'postgresql' and self.pool:
                async with self.pool.acquire() as conn:
                    count = await conn.fetchval("""
                        SELECT COUNT(*) FROM analyses WHERE user_id = $1
                    """, user_id)
                    return count or 0
            else:
                async with self.async_session() as session:
                    query = select(func.count(Analysis.id)).where(Analysis.user_id == user_id)
                    result = await session.execute(query)
                    return result.scalar() or 0
        except Exception as e:
            logger.error(f"Ошибка получения количества анализов: {e}")
            return 0
    
    async def get_recent_analyses(self, user_id: int, limit: int = 10) -> List[Dict[str, Any]]:
        """Получает последние анализы пользователя"""
        try:
            if self.db_type == 'postgresql' and self.pool:
                async with self.pool.acquire() as conn:
                    rows = await conn.fetch("""
                        SELECT id, group_id, group_name, created_at, analysis_data IS NOT NULL as has_data
                        FROM analyses
                        WHERE user_id = $1
                        ORDER BY created_at DESC
                        LIMIT $2
                    """, user_id, limit)
                    
                    return [
                        {
                            'id': r['id'],
                            'group_id': r['group_id'],
                            'group_name': r['group_name'],
                            'created_at': r['created_at'].isoformat() if r['created_at'] else None,
                            'has_data': r['has_data']
                        } for r in rows
                    ]
            else:
                async with self.async_session() as session:
                    query = select(Analysis).where(
                        Analysis.user_id == user_id
                    ).order_by(
                        Analysis.created_at.desc()
                    ).limit(limit)
                    
                    result = await session.execute(query)
                    analyses = result.scalars().all()
                    
                    return [
                        {
                            'id': a.id,
                            'group_id': a.group_id,
                            'group_name': a.group_name,
                            'created_at': a.created_at.isoformat() if a.created_at else None,
                            'has_data': bool(a.analysis_data)
                        } for a in analyses
                    ]
        except Exception as e:
            logger.error(f"Ошибка получения последних анализов: {e}")
            return []
    
    async def get_analysis_by_id(self, analysis_id: int, user_id: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """Получает анализ по ID"""
        try:
            if self.db_type == 'postgresql' and self.pool:
                async with self.pool.acquire() as conn:
                    query = """
                        SELECT id, user_id, group_id, group_name, analysis_data, created_at
                        FROM analyses
                        WHERE id = $1
                    """
                    params = [analysis_id]
                    
                    if user_id:
                        query += " AND user_id = $2"
                        params.append(user_id)
                    
                    row = await conn.fetchrow(query, *params)
                    
                    if row:
                        return {
                            'id': row['id'],
                            'user_id': row['user_id'],
                            'group_id': row['group_id'],
                            'group_name': row['group_name'],
                            'analysis_data': row['analysis_data'],
                            'created_at': row['created_at'].isoformat() if row['created_at'] else None
                        }
            else:
                async with self.async_session() as session:
                    query = select(Analysis).where(Analysis.id == analysis_id)
                    if user_id:
                        query = query.where(Analysis.user_id == user_id)
                    
                    result = await session.execute(query)
                    analysis = result.scalar_one_or_none()
                    
                    if analysis:
                        return analysis.to_dict()
            
            return None
            
        except Exception as e:
            logger.error(f"Ошибка получения анализа по ID: {e}")
            return None
    
    async def search_analyses(self, user_id: int, search_term: str, limit: int = 20) -> List[Dict[str, Any]]:
        """Ищет анализы по названию группы"""
        try:
            if self.db_type == 'postgresql' and self.pool:
                async with self.pool.acquire() as conn:
                    rows = await conn.fetch("""
                        SELECT id, group_name, group_id, created_at
                        FROM analyses
                        WHERE user_id = $1 AND group_name ILIKE $2
                        ORDER BY created_at DESC
                        LIMIT $3
                    """, user_id, f"%{search_term}%", limit)
                    
                    return [
                        {
                            'id': r['id'],
                            'group_name': r['group_name'],
                            'group_id': r['group_id'],
                            'created_at': r['created_at'].isoformat() if r['created_at'] else None
                        } for r in rows
                    ]
            else:
                async with self.async_session() as session:
                    query = select(Analysis).where(
                        Analysis.user_id == user_id,
                        Analysis.group_name.ilike(f"%{search_term}%")
                    ).order_by(
                        Analysis.created_at.desc()
                    ).limit(limit)
                    
                    result = await session.execute(query)
                    analyses = result.scalars().all()
                    
                    return [
                        {
                            'id': a.id,
                            'group_name': a.group_name,
                            'group_id': a.group_id,
                            'created_at': a.created_at.isoformat() if a.created_at else None
                        } for a in analyses
                    ]
        except Exception as e:
            logger.error(f"Ошибка поиска анализов: {e}")
            return []
    
    async def delete_analysis(self, analysis_id: int, user_id: int) -> bool:
        """Удаляет анализ"""
        try:
            if self.db_type == 'postgresql' and self.pool:
                async with self.pool.acquire() as conn:
                    async with conn.transaction():
                        # Удаляем анализ
                        result = await conn.execute("""
                            DELETE FROM analyses
                            WHERE id = $1 AND user_id = $2
                        """, analysis_id, user_id)
                        
                        # Если удаление успешно, обновляем статистику
                        if result == "DELETE 1":
                            await conn.execute("""
                                UPDATE user_stats
                                SET total_analyses = GREATEST(0, total_analyses - 1),
                                    updated_at = CURRENT_TIMESTAMP
                                WHERE user_id = $1
                            """, user_id)
                            
                            logger.info(f"Анализ {analysis_id} удален пользователем {user_id}")
                            return True
            else:
                async with self.async_session() as session:
                    query = select(Analysis).where(
                        Analysis.id == analysis_id,
                        Analysis.user_id == user_id
                    )
                    
                    result = await session.execute(query)
                    analysis = result.scalar_one_or_none()
                    
                    if analysis:
                        await session.delete(analysis)
                        
                        # Обновляем статистику
                        stats = await session.get(UserStats, user_id)
                        if stats and stats.total_analyses > 0:
                            stats.total_analyses -= 1
                            stats.updated_at = datetime.utcnow()
                        
                        await session.commit()
                        logger.info(f"Анализ {analysis_id} удален пользователем {user_id}")
                        return True
            
            return False
            
        except Exception as e:
            logger.error(f"Ошибка удаления анализа: {e}")
            return False
    
    async def cleanup_old_data(self, days: int = 30) -> int:
        """Очищает старые данные"""
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=days)
            
            if self.db_type == 'postgresql' and self.pool:
                async with self.pool.acquire() as conn:
                    # Удаляем старые анализы
                    result = await conn.execute("""
                        DELETE FROM analyses 
                        WHERE created_at < $1
                    """, cutoff_date)
                    
                    deleted_count = int(result.split()[-1])
                    
                    if deleted_count > 0:
                        logger.info(f"Удалено {deleted_count} старых анализов")
                    
                    return deleted_count
            else:
                async with self.async_session() as session:
                    # Для SQLite - удаляем через SQLAlchemy
                    query = select(Analysis).where(Analysis.created_at < cutoff_date)
                    result = await session.execute(query)
                    old_analyses = result.scalars().all()
                    
                    deleted_count = 0
                    for analysis in old_analyses:
                        await session.delete(analysis)
                        deleted_count += 1
                    
                    if deleted_count > 0:
                        await session.commit()
                        logger.info(f"Удалено {deleted_count} старых анализов")
                    
                    return deleted_count
                    
        except Exception as e:
            logger.error(f"Ошибка очистки старых данных: {e}")
            return 0
    
    async def check_health(self) -> Dict[str, Any]:
        """Проверяет здоровье базы данных"""
        try:
            if self.db_type == 'postgresql' and self.pool:
                async with self.pool.acquire() as conn:
                    # Проверяем соединение
                    await conn.execute("SELECT 1")
                    
                    # Получаем статистику
                    analyses_count = await conn.fetchval("SELECT COUNT(*) FROM analyses")
                    users_count = await conn.fetchval("SELECT COUNT(*) FROM user_stats")
                    
                    return {
                        'status': 'healthy',
                        'database_type': 'postgresql',
                        'analyses_count': analyses_count,
                        'users_count': users_count,
                        'timestamp': datetime.utcnow().isoformat()
                    }
            elif self.engine:
                async with self.async_session() as session:
                    await session.execute(select(1))
                    
                    analyses_count = await self.get_total_analyses_count()
                    users_count = await self.get_total_users_count()
                    
                    return {
                        'status': 'healthy',
                        'database_type': self.db_type or 'unknown',
                        'analyses_count': analyses_count,
                        'users_count': users_count,
                        'timestamp': datetime.utcnow().isoformat()
                    }
            else:
                return {
                    'status': 'unhealthy',
                    'database_type': 'unknown',
                    'error': 'База данных не инициализирована',
                    'timestamp': datetime.utcnow().isoformat()
                }
                
        except Exception as e:
            return {
                'status': 'unhealthy',
                'database_type': self.db_type or 'unknown',
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat()
            }
    
    async def get_total_analyses_count(self) -> int:
        """Получает общее количество анализов"""
        try:
            if self.db_type == 'postgresql' and self.pool:
                async with self.pool.acquire() as conn:
                    count = await conn.fetchval("SELECT COUNT(*) FROM analyses")
                    return count or 0
            else:
                async with self.async_session() as session:
                    query = select(func.count(Analysis.id))
                    result = await session.execute(query)
                    return result.scalar() or 0
        except Exception as e:
            logger.error(f"Ошибка получения общего количества анализов: {e}")
            return 0
    
    async def get_total_users_count(self) -> int:
        """Получает количество уникальных пользователей"""
        try:
            if self.db_type == 'postgresql' and self.pool:
                async with self.pool.acquire() as conn:
                    count = await conn.fetchval("SELECT COUNT(DISTINCT user_id) FROM analyses")
                    return count or 0
            else:
                async with self.async_session() as session:
                    query = select(func.count(func.distinct(Analysis.user_id)))
                    result = await session.execute(query)
                    return result.scalar() or 0
        except Exception as e:
            logger.error(f"Ошибка получения количества пользователей: {e}")
            return 0
    
    async def close(self):
        """Закрывает соединения с базой данных"""
        try:
            if self.pool:
                await self.pool.close()
                logger.info("✅ Пул соединений PostgreSQL закрыт")
            
            if self.engine:
                await self.engine.dispose()
                logger.info("✅ Соединения SQLAlchemy закрыты")
                
        except Exception as e:
            logger.error(f"Ошибка при закрытии соединений с БД: {e}")

# Глобальный экземпляр базы данных
db = Database()
