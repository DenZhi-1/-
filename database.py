import logging
import json
from datetime import datetime
from typing import Dict, List, Optional, Any
import asyncpg
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, Integer, String, JSON, DateTime, select, text
import sqlalchemy

from config import config

logger = logging.getLogger(__name__)

# SQLAlchemy модели (для совместимости)
Base = declarative_base()

class AnalysisResult(Base):
    __tablename__ = 'analyses'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, index=True)
    group_id = Column(String, index=True)
    group_name = Column(String)
    analysis_data = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)

class UserStats(Base):
    __tablename__ = 'user_stats'
    
    user_id = Column(Integer, primary_key=True)
    total_analyses = Column(Integer, default=0)
    saved_reports = Column(Integer, default=0)
    last_activity = Column(DateTime, default=datetime.utcnow)

class Database:
    def __init__(self):
        self.engine = None
        self.async_session = None
        self.pool = None  # Пул соединений asyncpg
        self.db_url = None
    
    def _normalize_db_url(self, db_url: str) -> str:
        """Нормализация URL базы данных"""
        if not db_url:
            logger.warning("DATABASE_URL пустой, используется SQLite")
            return "sqlite+aiosqlite:///database.db"
        
        if db_url.startswith("postgres://"):
            db_url = db_url.replace("postgres://", "postgresql+asyncpg://", 1)
            logger.info("Конвертирован postgres:// в postgresql+asyncpg://")
        
        self.db_url = db_url
        return db_url
    
    async def init_db(self) -> bool:
        """Инициализация базы данных"""
        try:
            db_url = self._normalize_db_url(config.DATABASE_URL)
            logger.info(f"Инициализация БД с URL: {db_url[:50]}...")
            
            # Для PostgreSQL создаем пул соединений asyncpg
            if "postgresql" in db_url:
                # Извлекаем DSN для asyncpg
                dsn = db_url.replace("postgresql+asyncpg://", "postgresql://")
                
                # Создаем пул соединений
                self.pool = await asyncpg.create_pool(
                    dsn=dsn,
                    min_size=1,
                    max_size=10,
                    command_timeout=60
                )
                
                # Проверяем соединение
                async with self.pool.acquire() as conn:
                    await conn.execute("SELECT 1")
                
                logger.info("✅ Пул соединений PostgreSQL создан")
                
                # Также инициализируем SQLAlchemy для совместимости
                self.engine = create_async_engine(db_url, echo=False)
                self.async_session = sessionmaker(
                    self.engine, 
                    class_=AsyncSession,
                    expire_on_commit=False
                )
                
                # Создаем таблицы через SQLAlchemy
                async with self.engine.begin() as conn:
                    await conn.run_sync(Base.metadata.create_all)
                
                logger.info("✅ База данных PostgreSQL инициализирована")
                return True
                
            else:
                # SQLite
                self.engine = create_async_engine(db_url, echo=False)
                self.async_session = sessionmaker(
                    self.engine, 
                    class_=AsyncSession,
                    expire_on_commit=False
                )
                
                async with self.engine.begin() as conn:
                    await conn.run_sync(Base.metadata.create_all)
                
                logger.info("✅ SQLite база данных инициализирована")
                return True
                
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации базы данных: {e}")
            
            # Fallback на in-memory SQLite
            try:
                logger.info("Создается in-memory SQLite база")
                self.engine = create_async_engine(
                    "sqlite+aiosqlite:///:memory:",
                    echo=False
                )
                self.async_session = sessionmaker(
                    self.engine,
                    class_=AsyncSession,
                    expire_on_commit=False
                )
                
                async with self.engine.begin() as conn:
                    await conn.run_sync(Base.metadata.create_all)
                
                logger.info("✅ In-memory SQLite база создана")
                return True
            except Exception as sqlite_error:
                logger.error(f"❌ Ошибка создания SQLite базы: {sqlite_error}")
                return False
    
    async def save_analysis(self, user_id: int, group_id: int, 
                           group_name: str, analysis: Dict[str, Any]) -> bool:
        """
        Сохраняет результат анализа (основной метод)
        
        ВАЖНО: group_id принимается как int, но сохраняется как VARCHAR
        """
        try:
            # Преобразуем group_id в строку
            group_id_str = str(group_id)
            
            # Если есть пул PostgreSQL, используем его
            if self.pool:
                return await self._save_analysis_postgresql(
                    user_id, group_id_str, group_name, analysis
                )
            else:
                # Используем SQLAlchemy
                return await self._save_analysis_sqlalchemy(
                    user_id, group_id_str, group_name, analysis
                )
                
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения анализа: {e}", exc_info=True)
            return False
    
    async def _save_analysis_postgresql(self, user_id: int, group_id: str, 
                                       group_name: str, analysis: Dict[str, Any]) -> bool:
        """Сохранение через asyncpg (PostgreSQL)"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    # Сохраняем анализ
                    await conn.execute("""
                        INSERT INTO analyses (user_id, group_id, group_name, analysis_data, created_at)
                        VALUES ($1, $2, $3, $4, $5)
                    """, user_id, group_id, group_name[:255], json.dumps(analysis), datetime.utcnow())
                    
                    # Обновляем статистику
                    await conn.execute("""
                        INSERT INTO user_stats (user_id, total_analyses, last_activity)
                        VALUES ($1, 1, $2)
                        ON CONFLICT (user_id) DO UPDATE SET
                        total_analyses = user_stats.total_analyses + 1,
                        last_activity = $2
                    """, user_id, datetime.utcnow())
                
                logger.info(f"✅ Анализ сохранен (PostgreSQL): user_id={user_id}, group_id={group_id}")
                return True
                
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения в PostgreSQL: {e}", exc_info=True)
            return False
    
    async def _save_analysis_sqlalchemy(self, user_id: int, group_id: str, 
                                       group_name: str, analysis: Dict[str, Any]) -> bool:
        """Сохранение через SQLAlchemy (SQLite)"""
        try:
            async with self.async_session() as session:
                analysis_record = AnalysisResult(
                    user_id=user_id,
                    group_id=group_id,
                    group_name=group_name[:255],
                    analysis_data=analysis,
                    created_at=datetime.utcnow()
                )
                session.add(analysis_record)
                
                # Обновляем статистику
                stats = await session.get(UserStats, user_id)
                if not stats:
                    stats = UserStats(user_id=user_id)
                    session.add(stats)
                
                stats.total_analyses += 1
                stats.last_activity = datetime.utcnow()
                
                await session.commit()
                logger.info(f"✅ Анализ сохранен (SQLAlchemy): user_id={user_id}, group_id={group_id}")
                return True
                
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения через SQLAlchemy: {e}", exc_info=True)
            return False
    
    async def get_user_stats(self, user_id: int) -> Dict[str, Any]:
        """Получает статистику пользователя"""
        try:
            if self.pool:
                # PostgreSQL через asyncpg
                return await self._get_user_stats_postgresql(user_id)
            else:
                # SQLite через SQLAlchemy
                return await self._get_user_stats_sqlalchemy(user_id)
                
        except Exception as e:
            logger.error(f"❌ Ошибка получения статистики: {e}")
            return {
                'total_analyses': 0,
                'saved_reports': 0,
                'last_analyses': []
            }
    
    async def _get_user_stats_postgresql(self, user_id: int) -> Dict[str, Any]:
        """Получение статистики через asyncpg"""
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
                last_analyses = await conn.fetch("""
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
                        } for a in last_analyses
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
                stats = await session.get(UserStats, user_id)
                
                if not stats:
                    return {
                        'total_analyses': 0,
                        'saved_reports': 0,
                        'last_analyses': []
                    }
                
                # Получаем последние анализы
                query = select(AnalysisResult).where(
                    AnalysisResult.user_id == user_id
                ).order_by(
                    AnalysisResult.created_at.desc()
                ).limit(5)
                
                result = await session.execute(query)
                last_analyses = result.scalars().all()
                
                return {
                    'total_analyses': stats.total_analyses,
                    'saved_reports': stats.saved_reports,
                    'last_analyses': [
                        {
                            'group_name': a.group_name,
                            'created_at': a.created_at.strftime('%d.%m.%Y %H:%M'),
                            'group_id': a.group_id
                        } for a in last_analyses
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
            if self.pool:
                async with self.pool.acquire() as conn:
                    count = await conn.fetchval("""
                        SELECT COUNT(*) FROM analyses WHERE user_id = $1
                    """, user_id)
                    return count or 0
            else:
                async with self.async_session() as session:
                    query = select(sqlalchemy.func.count(AnalysisResult.id)).where(
                        AnalysisResult.user_id == user_id
                    )
                    result = await session.execute(query)
                    return result.scalar() or 0
        except Exception as e:
            logger.error(f"Ошибка получения количества анализов: {e}")
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

# Создаем глобальный экземпляр
db = Database()
