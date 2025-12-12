import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, Integer, String, JSON, DateTime, select
import sqlalchemy

from config import config

logger = logging.getLogger(__name__)
Base = declarative_base()

class AnalysisResult(Base):
    __tablename__ = 'analyses'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, index=True)
    group_id = Column(String)
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
    
    def _normalize_db_url(self, db_url: str) -> str:
        if not db_url:
            logger.warning("DATABASE_URL пустой, используется SQLite")
            return "sqlite+aiosqlite:///database.db"
        
        if db_url.startswith("postgres://"):
            db_url = db_url.replace("postgres://", "postgresql+asyncpg://", 1)
            logger.info("Конвертирован postgres:// в postgresql+asyncpg://")
        elif db_url.startswith("postgresql://"):
            db_url = db_url.replace("postgresql://", "postgresql+asyncpg://", 1)
        
        if "localhost" in db_url or "127.0.0.1" in db_url or "::1" in db_url:
            logger.warning(f"Обнаружен localhost в DATABASE_URL, используется SQLite")
            return "sqlite+aiosqlite:///database.db"
        
        return db_url
    
    async def init_db(self) -> bool:
        try:
            db_url = self._normalize_db_url(config.DATABASE_URL)
            logger.info(f"Инициализация БД с URL: {db_url[:50]}...")
            
            self.engine = create_async_engine(
                db_url,
                echo=False,
                pool_size=10,
                max_overflow=20,
                pool_pre_ping=True
            )
            
            self.async_session = sessionmaker(
                self.engine,
                class_=AsyncSession,
                expire_on_commit=False
            )
            
            async with self.engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            
            async with self.async_session() as session:
                await session.execute(select(1))
            
            logger.info("База данных успешно инициализирована")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка инициализации базы данных: {e}")
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
            
            return False
    
    async def save_analysis(self, user_id: int, group_id: str, 
                           group_name: str, analysis: Dict[str, Any]) -> bool:
        try:
            async with self.async_session() as session:
                analysis_record = AnalysisResult(
                    user_id=user_id,
                    group_id=group_id,
                    group_name=group_name,
                    analysis_data=analysis
                )
                session.add(analysis_record)
                
                stats = await session.get(UserStats, user_id)
                if not stats:
                    stats = UserStats(user_id=user_id)
                    session.add(stats)
                
                stats.total_analyses += 1
                stats.last_activity = datetime.utcnow()
                
                await session.commit()
                return True
                
        except Exception as e:
            logger.error(f"Ошибка сохранения анализа: {e}")
            return False
    
    async def get_user_stats(self, user_id: int) -> Dict[str, Any]:
        try:
            async with self.async_session() as session:
                stats = await session.get(UserStats, user_id)
                
                if not stats:
                    return {
                        'total_analyses': 0,
                        'saved_reports': 0,
                        'last_analyses': []
                    }
                
                query = select(AnalysisResult).where(
                    AnalysisResult.user_id == user_id
                ).order_by(
                    AnalysisResult.created_at.desc()
                ).limit(3)
                
                result = await session.execute(query)
                last_analyses = result.scalars().all()
                
                return {
                    'total_analyses': stats.total_analyses,
                    'saved_reports': stats.saved_reports,
                    'last_analyses': [
                        {
                            'group_name': a.group_name,
                            'created_at': a.created_at.strftime('%d.%m.%Y %H:%M')
                        } for a in last_analyses
                    ]
                }
                
        except Exception as e:
            logger.error(f"Ошибка получения статистики: {e}")
            return {'total_analyses': 0, 'saved_reports': 0, 'last_analyses': []}
    
    async def close(self):
        if self.engine:
            await self.engine.dispose()
            logger.info("Соединения с базой данных закрыты")
