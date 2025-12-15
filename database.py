import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, Integer, String, JSON, DateTime, select, text
import sqlalchemy

from config import config

logger = logging.getLogger(__name__)
Base = declarative_base()

class AnalysisResult(Base):
    __tablename__ = 'analyses'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, index=True)
    group_id = Column(String, index=True)  # String для VK ID
    group_name = Column(String)
    analysis_data = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    def __repr__(self):
        return f"<AnalysisResult(user_id={self.user_id}, group_id={self.group_id})>"

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
            
            logger.info("✅ База данных успешно инициализирована")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации базы данных: {e}")
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
        """
        Сохраняет результат анализа в базу данных
        
        Args:
            user_id: ID пользователя Telegram
            group_id: ID группы ВК (должен быть строкой)
            group_name: Название группы
            analysis: Данные анализа
            
        Returns:
            bool: Успешно ли сохранено
        """
        try:
            async with self.async_session() as session:
                # Убедимся, что group_id - строка
                group_id_str = str(group_id) if not isinstance(group_id, str) else group_id
                
                analysis_record = AnalysisResult(
                    user_id=user_id,
                    group_id=group_id_str,  # Всегда строка
                    group_name=group_name[:255],  # Ограничиваем длину
                    analysis_data=analysis,
                    created_at=datetime.utcnow()
                )
                session.add(analysis_record)
                
                # Обновляем статистику пользователя
                stats = await session.get(UserStats, user_id)
                if not stats:
                    stats = UserStats(user_id=user_id)
                    session.add(stats)
                
                stats.total_analyses += 1
                stats.last_activity = datetime.utcnow()
                
                await session.commit()
                logger.info(f"✅ Анализ сохранен: user_id={user_id}, group_id={group_id_str}")
                return True
                
        except sqlalchemy.exc.IntegrityError as e:
            logger.error(f"❌ Ошибка целостности данных при сохранении анализа: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения анализа: {e}", exc_info=True)
            
            # Пробуем альтернативный метод сохранения
            try:
                return await self._save_analysis_raw(user_id, group_id, group_name, analysis)
            except Exception as e2:
                logger.error(f"❌ Альтернативный метод также не сработал: {e2}")
                return False
    
    async def _save_analysis_raw(self, user_id: int, group_id: str, 
                                group_name: str, analysis: Dict[str, Any]) -> bool:
        """Альтернативный метод сохранения через сырой SQL"""
        try:
            async with self.async_session() as session:
                # Используем сырой SQL с явным приведением типов
                group_id_str = str(group_id) if not isinstance(group_id, str) else group_id
                
                # Для PostgreSQL используем явное приведение типов
                if "postgresql" in str(self.engine.url):
                    query = text("""
                        INSERT INTO analyses (user_id, group_id, group_name, analysis_data, created_at)
                        VALUES (:user_id, :group_id::VARCHAR, :group_name, :analysis_data, :created_at)
                    """)
                else:
                    # Для SQLite
                    query = text("""
                        INSERT INTO analyses (user_id, group_id, group_name, analysis_data, created_at)
                        VALUES (:user_id, :group_id, :group_name, :analysis_data, :created_at)
                    """)
                
                await session.execute(query, {
                    'user_id': user_id,
                    'group_id': group_id_str,
                    'group_name': group_name[:255],
                    'analysis_data': analysis,
                    'created_at': datetime.utcnow()
                })
                
                # Обновляем статистику
                stats_query = text("""
                    INSERT INTO user_stats (user_id, total_analyses, last_activity)
                    VALUES (:user_id, 1, :now)
                    ON CONFLICT (user_id) DO UPDATE SET
                    total_analyses = user_stats.total_analyses + 1,
                    last_activity = :now
                """)
                
                await session.execute(stats_query, {
                    'user_id': user_id,
                    'now': datetime.utcnow()
                })
                
                await session.commit()
                logger.info(f"✅ Анализ сохранен (raw SQL): user_id={user_id}, group_id={group_id_str}")
                return True
                
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения анализа (raw SQL): {e}", exc_info=True)
            return False
    
    async def get_user_stats(self, user_id: int) -> Dict[str, Any]:
        """Получает статистику пользователя"""
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
            logger.error(f"❌ Ошибка получения статистики: {e}")
            return {
                'total_analyses': 0,
                'saved_reports': 0,
                'last_analyses': []
            }
    
    async def get_analyses_count(self, user_id: int) -> int:
        """Получает количество анализов пользователя"""
        try:
            async with self.async_session() as session:
                query = select(sqlalchemy.func.count(AnalysisResult.id)).where(
                    AnalysisResult.user_id == user_id
                )
                result = await session.execute(query)
                return result.scalar() or 0
        except Exception as e:
            logger.error(f"Ошибка получения количества анализов: {e}")
            return 0
    
    async def get_recent_analyses(self, user_id: int, limit: int = 10) -> List[Dict[str, Any]]:
        """Получает последние анализы пользователя"""
        try:
            async with self.async_session() as session:
                query = select(AnalysisResult).where(
                    AnalysisResult.user_id == user_id
                ).order_by(
                    AnalysisResult.created_at.desc()
                ).limit(limit)
                
                result = await session.execute(query)
                analyses = result.scalars().all()
                
                return [
                    {
                        'id': a.id,
                        'group_id': a.group_id,
                        'group_name': a.group_name,
                        'created_at': a.created_at,
                        'has_data': bool(a.analysis_data)
                    } for a in analyses
                ]
        except Exception as e:
            logger.error(f"Ошибка получения последних анализов: {e}")
            return []
    
    async def get_analysis_by_id(self, analysis_id: int) -> Optional[AnalysisResult]:
        """Получает анализ по ID"""
        try:
            async with self.async_session() as session:
                query = select(AnalysisResult).where(AnalysisResult.id == analysis_id)
                result = await session.execute(query)
                return result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"Ошибка получения анализа по ID: {e}")
            return None
    
    async def close(self):
        """Закрывает соединения с базой данных"""
        try:
            if self.engine:
                await self.engine.dispose()
                logger.info("✅ Соединения с базой данных закрыты")
        except Exception as e:
            logger.error(f"Ошибка при закрытии соединений с БД: {e}")

# Создаем глобальный экземпляр
db = Database()
