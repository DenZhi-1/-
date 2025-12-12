import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Telegram
    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
    ADMIN_IDS = [int(id.strip()) for id in os.getenv("ADMIN_IDS", "").split(",") if id.strip()]
    
    # VK API
    VK_SERVICE_TOKEN = os.getenv("VK_SERVICE_TOKEN", "")
    VK_API_VERSION = "5.199"
    
    # Database
    DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///database.db")
    
    # Redis
    REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    
    # Limits
    MAX_MEMBERS_PER_GROUP = 10000
    REQUEST_DELAY = 0.34  # ~3 запроса в секунду (ограничение VK)
    
    # Debug
    DEBUG = os.getenv("DEBUG", "false").lower() == "true"
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    
    @classmethod
    def validate(cls):
        """Проверка обязательных конфигурационных параметров"""
        errors = []
        
        if not cls.TELEGRAM_BOT_TOKEN:
            errors.append("TELEGRAM_BOT_TOKEN не установлен")
        if not cls.VK_SERVICE_TOKEN:
            errors.append("VK_SERVICE_TOKEN не установлен")
        if not cls.DATABASE_URL:
            errors.append("DATABASE_URL не установлен")
        
        if errors:
            error_msg = "Ошибки конфигурации:\n" + "\n".join(f"• {error}" for error in errors)
            raise ValueError(error_msg)
        
        # Предупреждения
        warnings = []
        if cls.DATABASE_URL.startswith("sqlite"):
            warnings.append("Используется SQLite база. Данные могут быть нестабильны в Railway.")
        if len(cls.ADMIN_IDS) == 0:
            warnings.append("ADMIN_IDS не установлен. Некоторые функции будут недоступны.")
        
        if warnings and cls.DEBUG:
            for warning in warnings:
                print(f"⚠️  Предупреждение: {warning}")

config = Config()
