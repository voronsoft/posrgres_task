from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String

# Создаем базовый класс
Base = declarative_base()


# Модель таблицы - Команды
class Users(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_name = Column(String, nullable=False)
    user_email = Column(String, nullable=False)
    user_description = Column(String, nullable=False)
