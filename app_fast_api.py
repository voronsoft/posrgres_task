"""
Этот файл эмулирует внешний API который запустит сервер с приложением фреймфорка FastAPI
В файле реализован роут по которому можно обращаться передав необходимый id - http://127.0.0.1:8000/docs
"""
import json
from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker


# Строка для подключения к БД test_a
engine_a = create_engine(f'postgresql://postgres:root@localhost:5432/test_a', echo=False)
Session_a = sessionmaker(bind=engine_a)

# Строка для подключения к БД test_b
engine_b = create_engine(f'postgresql://postgres:root@localhost:5432/test_b', echo=False)
Session_b = sessionmaker(bind=engine_b)

# Создаем приложение FastAPI
app = FastAPI()


# Эмуляция внешнего API
@app.post("/external_api")
async def external_api(id: int):
    try:
        # Создаем сессию для работы с БД test_a
        with Session_a() as session_a:
            # Вызываем хранимую процедуру для получения данных по ID
            result = session_a.execute(text('SELECT get_user_data_by_id(:id)'), {'id': id}).scalar()

            # Проверяем, найдены ли данные
            if result is not None:
                # Преобразуем словарь в строку JSON
                json_result = json.dumps(result)

                # Создаем сессию для работы с БД test_b
                with Session_b() as session_b:
                    # Записываем данные в таблицу data
                    session_b.execute(text('INSERT INTO data (data) VALUES (:json_result)'), {'json_result': json_result})
                    session_b.commit()

                return {"status": "success"}
            else:
                raise HTTPException(status_code=404, detail="Data not found")
    except Exception as e:
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# Что бы запустить приложение в терминале ввести - uvicorn app_fast_api:app --reload
# Что бы остановить сервер - Ctrl+C
