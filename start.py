import requests
from test_plpython import get_all_user_ids


# Не забываем что файл - app_fast_api.py должен быть запущен на исполнение через консоль
# Что бы запустить приложение в терминале ввести: uvicorn app_fast_api:app --reload
# Что бы остановить сервер - Ctrl+C

# URL нашего FastAPI-приложения
api_url = "http://127.0.0.1:8000/external_api?id="

# Получаем все ID пользователей
all_users_ids = get_all_user_ids()
print(all_users_ids)

# Обрабатываем каждый ID и отправляем запросы к API
for user_id in all_users_ids:
    # Составляем полный URL для каждого ID
    full_api_url = f"{api_url}{user_id}"

    # Отправляем запрос к вашему API
    response = requests.post(full_api_url)

    # Выводим результат
    print(f"User ID: {user_id}, Response: {response.status_code}, {response.json()}")
