import json
import psycopg2
from models import Base, Users
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError
from sqlalchemy import create_engine, exc, inspect

# Замени 'user', 'password', 'dbname' на свои реальные данные
# если в консоли для вас мало информации то замените echo=False на echo=True
engine = create_engine(f'postgresql://postgres:root@localhost:5432/db_test1', echo=False)


def check_database_connection(db_name):
    """Проверка соединения с базой данных"""

    try:
        # Пытаемся установить соединение
        with engine.connect():
            print("----------------------------")
            print(f"Соединение с базой данных {db_name} успешно установлено.")
            print("----------------------------")
            return True
    except OperationalError as e:
        print(f"Не удалось установить соединение с базой данных {db_name}.\nОшибка: {e}")
        return False


def create_new_database(db_name):
    """Функция создания новой базы данных"""

    # Данные для соединения с сервером
    conn = psycopg2.connect(dbname="postgres", user="postgres", password="root", host="localhost")
    cursor = conn.cursor()

    with cursor:
        try:
            conn.autocommit = True
            # команда для создания базы данных metanit
            sql = f"CREATE DATABASE {db_name}"

            # выполняем код sql
            cursor.execute(sql)
            print("----------------------------")
            print("База данных успешно создана")
            print("----------------------------")
        except Exception as err:
            print(f'Ошибка при создании БД:\n {err}')


def inspect_table(table_name: str):
    """Функция для проверки существования таблицы в БД"""
    inspector = inspect(engine)
    table_exists = inspector.has_table(table_name)
    # Проверка
    if table_exists:
        return True
    else:
        return False


def create_table(table_class, db_connection):
    """Функция создания таблицы в базе данных"""
    if not inspect_table(table_class.__tablename__):
        try:
            # Создаем сессию для взаимодействия с базой данных
            Session = sessionmaker(bind=db_connection)

            with Session() as session:
                # Создаем таблицу, если ее еще нет
                table_class.__table__.create(db_connection, checkfirst=True)

                print("----------------------------")
                print(f"Таблица {table_class.__tablename__} успешно создана.")
                print("----------------------------")
        except exc.ProgrammingError as e:
            print(f"Не удалось создать таблицу {table_class.__tablename__}.\nОшибка: {e}")
    else:
        print("----------------------------")
        print(f"Таблица {table_class.__tablename__} уже существует в БД.")
        print("----------------------------")


def add_user(db_connection, user_name, user_email, user_description):
    """Добавление пользователя в таблицу Users"""
    try:
        # Проверка на уникальность email
        Session = sessionmaker(bind=db_connection)
        with Session() as session:
            existing_user = session.query(Users).filter_by(user_email=user_email).first()
            if existing_user:
                print("----------------------------")
                print(f"Пользователь с email {user_email} уже существует. Невозможно добавить пользователя.")
                print("----------------------------")
                return

            # Создаем объект пользователя
            new_user = Users(user_name=user_name, user_email=user_email, user_description=user_description)

            # Добавляем пользователя в сессию
            session.add(new_user)

            # Сохраняем изменения в базе данных
            session.commit()

        print("----------------------------")
        print(f"Пользователь {user_name} успешно добавлен.")
        print("----------------------------")
    except exc.SQLAlchemyError as e:
        print(f"Не удалось добавить пользователя {user_name}.\nОшибка: {e}")


def procedure_exists(cursor, procedure_name):
    """Проверка существования процедуры в БД"""
    cursor.execute("""
        SELECT proname
        FROM pg_proc
        WHERE proname = %s;
    """, (procedure_name,))
    return cursor.fetchone() is not None


def create_stored_procedure():
    """Функция создания и сохранения хранимой процедуры в БД"""

    # Замени 'user', 'password', 'dbname' на свои реальные данные
    db_connection = psycopg2.connect("dbname=db_test1 user=postgres password=root host=localhost port=5432")
    db_cursor = db_connection.cursor()

    # ===========================================
    PROCEDURE_NAME = 'get_user_by_id'  # Название процедуры
    # Используем подход, библиотеки psycopg2 для выполнения сложного SQL-запроса
    # Описание кода PROCEDURE_CODE:
    # Код получает на вход номер id далее происходит поиск поля с записью по id.
    # Если id найден получаем данные из поля, создаем json и отправляем как результат выборки
    PROCEDURE_CODE = """
    CREATE OR REPLACE FUNCTION get_user_by_id(input_id INTEGER)
    RETURNS JSON AS
    $$
    DECLARE
        result_data JSON;
    BEGIN
        -- Здесь делаешь запрос к таблице по переданному ID и присваиваешь результат переменной result_data
        SELECT to_json(u) INTO result_data
        FROM users u
        WHERE u.id = input_id;

        -- Возвращаешь JSON
        RETURN result_data;
    END;
    $$
    LANGUAGE plpgsql;
    """
    # ===========================================

    # Проверяем существование процедуры в БД
    if procedure_exists(db_cursor, PROCEDURE_NAME):
        print("----------------------------")
        print(f"Процедура {PROCEDURE_NAME} уже существует в базе данных.")
        print("----------------------------")
    else:
        try:
            # Выполняем SQL-код
            db_cursor.execute(PROCEDURE_CODE)
            db_connection.commit()

            print("----------------------------")
            print(f"Хранимая процедура {PROCEDURE_NAME} успешно создана.")
            print("----------------------------")
        except Exception as e:
            print(f"Ошибка при создании хранимой процедуры: {str(e)}")
        finally:
            db_cursor.close()
            db_connection.close()


def get_user_data_by_id(user_id):
    """Функция отправки id в БД и получение результата"""
    try:
        # Подключаемся к базе данных
        # Так же используем драйвер psycopg2 для работы с данными
        connection = psycopg2.connect("dbname=db_test1 user=postgres password=root host=localhost port=5432")
        cursor = connection.cursor()

        # Вызываем хранимую процедуру
        cursor.execute("SELECT get_user_by_id(%s);", (user_id,))
        result = cursor.fetchone()[0]  # Получаем результат в формате JSON

        # Закрываем соединение
        cursor.close()
        connection.close()

        # Вывод результата в консоль
        print("----------------------------")
        print(json.dumps(result, indent=2, ensure_ascii=False))  # Преобразуем JSON в удобочитаемый вид
        print("----------------------------")
        return result  # Возвращаем ответ
    except Exception as e:
        print(f"Ошибка при выполнении запроса: {str(e)}")
        return None


if __name__ == "__main__":
    new_bd = 'db_test1'  # Имя БД

    # 1 шаг Создаем БД
    create_new_database(new_bd)

    # 2 шаг Проверяем соединение с БД
    check_database_connection(new_bd)

    # 3 шаг Вызываем функцию для создания таблицы
    create_table(Users, engine)

    # 4 шаг Вызываем функцию для добавления пользователя
    add_user(engine, 'Саша Норов', 'nn@example.com', 'Какой то текст для описания')

    # 5 шаг Создаем хранимую процедуру в БД, вызываем функцию для создания хранимой процедуры
    create_stored_procedure()

    # 6 шаг Пример вызова функции с передачей id
    user_id_to_query = 2
    get_user_data_by_id(user_id_to_query)
