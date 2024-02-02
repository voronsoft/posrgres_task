import psycopg2
from sqlalchemy.orm import sessionmaker, Session
from models import Users, Data  # Импорт класса таблицы (sqlalchemy)

from sqlalchemy import create_engine, inspect, text


# Замени 'user', 'password', 'dbname' на свои реальные данные
# если в консоли для вас мало информации то замените echo=False на echo=True
# engine_table_db_test_a = create_engine(f'postgresql://postgres:root@localhost:5432/{db_name}', echo=False)


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
            print(f"База данных {db_name} успешно создана")
            print("----------------------------")
        except Exception as err:
            print(f'Ошибка при создании БД:\n {err}')


def inspect_table(table_name: str, engine_conn):
    """Функция для проверки существования таблицы в БД"""
    inspector = inspect(engine_conn)
    table_exists = inspector.has_table(table_name)
    # Проверка
    if table_exists:
        return True
    else:
        return False


def create_table(table_class, db_connection):
    """Функция создания таблицы в базе данных"""
    if not inspect_table(table_class.__tablename__, db_connection):
        try:
            # Создаем сессию для взаимодействия с базой данных
            Session = sessionmaker(bind=db_connection)

            with Session() as session:
                # Создаем таблицу, если ее еще нет
                table_class.__table__.create(db_connection, checkfirst=True)
                print("----------------------------")
                print(f"Таблица {table_class.__tablename__} успешно создана. В БД ({str(db_connection.__dict__['url'])[str(db_connection.__dict__['url']).rfind('/') + 1:]})")
                print("----------------------------")
        except Exception as e:
            print(f"Не удалось создать таблицу {table_class.__tablename__}.\nОшибка: {e}")
    else:
        print("----------------------------")
        print(f"Таблица {table_class.__tablename__} уже существует в БД ({str(db_connection.__dict__['url'])[str(db_connection.__dict__['url']).rfind('/') + 1:]})")
        print("----------------------------")


def add_data_db(db_connection, user_name, user_email, user_description, user_data):
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
            new_user = Users(user_name=user_name,
                             user_email=user_email,
                             user_description=user_description,
                             user_data=user_data)

            # Добавляем пользователя в сессию
            session.add(new_user)

            # Сохраняем изменения в базе данных
            session.commit()

        print("----------------------------")
        print(f"Пользователь {user_name} успешно добавлен.")
        print("----------------------------")
    except Exception as e:
        session.rollback()  # Откатываем изменения если произошла ошибка
        print(f"Не удалось добавить пользователя {user_name} в БД.\nОшибка: {e}")


def get_all_user_ids():
    """Функция для получения всех ID из таблицы users из БД test_a"""
    # Строка для подключения к БД
    engine = create_engine(f'postgresql://postgres:root@localhost:5432/test_a', echo=False)
    # Создаем сессию
    Session = sessionmaker(bind=engine)

    try:
        # Создаем сессию с использованием менеджера контекста
        with Session() as session:
            # Используем сессию для выполнения запроса и получения всех ID
            ids = session.query(Users.id).all()
            all_id = [i for (i,) in ids]
            # print('Список ID ', all_id)
            return all_id
    except Exception as e:
        print(f"Произошла ошибка при получении id из бд: {e}")
        return []


def create_stored_procedure():
    """Функция для создания хранимой процедуры"""
    # Строка для подключения к БД
    engine = create_engine(f'postgresql://postgres:root@localhost:5432/test_a', echo=False)
    # Создаем сессию
    Session = sessionmaker(bind=engine)

    try:
        # Создаем сессию с использованием менеджера контекста
        with Session() as session:
            # SQL-запрос для создания хранимой процедуры - get_user_data_by_id
            # Описание хранимой процедуры:
            # Функция получает id и возвращает результат в json формате, если такое поле с id было найдено
            create_procedure_query = """
            CREATE OR REPLACE FUNCTION get_user_data_by_id(input_id INTEGER)
            RETURNS JSON AS $$
            DECLARE
                result_data JSON;
            BEGIN
                SELECT json_build_object(
                    'user_name', user_name,
                    'user_email', user_email,
                    'user_description', user_description,
                    'user_data', user_data
                ) INTO result_data
                FROM users
                WHERE id = input_id;

                RETURN result_data;
            END;
            $$ LANGUAGE plpgsql;
            """

            # Выполняем SQL-запрос
            session.execute(text(create_procedure_query))
            # Подтверждаем изменения в базе данных
            session.commit()
            print(f'Хранимая процедура - get_user_data_by_id - создана в бд test_a')

    except Exception as e:
        session.rollback()  # Откатываем изменения если что-то пошло не так
        print(f"Error: {e}")


if __name__ == "__main__":
    # 1 Создаем БД
    db_names = "test_a"
    create_new_database(db_names)

    db_names = "test_b"
    # 2  Создаем БД
    create_new_database(db_names)

    # 3 Создаем движок для соединения с БД test_a
    engine = create_engine(f'postgresql://postgres:root@localhost:5432/test_a', echo=False)
    # Вызываем функцию для создания таблицы для бд - test_a
    create_table(Users, engine)
    # 4 Добавляем данные в таблицу для примера
    add_data_db(engine, 'Саша', '1nn@example.com', 'Какой то текст для описания', 'Какие-то данные')
    add_data_db(engine, 'Петя', '2nn@example.com', 'Какой то текст для описания', 'Какие-то данные')
    add_data_db(engine, 'Додо', '3nn@example.com', 'Какой то текст для описания', 'Какие-то данные')
    add_data_db(engine, 'Кука', '4nn@example.com', 'Какой то текст для описания', 'Какие-то данные')
    add_data_db(engine, 'Така', '5nn@example.com', 'Какой то текст для описания', 'Какие-то данные')

    # Создаем движок для соединения с БД test_b
    engine = create_engine(f'postgresql://postgres:root@localhost:5432/test_b', echo=False)
    # 5 Вызываем функцию для создания таблицы для бд - test_b
    create_table(Data, engine)

    # 6 Получаем все id из бд test_a/users
    all_users_ids = get_all_user_ids()

    # 7 Вызываем функцию для создания хранимой процедуры(get_user_data_by_id) в БД test_a
    create_stored_procedure()
