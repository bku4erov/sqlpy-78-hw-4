from typing import Iterable

import psycopg2
from psycopg2 import sql


class ClientDatabase:
    
    # соответствие между полями для поиска клиентов и именами полей таблиц БД
    fields_to_search = {
        'имя': 'first_name',
        'фамилия': 'last_name',
        'email': 'email',
        'телефон': 'phone'
    }
    
    def __init__(self, database, user, password, host) -> None:
        self.db_conn_properies = {
            'database': database,
            'user': user,
            'password': password,
            'host': host
        }
        self.conn = psycopg2.connect(**self.db_conn_properies)
    

    def __del__(self):
        self.conn.close()
    

    # Функция, создающая структуру БД (таблицы)
    def create_db_schema(self):
        with self.conn.cursor() as cur:
            # создаить таблицу, содержащую сведения о клиентах
            cur.execute("""
                CREATE TABLE IF NOT EXISTS client (
                    client_id SERIAL PRIMARY KEY,
                    first_name varchar(100),
                    last_name varchar(100),
                    email varchar(100),
                    constraint email_regexp check (email ~ '^[\w\-\.]+\@[\w\-\.]+\.[\w]+$')
                );
            """)

            # создать таблицу, содержащую сведения о телефонах клиентов
            cur.execute("""
                CREATE TABLE IF NOT EXISTS client_phone (
                    client_phone_id SERIAL PRIMARY KEY,
                    client_id integer references client(client_id),
                    phone varchar(50),
                    constraint phone_regexp check (phone ~ '^[+]{0,1}\d{0,1}[(]{0,1}\d{1,4}[)]{0,1}[-\s\./\d]*$')
                );
            """)

            self.conn.commit()


    def _insert_client_phone(self, cur, client_id, phone):
        try:
            cur.execute("""
                    INSERT INTO client_phone (client_id, phone)
                    VALUES (%s, %s);
                """,
                (client_id, phone)
            )
        except psycopg2.errors.CheckViolation:
            print('Указан некорретный номер телефона. Телефон(ы) не добавлен(ы).')
            self.conn.rollback()


    # Функция, позволяющая добавить нового клиента
    def add_client(self, first_name, last_name, email=None, phone_list=None):
        with self.conn.cursor() as cur:
            # вставить в БД сведения о новом клиенте
            try:
                cur.execute("""
                        INSERT INTO client (first_name, last_name, email)
                        VALUES (%s, %s, %s)
                        RETURNING client_id;
                    """,
                    (first_name, last_name, email)
                )
                # получить ID вновь добавленного клиента
                new_client_id = cur.fetchone()[0]
                self.conn.commit()
            except psycopg2.errors.CheckViolation:
                print('Указан некорретный email. Клиент не добавлен.')
                self.conn.rollback()
                return
            
            # если также получен список телефонов клиента - вставить эти сведения в БД
            if phone_list is not None and isinstance(phone_list, Iterable):
                for phone in phone_list:
                    self._insert_client_phone(cur, new_client_id, phone)
                self.conn.commit()
            
            return new_client_id


    # Функция, позволяющая добавить телефон для существующего клиента
    def add_client_phone(self, client_id, phone):
        with self.conn.cursor() as cur:
            self._insert_client_phone(cur, client_id, phone)
        self.conn.commit()
    

    # Функция, позволяющая изменить данные о клиенте
    def update_client(self, client_id, new_first_name, new_last_name, new_email):
        with self.conn.cursor() as cur:
            # обновить в БД сведения о заданном клиенте
            cur.execute("""
                    UPDATE client
                    SET
                        first_name = %(first_name)s,
                        last_name = %(last_name)s,
                        email = %(email)s
                    WHERE client_id = %(client_id)s;
                """,
                {
                    'client_id': client_id,
                    'first_name': new_first_name,
                    'last_name': new_last_name,
                    'email': new_email
                }
            )
            self.conn.commit()
    

    # Функция, позволяющая удалить телефон для существующего клиента
    def del_client_phone(self, phone_id):
        with self.conn.cursor() as cur:
            # удалить телефон клиента по ID телефона
            cur.execute("""
                    DELETE FROM client_phone
                    WHERE client_phone_id = %s;
                """,
                (phone_id,)
            )
            self.conn.commit()
    

    # Функция, позволяющая удалить существующего клиента
    def del_client(self, client_id):
        with self.conn.cursor() as cur:
            # удалить все телефоны клиента по ID клиента
            cur.execute("""
                    DELETE FROM client_phone
                    WHERE client_id = %s;
                """,
                (client_id,)
            )
            # удалить сведения о клиенте
            cur.execute("""
                    DELETE FROM client
                    WHERE client_id = %s;
                """,
                (client_id,)
            )
            self.conn.commit()
    

    # Функция, позволяющая найти клиента по его данным: имени, фамилии, email или телефону
    def find_client(self, field_name:str, field_value):
        try:
            db_filed_name = ClientDatabase.fields_to_search[field_name.lower()]
        except KeyError:            
            print('Поиск возможен по одному из следующих полей: имя, фамилия, email, телефон. Пожалуйста, укажите корректное поле для поиска.')
            return

        with self.conn.cursor() as cur:
            cur.execute(
                sql.SQL("""
                    SELECT DISTINCT c.client_id, c.first_name, c.last_name, c.email
                    FROM client c LEFT JOIN client_phone cp ON c.client_id = cp.client_id 
                    WHERE {} = %s;
                """).format(sql.Identifier(db_filed_name)),
                (field_value,)
            )
            return cur.fetchall()
    

    # Вывод сведений о всех клиентах и их телефонах
    def print_all_clients(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT c.client_id, c.first_name, c.last_name, c.email, string_agg(cp.phone, ', ') phones
                FROM client c LEFT JOIN client_phone cp ON c.client_id = cp.client_id
                GROUP BY c.client_id, c.first_name, c.last_name, c.email;
            """)
            for client in cur:
                print(client)


if __name__ == '__main__':
    client_db = ClientDatabase('<database>', '<username>', '<user password>', '<host>')
    
    client_db.create_db_schema()
    
    # демонстрация работы функции, позволяющей добавить нового клиента
    client1_id = client_db.add_client('Иван', 'Иванов')
    client_db.add_client('Василий', 'Петров', 'petrov@mail.server.ru')
    client_db.add_client('Василий', 'Сидоров', 'v.sidorov@mail.com', ['+1-111-111-1111', '(495)000-00-00', '+0(000) 000 00 00'])
    print('Сведения о клиентах после добавления 3-х клиентов:')
    client_db.print_all_clients()
    print()

    # демонстрация работы функции, позволяющей добавить телефон для существующего клиента
    client_db.add_client_phone(client1_id, '1234567890')
    print('Сведения о клиентах после добавления телефона 1-ому клиенту:')
    client_db.print_all_clients()
    print()

    # демонстрация работы функции, позволяющей изменить данные о клиенте
    client_db.update_client(client1_id, 'Иван Иванович', 'Иванов', 'ivan.ii@mail.com')
    print('Сведения о клиентах после изменения данных о 1-м клиенте:')
    client_db.print_all_clients()
    print()

    # демонстрация работы функции, позволяющей удалить телефон для существующего клиента
    client_db.del_client_phone(1)
    print('Сведения о клиентах после удаления телефона существующего клиента:')
    client_db.print_all_clients()
    print()

    # демонстрация работы функции, позволяющей удалить существующего клиента
    client_db.del_client(client1_id)
    print('Сведения о клиентах после удаления 1-ого клиента:')
    client_db.print_all_clients()
    print()

    # демонстрация работы функции, позволяющей найти клиента по его данным: имени, фамилии, email или телефону
    search_result = client_db.find_client('имя', 'Василий')
    print(f'Результаты поиска клиента по имени (Василий):\n{search_result}\n')
    search_result = client_db.find_client('Фамилия', 'Петров')
    print(f'Результаты поиска клиента по фамилии (Петров):\n{search_result}\n')
    search_result = client_db.find_client('email', 'petrov@mail.server.ru')
    print(f'Результаты поиска клиента по email (petrov@mail.server.ru):\n{search_result}\n')
    search_result = client_db.find_client('телефон', '(495)000-00-00')
    print(f'Результаты поиска клиента по телефону ((495)000-00-00):\n{search_result}\n')
