'''
Было решено создать базу данных с использованием библиотеки sqlite3.
Класс DB обрабатывает ошибки, может подключаться к базе данных и создавать в ней таблицы.
Для удобства также добавил метод, который выводит всё содержимое базы данных.
Формат хранения в базе данных представлен в словаре COLUMN_NAMES:
COLUMN_NAMES = {
 'id': "string PRIMARY KEY",
 'url': 'string',
 'pub_date': 'integer',
 'fetch_time': 'integer',
 'text': 'string',
 'first_fetch_time': 'integer'
 }
'''

import sqlite3

# cписок имен колонн Базы Данных и их тип данных
# Можно также заменить на формат "тип данных":"название колонки". Для удобочитаемости не стал так делать.
COLUMN_NAMES = {
    'id': "string PRIMARY KEY",
    'url': 'string',
    'pub_date': 'integer',
    'fetch_time': 'integer',
    'text': 'string',
    'first_fetch_time': 'integer'}


class DB:
    def __init__(self, db_name: str = "", t_name: str = ""):
        self.create_table(db_name, t_name)

    def create_table(self, db_name: str, t_name: str):
        if not len(db_name):
            raise ValueError("DataBase name cannot be empty")
        if not len(t_name):
            raise ValueError("Table name cannot be empty")

        self.t_name = t_name
        self.db_name = db_name
        # соединение с базой данных
        self.sqliteConnection = sqlite3.connect(self.db_name)
        self.cursor = self.sqliteConnection.cursor()
        # команда создания таблицы с указанными параметрами
        command = f"CREATE TABLE IF NOT EXISTS {self.t_name} ({', '.join([el[0] + ' ' + el[1] for el in COLUMN_NAMES.items()])} )"
        self.cursor.execute(command)

    def is_exist(self, id):
        self.cursor.execute(f"SELECT * FROM {self.t_name} WHERE id = ?", (id,))
        data = self.cursor.fetchone()
        if data is None:
            # записи не существует
            return False
        # запись существует
        return True

    def insert(self, data):
        # формирование знаков вопроса для записи в БД
        str_q_marks = ('?,' * (len(data)))[:-1:]
        str_q_marks = "(" + str_q_marks + ")"

        if self.is_exist(data[0]):
            return False

        # Вставка данных в базу данных
        self.cursor.execute(f"INSERT INTO {self.t_name} ({', '.join([el[0] for el in COLUMN_NAMES.items()])})"
                            f" VALUES {str_q_marks}",
                            data)
        return True

    def commit(self):
        self.sqliteConnection.commit()

    def close(self):
        self.sqliteConnection.close()

    def select_everything(self):
        self.cursor.execute(f"SELECT * FROM {self.t_name}")
        print('\nСодержимое БД:')
        for row in self.cursor.fetchall():
            print(row)

