"""
FSTR_worker (лит. "ФСТР_работник") - рабочий модуль для осуществления REST API для принятия, хранения и проверки
записей о перевалах (местоположении), от пользователей мобильного приложения ФСТР. Осуществлены 4 функции по
требованиям: отправка записи (POST), редактирования записи (PUT), и два метода для получения записи(-ей) по ID записи
или по электронной почте пользователя (GET). Для каждой функции этот модуль подключается к базе данных по указанию
ФСТР, затем проделывает работу на этой базе, по каждому запросу.
Используются внешние ресурсы: PostgreSQL, FastAPI.
"""


import psycopg2
import pydantic
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Union, Dict
import MOD_DB_LOGIN as S
import json

# инициализация
# соединение с PostgreSQL БД
db_conn = psycopg2.connect(
    host=S.FSTR_DB_HOST,
    database=S.FSTR_DB_NAME,
    user=S.FSTR_DB_LOGIN,
    password=S.FSTR_DB_PASS,
    port=S.FSTR_DB_PORT
)


# класс для прямой работы с БД, предполагает правильность ввода данных
class DBWorker:
    # метод для новой записи - POST
    @staticmethod
    def post_pereval(pro_input):
        with db_conn.cursor() as cur:
            cur.execute("INSERT INTO pereval_added (raw_data) VALUES ('%s') RETURNING id" % pro_input)
            db_conn.commit()
            return cur.fetchall()[0]

    # метод для изменения записи - PATCH
    @staticmethod
    def patch_pereval(pereval_id, pro_input):
        # проверка через совпадение по почте - лучше метода не выдумал
        with db_conn.cursor() as cur:
            cur.execute("SELECT raw_data::json#>'{user}' FROM pereval_added WHERE id = %i AND status = 1 " % pereval_id)
            user_check_raw = cur.fetchall()
        # почему выдаётся словарь в кортеже в массиве??
        user_check = user_check_raw[0][0]
        if not user_check:
            raise AssertionError
        if pro_input.get("user") != user_check:
            raise SyntaxError
        cooked_input = json.dumps(pro_input, ensure_ascii=False)
        with db_conn.cursor() as cur:
            cur.execute("UPDATE pereval_added SET raw_data = '%s' WHERE id = %i " % (cooked_input, pereval_id))
            db_conn.commit()

    # метод для вывода записи по id - GET by ID
    @staticmethod
    def get_pereval_by_id(pereval_id):
        with db_conn.cursor() as cur:
            cur.execute("SELECT raw_data, status_name FROM pereval_added JOIN mod_status ON pereval_added.status = mod_status.id WHERE pereval_added.id = %i " % pereval_id)
            return cur.fetchall()

    # метод для вывода записи по почте - GET by E-MAIL
    # TODO
    @staticmethod
    def get_pereval_by_email(user_email):
        with db_conn.cursor() as cur:
            cur.execute("SELECT raw_data FROM pereval_added WHERE raw_data::json#>>'{user,email}' = '%s'" % user_email)
            return cur.fetchall()


# рекомендация от FastAPI - этот класс проверяет запрос и облегчает работу со словарями
# FIXME: однако какие поля здесь обязательны не известно, так что выбрано по здравому смыслу
# где "Union[foo, None] = None" - не обязательное поле
class PerevalInput(BaseModel):
    beauty_title: str
    title: str
    other_titles: Union[str, None] = None
    connect: Union[str, None] = None
    add_time: str
    user: Dict[str, str]
    coords: Dict[str, float]
    level: Union[Dict[str, str], None] = None
    images: list


# начало программы, тут стоит FastAPI
app = FastAPI()


# REST API: функция для отправления записи
@app.post("/submitData")
def post_entry(user_input):
    try:
        user_input = PerevalInput(**user_input)
    except pydantic.ValidationError:
        return {"status": 400, "message": "Введено недостаточно данных.", "id": 'null'}
    try:
        new_id = DBWorker.post_pereval(user_input.model_dump_json())
    except psycopg2.OperationalError:
        return {"status": 500, "message": "Ошибка подключения к базе данных.", "id": 'null'}
    else:
        return {"status": 200, "message": "Запрос успешно отправлен.", "id": '%s' % new_id}


# REST API: функция для изменения записи, с ограничениями на новые записи и на неизменность пользователя
@app.put("/submitData/{pereval_id}")
def update_entry(pereval_id, user_input):
    try:
        user_input = PerevalInput(**user_input)
    except pydantic.ValidationError:
        return {"state": 0, "message": "Введено недостаточно данных."}
    input_dict = user_input.model_dump()
    try:
        DBWorker.patch_pereval(pereval_id, input_dict)
    except AssertionError:
        return {"state": 0, "message": "Этой записи нет, или она была принята на модерацию."}
    except SyntaxError:
        return {"state": 0, "message": "Данные пользователя не совпадают, менять эти данные запрещено."}
    else:
        return {"state": 1, "message": "Запрос успешно изменён."}


# REST API: функция для получения записи по id
@app.get("/submitData/{pereval_id}")
def get_entry_by_id(pereval_id):
    try:
        entry = DBWorker.get_pereval_by_id(pereval_id)
    except psycopg2.DatabaseError:
        raise HTTPException(status_code=404, detail="Запрос не найден.")
    else:
        if not entry:
            raise HTTPException(status_code=404, detail="Запрос не найден.")
        else:
            return json.dumps(entry, ensure_ascii=False)


# REST API: функция для получения записи по эл. почте пользователя
@app.get("/submitData/?user__email=<{user_email}>")
def get_entry_by_email(user_email):
    try:
        entry = DBWorker.get_pereval_by_email(user_email)
    except psycopg2.DatabaseError:
        raise HTTPException(status_code=404, detail="Запрос(ы) не найден(ы).")
    else:
        if not entry:
            raise HTTPException(status_code=404, detail="Запрос(ы) не найден(ы).")
        else:
            return json.dumps(entry, ensure_ascii=False)


# здесь проводятся тесты
if __name__ == "__main__":
    # пример запроса записан в "ex_input.json"; этот код можно активировать для тестирования функции
    """with open("ex_input.json", "r", encoding="utf-8") as f:
        processed_input = json.loads(f.read())"""
    # пример на post_entry
    """print(post_entry(processed_input))"""
