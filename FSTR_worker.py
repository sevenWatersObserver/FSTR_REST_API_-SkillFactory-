import psycopg2
import json
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Union, Dict
import MOD_DB_LOGIN as S

# инициализация - массив для проверки наличия данных в input.json, соединение с PostgreSQL БД
# input_expect = {"beauty_title", "title", "other_titles", "connect",
#                 "add_time", "user", "coords", "level", "images"}

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
        user_check_raw = {}
        with db_conn.cursor() as cur:
            cur.execute("SELECT raw_data::json#>'{user}' FROM pereval_added WHERE id = %i " % pereval_id)
            user_check_raw = cur.fetchall()
        # почему выдаётся словарь в кортеже в массиве??
        user_check = user_check_raw[0][0]
        if pro_input.get("user") != user_check:
            raise SyntaxError
        with db_conn.cursor() as cur:
            cur.execute("UPDATE pereval_added SET raw_data = '%s' WHERE id = %i " % pro_input, pereval_id)
            db_conn.commit()

    # метод для вывода записи по id - GET by ID
    @staticmethod
    def get_pereval_by_id(pereval_id):
        with db_conn.cursor() as cur:
            cur.execute("SELECT raw_data, status FROM pereval_added WHERE id = %i " % pereval_id)
            return cur.fetchall()

    # метод для вывода записи по почте - GET by E-MAIL
    # TODO
    @staticmethod
    def get_pereval_by_email(user_email):
        user_check_raw = {}
        with db_conn.cursor() as cur:
            cur.execute("SELECT raw_data::json#>'{user,}' FROM pereval_added")
            user_check_raw = cur.fetchall()
        user_check = str(user_check_raw)
        user_email = user_email + " "
        pass


# рекомендация от FastAPI - этот класс проверяет запрос и облегчает работу со словарями
# FIXME: однако какие поля здесь обязательны не известно, здесь выбрано по здравому смыслу
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


@app.post("/submitData")
def post_entry(user_input):
    try:
        user_input = PerevalInput()
    except Exception:
        return {"status": 400, "message": "Введено недостаточно данных.", "id": 'null'}
    try:
        new_id = DBWorker.post_pereval(user_input)
    except psycopg2.OperationalError:
        return {"status": 500, "message": "Ошибка подключения к базе данных.", "id": 'null'}
    else:
        return {"status": 200, "message": "Запрос успешно отправлен.", "id": '%s' % new_id}


@app.put("/submitData/{pereval_id}")
def update_entry(pereval_id, user_input):
    try:
        user_input = PerevalInput()
    except Exception:
        return {"state": 0, "message": "Введено недостаточно данных."}
    input_dict = user_input.dict()
    try:
        DBWorker.patch_pereval(pereval_id, input_dict)
    except SyntaxError:
        return {"state": 0, "message": "Данные пользователя не совпадают, менять эти данные запрещено."}
    else:
        return {"state": 1, "message": "Запрос успешно изменён."}


@app.get("/submitData/{pereval_id}")
def get_entry_by_id(pereval_id):
    entry = None
    try:
        entry = DBWorker.get_pereval_by_id(pereval_id)
    except psycopg2.DatabaseError:
        raise HTTPException(status_code=404, detail="Запрос не найден.")
    else:
        return entry


# здесь всякие тесты
if __name__ == "__main__":
    # пример запроса записан в "ex_input.json"; этот код можно активировать для тестирования методов
    """
    read_f = open("ex_input.json", "r", encoding="utf8")
    processed_input = json.load(read_f)
    parsed_input = json.dumps(processed_input)
    read_f.close()
    """
    # тестирование здесь
    # DBWorker.patch_pereval(1, 0)
