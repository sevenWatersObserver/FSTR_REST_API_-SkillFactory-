import psycopg2
import json
from fastapi import FastAPI
import MOD_DB_LOGIN as S

# инициализация - массив для проверки наличия данных в input.json, соединение с PostgreSQL БД
input_expect = {"beauty_title", "title", "other_titles", "connect",
                "add_time", "user", "coords", "level", "images"}

conn = psycopg2.connect(
    host=S.FSTR_DB_HOST,
    database=S.FSTR_DB_NAME,
    user=S.FSTR_DB_LOGIN,
    password=S.FSTR_DB_PASS,
    port=S.FSTR_DB_PORT
)


class NotEnoughData(Exception):
    pass


# e
class DBdrone:
    # этот метод просто читает "input.json" и отправляет его
    @staticmethod
    def post_raw():
        read_f = open("input.json", "r", encoding="utf8")
        # json.load здесь, возможно, магия; неясно что будет если убрать, или как
        processed_input = json.load(read_f)
        parsed_input = json.dumps(processed_input)
        read_f.close()
        # проверка на наличие данных?
        if processed_input.keys() < input_expect:
            raise NotEnoughData
        else:
            return parsed_input


# начало программы, тут стоит FastAPI
drone = DBdrone
app = FastAPI()


@app.post("/submitData")
def post_entry():
    with conn.cursor() as cur:
        try:
            cur.execute("INSERT INTO pereval_added (raw_data) VALUES ('%s') RETURNING id" % drone.post_raw())
        except psycopg2.OperationalError:
            return {"status": 500, "message": "Ошибка подключения к базе данных", "id": 'null'}
        except NotEnoughData:
            return {"status": 400, "message": "Недостаточно информации", "id": 'null'}
        else:
            conn.commit()
            post_return = cur.fetchall()[0]
            return {"status": 200, "message": "Запрос успешно отправлен.", "id": '%s' % post_return}


# здесь всякие тесты
if __name__ == "__main__":
    # test function, updating the first example entry of the database
    """with conn.cursor() as cur:
        cur.execute("UPDATE pereval_added SET raw_data = '%s' WHERE id = 1" % (drone.post_raw())),
        cur.execute("SELECT raw_data FROM pereval_added WHERE id = 1")
        print(cur.fetchall())"""
    # testing anything else here
