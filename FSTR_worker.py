import psycopg2
import json
from fastapi import FastAPI
import MOD_DB_LOGIN as S


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


class DBdrone:
    # method that just posts what it reads from "input.json"
    @staticmethod
    def post_raw():
        read_f = open("input.json", "r", encoding="utf8")
        processed_input = json.load(read_f)
        parsed_input = json.dumps(processed_input)
        read_f.close()
        if processed_input.keys() < input_expect:
            raise NotEnoughData
        else:
            return parsed_input


# REST API goes here
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


# do testing here
if __name__ == "__main__":
    # test function, testing cursor
    """with conn.cursor() as cur:
        cur.execute("SELECT raw_data FROM pereval_added"),
        print(cur.fetchall())"""
    # test function, updating the first example entry of the database
    """with conn.cursor() as cur:
        cur.execute("UPDATE pereval_added SET raw_data = '%s' WHERE id = 1" % (drone.dummy_input_process())),
        cur.execute("SELECT raw_data FROM pereval_added WHERE id = 1")
        print(cur.fetchall())"""
    # general testing...
    # print(post_entry())
    # with conn.cursor() as cur:
        # cur.execute("INSERT INTO pereval_added (raw_data) VALUES ('%s')" % drone.post_raw())
        # cur.execute("SELECT * FROM pereval_added")
        # print(cur.fetchall())
