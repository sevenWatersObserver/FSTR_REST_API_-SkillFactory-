import psycopg2
import json
import MOD_DB_LOGIN as S

conn = psycopg2.connect(
    host=S.FSTR_DB_HOST,
    database=S.FSTR_DB_NAME,
    user=S.FSTR_DB_LOGIN,
    password=S.FSTR_DB_PASS,
    port=S.FSTR_DB_PORT
)
# cur = conn.cursor()


class FSTRDBWorker:
    # dummy method 1, just returns input data but this does it on the example input
    # TODO
    @staticmethod
    def dummy_input_process():
        with open("example_input.json", "r", encoding="utf8") as read_f:
            raw_data = json.load(read_f)
            return json.dumps(raw_data)


# do testing here
if __name__ == "__main__":
    drone = FSTRDBWorker()
    # test function, testing cursor
    """with conn.cursor() as cur:
        cur.execute("SELECT * FROM spr_activities_types"),
        print(cur.fetchall())
        print(type(cur.fetchall()))"""
    # test function, updating the first example entry of the database
    with conn.cursor() as cur:
        cur.execute("UPDATE pereval_added SET raw_data = '%s' WHERE id = 1" % (drone.dummy_input_process())),
        cur.execute("SELECT raw_data FROM pereval_added WHERE id = 1")
        print(cur.fetchall())
