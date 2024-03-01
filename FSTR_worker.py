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
    # method that does the thing for submitData
    # TODO: complete
    def input_getter(self):
        with open("example_input_fixed.json", "r", encoding="utf8") as read_f:
            raw_data = json.load(read_f)
            print(raw_data)
            print(type(raw_data.get("user")))


# do testing here
if __name__ == "__main__":
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM spr_activities_types"),
        print(cur.fetchall())
        print(type(cur.fetchall()))
