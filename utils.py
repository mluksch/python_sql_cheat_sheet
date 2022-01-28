import pprint

import sqlalchemy


def print_table(engine, table_name: str):
    with engine.connect() as c:
        r = c.execute(sqlalchemy.text(f"Select * from {table_name}"))
        t_rows = r.fetchall()
        pprint.pprint(t_rows)
