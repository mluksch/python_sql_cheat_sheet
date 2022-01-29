import pandas as pd
import sqlalchemy


def print_table(engine, table_name: str):
    with engine.connect() as c:
        # fetch all rows & pipe into pandas:
        res = c.execute(sqlalchemy.text(f"Select * from {table_name}"))
        rows = res.fetchall()
        # use reflection in SqlAlchemy for getting columns of table:
        inspector = sqlalchemy.inspect(engine)
        columns = [col.get("name") for col in inspector.get_columns(table_name)]
        df = pd.DataFrame.from_records(rows, columns=columns)
        print(f"************ Table {table_name} ************".upper())
        print(df)
