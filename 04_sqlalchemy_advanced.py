import os
import pprint

import pandas as pd
import sqlalchemy

import utils

# Define Table-Objects:
metadata = sqlalchemy.MetaData()
animal_table = sqlalchemy.Table(
    "animal",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer(), primary_key=True),
    sqlalchemy.Column("gender", sqlalchemy.Enum("male", "female", name='gender_types')),
    sqlalchemy.Column("name", sqlalchemy.String()),
    sqlalchemy.Column("species", sqlalchemy.ForeignKey("species.id")),
    sqlalchemy.Column("birthday", sqlalchemy.DateTime()),
    sqlalchemy.Column("price", sqlalchemy.Numeric(10, 2)),
)
species_table = sqlalchemy.Table(
    "species",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.String(), primary_key=True),
    sqlalchemy.Column("description", sqlalchemy.String()),
)

# Fill tables with CSV-data:
engine = sqlalchemy.create_engine(os.getenv("POSTGRES_URL_SQL_ALCHEMY"), future=True, echo=False)
df_animal = pd.read_csv("metadata_animal.csv", header=0)
df_species = pd.read_csv("metadata_species.csv", header=0)
with engine.begin() as \
        con, \
        open("metadata_animal.csv") as f_animal, \
        open("metadata_species.csv") as f_species:
    metadata.drop_all(con)
    metadata.create_all(con)
    # pandas works for inserting:
    df_species.to_sql("species", con=con, if_exists="append", index=False)
    df_animal.to_sql("animal", con=con, if_exists="append", index=False)

utils.print_table(engine, table_name="animal")
utils.print_table(engine, table_name="species")

# (1) Join-clause
# (1.a) Outer Join
with engine.connect() as con:
    db_stmt = sqlalchemy.select([animal_table, species_table]).outerjoin(species_table)
    print(f"stmt: {db_stmt}")
    rows = con.execute(db_stmt).fetchall()
    print(f"rows: {pprint.pformat(rows)}")

# Sub-Selects
