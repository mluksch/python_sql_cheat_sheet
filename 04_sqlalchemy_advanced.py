import os

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
menu_table = sqlalchemy.Table(
    "menu",
    metadata,
    # animal,menu,price
    sqlalchemy.Column("animal", sqlalchemy.String()),
    sqlalchemy.Column("menu", sqlalchemy.String()),
    sqlalchemy.Column("price", sqlalchemy.Numeric(10, 2))
)

# Fill tables with CSV-data:
engine = sqlalchemy.create_engine(os.getenv("POSTGRES_URL_SQL_ALCHEMY"), future=True, echo=False)
df_animal = pd.read_csv("metadata_animal.csv", header=0)
df_species = pd.read_csv("metadata_species.csv", header=0)
df_menu = pd.read_csv("metadata_menu.csv", header=0)

with engine.begin() as \
        con, \
        open("metadata_animal.csv") as f_animal, \
        open("metadata_species.csv") as f_species:
    metadata.drop_all(con)
    metadata.create_all(con)
    # pandas works for inserting:
    df_species.to_sql("species", con=con, if_exists="append", index=False)
    df_animal.to_sql("animal", con=con, if_exists="append", index=False)
    df_menu.to_sql("menu", con=con, if_exists="append", index=False)

utils.print_table(engine, table_name="animal")
utils.print_table(engine, table_name="species")
utils.print_table(engine, table_name="menu")

# (1) Join-clause:
print(f"You can use join-method on select-method: method chaining")

# (1.a) Inner Join without explicit on-clause
# select(...).join(<table>, <on-clause>)
print(f"- Inner join: 'select(...).join(<table>, <on-clause>)'")

# not necessary, if in table schema foreign keys are defined
inner_join = animal_table.join(
    # join-table:
    species_table
    # on-clause not required here:
    # empty
)
print(f"inner-join-clause: {inner_join}")
stmt = sqlalchemy.select([animal_table, species_table]).join(species_table)
utils.execute_stmt(engine, stmt)

# (1.b) Inner Join with explicit on-clause
# select(...).join(<table>, <on-clause>)
# necessary, if there havent been defined any foreign keys in table schemas,
# we need to explicitly provide an on-clause on joining
inner_join = species_table.join(
    # join-table:
    menu_table,
    # on-clause:
    species_table.c.id == menu_table.c.animal)
print(f"inner-join-clause: {inner_join}")
stmt = sqlalchemy.select([species_table, menu_table]).join(menu_table, species_table.c.id == menu_table.c.animal)
utils.execute_stmt(engine, stmt)
# only rows are selected by inner join, which exist in both tables
# 0  Fish  Swims in the water  Fish   Fish brulee  10.00
# 1  Fish  Swims in the water  Fish  Fish flambee  19.00

# (1.c) Left outer Join with explicit on-clause
# select(...).outerjoin(<table>, <on-clause>)
print(f"- Left Join: 'select(...).outerjoin(<table>, <on-clause>)'")
left_join = species_table.join(
    # join-table:
    menu_table,
    # on-clause:
    species_table.c.id == menu_table.c.animal)
print(f"Left-outer-Join-clause: {left_join}")
stmt = sqlalchemy.select([species_table, menu_table]).outerjoin(menu_table,
                                                                species_table.c.id == menu_table.c.animal)
utils.execute_stmt(engine, stmt)
# only rows are selected by left outer join, which exist in left table
# 0  Fish  Swims in the water  Fish   Fish brulee  10.00
# 1  Fish  Swims in the water  Fish  Fish flambee  19.00


# Alias in SQLAlchemy
# my_table_alias = table.alias()
# If a table is referenced multiple times in a Query, use an alias:
species_table_alias_1 = species_table.alias()
animal_table_alias_1 = animal_table.alias()
menu_table_alias_1 = menu_table.alias()

# Table-Self-Join:
# select([<table>, <table_alias>]).join(<table_alias>, <on-clause>)
print(f"- Self-Join: 'select([<table>, <table_alias>]).join(<table_alias>, <on-clause>)'")
# Example: Double Menu combinations for 2-Menu-combinations of the same animal
# For example: 2-menu-combinations for Pescarias (Fish-only-eater)
double_menu_if_same_animal_stmt = \
    sqlalchemy.select([menu_table, menu_table_alias_1]).join(menu_table_alias_1,
                                                             # on-clause on "animal" for both tables
                                                             menu_table.c.animal == menu_table_alias_1.c.animal)
utils.execute_stmt(engine, double_menu_if_same_animal_stmt)

# Sub-Selects:
# select(...).select_from(<select-Stmt>)
print(f"- Sub-Select: 'select(...).select_from(<select-Stmt>)'")
subselect_stmt = sqlalchemy.select([menu_table, menu_table_alias_1]).select_from(
    double_menu_if_same_animal_stmt
)
utils.execute_stmt(engine, subselect_stmt)
