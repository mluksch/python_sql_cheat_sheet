import sqlalchemy

# SqlAlchemy Query Language:
# - is built on MetaData
# - uses Method Chaining

metadata = sqlalchemy.MetaData()

# Define Tables & add them to metadata:
user_table = sqlalchemy.Table(
    "user",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer(), primary_key=True),
    sqlalchemy.Column("first_name", sqlalchemy.String(), nullable=False),
    sqlalchemy.Column("last_name", sqlalchemy.String(), nullable=False),
    sqlalchemy.Column("age", sqlalchemy.Integer(), nullable=False),
)

# Create Engine:
engine = sqlalchemy.create_engine("sqlite+sqlite2:///:memory:")

# Create connection, start transaction and create Tables:
with engine.begin() as con:
    metadata.create_all(con)
