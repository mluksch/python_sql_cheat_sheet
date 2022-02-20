import sqlmodel
import sqlmodel_db as db
import sqlalchemy

engine: sqlmodel = sqlmodel.create_engine(
    "sqlite:///shopsystem.db", future=True, echo=False)

# create all tables:
sqlmodel.SQLModel.metadata.drop_all(engine)
sqlmodel.SQLModel.metadata.create_all(engine)

# Create:
with sqlmodel.Session(engine) as session:
    # Create some Shops:
    session.add_all([
        db.Shop(name="Homer's Drinks and Jinx"),
        db.Shop(name="Johnny's Dogfood Store"),
        db.Shop(name="Ben's Fishing Accessoires"),
        db.Shop(name="Uncle Sam's Food Shop"),
        db.Shop(name="Carlo's Car Repair"),
    ])
    # Create some Customers:
    session.add_all([
        db.Customer(first_name="Apple", last_name="McBook", age=31),
        db.Customer(first_name="Johan", last_name="Johanson", age=22),
        db.Customer(first_name="Jack", last_name="Stack", age=36),
        db.Customer(first_name="Veronica", last_name="Acinorev", age=13),
        db.Customer(first_name="Anna", last_name="Gramm", age=11),
        db.Customer(first_name="Winnie", last_name="Bro", age=13),
        db.Customer(first_name="Steven", last_name="Everick", age=66),
        db.Customer(first_name="Simon", last_name="Spiegel", age=24),
        db.Customer(first_name="Werner", last_name="Weinberg", age=5),
        db.Customer(first_name="Kevin", last_name="Rester", age=12),
        db.Customer(first_name="Bernie", last_name="Sandberg", age=55),
        db.Customer(first_name="Bogomir", last_name="Gondorian", age=27),
    ])
    # Create some Products:
    session.add_all([
        db.Product(title="Bubble gum", price=3.44),
        db.Product(title="Cherries", price=4.11),
        db.Product(title="Shoes", price=74.77),
        db.Product(title="Beer", price=8.13),
        db.Product(title="Tires", price=126.38),
        db.Product(title="Dog Food", price=4.24),
        db.Product(title="Bugs", price=2.11),
        db.Product(title="Batteries", price=4.11),
        db.Product(title="Torchlight", price=5.24),
        db.Product(title="Potatoes", price=1.44),
        db.Product(title="Energydrinks", price=6.44),
        db.Product(title="Oil check", price=56.05),
        db.Product(title="Wax", price=22.77),
        db.Product(title="Car wash", price=12.29)
    ])
    # Create some.Purchases:
    session.add_all([
        db.Purchase(shop_id=0, customer_id=1, product_id=3),
        db.Purchase(shop_id=1, customer_id=8, product_id=5),
        db.Purchase(shop_id=4, customer_id=9, product_id=11),
        db.Purchase(shop_id=1, customer_id=1, product_id=5),
        db.Purchase(shop_id=3, customer_id=3, product_id=1),
        db.Purchase(shop_id=2, customer_id=11, product_id=6),
        db.Purchase(shop_id=1, customer_id=8, product_id=5),
        db.Purchase(shop_id=3, customer_id=1, product_id=4),
        db.Purchase(shop_id=0, customer_id=5, product_id=10),
        db.Purchase(shop_id=2, customer_id=6, product_id=7),
        db.Purchase(shop_id=4, customer_id=7, product_id=10),
        db.Purchase(shop_id=1, customer_id=1, product_id=5),
        db.Purchase(shop_id=4, customer_id=4, product_id=9),
        db.Purchase(shop_id=3, customer_id=7, product_id=5),
        db.Purchase(shop_id=3, customer_id=10, product_id=4)
    ])
    session.commit()


# Read:
with sqlmodel.Session(engine) as session:
    # (1) Simple Select
    # select-statements returning dicts:
    customer = session.execute(
        # use sqlalchemy's select to avoid warnings
        sqlalchemy.select(db.Customer)
        .order_by(db.Customer.age)
    ).first()
    print(f"select-statements returning dicts (Dictionaries): {customer}")

    # select-statements returning custom SQLModel-class 
    # (Pydantic-Models): scalars()
    customer = session.execute(
        # use sqlalchemy's select to avoid warnings
        sqlalchemy.select(db.Customer)
        .order_by(db.Customer.age)
    ).scalars().first()
    print(f"select-statements returning custom SQLModel-class (Pydantic-Models): {customer}")

    # (2) limit-statements

    # (3) where-statements

    # (4) Aggregation with group-statements

    # (5) Eager-Fetch

# Update:
with sqlmodel.Session(engine) as session:
    pass

# Delete:
with sqlmodel.Session(engine) as session:
    pass
