import sqlmodel
import sqlmodel_db as db

engine: sqlmodel = sqlmodel.create_engine("sqlite:///shopsystem.db")

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
        db.Shop(name="Homer's Drinks and Jinx"),
        db.Shop(name="Johnny's Dogfood Store"),
        db.Shop(name="Ben's Fishing Accessoires"),
        db.Shop(name="Uncle Sam's Food Shop"),
        db.Shop(name="Carlo's Car Repair"),
    ])
        # Create some Customers:
    session.add_all([
        db.Shop(name="Homer's Drinks and Jinx"),
        db.Shop(name="Johnny's Dogfood Store"),
        db.Shop(name="Ben's Fishing Accessoires"),
        db.Shop(name="Uncle Sam's Food Shop"),
        db.Shop(name="Carlo's Car Repair"),
    ])
    # Create some Customers:
    session.add_all([
        db.Shop(name="Homer's Drinks and Jinx"),
        db.Shop(name="Johnny's Dogfood Store"),
        db.Shop(name="Ben's Fishing Accessoires"),
        db.Shop(name="Uncle Sam's Food Shop"),
        db.Shop(name="Carlo's Car Repair"),
    ])
    session.commit()


# Read:
with sqlmodel.Session(engine) as session:
    pass

# Update:
with sqlmodel.Session(engine) as session:    
    pass

# Delete:
with sqlmodel.Session(engine) as session:    
    pass

