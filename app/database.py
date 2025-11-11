# backend/app/database.py

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import os
from .config import settings # Import the single settings instance

# Use the validated URL from our settings object
engine = create_engine(settings.DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()
# This is the address of our database.
# - "mysql+pymysql": Use the MySQL dialect with the PyMySQL driver.
# - "root:@localhost": Connect as the user 'root' with an empty password (':') to the server on our local machine ('localhost').
# - "/po_data_app": The specific database to connect to.

# Line 2: The Engine
# The "engine" is the heart of SQLAlchemy. It's not a connection itself,
# but a factory that knows HOW to create connections and communicate with
# our specific database (MySQL in this case) using the URL we provided.

# Line 3: The Session Factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
# This creates a "Session" class. Think of a Session as a short-lived conversation
# or a single work session with the database. We create a new one for each API
# request and then close it. This is the standard, safe way to interact.
# - `bind=engine`: We tell our session factory to use our engine when it creates sessions.

# Line 4: The Declarative Base
Base = declarative_base()
# This is a special "base" class. Any class we create later that inherits from
# this `Base` (like our `PurchaseOrderData` model) will automatically be
# registered by SQLAlchemy as a table to be managed. It's the magic glue
# between our Python classes and the database tables.