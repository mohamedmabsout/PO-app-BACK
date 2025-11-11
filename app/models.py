import datetime
from sqlalchemy import Boolean, Column, Date, Enum, ForeignKey, Integer, String, Float, DateTime

from .enum import ProjectType, UserRole
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, Text, Date, Enum, ForeignKey
from .database import Base
from sqlalchemy.orm import relationship

class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    
    # --- From your form ---
    # Informations Personnelles
    first_name = Column(String(100), nullable=False)
    last_name = Column(String(100), nullable=False)
    birth_date = Column(Date, nullable=True)
    cin = Column(String(50), unique=True, nullable=True)
    cnss = Column(String(50), unique=True, nullable=True)
    rib = Column(String(24), unique=True, nullable=True)

    # Contact & Recrutement
    username = Column(String(100), unique=True, index=True, nullable=False)
    email = Column(String(100), unique=True, index=True, nullable=False)
    hashed_password = Column(String(255), nullable=False) # Will store the HASH, not password
    phone_number = Column(String(50), nullable=True)
    address = Column(String(255), nullable=True)
    city = Column(String(100), nullable=True)
    hire_date = Column(Date, nullable=True)

    # Informations Professionnelles
    job_title = Column(String(100), nullable=True)
    role = Column(Enum(UserRole), nullable=False)
    daily_rate = Column(Float, nullable=True, default=0.0)
    is_active = Column(Boolean, default=True)

class Account(Base):
    __tablename__ = "accounts"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), unique=True, nullable=False, index=True)
    # Add other account-specific fields here if needed
    # e.g., account_number, created_by, etc.

# --- NEW MODEL: Customer ---
class Customer(Base):
    __tablename__ = "customers"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), unique=True, nullable=False, index=True)
    short_name = Column(String(100))
    # Add other customer-specific fields here

# --- UPDATED MODEL: Project ---
class Project(Base):
    __tablename__ = 'projects'
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), unique=True, index=True, nullable=False)
    
    # --- CHANGE 1: Use the Enum for project_type ---
    project_type = Column(Enum(ProjectType))
    
    start_date = Column(Date)
    plan_end_date = Column(Date)
    has_extension_possibility = Column(Boolean, default=False)
    
    # We are removing 'project_manager_name'
    # project_manager_name = Column(String(255)) 
    
    forecast_plan_details = Column(String(1000))
    budget_assigned = Column(Float)
    budget_period = Column(String(50))

    # --- RELATIONSHIPS ---
    account_id = Column(Integer, ForeignKey("accounts.id"), nullable=True)
    direct_customer_id = Column(Integer, ForeignKey("customers.id"), nullable=True)
    final_customer_id = Column(Integer, ForeignKey("customers.id"), nullable=True)

    # --- CHANGE 2: Add a ForeignKey to the User table ---
    project_manager_id = Column(Integer, ForeignKey("users.id"), nullable=True)

    # SQLAlchemy relationship attributes
    account = relationship("Account")
    direct_customer = relationship("Customer", foreign_keys=[direct_customer_id])
    final_customer = relationship("Customer", foreign_keys=[final_customer_id])
    
    # --- CHANGE 3: Add the relationship to the User model ---
    project_manager = relationship("User")



class PurchaseOrder(Base):
    __tablename__ = "purchase_orders"

    id = Column(Integer, primary_key=True, index=True)
    due_qty = Column(Float)
    po_status = Column(String(50))
    unit_price = Column(Float)
    line_amount = Column(Float)
    billed_quantity = Column(Float)
    po_no = Column(String(100), index=True) # Index this for faster lookups
    po_line_no = Column(Integer)
    item_code = Column(String(100))
    requested_qty = Column(Float)
    publish_date = Column(DateTime)
    project_code = Column(String(50), index=True) # Index this too
    site_code = Column(String(100), index=True) # Index this too
    payment_terms_raw = Column(String(500), nullable=True) # New field for the raw text
    is_processed = Column(Boolean, default=False) # Our new tracking flag!


class MergedPO(Base):
    __tablename__ = "merged_pos"
    id = Column(Integer, primary_key=True, index=True)
    # --- NEW & TRANSFORMED FIELDS ---
    po_id = Column(String(255), unique=True, index=True) # "PO NO - PO LINE"
    project_name = Column(String(255), nullable=True) # We need to decide where this comes from
    site_code = Column(String(100), nullable=True) # We need to decide where this comes from
    po_no = Column(String(100), index=True)
    po_line_no = Column(Integer)
    item_description = Column(String(500), nullable=True)
    payment_term = Column(String(100)) # The abbreviated version
    unit_price = Column(Float)
    requested_qty = Column(Float)
    internal_control = Column(Integer, default=1) # Defaults to 1
    line_amount_hw = Column(Float) # The calculated amount
    publish_date = Column(DateTime)
    
class UploadHistory(Base):
    __tablename__ = "upload_history"

    id = Column(Integer, primary_key=True, index=True)
    
    # What & When
    original_filename = Column(String(255), nullable=False)
    uploaded_at = Column(DateTime, default=datetime.datetime.now(datetime.timezone.utc))
    
    # Outcome
    status = Column(String(50), nullable=False) # e.g., "SUCCESS", "FAILURE"
    total_rows = Column(Integer, default=0)
    error_message = Column(Text, nullable=True) # To store detailed errors
    
    # Who
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    uploader = relationship("User")
