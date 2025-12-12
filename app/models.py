import datetime
from sqlalchemy import Boolean, Column, Date, Enum, ForeignKey, Integer, String, Float, DateTime

from .enum import ProjectType, UserRole, SBCStatus, BCStatus
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, Text, Date, Enum, ForeignKey
from .database import Base
from sqlalchemy.orm import relationship
import sqlalchemy as sa 
from sqlalchemy.sql import func # <--- AJOUTER CET IMPORT

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
class Site(Base):
    __tablename__ = 'sites'
    id = Column(Integer, primary_key=True, index=True)
    site_code = Column(String(300), unique=True, index=True, nullable=False)
    site_name = Column(String(300), nullable=True) # Optional: nice to have

class InternalProject(Base):
    __tablename__ = 'internal_projects'
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), unique=True, index=True, nullable=False)
    
    # Use your existing Enum
    project_type = Column(Enum(ProjectType))
    
    start_date = Column(Date)
    plan_end_date = Column(Date)
    has_extension_possibility = Column(Boolean, default=False)
    forecast_plan_details = Column(String(1000))
    budget_assigned = Column(Float)
    budget_period = Column(String(50))

    # --- RELATIONSHIPS ---
    account_id = Column(Integer, ForeignKey("accounts.id"), nullable=True)
    direct_customer_id = Column(Integer, ForeignKey("customers.id"), nullable=True)
    final_customer_id = Column(Integer, ForeignKey("customers.id"), nullable=True)
    project_manager_id = Column(Integer, ForeignKey("users.id"), nullable=True)

    account = relationship("Account")
    direct_customer = relationship("Customer", foreign_keys=[direct_customer_id])
    final_customer = relationship("Customer", foreign_keys=[final_customer_id])
    project_manager = relationship("User")

    # --- CRITICAL FIX: REMOVED customer_projects relationship ---
    # Since CustomerProject no longer has a foreign key to this table, 
    # this relationship cannot exist.
    
    # Direct link to Merged POs
    merged_pos = relationship("MergedPO", back_populates="internal_project")


class CustomerProject(Base):
    __tablename__ = 'customer_projects'
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), unique=True, index=True, nullable=False)
    
    # Correct: No internal_project_id here anymore.
    
    merged_pos = relationship("MergedPO", back_populates="customer_project")


class SiteAssignmentRule(Base): # Inherit from Base, not Pydantic BaseModel (Typo fix in concept)
    __tablename__ = 'site_assignment_rules'
    
    id = Column(Integer, primary_key=True, index=True)
    
    # --- 1. String Pattern Criteria (All Nullable) ---
    # If a field is NULL, we ignore it. If populated, it MUST match.
    starts_with = Column(String(100), nullable=True) 
    ends_with = Column(String(100), nullable=True)
    contains_str = Column(String(100), nullable=True)
    
    # --- 2. Context Criteria ---
    customer_project_id = Column(Integer, ForeignKey("customer_projects.id"), nullable=True)
    
    # --- 3. Date Criteria ---
    min_publish_date = Column(Date, nullable=True)
    max_publish_date = Column(Date, nullable=True)
    
    # --- 5. Outcome ---
    internal_project_id = Column(Integer, ForeignKey("internal_projects.id"), nullable=False)
    
    # Relationships
    internal_project = relationship("InternalProject")
    customer_project = relationship("CustomerProject")


class SiteProjectAllocation(Base):
    """
    Manual Overrides: "Specific Site ID X belongs to Project Y, ignoring rules."
    """
    __tablename__ = "site_project_allocations"
    
    id = Column(Integer, primary_key=True, index=True)
    
    customer_project_id = Column(Integer, ForeignKey("customer_projects.id"), nullable=False)
    site_id = Column(Integer, ForeignKey("sites.id"), nullable=False)
    internal_project_id = Column(Integer, ForeignKey("internal_projects.id"), nullable=False)

    internal_project = relationship("InternalProject")
    site = relationship("Site")


class RawPurchaseOrder(Base):
    __tablename__ = 'raw_purchase_orders'

    id = Column(Integer, primary_key=True, index=True)
    po_status = Column(String(50))
    unit_price = Column(Float)
    line_amount = Column(Float)
    po_no = Column(String(100), index=True)
    po_line_no = Column(Integer)
    item_description = Column(String(500), nullable=True)
    requested_qty = Column(Float)
    publish_date = Column(DateTime)
    payment_terms_raw = Column(String(500), nullable=True)
    
    project_code = Column(String(260), nullable=True, index=True)
    
    # Note: These are nullable because raw data might not match perfectly immediately
    internal_project_id = Column(Integer, ForeignKey("internal_projects.id"), nullable=True, index=True)
    site_id = Column(Integer, ForeignKey("sites.id"), nullable=True, index=True)
    customer_id = Column(Integer, ForeignKey("customers.id"), nullable=True, index=True)

    internal_project = relationship("InternalProject")
    site = relationship("Site")
    customer = relationship("Customer")
    
    is_processed = Column(Boolean, default=False, index=True)
    uploaded_at = Column(DateTime, server_default=func.now()) # <--- CHANGER POUR server_default
    uploader_id = Column(Integer, ForeignKey("users.id"))
    uploader = relationship("User")


class RawAcceptance(Base):
    __tablename__ = "raw_acceptances"
    id = Column(Integer, primary_key=True, index=True)
    
    po_no = Column(String(100), index=True)
    po_line_no = Column(Integer)
    shipment_no = Column(Integer)
    acceptance_qty = Column(Float)
    application_processed_date = Column(DateTime)
    
    is_processed = Column(Boolean, default=False, index=True)
    uploaded_at = Column(DateTime, default=datetime.datetime.now(datetime.timezone.utc))
    
    uploader_id = Column(Integer, ForeignKey("users.id"))
    uploader = relationship("User")


class MergedPO(Base):
    __tablename__ = "merged_pos"
    id = Column(Integer, primary_key=True, index=True)
    
    po_id = Column(String(255), unique=True, index=True)
    raw_po_id = Column(Integer, ForeignKey("raw_purchase_orders.id"), unique=True)
    raw_po = relationship("RawPurchaseOrder", backref="merged_po")
    
    # --- RELATIONSHIPS (Corrected with back_populates) ---
    customer_project_id = Column(Integer, ForeignKey("customer_projects.id"), nullable=False)
    customer_project = relationship("CustomerProject", back_populates="merged_pos")

    # This is the computed field (Based on Rule or Manual Override)
    internal_project_id = Column(Integer, ForeignKey("internal_projects.id"), nullable=True)
    internal_project = relationship("InternalProject", back_populates="merged_pos") 
    
    site_id = Column(Integer, ForeignKey("sites.id"), nullable=True)
    site = relationship("Site")

    project_name = Column(String(255), nullable=True)   
    site_code = Column(String(100), nullable=True)
    po_no = Column(String(100), index=True)
    po_line_no = Column(Integer)
    item_description = Column(String(500), nullable=True)
    payment_term = Column(String(100))
    unit_price = Column(Float)
    requested_qty = Column(Float)
    internal_control = Column(Integer, default=1)
    line_amount_hw = Column(Float)
    publish_date = Column(DateTime)

    category = Column(String(100), nullable=True)
    
    total_ac_amount = Column(Float, nullable=True)
    accepted_ac_amount = Column(Float, nullable=True)
    date_ac_ok = Column(Date, nullable=True)
    
    total_pac_amount = Column(Float, nullable=True)
    accepted_pac_amount = Column(Float, nullable=True)
    date_pac_ok = Column(Date, nullable=True)
    
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
# backend/app/models.py

class UserPerformanceTarget(Base):
    __tablename__ = "user_performance_targets"

    id = Column(Integer, primary_key=True, index=True)
    
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    year = Column(Integer, nullable=False)  # e.g., 2025
    month = Column(Integer, nullable=False) # 1-12
    
    # The Admin Inputs
    target_po_amount = Column(Float, default=0.0)      # "Plan" for PO Received
    target_invoice_amount = Column(Float, default=0.0) # "Plan" for Invoicing (Paid)

    # Constraint: One target per user per month
    __table_args__ = (
        sa.UniqueConstraint('user_id', 'year', 'month', name='uix_user_year_month'),
    )
    
    user = relationship("User")

# --- 1. TAX CONFIGURATION TABLE ---
class TaxRule(Base):
    __tablename__ = 'tax_rules'
    
    id = Column(Integer, primary_key=True, index=True)
    category = Column(String(50), nullable=False) # "Service", "Transportation", etc.
    year = Column(Integer, nullable=False) # e.g. 2025
    tax_rate = Column(Float, nullable=False) # e.g. 0.20 for 20%
    
    # Unique constraint: Only one rate per Category per Year
    __table_args__ = (
        sa.UniqueConstraint('category', 'year', name='uix_tax_category_year'),
    )

class SBC(Base):
    __tablename__ = 'sbcs'

    id = Column(Integer, primary_key=True, index=True)
    
    # --- IDENTITY & ACCESS ---
    sbc_code = Column(String(50), unique=True, index=True, nullable=False) # "ID SBC"
    password_hash = Column(String(255)) # "Password" (Stored securely)
    short_name = Column(String(50), nullable=False) # "SBC Short Name"
    name = Column(String(255), nullable=False) # "SBC Name (Complete Name)"
    
    start_date = Column(Date) # "Date Start"
    
    # "Status SBC (Active; Blacklisted; under approval)"
    status = Column(Enum(SBCStatus), default=SBCStatus.UNDER_APPROVAL) 
    
    # --- CONTACT INFO ---
    ceo_name = Column(String(255)) # "CEO Subcontractor"
    phone_1 = Column(String(50), unique = True) # "Phone 1"
    phone_2 = Column(String(50)) # "Phone 2"
    email = Column(String(255), unique = True, index=True) # "Mail"
    
    # --- CONTRACTUAL INFO ---
    contract_ref = Column(String(100)) # "Contract" (Reference Number)
    has_contract_attachment = Column(Boolean, default=False) # "Attachment Contract Exist"
    contract_upload_date = Column(DateTime) # "Date upload Contract"
    
    # --- TAX REGULARIZATION ---
    has_tax_regularization = Column(Boolean, default=False) # "Attestation de regularisation fiscal"
    tax_reg_upload_date = Column(DateTime) # "Date upload" (for tax doc)
    tax_reg_end_date = Column(Date) # "Plan end date of Reg Fiscal"
    
    # --- FINANCIAL INFO ---
    rib = Column(String(50)) # "RIB"
    bank_name = Column(String(100)) # "Name of the Bank"
    
    # --- METADATA & APPROVALS ---
    created_at = Column(DateTime, default=datetime.datetime.utcnow) # "Date creation"
    
    # "Creator of the SBC (RAF)"
    creator_id = Column(Integer, ForeignKey("users.id"))
    
    # "Approver L1 (PD)"
    approver_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    
    # Relationships
    creator = relationship("User", foreign_keys=[creator_id])
    approver = relationship("User", foreign_keys=[approver_id])
class BonDeCommande(Base):
    __tablename__ = 'bon_de_commandes'

    id = Column(Integer, primary_key=True, index=True)
    bc_number = Column(String(100), unique=True, index=True) # BC-25-TEL-001
    year = Column(Integer) 

    project_id = Column(Integer, ForeignKey("internal_projects.id"), nullable=False)
    sbc_id = Column(Integer, ForeignKey("sbcs.id"), nullable=False)
    
    # Financials
    total_amount_ht = Column(Float, default=0.0) # Sum of lines (Unit * Qty)
    total_tax_amount = Column(Float, default=0.0) # Sum of (Line Amount * Tax Rate)
    total_amount_ttc = Column(Float, default=0.0) # HT + Tax
    
    status = Column(Enum(BCStatus), default=BCStatus.DRAFT)

    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    rejection_reason = Column(String(500), nullable=True)

    creator_id = Column(Integer, ForeignKey("users.id"))
    creator = relationship("User", foreign_keys=[creator_id])
    # Relationships
    internal_project = relationship("InternalProject")
    sbc = relationship("SBC")
    items = relationship("BCItem", back_populates="bc")

# --- 4. BC ITEMS ---
class BCItem(Base):
    __tablename__ = 'bc_items'

    id = Column(Integer, primary_key=True, index=True)
    bc_id = Column(Integer, ForeignKey("bon_de_commandes.id"), nullable=False)
    merged_po_id = Column(Integer, ForeignKey("merged_pos.id"), nullable=False)
    
    # Inputs
    rate_sbc = Column(Float, default=0.0) 
    quantity_sbc = Column(Float)
    
    # Calculations
    unit_price_sbc = Column(Float) 
    line_amount_sbc = Column(Float)
    
    # Tax Snapshot (We store it here in case rates change later)
    applied_tax_rate = Column(Float, default=0.0) 
    
    bc = relationship("BonDeCommande", back_populates="items")
    merged_po = relationship("MergedPO")
