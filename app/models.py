# app/models.py
import datetime
import sqlalchemy as sa
from sqlalchemy import Boolean, Column, Date, Enum, ForeignKey, Integer, String, Float, DateTime

from .enum import ProjectType, UserRole, SBCStatus, BCStatus, NotificationType, BCType,AssignmentStatus, ValidationState, ItemGlobalStatus, SBCType,FundRequestStatus,TransactionType
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, Text, Date, Enum, ForeignKey
from .database import Base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from .database import Base
from .enum import (
    ProjectType,
    UserRole,
    SBCStatus,
    BCStatus,
    NotificationType,
    BCType,
    AssignmentStatus,
    ValidationState,
    ItemGlobalStatus,
    SBCType,
)

# =========================================================
# USERS
# =========================================================
class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)

    first_name = Column(String(100), nullable=False)
    last_name = Column(String(100), nullable=False)
    birth_date = Column(Date, nullable=True)
    cin = Column(String(50), unique=True, nullable=True)
    cnss = Column(String(50), unique=True, nullable=True)
    rib = Column(String(24), unique=True, nullable=True)

    username = Column(String(100), unique=True, index=True, nullable=False)
    email = Column(String(100), unique=True, index=True, nullable=False)
    hashed_password = Column(String(255), nullable=False)

    phone_number = Column(String(50), nullable=True)
    address = Column(String(255), nullable=True)
    city = Column(String(100), nullable=True)
    hire_date = Column(Date, nullable=True)

    job_title = Column(String(100), nullable=True)
    role = Column(Enum(UserRole), nullable=False)
    daily_rate = Column(Float, nullable=True, default=0.0)
    is_active = Column(Boolean, default=True)
    reset_token = Column(String(100), nullable=True)

    sbc_id = Column(Integer, ForeignKey("sbcs.id"), nullable=True)
    sbc = relationship("SBC", back_populates="users", foreign_keys=[sbc_id])

    created_bcs = relationship(
        "BonDeCommande",
        back_populates="creator",
        foreign_keys="BonDeCommande.creator_id",
    )

    notifications = relationship("Notification", back_populates="recipient")


# =========================================================
# BASIC TABLES
# =========================================================
class Account(Base):
    __tablename__ = "accounts"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), unique=True, nullable=False, index=True)


class Customer(Base):
    __tablename__ = "customers"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), unique=True, nullable=False, index=True)
    short_name = Column(String(100))


class Site(Base):
    __tablename__ = "sites"
    id = Column(Integer, primary_key=True, index=True)
    site_code = Column(String(300), unique=True, index=True, nullable=False)
    site_name = Column(String(300), nullable=True)


class InternalProject(Base):
    __tablename__ = "internal_projects"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), unique=True, index=True, nullable=False)
    project_type = Column(Enum(ProjectType))

    start_date = Column(Date)
    plan_end_date = Column(Date)
    has_extension_possibility = Column(Boolean, default=False)
    forecast_plan_details = Column(String(1000))
    budget_assigned = Column(Float)
    budget_period = Column(String(50))

    account_id = Column(Integer, ForeignKey("accounts.id"), nullable=True)
    direct_customer_id = Column(Integer, ForeignKey("customers.id"), nullable=True)
    final_customer_id = Column(Integer, ForeignKey("customers.id"), nullable=True)
    project_manager_id = Column(Integer, ForeignKey("users.id"), nullable=True)

    account = relationship("Account")
    direct_customer = relationship("Customer", foreign_keys=[direct_customer_id])
    final_customer = relationship("Customer", foreign_keys=[final_customer_id])
    project_manager = relationship("User", foreign_keys=[project_manager_id])

    merged_pos = relationship("MergedPO", back_populates="internal_project")


class CustomerProject(Base):
    __tablename__ = "customer_projects"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), unique=True, index=True, nullable=False)

    merged_pos = relationship("MergedPO", back_populates="customer_project")


class SiteAssignmentRule(Base):
    __tablename__ = "site_assignment_rules"

    id = Column(Integer, primary_key=True, index=True)

    starts_with = Column(String(100), nullable=True)
    ends_with = Column(String(100), nullable=True)
    contains_str = Column(String(100), nullable=True)

    customer_project_id = Column(Integer, ForeignKey("customer_projects.id"), nullable=True)
    min_publish_date = Column(Date, nullable=True)
    max_publish_date = Column(Date, nullable=True)

    internal_project_id = Column(Integer, ForeignKey("internal_projects.id"), nullable=False)

    internal_project = relationship("InternalProject")
    customer_project = relationship("CustomerProject")


class SiteProjectAllocation(Base):
    __tablename__ = "site_project_allocations"

    id = Column(Integer, primary_key=True, index=True)
    customer_project_id = Column(Integer, ForeignKey("customer_projects.id"), nullable=False)
    site_id = Column(Integer, ForeignKey("sites.id"), nullable=False)
    internal_project_id = Column(Integer, ForeignKey("internal_projects.id"), nullable=False)

    internal_project = relationship("InternalProject")
    site = relationship("Site")


# =========================================================
# RAW IMPORTS
# =========================================================
class RawPurchaseOrder(Base):
    __tablename__ = "raw_purchase_orders"

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

    internal_project_id = Column(Integer, ForeignKey("internal_projects.id"), nullable=True, index=True)
    site_id = Column(Integer, ForeignKey("sites.id"), nullable=True, index=True)
    customer_id = Column(Integer, ForeignKey("customers.id"), nullable=True, index=True)

    internal_project = relationship("InternalProject")
    site = relationship("Site")
    customer = relationship("Customer")

    is_processed = Column(Boolean, default=False, index=True)
    uploaded_at = Column(DateTime, server_default=func.now())
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
    uploaded_at = Column(DateTime, server_default=func.now())

    uploader_id = Column(Integer, ForeignKey("users.id"))
    uploader = relationship("User")


class UploadHistory(Base):
    __tablename__ = "upload_history"

    id = Column(Integer, primary_key=True, index=True)
    original_filename = Column(String(255), nullable=False)
    uploaded_at = Column(DateTime, server_default=func.now())

    status = Column(String(50), nullable=False)
    total_rows = Column(Integer, default=0)
    error_message = Column(Text, nullable=True)

    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    uploader = relationship("User")


# =========================================================
# MERGED PO
# =========================================================
class MergedPO(Base):
    __tablename__ = "merged_pos"

    id = Column(Integer, primary_key=True, index=True)

    po_id = Column(String(255), unique=True, index=True)
    raw_po_id = Column(Integer, ForeignKey("raw_purchase_orders.id"), unique=True)
    raw_po = relationship("RawPurchaseOrder", backref="merged_po")

    customer_project_id = Column(Integer, ForeignKey("customer_projects.id"), nullable=False)
    customer_project = relationship("CustomerProject", back_populates="merged_pos")

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
    assignment_status = Column(Enum(AssignmentStatus), default=AssignmentStatus.APPROVED, nullable=False)

    total_ac_amount = Column(Float, nullable=True)
    accepted_ac_amount = Column(Float, nullable=True)
    date_ac_ok = Column(Date, nullable=True)

    total_pac_amount = Column(Float, nullable=True)
    accepted_pac_amount = Column(Float, nullable=True)
    date_pac_ok = Column(Date, nullable=True)
    assignment_date = Column(DateTime, nullable=True)


# =========================================================
# TAX RULE
# =========================================================
class TaxRule(Base):
    __tablename__ = "tax_rules"

    id = Column(Integer, primary_key=True, index=True)
    category = Column(String(50), nullable=False)
    year = Column(Integer, nullable=False)
    tax_rate = Column(Float, nullable=False)

    __table_args__ = (sa.UniqueConstraint("category", "year", name="uix_tax_category_year"),)


# =========================================================
# SBC
# =========================================================
class SBC(Base):
    __tablename__ = "sbcs"

    id = Column(Integer, primary_key=True, index=True)
    
    # --- IDENTITY & ACCESS ---
    sbc_code = Column(String(50), unique=True, index=True, nullable=False) # "ID SBC"
    password_hash = Column(String(255)) # "Password" (Stored securely)
    short_name = Column(String(50), nullable=False) # "SBC Short Name"
    name = Column(String(255), nullable=False) # "SBC Name (Complete Name)"
    address = Column(String(255), nullable=True) # New
    city = Column(String(100), nullable=True)    # New

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
    contract_filename = Column(String(255), nullable=True) 

    # --- TAX REGULARIZATION ---
    has_tax_regularization = Column(Boolean, default=False) # "Attestation de regularisation fiscal"
    tax_reg_upload_date = Column(DateTime) # "Date upload" (for tax doc)
    tax_reg_end_date = Column(Date) # "Plan end date of Reg Fiscal"
    tax_reg_filename = Column(String(255), nullable=True)
  
    # --- FINANCIAL INFO ---
    rib = Column(String(50)) # "RIB"
    bank_name = Column(String(100)) # "Name of the Bank"
    ice = Column(String(50), nullable=True) # Identifiant Commun de l'Entreprise
    rc = Column(String(50), nullable=True)  
    # --- METADATA & APPROVALS ---
    created_at = Column(DateTime, default=datetime.datetime.utcnow) # "Date creation"
    
    # "Creator of the SBC (RAF)"
    creator_id = Column(Integer, ForeignKey("users.id"))
    approver_id = Column(Integer, ForeignKey("users.id"), nullable=True)

    sbc_type = Column(Enum(SBCType), nullable=False)

    creator = relationship("User", foreign_keys=[creator_id])
    approver = relationship("User", foreign_keys=[approver_id])

    users = relationship("User", back_populates="sbc", foreign_keys=[User.sbc_id])


# =========================================================
# ✅ BON DE COMMANDE (BC)  <-- هذا هو الوحيد اللي غادي تستعمل
# =========================================================
class BonDeCommande(Base):
    __tablename__ = "bon_de_commandes"

    id = Column(Integer, primary_key=True, index=True)
    bc_number = Column(String(100), unique=True, index=True)
    year = Column(Integer)

    project_id = Column(Integer, ForeignKey("internal_projects.id"), nullable=False)
    sbc_id = Column(Integer, ForeignKey("sbcs.id"), nullable=False)

    total_amount_ht = Column(Float, default=0.0)
    total_tax_amount = Column(Float, default=0.0)
    total_amount_ttc = Column(Float, default=0.0)

    status = Column(Enum(BCStatus), default=BCStatus.DRAFT)
    bc_type = Column(Enum(BCType), default=BCType.STANDARD, nullable=False)

    created_at = Column(DateTime, server_default=func.now())
    rejection_reason = Column(String(500), nullable=True)

    creator_id = Column(Integer, ForeignKey("users.id"))
    creator = relationship("User", back_populates="created_bcs", foreign_keys=[creator_id])

    internal_project = relationship("InternalProject")
    sbc = relationship("SBC")

    items = relationship(
        "BCItem",
        back_populates="bc",
        cascade="all, delete-orphan",
    )

    submitted_at = Column(DateTime, nullable=True)

    # L1 Approval (PD)
    approver_l1_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    approved_l1_at = Column(DateTime, nullable=True)

    # L2 Approval (Admin)
    approver_l2_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    approved_l2_at = Column(DateTime, nullable=True)

    approver_l1 = relationship("User", foreign_keys=[approver_l1_id])
    approver_l2 = relationship("User", foreign_keys=[approver_l2_id])


class BCItem(Base):
    __tablename__ = "bc_items"

    id = Column(Integer, primary_key=True, index=True)
    bc_id = Column(Integer, ForeignKey("bon_de_commandes.id"), nullable=False)
    merged_po_id = Column(Integer, ForeignKey("merged_pos.id"), nullable=False)

    rate_sbc = Column(Float, default=0.0)
    quantity_sbc = Column(Float)

    unit_price_sbc = Column(Float)
    line_amount_sbc = Column(Float)

    applied_tax_rate = Column(Float, default=0.0)

    bc = relationship("BonDeCommande", back_populates="items")
    merged_po = relationship("MergedPO")

    qc_validation_status = Column(Enum(ValidationState), default=ValidationState.PENDING)
    pm_validation_status = Column(Enum(ValidationState), default=ValidationState.PENDING)

    global_status = Column(Enum(ItemGlobalStatus), default=ItemGlobalStatus.PENDING)

    rejection_count = Column(Integer, default=0)
    postponed_until = Column(DateTime, nullable=True)

    act_id = Column(Integer, ForeignKey("service_acceptances.id"), nullable=True)
    act = relationship("ServiceAcceptance", back_populates="items")

    rejection_history = relationship("ItemRejectionHistory", back_populates="item")


# =========================================================
# NOTIFICATIONS + ACT
# =========================================================
class Notification(Base):
    __tablename__ = "notifications"

    id = Column(Integer, primary_key=True, index=True)
    recipient_id = Column(Integer, ForeignKey("users.id"), nullable=False)

    type = Column(Enum(NotificationType), nullable=False)
    title = Column(String(255), nullable=False)
    message = Column(Text, nullable=False)
    link = Column(String(500), nullable=True)

    is_read = Column(Boolean, default=False)
    created_at = Column(DateTime, server_default=func.now())

    recipient = relationship("User", back_populates="notifications")


class ServiceAcceptance(Base):
    __tablename__ = "service_acceptances"

    id = Column(Integer, primary_key=True, index=True)
    act_number = Column(String(50), unique=True, nullable=False)

    bc_id = Column(Integer, ForeignKey("bon_de_commandes.id"))
    created_at = Column(DateTime, server_default=func.now())

    creator_id = Column(Integer, ForeignKey("users.id"))
    creator = relationship("User")

    bc = relationship("BonDeCommande")
    items = relationship("BCItem", back_populates="act")

    file_path = Column(String(500), nullable=True)

    total_amount_ht = Column(Float, default=0.0)
    total_tax_amount = Column(Float, default=0.0)
    total_amount_ttc = Column(Float, default=0.0)
    applied_tax_rate = Column(Float, default=0.0)


class ItemRejectionHistory(Base):
    __tablename__ = "item_rejection_history"

    id = Column(Integer, primary_key=True)

    bc_item_id = Column(Integer, ForeignKey("bc_items.id"), nullable=False)
    rejected_by_id = Column(Integer, ForeignKey("users.id"), nullable=False)

    comment = Column(Text, nullable=False)
    rejected_at = Column(DateTime, server_default=func.now())

    item = relationship("BCItem", back_populates="rejection_history")
    rejected_by = relationship("User")
# 1. The Wallet (One per User)
class Caisse(Base):
    __tablename__ = "caisses"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), unique=True, nullable=False)
    balance = Column(Float, default=0.0)
    
    # Relationships
    user = relationship("User", backref="caisse")
    transactions = relationship("Transaction", back_populates="caisse")


# 2. The Parent Request (Created by PD)
class FundRequest(Base):
    __tablename__ = "fund_requests"
    
    id = Column(Integer, primary_key=True, index=True)
    request_number = Column(String(50), unique=True, index=True) # e.g. REQ-2025-001
    
    requester_id = Column(Integer, ForeignKey("users.id")) # The PD
    approver_id = Column(Integer, ForeignKey("users.id"), nullable=True) # The Admin
    
    status = Column(String(50), default=FundRequestStatus.PENDING_APPROVAL)
    paid_amount = Column(Float, default=0.0) 
    admin_comment = Column(Text, nullable=True) # For rejection or partial notes

    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    approved_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    
    # Relationships
    requester = relationship("User", foreign_keys=[requester_id])
    approver = relationship("User", foreign_keys=[approver_id])
    items = relationship("FundRequestItem", back_populates="request")


# 3. The Items inside a Request (Amount per PM)
class FundRequestItem(Base):
    __tablename__ = "fund_request_items"
    
    id = Column(Integer, primary_key=True, index=True)
    request_id = Column(Integer, ForeignKey("fund_requests.id"), nullable=False)
    
    target_pm_id = Column(Integer, ForeignKey("users.id"), nullable=False) # Who gets the money?
    
    requested_amount = Column(Float, nullable=False) # Amount asked by PD
    approved_amount = Column(Float, nullable=True)   # Amount approved by Admin (can be different)
    remarque = Column(String(255), nullable=True) # Description for this line item
    admin_note = Column(String(255), nullable=True) 
    # Relationships
    request = relationship("FundRequest", back_populates="items")
    target_pm = relationship("User", foreign_keys=[target_pm_id])


# 4. The Ledger (History of movements)
class Transaction(Base):
    __tablename__ = "transactions"
    
    id = Column(Integer, primary_key=True, index=True)
    caisse_id = Column(Integer, ForeignKey("caisses.id"), nullable=False)
    
    type = Column(Enum(TransactionType), nullable=False) # CREDIT or DEBIT
    amount = Column(Float, nullable=False)
    description = Column(String(255))
    
    # Optional links for traceability
    related_request_id = Column(Integer, ForeignKey("fund_requests.id"), nullable=True)
    
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    created_by_id = Column(Integer, ForeignKey("users.id")) # Who performed the action
    
    # Relationships
    caisse = relationship("Caisse", back_populates="transactions")
    created_by = relationship("User", foreign_keys=[created_by_id])
    
class UserPerformanceTarget(Base):
    __tablename__ = "user_performance_targets"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    year = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)

    # --- PO RECEIVE ---
    # The target set at the start of the year
    po_master_plan = Column(Float, default=0.0) 
    # The adjusted target set at the start of the month
    po_monthly_update = Column(Float, default=0.0) 
    
    # --- ACCEPTANCE ---
    # The target set at the start of the year
    acceptance_master_plan = Column(Float, default=0.0) 
    # The adjusted target set at the start of the month
    acceptance_monthly_update = Column(Float, default=0.0)

    user = relationship("User")

    # Add a unique constraint to prevent duplicate rows for same user/year/month
    __table_args__ = (
        sa.UniqueConstraint('user_id', 'year', 'month', name='uix_user_year_month'),
    )
