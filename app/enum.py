# in app/enums.py
import enum

class UserRole(str, enum.Enum):
    SBC = "SBC"
    QUALITY = "Quality"
    RAF = "RAF"
    PM = "PM"  # Changez de "Project Manager" à "PM"
    PD = "PD"  # Changez de "Project Director" à "PD"
    ADMIN = "ADMIN"  # Changez de "Admin" à "ADMIN"
    COORDINATEUR = "coordinateur"
    CEO = "ceo"
class ProjectType(str, enum.Enum):
    FIXED_PRICE = "Fixed Price"
    TIME_MATERIAL = "Time & Material"
    INTERNAL = "Internal"
    TBD = "TBD"
class SBCStatus(str, enum.Enum):
    UNDER_APPROVAL = "Under Approval"
    ACTIVE = "Active"
    BLACKLISTED = "Blacklisted"
    DRAFT = "Draft" # Optional, if RAF saves before submitting
class BCStatus(str, enum.Enum):
    PENDING_L2 = "PENDING_L2"
    CEO_APPROVAL = "Under CEO Approval"
    ACTIVE = "ACTIVE"
    BLACKLISTED = "BLACKLISTED"
    DRAFT = "DRAFT" # Optional, if RAF saves before submitting
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    SUBMITTED = "SUBMITTED"   # <-- NEW STATE

class BCType(str, enum.Enum):
    STANDARD = "STANDARD"
    PERSONNE_PHYSIQUE = "PERSONNE_PHYSIQUE"
class SBCType(str, enum.Enum):
    PP = "PP"
    ENTREPRISE = "ENTREPRISE"

class NotificationType(str, enum.Enum):
    TODO = "TODO"       # Action Required (e.g., Approve BC)
    APP = "APP"         # Status Update (e.g., Your BC was approved)
    SYSTEM = "SYSTEM"  
    ALERT = "ALERT" 


class NotificationModule(str, enum.Enum):
    BC = "BC"                 # Purchase Orders
    EXP = "EXP"               # Expenses / Petty Cash
    CAISSE = "CAISSE"         # Fund Requests / Refills
    ACCEPTANCE = "ACCEPTANCE" # ACT / Work Validation
    DISPATCH = "DISPATCH"     # Site Assignments
    SYSTEM = "SYSTEM"         # SBC Approval, Targets, Compliance
    FACTURATION = "FACTURATION"
    SBC_ACCOUNT = "SBC_ACCOUNT" # New module for Ledger/Balance alerts



class AssignmentStatus(str, enum.Enum):
    APPROVED = "APPROVED" # Normal state
    PENDING_APPROVAL = "PENDING_APPROVAL" # Waiting for PM
    REJECTED = "REJECTED" # PM rejected the assignment


 # Alerts (e.g., Export finished, Maintenance)
class ValidationState(str, enum.Enum):
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"

class ItemGlobalStatus(str, enum.Enum):
    PENDING = "PENDING"
    PENDING_PD_APPROVAL = "PENDING_PD_APPROVAL" # <-- NEW
    OPEN = "OPEN"               # Validating
    POSTPONED = "POSTPONED"     # In the 3-week penalty box
    READY_FOR_ACT = "READY_FOR_ACT" # QC & PM both Approved
    ACCEPTED = "ACCEPTED"       # Included in an ACT
    PERMANENTLY_REJECTED = "PERMANENTLY_REJECTED" # > 5 rejections
class FundRequestStatus(str, enum.Enum):
    PENDING_APPROVAL = "PENDING_APPROVAL"
    APPROVED_WAITING_FUNDS = "APPROVED_WAITING_FUNDS"
    COMPLETED = "COMPLETED"
    REJECTED = "REJECTED"
    # --- NEW STATUSES ---
    PARTIALLY_PAID = "PARTIALLY_PAID"
    CLOSED_PARTIAL = "CLOSED_PARTIAL" 

class TransactionType(str, enum.Enum):
    CREDIT = "CREDIT"   # Money IN (Refill)
    DEBIT = "DEBIT"     # Money OUT (Expense)
class RefillRequestStatus(str, enum.Enum):
    PENDING = "PENDING"
    APPROVED = "APPROVED"       # Fully paid
    REJECTED = "REJECTED"
    PARTIALLY_PAID = "PARTIALLY_PAID" # New: Still open, but some money given
    CLOSED_PARTIAL = "CLOSED_PARTIAL" # New: Closed, but not fully paid (e.g. 500/1000 given and closed)

class ExpenseStatus(str, enum.Enum):
    DRAFT = "DRAFT"
    SUBMITTED = "SUBMITTED"     # PM submitted (Reserved)
    PENDING_L1 = "PENDING_L1"   # Wait PD
    APPROVED_L1 = "APPROVED_L1" # PD Approved
    PENDING_L2 = "PENDING_L2"   # Wait Admin
    APPROVED_L2 = "APPROVED_L2" # Admin Approved (Ready for Payment)
    PAID = "PAID"               # PD Confirmed Payment (Deducted)
    ACKNOWLEDGED = "ACKNOWLEDGED" # Beneficiary confirmed receipt
    REJECTED = "REJECTED"
class TransactionStatus(str, enum.Enum):
    PENDING = "PENDING"
    COMPLETED = "COMPLETED"


class InvoiceStatus(str, enum.Enum):
    DRAFT = "DRAFT"
    SUBMITTED = "SUBMITTED"   # Pending RAF Verification
    VERIFIED = "VERIFIED"     # Pending Payment
    PAID = "PAID"             # Payment confirmation uploaded
    ACKNOWLEDGED = "ACKNOWLEDGED" # SBC confirmed receipt
    REJECTED = "REJECTED"
