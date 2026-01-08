# in app/enums.py
import enum

class UserRole(str, enum.Enum):
    SBC = "SBC"
    QUALITY = "Quality"
    RAF = "RAF"
    PM = "Project Manager"
    PD = "Project Director"
    ADMIN = "Admin"

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
    PENDING_APPROVAL = "PENDING_APPROVAL"     # PD asked, Admin hasn't seen it
    APPROVED_WAITING_FUNDS = "APPROVED_WAITING_FUNDS" # Admin approved, money on the way
    COMPLETED = "COMPLETED"                   # PD received money, added to wallets
    REJECTED = "REJECTED"

class TransactionType(str, enum.Enum):
    CREDIT = "CREDIT"   # Money IN (Refill)
    DEBIT = "DEBIT"     # Money OUT (Expense)
