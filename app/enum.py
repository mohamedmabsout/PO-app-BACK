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
