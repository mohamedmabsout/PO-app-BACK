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
    # Add this line:
    TBD = "TBD"
class SBCStatus(str, enum.Enum):
    UNDER_APPROVAL = "Under Approval"
    ACTIVE = "Active"
    BLACKLISTED = "Blacklisted"
    DRAFT = "Draft" # Optional, if RAF saves before submitting
class BCStatus(str, enum.Enum):
    PD_APPROVAL = "Under PD Approval"
    CEO_APPROVAL = "Under CEO Approval"
    ACTIVE = "Active"
    BLACKLISTED = "Blacklisted"
    DRAFT = "Draft" # Optional, if RAF saves before submitting