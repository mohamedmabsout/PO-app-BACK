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
    INTERNAL = "Internal"
    CUSTOMER_FIXED_PRICE = "Customer - Fixed Price"
    CUSTOMER_TIME_MATERIAL = "Customer - Time & Material"