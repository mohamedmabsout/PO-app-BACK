from fastapi import Depends, HTTPException, status
from app.auth import get_current_user
from app import models
from app.routers import auth


def is_pd_or_admin(user: models.User = Depends(get_current_user)):
    if user.role not in ["Project Director", "PD", "Admin"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only Project Director or Admin allowed"
        )
    return user

def _role_str(user: models.User) -> str:
    # au cas où role est Enum ou string
    return str(user.role).upper()

def is_admin(user: models.User = Depends(get_current_user)):
    if user.role != "Admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only Admin allowed"
        )
    return user
def is_pm(current_user: models.User = Depends(auth.get_current_user)):
    if "PM" not in _role_str(current_user):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="PM only")
    return current_user

def is_pd(current_user = Depends(get_current_user)):
    if current_user.role != "Project Director":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only Project Director can do this action"
        )
    return current_user
