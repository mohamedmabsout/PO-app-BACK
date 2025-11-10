# in app/routers/users.py
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List

from .. import crud, schemas, models, auth
from ..dependencies import get_db

router = APIRouter(
    prefix="/api/users",
    tags=["users"]
)

@router.post("/", response_model=schemas.User)
def create_new_user(user: schemas.UserCreate, db: Session = Depends(get_db)):
    db_user_email = crud.get_user_by_email(db, email=user.email)
    if db_user_email:
        raise HTTPException(status_code=400, detail="Email already registered")
    
    db_user_username = crud.get_user_by_username(db, username=user.username)
    if db_user_username:
        raise HTTPException(status_code=400, detail="Username already taken")
        
    return crud.create_user(db=db, user=user)
@router.get("/", response_model=List[schemas.User])
def read_users(
    db: Session = Depends(get_db), 
    skip: int = 0, 
    limit: int = 100,
    # This is the magic line. FastAPI will not run this function unless
    # get_current_user succeeds.
    current_user: models.User = Depends(auth.get_current_user)
):
    users = crud.get_users(db, skip=skip, limit=limit) # We need to create this CRUD function
    return users
