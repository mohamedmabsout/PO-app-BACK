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

# --- Endpoint pour créer un utilisateur (inchangé) ---
@router.post("/", response_model=schemas.User)
def create_new_user(user: schemas.UserCreate, db: Session = Depends(get_db)):
    db_user_email = crud.get_user_by_email(db, email=user.email)
    if db_user_email:
        raise HTTPException(status_code=400, detail="Email already registered")
    
    db_user_username = crud.get_user_by_username(db, username=user.username)
    if db_user_username:
        raise HTTPException(status_code=400, detail="Username already taken")
        
    return crud.create_user(db=db, user=user)

# --- Endpoint pour lire tous les utilisateurs (inchangé) ---
@router.get("/", response_model=List[schemas.User])
def read_users(
    db: Session = Depends(get_db), 
    skip: int = 0, 
    limit: int = 100,
    current_user: models.User = Depends(auth.get_current_user)
):
    users = crud.get_users(db, skip=skip, limit=limit)
    return users

# --- NOUVEAU : Endpoint pour lire UN SEUL utilisateur par ID ---
# C'est cette route qui manquait et causait l'erreur 404 sur votre page d'édition.
@router.get("/{user_id}", response_model=schemas.User)
def read_user(
    user_id: int, 
    db: Session = Depends(get_db), 
    current_user: models.User = Depends(auth.get_current_user)
):
    db_user = crud.get_user(db, user_id=user_id)
    if db_user is None:
        raise HTTPException(status_code=404, detail="User not found")
    return db_user

# --- NOUVEAU : Endpoint pour MODIFIER un utilisateur (Edit / PUT) ---
@router.put("/{user_id}", response_model=schemas.User)
def update_user_details(
    user_id: int,
    user_update: schemas.UserUpdate, # On utilise un schéma de mise à jour
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    db_user = crud.get_user(db, user_id=user_id)
    if db_user is None:
        raise HTTPException(status_code=404, detail="User not found")
    
    # On passe l'objet utilisateur existant et les données de mise à jour à la fonction CRUD
    updated_user = crud.update_user(db=db, db_user=db_user, user_update=user_update)
    return updated_user

# --- NOUVEAU : Endpoint pour SUPPRIMER un utilisateur (Delete) ---
@router.delete("/{user_id}", response_model=schemas.User)
def delete_user_by_id(
    user_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    db_user = crud.get_user(db, user_id=user_id)
    if db_user is None:
        raise HTTPException(status_code=404, detail="User not found")

    deleted_user = crud.delete_user(db=db, db_user=db_user)
    return deleted_user
