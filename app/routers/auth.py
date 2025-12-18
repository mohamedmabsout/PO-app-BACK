# in app/routers/auth.py
from datetime import timedelta
import secrets
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from fastapi_mail import FastMail, MessageSchema, MessageType
from sqlalchemy.orm import Session
from ..config import conf

from .. import crud, auth, schemas, config, models
from ..dependencies import get_db,require_admin

router = APIRouter(
    prefix="/api/auth",  # Let's prefix all auth routes with /api/auth
    tags=["authentication"],
)

@router.post("/login", response_model=schemas.Token)
def login_for_access_token(db: Session = Depends(get_db), form_data: OAuth2PasswordRequestForm = Depends()):
    user = crud.get_user_by_username(db, username=form_data.username)
    if not user or not auth.verify_password(form_data.password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    access_token_expires = timedelta(days=config.settings.ACCESS_TOKEN_EXPIRE_DAYS)
    
    # --- CHANGE IS HERE ---
    # We add the role to the payload
    access_token = auth.create_access_token(
        data={
            "sub": user.username, 
            "role": user.role.value, # Convert Enum to string
            "id": user.id 
        }, 
        expires_delta=access_token_expires
    )

    return {"access_token": access_token, "token_type": "bearer"}


# --- NEW REGISTRATION ENDPOINT ---
@router.post("/register", response_model=schemas.User)
def register_user(user: schemas.UserCreate, db: Session = Depends(get_db)): # <--- CORRECTED
    db_user_email = crud.get_user_by_email(db, email=user.email)
    if db_user_email:
        raise HTTPException(status_code=400, detail="Email already registered")
    
    db_user_username = crud.get_user_by_username(db, username=user.username)
    if db_user_username:
        raise HTTPException(status_code=400, detail="Username already taken")
            
    return crud.create_user(db=db, user=user)

@router.post("/reset-password")
def reset_password(
    payload: schemas.PasswordResetRequest,
    db: Session = Depends(get_db)
):
    # 1. Find user by token
    user = db.query(models.User).filter(models.User.reset_token == payload.token).first()
    
    if not user:
        raise HTTPException(status_code=400, detail="Invalid or expired token.")
    
    # 2. Hash new password
    hashed_password = auth.get_password_hash(payload.new_password)
    
    # 3. Update User
    user.hashed_password = hashed_password
    user.reset_token = None # Invalidate token (important for security!)
    db.commit()
    
    return {"message": "Password updated successfully"}
@router.post("/forgot-password")
async def forgot_password(
    payload: schemas.ForgotPasswordRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    user = crud.get_user_by_email(db, payload.email)
    if not user:
        # Security: Don't reveal if user exists. Just return 200.
        return {"message": "If that email exists, a reset link has been sent."}

    # Generate token
    token = secrets.token_urlsafe(32)
    user.reset_token = token
    db.commit()

    # Send Email
    reset_link = f"https://po.sib.co.ma/reset-password?token={token}"
    message = MessageSchema(
        subject="Welcome to SIB PO App - Set your Password",
        recipients=[user.email],
        body=f"""
        <p>Hello {user.first_name},</p>
        <p>Your account has been created.</p>
        <p>Please click the link below to set your password and access the system:</p>
        <a href="{reset_link}">Set Password</a>
        """,
        subtype=MessageType.html
    )
    
    fm = FastMail(conf)
    await fm.send_message(message)
    
    return {"message": "User invited and email sent."}
@router.post("/impersonate/{user_id}", response_model=schemas.Token)
def impersonate_user(
    user_id: int, 
    db: Session = Depends(get_db), 
    current_admin: models.User = Depends(require_admin) # SECURITY CRITICAL
):
    """
    Generates a token for the target user without password check.
    Only accessible by Admins.
    """
    # 1. Find the target user
    target_user = crud.get_user(db, user_id)
    if not target_user:
        raise HTTPException(status_code=404, detail="User not found")
        
    # 2. Prevent impersonating another Admin (optional safety check)
    if target_user.role == "ADMIN" and target_user.id != current_admin.id:
         # You might want to allow this, but blocking it prevents 'super-admin' escalation issues
         pass 

    # 3. Create a token for the target user
    access_token_expires = timedelta(minutes=conf.settings.ACCESS_TOKEN_EXPIRE_DAYS)
    access_token = auth.create_access_token(
        data={"sub": target_user.username, "role": target_user.role}, # Add claims as needed
        expires_delta=access_token_expires
    )
    
    return {"access_token": access_token, "token_type": "bearer"}
@router.post("/change-password")
def change_password(
    payload: ChangePasswordSchema,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_active_user)
):
    # 1. Verify Old Password
    if not auth.verify_password(payload.current_password, current_user.hashed_password):
        raise HTTPException(status_code=400, detail="Incorrect current password")
    
    # 2. Set New Password
    current_user.hashed_password = auth.get_password_hash(payload.new_password)
    db.commit()
    
    return {"message": "Password changed successfully"}
