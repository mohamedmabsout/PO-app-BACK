# backend/app/routers/notifications.py
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import List, Dict
from .. import crud, models, schemas, auth
from ..dependencies import get_db

router = APIRouter(
    prefix="/api/notifications",
    tags=["Notifications"],
    dependencies=[Depends(auth.get_current_user)]
)

# @router.get("/dashboard-widget")
# def get_dashboard_notifications(
#     db: Session = Depends(get_db), 
#     current_user: models.User = Depends(auth.get_current_user)
# ):
#     """
#     Returns notifications split into 'todo' and 'alerts' for the dashboard widget.
#     """
#     # Fetch all unread notifications for the user
#     all_notifs = db.query(models.Notification).filter(
#         models.Notification.recipient_id == current_user.id,
#         models.Notification.is_read == False
#     ).order_by(models.Notification.created_at.desc()).limit(50).all()

#     todos = []
#     alerts = []

#     for n in all_notifs:
#         item = {
#             "id": n.id,
#             "title": n.title,
#             "desc": n.message,
#             "link": n.link,
#             "time": n.created_at.isoformat() if n.created_at else "", # <--- FIX HERE
#             # Map types to UI styling
#             "priority": "High" if n.type == "TODO" else "Info",
#             "badgeBg": "danger" if n.type == "TODO" else "info",
#             "icon": "fe fe-check-circle" if n.type == "TODO" else "fe fe-bell",
#             "color": "text-danger" if n.type == "TODO" else "text-primary"
#         }
        
#         if n.type == models.NotificationType.TODO:
#             todos.append(item)
#         else:
#             alerts.append(item)

#     # 2. Get Virtual Notifications
#     virtual_todos = crud.check_system_state_notifications(db, current_user)
    
#     # 3. Combine them (Virtual ones first as they are actionable)
#     final_todos = virtual_todos + todos

#     return {"todos": final_todos, "alerts": alerts}
# # Also add an endpoint to mark as read


@router.get("/dashboard-widget")
def get_notification_widget_data(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    # 1. Fetch Real Notifications from DB
    notifications = db.query(models.Notification).filter(
        models.Notification.recipient_id == current_user.id,
        models.Notification.is_read == False
    ).order_by(models.Notification.created_at.desc()).all()

    # 2. Get Virtual Notifications (Calculated on the fly)
    # These are always 'TODO' type and 'SYSTEM' module
    virtual_items = crud.check_system_state_notifications(db, current_user)

    # 3. Organize the Response
    response = {
        "todos": {
            "BC": [], "EXP": [], "CAISSE": [], 
            "ACCEPTANCE": [], "DISPATCH": [], "SYSTEM": []
        },
        "alerts": []
    }

    # Process DB Notifications
    for n in notifications:
        item = {
            "id": n.id,
            "title": n.title,
            "desc": n.message,
            "link": n.link,
            "time": n.created_at,
            "type": n.type
        }
        
        if n.type == models.NotificationType.TODO:
            mod_key = n.module.value if n.module else "SYSTEM"
            if mod_key in response["todos"]:
                response["todos"][mod_key].append(item)
        else:
            # Alerts / APP updates go here
            response["alerts"].append(item)

    # Add Virtual Items to SYSTEM To-Do
    for v in virtual_items:
        response["todos"]["SYSTEM"].append({
            "id": v["id"],
            "title": v["title"],
            "desc": v["desc"],
            "link": v["link"],
            "time": v["created_at"],
            "is_virtual": True
        })

    return response



@router.get("/my", response_model=List[schemas.NotificationItem]) # Define schema
def get_my_notifications_endpoint(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    return crud.get_my_notifications(db, current_user.id)

@router.post("/{id}/read")
def mark_notification_read_endpoint(
    id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    crud.mark_notification_read(db, id, current_user.id)
    return {"ok": True}

@router.get("/dashboard-summary")
def get_dashboard_summary(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    # This query groups by module and counts unread items
    results = db.query(
        models.Notification.module, 
        func.count(models.Notification.id)
    ).filter(
        models.Notification.recipient_id == current_user.id,
        models.Notification.is_read == False
    ).group_by(models.Notification.module).all()

    # Initialize all modules to zero
    summary = { m.value: 0 for m in models.NotificationModule }
    
    # Fill with actual data
    for module_name, count in results:
        summary[module_name] = count

    return summary
