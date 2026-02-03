import os
import re
from sqlalchemy.orm import Session
from app.database import SessionLocal
from app import models

def repair_database_links():
    db = SessionLocal()
    upload_dir = "uploads/expenses"
    
    if not os.path.exists(upload_dir):
        print("Upload directory not found!")
        return

    files = os.listdir(upload_dir)
    updated_count = 0

    print(f"Scanning {len(files)} files...")

    for filename in files:
        # Regex to find the ID: Looks for EXP_ followed by digits
        match = re.search(r'EXP_(\d+)_', filename)
        
        if match:
            expense_id = int(match.group(1))
            
            # Find this expense in the DB
            expense = db.query(models.Expense).get(expense_id)
            
            if expense:
                # Update the columns
                expense.attachment = filename
                expense.signed_doc_url = filename
                expense.is_signed_copy_uploaded = True
                updated_count += 1
                print(f"✅ Linked file {filename} to Expense #{expense_id}")
            else:
                print(f"⚠️ File {filename} found, but Expense #{expense_id} does not exist in DB.")
        else:
            # Handle files like Voucher_90.pdf (if they don't follow the EXP_ID pattern)
            print(f"ℹ️ Skipping {filename} (Pattern not recognized)")

    db.commit()
    db.close()
    print(f"\nDONE! Successfully repaired {updated_count} database records.")

if __name__ == "__main__":
    repair_database_links()