from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from starlette.responses import StreamingResponse
import pandas as pd
import io
from typing import Optional

from .. import crud, models, auth
from ..dependencies import get_db

router = APIRouter(
    prefix="/api/export",
    tags=["Exports"],
    dependencies=[Depends(auth.get_current_user)] # On sécurise l'endpoint
)

@router.get("/remaining-to-accept")
def export_remaining_to_accept(
    # --- On ajoute les mêmes paramètres de filtre que le frontend envoie ---
    filter_stage: str = "ALL",
    search: Optional[str] = None,
    internal_project_id: Optional[int] = None,
    customer_project_id: Optional[int] = None,
    db: Session = Depends(get_db),
        current_user: models.User = Depends(auth.get_current_user)

):
    """
    Génère et renvoie un fichier Excel en se basant sur les filtres fournis.
    """
    # On passe tous les filtres à la fonction CRUD
    df = crud.get_remaining_to_accept_dataframe(
        db=db,
        filter_stage=filter_stage,
        search=search,
        internal_project_id=internal_project_id,
        customer_project_id=customer_project_id
    )

    if df.empty:
        raise HTTPException(status_code=404, detail="No data to export for the selected filters.")
    # 2. On prépare le fichier Excel en mémoire
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Remaining_To_Accept')

        # Optionnel : ajuster la largeur des colonnes
        worksheet = writer.sheets['Remaining_To_Accept']
        for idx, col in enumerate(df):
            series = df[col]
            max_len = max((series.astype(str).map(len).max(), len(str(series.name)))) + 2
            worksheet.set_column(idx, idx, max_len)

    output.seek(0)

    # 3. On définit les en-têtes pour le téléchargement
    headers = {
        'Content-Disposition': 'attachment; filename="remaining_to_accept.xlsx"'
    }

    # 4. On renvoie le fichier
    return StreamingResponse(
        output, 
        headers=headers, 
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
