# Python
import json

# FastApi

from fastapi import HTTPException
from fastapi import status
from fastapi import Query
from fastapi import APIRouter

# Modulos

from queries.data_inflacion import get_inflation

router = APIRouter()


# Obtengo el indíce de inflacion y la inflación acumulada en periodo seleccionado en la categoría seleccionada
@router.get(
    path='/inflacion/',
    status_code=status.HTTP_200_OK,
    summary="Get information about  inflation by date and category (base=2016)",
    tags=["Inflation"]
    )
async def get_inflation_by_date(
    inflation_type:str = Query(...,
                      title="Inflation Type",
                      description="Ingresar una categoría listada en la base de datos: Nivel general, Servicios, Núcleo, etc", 
                      min_length=1,
                      example="Nivel general"),
    date_from:str = Query(...,
                      title="date from",
                      description="Ingresar una fecha desde la cual hacer la consulta", 
                      min_length=1,
                      example="2022-01-01"),
    date_to:str = Query(...,
                      title="date from",
                      description="Ingresar una fecha hasta la cual hacer la consulta", 
                      min_length=1,
                      example="2023-02-01")                                    
    ):
    """
        Obtención de la información correspondiente a la categoría seleccionada por fecha
    """        
    df = get_inflation(inflation_type)
    df = df.query(f"Date >= '{date_from}' and Date <= '{date_to}'")
    for col in df.columns[1:]:
        df[f"Cumalative Inflation-{col.title()}"] = ((1 + (df[col].pct_change())).cumprod()-1)*100
    df = df[~df[df.columns[0]].isnull()]
    if len (df)!=0:
        js =  json.loads(df.to_json(orient = 'records'))
        return js
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Categoría no encontrada")
