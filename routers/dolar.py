# Python
import json
from pydantic import BaseModel

# FastApi

from fastapi import  HTTPException
from fastapi import status
from fastapi import Path, Query
from fastapi import APIRouter

# Modulos
from queries.data_dolar import get_individual_dolar, data_dolar


router = APIRouter()



# Obtengo tipo de dolar solicitado y su rendimiento historico

@router.get(
    path='/dolar/{dolar_type}',
    status_code=status.HTTP_200_OK,
    summary="Get information about a dolar type",
    tags=["Dolar"]
    )
async def get_dolar_and_return(
    dolar_type:str = Path(...,
                      title="Dolar Type",
                      description="Ingresar un tipo de dolar de las siguientes opciones: (Dolar CCL, Dolar Mayorista, BLUE, Dolar MEP, Dolar Oficial, Dolar Solidario)", 
                      min_length=1,
                      example="BLUE")                                    
    ):
    """
        Obtención de la información correspondiente al dolar seleccionado y su rendimiento
    """        
    df = get_individual_dolar(dolar_type)
    for col in df.columns[1:]:
        df[f"Return-{col.title()}"] = ((1 + (df[col].pct_change())).cumprod()-1)*100
    df = df[~df[df.columns[2]].isnull()]   
    if len (df)!=0:
        js =  json.loads(df.to_json(orient = 'records'))
        return js
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dolar no encontrado")

# Obtengo el precio del dolar solicitado por fecha seleccionada
@router.get(
    path='/dolar/',
    status_code=status.HTTP_200_OK,
    summary="Get information about a dolar type by date",
    tags=["Dolar"]
    )
async def get_dolar_by_date(
    dolar_type:str = Query(...,
                      title="Dolar Type",
                      description="Ingresar un tipo de dolar listado en la base de datos: Dolar CCL, Dolar Mayorista, BLUE, Dolar MEP, Dolar Oficial, Dolar Solidario", 
                      min_length=1,
                      example="BLUE"),
    date_from:str = Query(...,
                      title="date from",
                      description="Ingresar una fecha desde la cual hacer la consulta", 
                      min_length=1,
                      example="2022-01-01"),
    date_to:str = Query(...,
                      title="date from",
                      description="Ingresar una fecha hasta la cual hacer la consulta", 
                      min_length=1,
                      example="2023-03-01")                                    
    ):
    """
        Obtención de la información correspondiente al dolar seleccionado por fecha
    """        
    df = get_individual_dolar(dolar_type)
    df = df.query(f"Date >= '{date_from}' and Date <= '{date_to}'")
    for col in df.columns[1:]:
        df[f"Return-{col.title()}"] = ((1 + (df[col].pct_change())).cumprod()-1)*100
    df = df[~df[df.columns[2]].isnull()]
    if len (df)!=0:
        js =  json.loads(df.to_json(orient = 'records'))
        return js
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dolar no encontrado")
