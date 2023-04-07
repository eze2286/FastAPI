# Typing
from typing import Union

# FastApi
from fastapi import HTTPException
from fastapi import status
from fastapi import Path
from fastapi import APIRouter

# Modulos
from queries.data_dolar import data_dolar
from queries.data_inflacion import get_connection_to_data_base, data_inflation
from queries.data_assets import list_assets, acciones_urls

router = APIRouter()

#Actualizar la data de la tabla seleccionada por el usuario. En caso de seleccionar la tabla de assets se debe seleccionar que activo se quiere actualizar. Por ejemplo: TSLA
@router.put(
path='/upload_data/{table_to_update}',
status_code= status.HTTP_201_CREATED,
summary="Update a choice table",
tags=["Data Update"]
)
async def post_new_asset(
    table_to_update:str = Path(...,
                      title="Table to update",
                      description="Ingresar el nombre de 1 de las 3 tablas disponibles para que sea actualizada (dolar, inflacion, assets). En caso de elegir la tabla de assets, debe elegir que activo quiere que sea actualzado. Ejemplo: TSLA", 
                      min_length=1,
                      example="dolar"),
    asset_code: Union[str, None] = None
    ):
    """
    Actualizar la tabla seleccionada por el usuario
    """
    con = get_connection_to_data_base()
    if table_to_update == 'inflacion':
        try:
            df_to_update = data_inflation()
            records = list(df_to_update.itertuples(index=True, name=None))
            cursor = con.cursor()
            cursor.executemany('INSERT OR IGNORE INTO inflacion VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);', records)
            con.commit()
            cursor.close()
            con.close()
            return {f"Table <<{table_to_update}>> ": "was updated successfully"}
        except:
            con.close()
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="table not found")
    elif table_to_update == 'dolar':
        try:
            df_to_update = data_dolar()
            records = list(df_to_update.itertuples(index=True, name=None))
            cursor = con.cursor()
            cursor.executemany('INSERT OR IGNORE INTO dolar VALUES(?,?,?,?,?,?,?);', records)
            con.commit()
            cursor.close()
            con.close()
            return {f"Table <<{table_to_update}>> ": "was updated successfully"}
        except:
            con.close()
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="table not found")
    else:    
        asset = asset_code
        list_of_assets = list_assets(con)
        if asset in list_of_assets:
            try:    
                df_with_asset_upload = acciones_urls(asset)
                df_with_asset_upload.to_sql('assets',con, if_exists="append", index=False)
                con.close()        
                return {f"Asset <<{asset}>> ": "was uploaded successfully"}
            except:
                con.close()
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Asset not found")
        else:
            con.close()
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail = f"Asset {asset}: " "is not in database, load it with POST method")
