from fastapi import APIRouter, UploadFile, File, HTTPException, Query
import shutil
import os
from app.services.futebol_service import FutebolService

router = APIRouter()

DB_PATH = "futebol.duckdb"
TABLE_NAME = "calendario_libertadores"


@router.post("/futebol/add-calendario-libertadores")
async def add_calendario_libertadores(file: UploadFile = File(...)):
    if file.content_type != "application/pdf":
        raise HTTPException(status_code=400, detail="O arquivo deve ser um PDF.")

    futebol_service = FutebolService(DB_PATH)
    file_path = f"temp_{file.filename}"
    
    try:
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        data = futebol_service.extract_data_from_pdf(pdf_path=file_path)
        futebol_service.save_to_duckdb(TABLE_NAME, data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao processar o arquivo: {str(e)}")
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)

    return {"message": "Dados salvos no banco de dados DuckDB com sucesso!"}

@router.get("/futebol/calendario-libertadores")
def get_calendario_libertadores(team_name: str = Query(
    ...,
    title="Digite o nome do seu time",
    description="Nome do time",
    example="Fortaleza"
)):
    futebol_service = FutebolService(DB_PATH)
    
    try:
        data = futebol_service.get_all_texts(TABLE_NAME, team_name)
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao recuperar dados: {str(e)}")