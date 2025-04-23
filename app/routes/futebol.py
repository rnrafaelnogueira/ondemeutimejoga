from fastapi import APIRouter, UploadFile, File, HTTPException, Query
import shutil
import os
from app.services.futebol_libertadores_service import FutebolLibertadoresService
from app.services.futebol_brasileirao_a_service import FutebolBrasileiraoAService
from app.services.futebol_copa_nordeste_service import FutebolCopaNordesteService
from app.services.futebol_sudamericana_service import FutebolSudamericanaService
from app.services.futebol_copa_brasil_service import FutebolCopaBrasilService

router = APIRouter()

DB_PATH = "futebol.duckdb"
TABLE_NAME_LIBERTADORES = "calendario_libertadores"
TABLE_NAME_BRASILEIRAO_A = "calendario_brasileirao_a"
TABLE_NAME_COPA_NORDESTE = "calendario_copa_nordeste"
TABLE_NAME_SUDAMERICANA = "calendario_sudamericana"
TABLE_NAME_COPA_BRASIL = "calendario_copa_brasil"



@router.post("/futebol/add-calendario-libertadores")
async def add_calendario_libertadores(file: UploadFile = File(...)):
    if file.content_type != "application/pdf":
        raise HTTPException(status_code=400, detail="O arquivo deve ser um PDF.")

    futebol_service = FutebolLibertadoresService(DB_PATH)
    file_path = f"temp_{file.filename}"
    
    try:
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        data = futebol_service.extract_data_from_pdf(pdf_path=file_path)
        futebol_service.save_to_duckdb(TABLE_NAME_LIBERTADORES, data)
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
    futebol_service = FutebolLibertadoresService(DB_PATH)
    
    try:
        data = futebol_service.get_all_texts(TABLE_NAME_LIBERTADORES, team_name)
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao recuperar dados: {str(e)}")

@router.post("/futebol/add-calendario-brasileirao-a")
async def add_calendario_calendario_brasileirao_a(file: UploadFile = File(...)):
    if file.content_type != "application/pdf":
        raise HTTPException(status_code=400, detail="O arquivo deve ser um PDF.")

    futebol_service = FutebolBrasileiraoAService(DB_PATH)
    file_path = f"temp_{file.filename}"
    
    try:
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        data = futebol_service.extract_data_from_pdf(pdf_path=file_path)
        futebol_service.save_to_duckdb(TABLE_NAME_BRASILEIRAO_A, data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao processar o arquivo: {str(e)}")
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)

    return {"message": "Dados salvos no banco de dados DuckDB com sucesso!"}

@router.get("/futebol/calendario-brasileirao-a")
def get_calendario_calendario_brasileirao_a(team_name: str = Query(
    ...,
    title="Digite o nome do seu time",
    description="Nome do time",
    example="Fortaleza"
)):
    futebol_service = FutebolBrasileiraoAService(DB_PATH)

    try:
        data = futebol_service.get_all_texts(TABLE_NAME_BRASILEIRAO_A, team_name)
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao recuperar dados: {str(e)}")


@router.post("/futebol/add-calendario-copa-nordeste")
async def add_calendario_calendario_copa_nordeste(file: UploadFile = File(...)):
    if file.content_type != "application/pdf":
        raise HTTPException(status_code=400, detail="O arquivo deve ser um PDF.")

    futebol_service = FutebolCopaNordesteService(DB_PATH)
    file_path = f"temp_{file.filename}"
    
    try:
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        data = futebol_service.extract_data_from_pdf(pdf_path=file_path)
        futebol_service.save_to_duckdb(TABLE_NAME_COPA_NORDESTE, data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao processar o arquivo: {str(e)}")
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)

    return {"message": "Dados salvos no banco de dados DuckDB com sucesso!"}

@router.get("/futebol/calendario-copa-nordeste")
def get_calendario_calendario_copa_nordeste(team_name: str = Query(
    ...,
    title="Digite o nome do seu time",
    description="Nome do time",
    example="Fortaleza"
)):
    futebol_service = FutebolCopaNordesteService(DB_PATH)

    try:
        data = futebol_service.get_all_texts(TABLE_NAME_COPA_NORDESTE, team_name)
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao recuperar dados: {str(e)}")


@router.post("/futebol/add-calendario-sudamericana")
async def add_calendario_calendario_sudamericana(file: UploadFile = File(...)):
    if file.content_type != "application/pdf":
        raise HTTPException(status_code=400, detail="O arquivo deve ser um PDF.")

    futebol_service = FutebolSudamericanaService(DB_PATH)
    file_path = f"temp_{file.filename}"
    
    try:
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        data = futebol_service.extract_data_from_pdf(pdf_path=file_path)
        futebol_service.save_to_duckdb(TABLE_NAME_SUDAMERICANA, data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao processar o arquivo: {str(e)}")
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)

    return {"message": "Dados salvos no banco de dados DuckDB com sucesso!"}

@router.get("/futebol/calendario-sudamericana")
def get_calendario_calendario_sudamericana(team_name: str = Query(
    ...,
    title="Digite o nome do seu time",
    description="Nome do time",
    example="Fortaleza"
)):
    futebol_service = FutebolSudamericanaService(DB_PATH)

    try:
        data = futebol_service.get_all_texts(TABLE_NAME_SUDAMERICANA, team_name)
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao recuperar dados: {str(e)}")

@router.post("/futebol/add-calendario-copa-brasil")
async def add_calendario_calendario_copa_brasil(file: UploadFile = File(...)):
    if file.content_type != "application/pdf":
        raise HTTPException(status_code=400, detail="O arquivo deve ser um PDF.")

    futebol_service = FutebolCopaBrasilService(DB_PATH)
    file_path = f"temp_{file.filename}"
    
    try:
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        data = futebol_service.extract_data_from_pdf(pdf_path=file_path)
        futebol_service.save_to_duckdb(TABLE_NAME_COPA_BRASIL, data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao processar o arquivo: {str(e)}")
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)

    return {"message": "Dados salvos no banco de dados DuckDB com sucesso!"}

@router.get("/futebol/calendario-copa-brasil")
def get_calendario_calendario_copa_brasil(team_name: str = Query(
    ...,
    title="Digite o nome do seu time",
    description="Nome do time",
    example="Fortaleza"
)):
    futebol_service = FutebolCopaBrasilService(DB_PATH)

    try:
        data = futebol_service.get_all_texts(TABLE_NAME_COPA_BRASIL, team_name)
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao recuperar dados: {str(e)}")
