from fastapi import FastAPI, Request
from app.routes import example, futebol
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, PlainTextResponse
from fastapi.templating import Jinja2Templates

app = FastAPI(
    docs_url=None,        # Remove o Swagger UI (default: "/docs")
    redoc_url=None,       # Remove o ReDoc (default: "/redoc")
    openapi_url=None      # Remove o OpenAPI JSON schema (default: "/openapi.json")
)

app.mount("/static", StaticFiles(directory="app/static"), name="static")

# Configuração do CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Permite todas as origens (não recomendado para produção)
    allow_credentials=True,
    allow_methods=["*"],  # Permite todos os métodos (GET, POST, etc.)
    allow_headers=["*"],  # Permite todos os cabeçalhos
)

# Incluir as rotas
app.include_router(example.router)
app.include_router(futebol.router)

# Configura o Jinja2 para renderizar os templates
templates = Jinja2Templates(directory="app/templates")

@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

# Rota para o ads.txt
@app.get("/ads.txt", response_class=PlainTextResponse)
async def serve_ads_txt():
    ads_txt_content = """
    google.com, pub-9110858767534450, DIRECT, f08c47fec0942fa0
    """
    return ads_txt_content.strip()