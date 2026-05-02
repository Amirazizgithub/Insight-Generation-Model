from fastapi import FastAPI
from contextlib import asynccontextmanager
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from app.api import health_router, cdp_insight_router, intelligence_insight_router
from app.models.insight_model import Generate_Insight
from app.prompts.prompt import Insight_Gen_Prompt
from app.models.data_loader import DataLoader
from app.utils.main_utils import MainUtils


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.insight_engine = Generate_Insight()
    app.state.prompt_manager = Insight_Gen_Prompt()
    app.state.data_loader = DataLoader()
    app.state.main_utils = MainUtils()

    yield
    app.state.insight_engine = None


app = FastAPI(title="Insight Generation Model", version="1.0.0", lifespan=lifespan)

origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    try:
        return JSONResponse(
            content={"message": "Welcome to the insight generation API!"},
            status_code=200,
        )
    except Exception as e:
        return JSONResponse(content={"message": str(e)}, status_code=500)


app.include_router(health_router.health_router, prefix="/api/v1")
app.include_router(cdp_insight_router.cdp_insight_router, prefix="/api/v1")
app.include_router(
    intelligence_insight_router.intelligence_insight_router, prefix="/api/v1"
)
