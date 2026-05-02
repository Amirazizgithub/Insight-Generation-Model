from app.utils.logger import log
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

health_router = APIRouter()


@health_router.get("/check_health")
async def health_check(request: Request):
    try:
        # Check if core components in app.state are initialized
        engine_ready = hasattr(request.app.state, "insight_engine")
        loader_ready = hasattr(request.app.state, "data_loader")

        if not engine_ready or not loader_ready:
            log.warning(
                "Health check failed: App state components not fully initialized."
            )
            return JSONResponse(
                content={"status": "unhealthy", "reason": "Engines not initialized"},
                status_code=503,
            )

        log.info("Health check API called - System Healthy")
        return JSONResponse(
            content={"status": "healthy", "message": "API Healthy"}, status_code=200
        )

    except Exception as e:
        log.error(f"API Unhealthy. Error: {str(e)}")
        return JSONResponse(
            content={"status": "unhealthy", "error": str(e)}, status_code=500
        )
