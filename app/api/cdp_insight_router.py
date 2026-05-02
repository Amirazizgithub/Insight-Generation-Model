from app.utils.logger import log
from fastapi import Request, APIRouter
from fastapi.responses import JSONResponse

cdp_insight_router = APIRouter()


# Endpoint to generate insights for l1 data of CDP dashboard
@cdp_insight_router.post("/cdp/generate_insight_l1")
async def cdp_generate_insight_l1(request: Request):
    try:
        # 1. Reuse existing objects from app state
        engine = request.app.state.insight_engine
        prompts = request.app.state.prompt_manager
        try:
            card_data = await request.json()
            log.info("Card data loaded successfully")
        except Exception:
            log.error("Error loading paylaod")
            return JSONResponse(
                content={"message": "Error loading paylaod"},
                status_code=400,
            )

        if not card_data:
            log.error("Request data is missing")
            return JSONResponse(
                content={"message": "Request data is missing"},
                status_code=400,
            )

        cdp_l1_input_prompt = prompts.cdp_l1_input_prompt
        log.info("CDP L1 input prompt loaded successfully")

        return engine.generate_insights_lite(data=card_data, prompt=cdp_l1_input_prompt)
    except Exception as e:
        log.error(f"API Error. Error: {str(e)}")
        return JSONResponse(
            content={"message": f"API Error. Error: {str(e)}"}, status_code=500
        )


# Endpoint to generate insights for l2 data of CDP dashboard
@cdp_insight_router.post("/cdp/generate_insight_l2")
async def cdp_generate_insight_l2(
    request: Request, start_date_str: str, end_date_str: str
):
    try:
        # 1. Reuse existing objects from app state
        engine = request.app.state.insight_engine
        prompts = request.app.state.prompt_manager
        utils = request.app.state.main_utils

        try:
            data = await request.json()
            log.info("Card data loaded successfully")
        except Exception:
            log.error("Error loading paylaod")
            return JSONResponse(
                content={"message": "Error loading paylaod"},
                status_code=400,
            )

        if not start_date_str or not end_date_str:
            log.error("start_date or end_date is missing")
            return JSONResponse(
                content={"message": "start_date or end_date is missing"},
                status_code=400,
            )

        if not data:
            log.error("Request data is missing")
            return JSONResponse(
                content={"message": "Request data is missing"},
                status_code=400,
            )

        # Load the data as a dataframe using the warm utils
        df = utils.make_dataframe_of_cdp_l2_data(
            data=data,
            start_date_str=start_date_str,
            end_date_str=end_date_str,
        )
        log.info("Dataframe created successfully")

        response_json_str = df.to_json(orient="index")
        log.info("Dataframe converted to JSON successfully")

        cdp_l2_input_prompt = prompts.cdp_l2_input_prompt
        log.info("CDP L2 input prompt loaded successfully")

        response_json = engine.generate_insights_lite(
            data=response_json_str, prompt=cdp_l2_input_prompt
        )
        log.info("Insights generated successfully")
        return response_json
    except Exception as e:
        log.error(f"API Error. Error: {str(e)}")
        return JSONResponse(
            content={"message": f"API Error. Error: {str(e)}"}, status_code=500
        )
