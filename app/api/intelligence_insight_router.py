from app.utils.logger import log
from fastapi import Request, APIRouter
from fastapi.responses import JSONResponse
from app.models.data_analysis import analyze_cohorts

intelligence_insight_router = APIRouter()


@intelligence_insight_router.post("/intelligence/generate_insight_l1")
async def intelligence_generate_insight_l1(request: Request):
    try:
        # 1. Reuse existing objects from app state
        engine = request.app.state.insight_engine
        prompts = request.app.state.prompt_manager

        try:
            card_data = await request.json()
        except Exception:
            return JSONResponse(
                content={"message": "Error loading payload"}, status_code=400
            )

        if not card_data:
            return JSONResponse(
                content={"message": "Request data is missing"}, status_code=400
            )

        # 2. Use the warm prompt and engine
        prompt = prompts.intelligence_l1_input_prompt
        log.info("Generating L1 insights using warm engine...")

        return engine.generate_insights_lite(data=card_data, prompt=prompt)

    except Exception as e:
        log.error(f"L1 Error: {str(e)}")
        return JSONResponse(
            content={"message": f"API Error: {str(e)}"}, status_code=500
        )


@intelligence_insight_router.post("/intelligence/generate_insight_l2")
async def intelligence_generate_insight_l2(request: Request):
    try:
        data = await request.json()
        # 1. Reuse objects from app state
        engine = request.app.state.insight_engine
        prompts = request.app.state.prompt_manager
        data_loader = request.app.state.data_loader

        if (
            not data.get("industry")
            or not data.get("domain")
            or not data.get("dimension_col")
        ):
            return JSONResponse(
                content={"message": "Request data is missing required fields"},
                status_code=400,
            )
        industry = data["industry"]
        domain = data["domain"]
        dimension_col = data["dimension_col"]
        window = data.get("window", 30)
        log.info(f"Window days: {window}")

        if dimension_col == "date":
            dimension_col = "source"

        # 2. Load data from big query
        dataframe = data_loader.data_loading_from_bigquery(
            industry=industry, domain=domain, dimension_col=dimension_col, window=window
        )
        if dataframe.empty or len(dataframe) == 0:
            return JSONResponse(
                content={"message": "Data not found for insights generation."},
                status_code=503,
            )

        # 3. Process data using warm utils
        analysis_data = analyze_cohorts(
            df=dataframe, date_col="Date", dimension_col=dimension_col, windows=[window]
        )
        report_data = analysis_data.report
        print(report_data)

        # 3. Generate insights
        prompt = prompts.report_analysis_input_prompt
        log.info("Generating L2 insights using warm engine...")

        return engine.generate_insights_lite(data=report_data, prompt=prompt)

    except Exception as e:
        log.error(f"L2 Error: {str(e)}")
        return JSONResponse(
            content={"message": f"API Error: {str(e)}"}, status_code=500
        )


# @intelligence_insight_router.post("/intelligence/generate_insight_l2")
# async def intelligence_generate_insight_l2(
#     request: Request, start_date_str: str, end_date_str: str
# ):
#     try:
#         # 1. Reuse objects from app state
#         engine = request.app.state.insight_engine
#         prompts = request.app.state.prompt_manager
#         utils = request.app.state.main_utils

#         try:
#             data = await request.json()
#         except Exception:
#             return JSONResponse(
#                 content={"message": "Error loading payload"}, status_code=400
#             )
#         if not data:
#             return JSONResponse(
#                 content={"message": "Request data is missing"}, status_code=400
#             )
#         # Check if the parameters are provided AND not just empty strings
#         if not start_date_str.strip() or not end_date_str.strip():
#             log.error("start_date or end_date is empty")
#             return JSONResponse(
#                 content={"message": "start_date or end_date is missing."},
#                 status_code=400,
#             )

#         # 2. Process data using warm utils
#         df = utils.make_dataframe_of_intelligence_l2_data(
#             data=data, start_date_str=start_date_str, end_date_str=end_date_str
#         )
#         response_json_str = df.to_json(orient="index")

#         # 3. Generate insights
#         prompt = prompts.intelligence_l2_input_prompt
#         log.info("Generating L2 insights using warm engine...")

#         return engine.generate_insights_lite(data=response_json_str, prompt=prompt)

#     except Exception as e:
#         log.error(f"L2 Error: {str(e)}")
#         return JSONResponse(
#             content={"message": f"API Error: {str(e)}"}, status_code=500
#         )
