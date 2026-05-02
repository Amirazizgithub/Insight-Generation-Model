import os
import json
import pandas as pd
from google import genai
from google.genai import types
from app.utils.logger import log
from dotenv import load_dotenv
from fastapi.responses import JSONResponse

load_dotenv()


class Generate_Insight:
    def __init__(self):
        # Recommended: Use 'gemini-2.0-flash-001' for lowest latency and best performance
        self.llm_model = os.environ.get("LLM_MODEL", "gemini-2.5-flash")
        self.project_id = os.environ.get("CENTRAL_PROJECT_ID")
        self.project_region = os.environ.get("CENTRAL_PROJECT_REGION")
        log.info(f"LLM Model: {self.llm_model}")
        log.info(f"Project ID: {self.project_id}")
        log.info(f"Project Region: {self.project_region}")

        # Define a System Instruction to keep the model focused and fast
        system_instruction = (
            "You are an expert data analyst. Generate concise, actionable insights based on the provided data."
            "Do not make up or infer any data points that are not explicitly present in the data."
        )

        # 4. Model Config with Safety & Parameters
        self.client = self._get_client()
        self.config = types.GenerateContentConfig(
            system_instruction=system_instruction,
            response_mime_type="application/json",
            temperature=0.2,
        )
        log.info("Model initialized")

    def _get_client(self):
        """Creates a scoped client for a specific GCP project."""
        return genai.Client(
            vertexai=True, project=self.project_id, location=self.project_region
        )

    def generate_insights_lite(self, data: str | dict, prompt: str) -> JSONResponse:
        """
        Optimized for speed using Gemini 2.0 Flash and native JSON output.
        """
        try:
            log.info(f"{25*'-'} Testing: Generating insights {'-'*25}")
            log.info("Generating insights...")
            # Ensure data is a string for the prompt
            json_data = data if isinstance(data, str) else json.dumps(data)
            log.info(f"JSON Data: {len(json_data)} characters")

            full_user_content = f"Instruction: {prompt}\n\nData: {json_data}"
            log.info(f"Full User Content: {len(full_user_content)} characters")

            # Generation config for speed and JSON structure
            log.info("Generation Config is prepared")

            log.info("Generating response...")
            response = self.client.models.generate_content(
                model=self.llm_model, contents=full_user_content, config=self.config
            )
            log.info("Response generated")

            if response and response.text:
                # model.generate_content returns a string; we parse it to return a proper JSONResponse
                log.info("Response.text is present")
                return JSONResponse(
                    content={"model_response": json.loads(response.text)},
                    status_code=200,
                )

            log.info("No insights generated for given database connection.")
            return JSONResponse(
                content={
                    "message": "No insights generated for given database connection."
                },
                status_code=500,
            )

        except Exception as e:
            log.error(f"Error in generate_insights_lite: {str(e)}")
            return JSONResponse(
                content={"message": f"Error in generate_insights_lite: {str(e)}"},
                status_code=500,
            )

    def generate_insights_deep(self, df: pd.DataFrame, prompt: str):
        """
        For 'Deep' insights, feeding a structured summary is often faster
        than using a Pandas Agent for every query.
        """
        try:
            # Convert DF summary to context to avoid the overhead of an Agent Executor
            df_context = {
                "summary": df.describe(include="all").to_dict(),
                "head": df.head(10).to_dict(),
                "columns": df.columns.tolist(),
            }
            log.info("Context data has prepared for deep insight generation")
            log.info("Generating deep insights...")
            return self.generate_insights_lite(data=df_context, prompt=prompt)

        except Exception as e:
            log.error(f"Error in generate_insights_deep: {str(e)}")
            return JSONResponse(
                content={"message": f"Error in generate_insights_deep: {str(e)}"},
                status_code=500,
            )
