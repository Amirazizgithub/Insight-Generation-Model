import os
import json
import pandas as pd
import vertexai
from app.utils.logger import log
from dotenv import load_dotenv
from fastapi.responses import JSONResponse
from vertexai.generative_models import GenerativeModel, GenerationConfig

load_dotenv()


class Generate_Insight:
    def __init__(self):
        # Recommended: Use 'gemini-2.0-flash-001' for lowest latency and best performance
        self.llm_model = os.environ.get("LLM_MODEL")
        self.project_id = os.environ.get("CENTRAL_PROJECT_ID")
        self.project_region = os.environ.get("CENTRAL_PROJECT_REGION")
        log.info(f"LLM Model: {self.llm_model}")
        log.info(f"Project ID: {self.project_id}")
        log.info(f"Project Region: {self.project_region}")

        # Initialize Vertex AI once at the class level
        vertexai.init(project=self.project_id, location=self.project_region)
        log.info("Vertex AI initialized")

        # Define a System Instruction to keep the model focused and fast
        system_instruction = (
            "You are an expert data analyst. Generate concise, actionable insights based on the provided data."
            "Do not make up or infer any data points that are not explicitly present in the data."
        )

        self.model = GenerativeModel(
            model_name=self.llm_model, system_instruction=[system_instruction]
        )
        log.info("Model initialized")

    def generate_insights_lite(self, data: str | dict, prompt: str) -> JSONResponse:
        """
        Optimized for speed using Gemini 2.0 Flash and native JSON output.
        """
        try:
            log.info(f"{25*'-'} Production: Generating insights {'-'*25}")
            log.info("Generating insights...")
            # Ensure data is a string for the prompt
            json_data = data if isinstance(data, str) else json.dumps(data)
            log.info(f"JSON Data: {len(json_data)} characters")

            full_user_content = f"Instruction: {prompt}\n\nData: {json_data}"
            log.info(f"Full User Content: {len(full_user_content)} characters")

            # Generation config for speed and JSON structure
            config = GenerationConfig(
                response_mime_type="application/json",
                temperature=0.2,  # Lower temperature = more consistent, faster insights
            )
            log.info("Generation Config is prepared")

            log.info("Generating response...")
            response = self.model.generate_content(
                full_user_content, generation_config=config
            )
            log.info("Response generated")

            if response and response.text:
                # model.generate_content returns a string; we parse it to return a proper JSONResponse
                log.info("Response.text is present")
                return JSONResponse(
                    content={"model_response": json.loads(response.text)},
                    status_code=200,
                )

            log.info("Empty response from model.")
            return JSONResponse(
                content={"message": "Empty response from model."}, status_code=500
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
