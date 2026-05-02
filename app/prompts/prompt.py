from app.utils.logger import log


class Insight_Gen_Prompt:
    def __init__(self) -> None:
        try:
            # fmt: off
            log.info("Insight_Gen_Prompt class initialized")

            # --- L1: CONCISE SUMMARY PROMPT ---
            self.cdp_l1_input_prompt = """
            ROLE: Expert Data Analyst
            TASK: Generate a concise summary (100-150 words) using ONLY the provided metrics.
            
            DATA CONTEXT:
            - leads: Lead creation data (pre-conversion).
            - customers: Converted lead data.
            - Miscellaneous: Records with undefined/missing fields.

            STRICT RULES:
            1. Use ONLY the provided data. Do not fabricate metrics or assume trends.
            2. If data is missing for a requirement, state "data not available".
            3. Audience: Business leaders and marketers.
            4. Strong terms: <strong>numerical values</strong>, <strong>leads</strong>, <strong>Customers</strong>, <strong>Miscellaneous</strong>.

            OUTPUT FORMAT:
            - STRICT HTML format.
            - Summary in <p> tag.
            - Actionables in <ul><li> tags (no sub-headings).
            - NO NEWLINES (\\n) in the entire response. Single line only.
            """

            # --- L2: INTELLIGENCE DICTIONARY PROMPT ---
            self.cdp_l2_input_prompt = """
            ROLE: Expert Marketing Analyst
            TASK: Perform a comparative analysis between 'previous' and 'current' periods.

            "Data Schema:"
            - Date_1: Event date.
            - type: 'lead' or 'customer'.
            - total: Count per day/type.
            - status: 'previous' (baseline period) vs 'current' (comparison period).

            "Categorization Logic:"
            - Why: Drivers of performance and trend shifts.
            - Anomaly: Outliers or unexpected data spikes/drops.
            - StepsForward: Specific, high-impact recommendations.

            "Strict Business Rules:"
            1. ALL metrics must be calculated from the provided data.
            2. Convert any USD values to ₹ (INR) using current market logic.
            3. Use <strong> tags for every numerical value and percentage.
            4. Skip a category if no significant finding exists.

            STRICT OUTPUT FORMAT:
            - Return ONLY a single-line JSON dictionary.
            - NO markdown (```), NO backticks, NO newlines (\\n).
            - Each category must be a LIST of strings (Array), NOT a single HTML block.
            - Minimize whitespace. Start with { and end with }.

            EXAMPLE STRUCTURE:
            {"Why": ["<strong>Revenue</strong> increased by <strong>10%</strong> due to...", "<strong>CPA</strong> dropped by..."], "Anomaly": ["Unexpected spike in..."], "StepsForward": ["Scale campaign X...", "Optimize landing page..."]}
            """

            # Simplified L1 for quick intelligence
            self.intelligence_l1_input_prompt = """
            ROLE: Senior Business Intelligence Analyst
            TASK: Generate a data-driven executive summary (50-70 words).

            STRICT DATA RULES:
            1. Use ONLY the provided metrics. No external context or "hallucinated" trends.
            2. Focus on high-level performance: total volume, growth/decline, and primary drivers.
            3. If data is insufficient, state "Insufficient data for summary" instead of estimating.

            OUTPUT CONSTRAINTS:
            - FORMAT: STRICT HTML. Use exactly one <p> tag. 
            - NO bullet points (<ul>/<li>), NO recommendations, and NO conversational filler.
            - NO newline characters (\\n). Everything must be on a single continuous line.
            - Strong specific metrics: Use <strong> for all numbers and key business terms.

            START OUTPUT WITH: <p>
            """

            self.intelligence_l2_input_prompt = """
            ROLE: Senior Marketing Intelligence Analyst
            TASK: Compare 'current' vs 'previous' periods and return insights as JSON arrays.

            DIMENSIONS:
            - Date: Record timestamp.
            - Status: 'previous' (baseline) vs 'current' (comparison period).

            ANALYSIS CATEGORIES:
            1. Why: Drivers of performance and trend shifts. Generate only 2-3 short & concise insights.
            2. Anomaly: Significant deviations, outliers, or missing data flags. Generate only 1-2 short & concise anomalies.
            3. StepsForward: Specific, high-impact tactical recommendations. Generate only 2-3 short & concise recommendations.

            BUSINESS RULES:
            - Convert all monetary values to ₹ (INR).
            - Calculate exact percentage changes (e.g., "increased by 15.4%").
            - Use <strong> tags ONLY around numerical values and key metrics (e.g., <strong>₹500</strong>, <strong>12.5%</strong>, <strong>Revenue</strong>).

            STRICT OUTPUT FORMAT:
            - Return ONLY a single-line JSON dictionary.
            - NO markdown (```), NO backticks, NO newlines (\\n).
            - Each category must be a LIST of strings (Array), NOT a single HTML block.
            - Minimize whitespace. Start with { and end with }.

            EXAMPLE STRUCTURE:
            {"Why": ["<strong>Revenue</strong> increased by <strong>10%</strong> due to...", "<strong>CPA</strong> dropped by..."], "Anomaly": ["Unexpected spike in..."], "StepsForward": ["Scale campaign X...", "Optimize landing page..."]}
            """

            # self.prediction_input_prompt = """
            # ### ROLE
            # Senior Predictive Intelligence Strategist & Systems Architect.

            # ### OBJECTIVE
            # Perform a deep-tier analytical audit of the provided performance report. Your goal is to move beyond surface-level observations and identify the latent "predictive DNA" of the system to guide long-term strategy.

            # ### ANALYSIS PROTOCOL (Chain-of-Thought)

            # 1. **Leverage & Efficiency Calculation:** - Calculate the ratio of **Output Growth** (Results/Revenue) relative to **Input Growth** (Spend/Effort).
            # - Define the state: **Positive Operating Leverage** (Efficiency Scaling), **Linear Growth**, or **Diminishing Returns**.

            # 2. **Statistical Stability Audit:**
            # - Critically evaluate the **p-values** and **ANOVA/Kruskal-Wallis** results.
            # - Decide: Is this data "predictive gold" (high significance) or "dangerous noise" (low significance/small sample)? Should this window be weighted heavily in a 12-month forecast?

            # 3. **Temporal & Seasonal Mapping:**
            # - Analyze **Seasonal Strength** (0-100%).
            # - Identify if current performance is driven by **Internal Strategy** or **External Market Cycles**.
            # - Map out "Preparation Windows": When must resources be deployed to catch the next identified peak?

            # 4. **Forecasting & Bottleneck Identification:**
            # - Categorize metrics into **Leading Indicators** (Early signals like Reach/Clicks) and **Lagging Indicators** (Final outcomes like Revenue).
            # - **Stress Test:** If the current growth rate persists, which resource (Budget, Infrastructure, or Inventory) will break first?

            # ### OUTPUT STRUCTURE

            # #### ## 1. Strategic Intelligence Summary
            # - Provide a high-density executive overview. What is the "Ground Truth" of this data window?

            # #### ## 2. Predictive Validity & Scaling Logic
            # - **Verdict:** [SCALE / HOLD / PIVOT / RECALIBRATE]
            # - **Justification:** Use the **Statistical Significance** and **Efficiency Ratios** to justify this stance.

            # #### ## 3. The 90-Day Forecast Impact
            # - How does this specific report change our expectations for the next quarter?
            # - Identify the "Golden Window" for future aggressive investment based on seasonality.

            # #### ## 4. Outlier & Integrity Alerts
            # - Flag "Toxic Data" (Anomalies like infinite growth or sudden drops) that should be excluded from training future automated bidding or prediction models.

            # ### STRICT FORMATTING
            # - Use **bolding** for all **numerical values**, **percentages**, and **KPIs**.
            # - Use Markdown headers and bullet points for high scannability.
            # - Avoid fluff; maintain a clinical, data-driven, and decisive tone.
            # """

            self.report_analysis_input_prompt: str = """
            ### ROLE: Senior Marketing Intelligence 

            ### AnalystTASK: Analyze the provided ecommerce analytics data across four areas — metric growth trends, dimension performance, anomalies/outliers, and data quality — and return structured insights as a single-line JSON dictionary.

            ### DATA STRUCTURE: 
            - **window_growth**: Each metric has a previous_sum, current_sum, and pct_change for the analysis window.- dimension_breakdown: Top 5 and bottom 5 dimension values (e.g. Category) ranked by current performance for each metric, with their current values and period-over-period % change.
            - **outliers**: Flagged observations per metric detected via IQR, Z-score, or Isolation Forest methods.- data_quality: Null rates, flat-line segments, duplicate rows, and negative value flags.

            ### ANALYSIS CATEGORIES:
            1. Why: Drivers of performance and trend shifts. Generate only 2-3 short & concise insights.
            2. Anomaly: Significant deviations, outliers, or missing data flags. Generate only 1-2 short & concise anomalies.
            3. StepsForward: Specific, high-impact tactical recommendations. Generate only 2-3 short & concise recommendations.

            ### BUSINESS RULES:- Convert all monetary values to ₹ (INR).
            - Calculate and reference exact percentage changes (e.g., "increased by 15.4%").
            - Use  tags ONLY around numerical values and key metric names (e.g., ₹500, 12.5%, ROAS).
            - When referencing dimension values, bold the category name (e.g., Furniture).

            ### STRICT OUTPUT FORMAT:
            - Return ONLY a single-line JSON dictionary.
            - NO markdown (```), NO backticks, NO newlines (\\n).
            - Each category must be a LIST of strings (Array), NOT a single HTML block.
            - Minimize whitespace. Start with { and end with }.

            ### EXAMPLE STRUCTURE:
            {"Why": ["<strong>Revenue</strong> increased by <strong>10%</strong> due to...", "<strong>CPA</strong> dropped by..."], "Anomaly": ["Unexpected spike in..."], "StepsForward": ["Scale campaign X...", "Optimize landing page..."]}
            """
            # fmt: on

        except Exception as e:
            log.error(f"Error in loading input_prompts: {str(e)}")
            # Note: In __init__, you shouldn't return a JSONResponse.
            # Better to raise an exception or handle it in the calling function.
            raise e

    def __str__(self) -> str:
        return "Insight_Gen_Prompt class to generate prompts for insight generation."

    def __repr__(self) -> str:
        return f"Insight_Gen_Prompt(cdp_l1_input_prompt='{self.cdp_l1_input_prompt}', cdp_l2_input_prompt='{self.cdp_l2_input_prompt}'), intelligence_l1_input_prompt='{self.intelligence_l1_input_prompt}', intelligence_l2_input_prompt='{self.intelligence_l2_input_prompt}'"
