from app.utils.logger import log
from typing import Tuple
from google.cloud import bigquery
from datetime import datetime, timedelta
import pandas as pd


class DataLoader:
    """
    Production-ready SQL generator for multi-client GCP infrastructure.
    """

    def __init__(self, project_id: str = "gcp_project_id"):
        self.project_id = project_id.strip()
        self._industry_map = {
            "ecommerce": "D2C",
            "healthcare": "Healthcare_2",
            "automobile": "Automobile",
        }
        try:
            # It's better to initialize the client once per instance
            self.bq_client = bigquery.Client(project=self.project_id)
            log.info(f"BigQuery client initialized for project: {self.project_id}")
        except Exception as e:
            log.error(f"Error initializing BigQuery client: {str(e)}")
            raise e

    def __repr__(self) -> str:
        return f"DataLoader(project_id='{self.project_id}')"

    def __str__(self) -> str:
        return self.__repr__()

    def _get_date_range(self, window: int) -> Tuple[str, str]:
        """Calculates start and end dates based on a trailing window."""
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=2 * window)
        return str(start_date), str(end_date)

    # fmt: off
    def _ecommerce_marketing(
        self, dataset: str, start_date: str, end_date: str, dimension_col: str
    ) -> str:
        dimension_mapping = {
            "source": "Platform",
            "Creative_Type": "Creative_Type",
            "Product_Name": "Product_Name",
            "Campaigns": "Campaign_Name",
            "Ad_Name": "Ad_Name",
            "Ad_Creatives": "Creative_URL",
            "city": "City",
            "Product_Name": "Product_Name",
        }
        if dimension_col not in dimension_mapping.keys(): 
            dimension_col = "source"
        actual_dim = dimension_mapping.get(dimension_col)
        return f"""
            SELECT
                Date,
                {actual_dim} AS {dimension_col},
                COALESCE(sum(Impressions), 0) AS Impressions,
                COALESCE(sum(Reach), 0) AS Reach,
                COALESCE(sum(Clicks), 0) AS Clicks,
                COALESCE(sum(Ad_Spend_INR), 0) AS Ad_Spend_INR,
                COALESCE(sum(Conversions), 0) AS Conversions,
                COALESCE(sum(Revenue_INR), 0) AS Revenue_INR,
                COALESCE(sum(Likes), 0) AS Likes,
                COALESCE(sum(Shares), 0) AS Shares
            FROM `{self.project_id}.{dataset}.df_Ad_Performance`
            WHERE Date BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY 1, 2;
        """

    def _healthcare_marketing(
        self, dataset: str, start_date: str, end_date: str, dimension_col: str
    ) -> str:
        dimension_mapping = {
            "source": "Platform",
            "Ad_Name": "Ad_Name",
            "Ad_Creatives": "adcreative_url",
            "Campaigns": "Campaign_Name",
            "Channel": "Platform",
            "Campaign_Objective": "Campaign_Objective",
            "Hospital": "Hospital_Name",
            "Speciality": "Specialist_Featured",
            "Campaign_Status": "Campaign_Status",
        }
        if dimension_col not in dimension_mapping.keys(): 
            dimension_col = "source"
        actual_dim = dimension_mapping.get(dimension_col)
        return f"""
            SELECT
                CAST(Date AS STRING) AS Date,
                {actual_dim} AS {dimension_col},
                
                -- Metrics
                COALESCE(ROUND(SUM(Spend_INR), 2), 0) AS Spends,
                COALESCE(ROUND(SUM(Impressions), 2), 0) AS Impressions,
                COALESCE(ROUND(SUM(Clicks), 2), 0) AS Clicks,
                COALESCE(ROUND(SUM(Conversions), 2), 0) AS Leads,
                COALESCE(ROUND(SUM(Appointments_Booked), 2), 0) AS Appoinments,
                
                -- Pre-calculated column average
                COALESCE(ROUND(SUM(Actual_Revenue_INR), 2), 0) AS Revenue,
                COALESCE(ROUND(SUM(Predicted_Revenue_INR), 2), 0) AS Predicted_Revenue,
                
                -- Ratios with Division by Zero Protection
                COALESCE(ROUND(SAFE_DIVIDE(SUM(Conversions-Appointments_Booked) , NULLIF(SUM(Conversions), 0)) * 100, 2), 0) AS L2P,
                COALESCE(ROUND(SAFE_DIVIDE(SUM(Appointments_Booked) , NULLIF(SUM(Conversions), 0)) * 100, 2), 0) AS L2A,
                COALESCE(ROUND(SAFE_DIVIDE(SUM(Spend_INR) , NULLIF(SUM(Appointments_Booked), 0)), 2), 0) AS CPA,
                COALESCE(ROUND(SAFE_DIVIDE(SUM(Actual_Revenue_INR) , NULLIF(SUM(Appointments_Booked), 0)), 2), 0) AS ARPA,
                COALESCE(ROUND(SAFE_DIVIDE(SUM(Clicks) , NULLIF(SUM(Impressions), 0)) * 100, 2), 0) AS CTR,
                COALESCE(ROUND(SAFE_DIVIDE(SUM(Actual_Revenue_INR) , NULLIF(SUM(Spend_INR), 0)), 2), 0) AS ROAS,
                
                -- Fixed CPC logic (Spends / Clicks)
                COALESCE(ROUND((SUM(Spend_INR) / NULLIF(SUM(Clicks), 0)), 2), 0) AS CPC

            FROM `{self.project_id}.{dataset}.Ad_stats`
            WHERE Date BETWEEN '{start_date}' and '{end_date}'
            GROUP BY 1, 2;
        """

    def _automobile_marketing(
        self, dataset: str, start_date: str, end_date: str, dimension_col: str
    ) -> str:
        dimension_mapping = {
            "Channel": "Platform",
            "Ad_Name": "Ad_Name",
            "Ad_Creatives": "Ad_Creatives",
            "Campaign_Name": "Campaign_Name",
            "Type": "type",
            "Creative_Type": "type",
            "Target_Model": "Target_Model",
            "City": "City",
            "Dealer_Name":"",
        }
        if dimension_col not in dimension_mapping.keys() or dimension_col.lower() == "date": 
            dimension_col = "Channel"
        actual_dim = dimension_mapping.get(dimension_col)
        return f"""
            WITH
            camp_meta AS (
                SELECT DISTINCT
                    cam.Campaign_ID, cam.Start_Date, Campaign_Name, AdSet_ID, AdSet_Name
                FROM `{self.project_id}.{dataset}.Dim_Campaigns` cam
                JOIN `{self.project_id}.{dataset}.Dim_AdSets` adds
                  ON 
                    cam.Campaign_ID = adds.Campaign_ID AND cam.Start_Date = adds.Start_Date
                WHERE cam.Start_Date BETWEEN '{start_date}' AND '{end_date}'
            ),
            all_meta AS (
                SELECT
                cam.Campaign_ID,
                cam.AdSet_ID,
                ad.Ad_ID,
                ad.Creative_Id,
                ad.Ad_Name,
                Campaign_Name,
                ad.Landing_Page_URL AS Ad_Creatives,
                ct.Format AS type
                FROM `{self.project_id}.{dataset}.Dim_Ads` ad
                JOIN camp_meta cam
                ON cam.Campaign_ID = ad.Campaign_ID AND cam.AdSet_ID = ad.AdSet_ID
                JOIN `{self.project_id}.{dataset}.Dim_Creatives` ct
                ON ct.Campaign_ID = ad.Campaign_ID AND ct.AdSet_ID = ad.AdSet_ID
            )
            SELECT
                Date,
                {actual_dim} AS {dimension_col},
                sum(Impressions) AS Impressions,
                sum(Clicks) AS Clicks,
                sum(Video_Views) AS Video_Views,
                sum(Engagements) AS Engagements,
                sum(Leads) AS Leads,
                sum(Test_Drives_Booked) AS Test_Drives,
                SAFE_DIVIDE(sum(Clicks), sum(Impressions)) * 100 AS CTR,
                SAFE_DIVIDE(sum(Spend_INR), sum(Clicks)) * 100 AS CPC,
                SAFE_DIVIDE(sum(Spend_INR), sum(Test_Drives_Booked)) * 100 AS CPA,
                SAFE_DIVIDE(sum(Video_Completions), sum(Video_Views)) * 100 AS VTR,
                SAFE_DIVIDE(sum(Engagements), sum(Impressions)) * 100 AS Engagement_Rate,
            FROM `{self.project_id}.{dataset}.Fact_Ad_Performance_Daily` stat
            JOIN all_meta am
              ON
                stat.Campaign_ID = am.Campaign_ID
                AND stat.AdSet_ID = am.AdSet_ID
                AND stat.Ad_ID = am.Ad_ID
            WHERE Date BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY 1, 2;
        """
    # fmt: on

    def query_handler(
        self, industry: str, domain: str, dimension_col: str, window: int
    ) -> str:
        industry = industry.strip().lower()
        domain = domain.strip().lower()

        routes = {
            ("ecommerce", "marketing"): self._ecommerce_marketing,
            ("healthcare", "marketing"): self._healthcare_marketing,
            ("automobile", "marketing"): self._automobile_marketing,
        }

        try:
            dataset = self._industry_map.get(industry)
            func = routes.get((industry, domain))

            if not dataset or not func:
                raise ValueError(f"Unsupported Industry/Domain: {industry}/{domain}")

            # Use the window logic consistently
            start_date, end_date = self._get_date_range(window)
            log.info(f"start_date: {start_date}    &&   end_date: {end_date}")

            query = func(dataset, start_date, end_date, dimension_col)
            return query.strip()
        except Exception as e:
            log.error(f"SQL Generation Error: {str(e)}")
            raise e

    def data_loading_from_bigquery(
        self, industry: str, domain: str, dimension_col: str, window: int
    ) -> pd.DataFrame:
        """Load data from BigQuery using initialized client."""
        try:
            bq_query = self.query_handler(
                industry=industry,
                domain=domain,
                dimension_col=dimension_col,
                window=window,
            )
            log.info(f"Executing query for {industry}...")

            # Use the existing client instance
            dataframe = self.bq_client.query(bq_query).to_dataframe(
                create_bqstorage_client=True
            )
            if dataframe.empty:
                log.warning(f"No data found for {industry} in the last {window} days.")
                # Return an empty DF with expected columns to prevent downstream crashes
                return pd.DataFrame

            log.info(f"Loaded {len(dataframe)} rows. Shape: {dataframe.shape}")
            return dataframe
        except Exception as e:
            log.error(f"Data loading failed: {str(e)}")
            raise e
