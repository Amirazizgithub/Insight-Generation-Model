import json
from app.utils.logger import log
import pandas as pd
from datetime import datetime


class MainUtils:
    def find_middle_date(self, start_date_str: str, end_date_str: str) -> str:
        """
        Calculates the middle date between a start and end date.

        Args:
            start_date_str (str): The start date in 'DD-MM-YYYY' format.
            end_date_str (str): The end date in 'DD-MM-YYYY' format.

        Returns:
            datetime.date: The date object for the middle date.
        """
        try:
            # Define the date format string
            date_format = "%Y-%m-%d"

            # Convert the string dates to datetime objects
            start_date = datetime.strptime(start_date_str, date_format).date()
            end_date = datetime.strptime(end_date_str, date_format).date()
            log.info(
                f"Start date: {start_date}, End date: {end_date} are converted to string"
            )

        except Exception:
            # Define the date format string
            date_format = "%d-%m-%Y"

            # Convert the string dates to datetime objects
            start_date = datetime.strptime(start_date_str, date_format).date()
            end_date = datetime.strptime(end_date_str, date_format).date()
            log.info(
                f"Start date: {start_date}, End date: {end_date} are converted to string"
            )

        # Calculate the total duration between the two dates
        total_duration = end_date - start_date
        log.info(f"Total duration: {total_duration} is calculated")

        # Calculate half of the total duration to find the midpoint
        half_duration = total_duration / 2
        log.info(f"Half duration: {half_duration} is calculated")

        # Add the half duration to the start date to get the middle date
        middle_date = start_date + half_duration
        log.info(f"Middle date: {middle_date} is calculated")
        return middle_date

    def make_dataframe_of_cdp_l2_data(
        self, data: str | dict, start_date_str: str, end_date_str: str
    ) -> pd.DataFrame:
        try:
            # Convert the nested dictionary into a flattened list of dictionaries
            leads_list = list(data["leads"].values())
            customers_list = list(data["customers"].values())
            combined_list = leads_list + customers_list
            log.info(f"Combined list: length of combined list is {len(combined_list)}")
        except Exception:
            # Convert Python object to JSON string
            data_json = json.dumps(data)
            data_dict = json.loads(data_json)

            # Now you can proceed with the rest of your code
            leads_list = list(data_dict["leads"].values())
            customers_list = list(data_dict["customers"].values())
            combined_list = leads_list + customers_list
            log.info(f"Combined list: length of combined list is {len(combined_list)}")

        try:
            # Create the DataFrame
            df = pd.DataFrame(combined_list)
            df = df[["Date_1", "type"]]

            # Add a 'count' column based on the count of 'Date' and 'type'
            df = df.groupby(["Date_1", "type"]).size().reset_index(name="total")

            # # Sort the DataFrame by 'Date' to make the output more readable
            df = df.sort_values(by="Date_1").reset_index(drop=True)

            # Calculate the middle date for the specified range
            middle_date = self.find_middle_date(
                start_date_str=start_date_str, end_date_str=end_date_str
            )

            # Convert the 'Date_1' column to datetime objects to enable comparison
            df["Date_1"] = pd.to_datetime(df["Date_1"], errors="coerce")
            log.info(f"Date_1 column is converted to datetime objects")

            # Add the new "status" column based on the comparison
            # The 'dt.date' is used to compare only the date part, ignoring time
            df["status"] = df["Date_1"].apply(
                lambda x: (
                    "current"
                    if pd.notnull(x) and x.date() > middle_date
                    else "previous"
                )
            )
            log.info(f"Status column is added to the dataframe")
            return df
        except Exception as e:
            log.error(
                f"Error in loadind l2 data of CDP dashboard or building the dataframe of l2 data of CDP dashboard. Error: {e}"
            )
            err_message = f"Error in loadind l2 data of CDP dashboard or building the dataframe of l2 data of CDP dashboard. Error: {e}"
            raise Exception(err_message)

    def make_dataframe_of_intelligence_l2_data(
        self, data: str | dict, start_date_str: str, end_date_str: str
    ) -> pd.DataFrame:
        try:
            # Create the DataFrame
            df = pd.DataFrame(data=data)
            log.info(f"Dataframe is created from the data")

            # Convert the 'Date' column to datetime objects to enable comparison
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
            log.info(f"Date column is converted to datetime objects")

            # Sort the DataFrame by 'Date' to make the output more readable
            df = df.sort_values(by="Date").reset_index(drop=True)
            log.info(f"Dataframe is sorted by Date")

            # Calculate the middle date for the specified range
            middle_date = self.find_middle_date(
                start_date_str=start_date_str, end_date_str=end_date_str
            )

            # Add the new "status" column based on the comparison
            # The 'dt.date' is used to compare only the date part, ignoring time
            df["status"] = df["Date"].apply(
                lambda x: (
                    "current"
                    if pd.notnull(x) and x.date() > middle_date
                    else "previous"
                )
            )
            log.info(f"Status column is added to the dataframe")
            return df
        except Exception as e:
            log.error(
                f"Error either loading or building the dataframe of l2 data of intelligence dashboard. Error: {e}"
            )
            err_message = f"Error either loading or building the dataframe of l2 data of intelligence dashboard. Error: {e}"
            raise Exception(err_message)
