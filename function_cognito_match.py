import itertools
import os
from math import atan2, cos, radians, sin, sqrt
from typing import Dict, List
from urllib.parse import quote_plus as urlquote

import pandas as pd
import pyarrow.feather as feather
from dotenv import load_dotenv
from rapidfuzz import fuzz, process
from sqlalchemy import create_engine

from cognito_preprocessing import latest_cognito

load_dotenv()


 # Loading database variables
host = os.getenv("SANDBOX_HOST")
user = os.getenv("SANDBOX_USER")
db_name = os.getenv("SANDBOX_NAME")
password = str(os.getenv("SANDBOX_PASSWORD"))
port = int(os.getenv("SANDBOX_PORT"))

# Database Connection String
ssl_args = {'ssl_ca': 'global-bundle.pem'}
mariadb_engine = create_engine(f"mariadb+mariadbconnector://{user}:{urlquote(password)}@{host}:{port}/{db_name}",
                                connect_args=ssl_args)

cursor = mariadb_engine.connect()

class matching_Algorithm:
    def __init__(
        self,
        first_name="",
        last_name="",
        date_of_birth="",
        phone="",
        street="",
        zipcode="",
        state="",
    ):
        self.first_name = first_name.strip().upper()
        self.last_name = last_name.strip().upper()
        self.date_of_birth = date_of_birth.strip()
        self.phone = phone.strip()
        self.street = street.strip().upper()
        self.zipcode = zipcode.strip().split("-")[0]
        self.state = state.strip().upper()

    def calculate_distance(self, lat1, lon1, lat2, lon2) -> float:
        """
        Calculate the Distance between two points in latitude and longitude on the Earth surface.

        Args:
        lat1 (float) : The latitude of the first point.
        lat2 (float) : The latitude of the second point.
        lon1 (float) : The longitude of the first point.
        lon2 (float) : The longitude of the second point.

        Returns:
        distance (float) : The distance between the two points in kilometers.

        """

        R = 6373.0
        lat1 = radians(lat1)
        lon1 = radians(lon1)
        lat2 = radians(lat2)
        lon2 = radians(lon2)
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))
        distance = R * c
        return distance

    def read_and_process_data(self, cursor) -> pd.DataFrame:
        """
        Read and Process data

        Returns:
            pd.DataFrame: Returns a pre-processed data frame
        """

        df = latest_cognito(cursor)

        # date from day , month , year
        df = df[~((df.data_name_first.isna()) | (df.data_name_last.isna()))]
        df["data_birth_date"] = pd.to_datetime(
            df["data_birth_year"].astype(str)
            + "-"
            + df["data_birth_month"].astype(str)
            + "-"
            + df["data_birth_day"].astype(str)
        )
        df.drop(
            [
                "data_birth_year",
                "data_birth_month",
                "data_birth_day",
                "data_address_street",
                "data_address_city",
                "data_address_postal",
                "data_address_subdivision",
            ],
            axis=1,
            inplace=True,
        )

        df.fillna(value="", inplace=True)

        df.drop_duplicates(
            ["data_name_first", "data_name_last", "data_phone_num", "data_birth_date"],
            inplace=True,
        )

        def convert_to_list(x: str, wordPosition: int) -> List:
            """Converts string to list of separate entity

            Args:
                x (str): column that has to be converted
                wordPosition (int): position of required entity
                -2 : pincode
                -1 : state
                0 : street

            Returns:
                List: final list in columns
            """

            list1 = x.strip(";").split(";")
            my_list = list(
                map(
                    lambda x: x.split(",")[wordPosition] if list1[0] != "" else "",
                    list1,
                )
            )

            return my_list

        def name_to_list(x: str, wordPosition: int) -> List:
            """seperates first name and last name list

            Args:
                x (str): name column
                wordPosition (int):
                0 : first name
                -1 : last name

            Returns:
                List: final list saves in column with separate entity
            """

            list2 = str(x).strip(",").split(",")
            my_list = list(
                map(
                    lambda x: x.split(" ")[wordPosition] if list2[0] != "" else "",
                    list2,
                )
            )

            return my_list

        df["pincode"] = df.addresses_list.apply(lambda x: convert_to_list(x, -2))
        df["state"] = df.addresses_list.apply(lambda x: convert_to_list(x, -1))
        df["street"] = df.addresses_list.apply(lambda x: convert_to_list(x, 0))

        df["first_name_list"] = df.name_list.apply(lambda x: name_to_list(x, 0))
        df["last_name_list"] = df.name_list.apply(lambda x: name_to_list(x, -1))

        df["phone_list"] = df.phone_list.apply(lambda x: x.strip(",").split(","))
        df["dob_list"] = df.dob_list.apply(lambda x: x.strip(",").split(","))

        return df

    def flag(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add latutude and longitude using zip code and calculate distance

        Args:
            df (pd.DataFrame)

        Returns:
            pd.DataFrame: Data frame with extra column of flag indicates if entry is within 80km radius
        """
        zip_list = list(set(itertools.chain(*list(df.pincode.values))))
        zip_data = feather.read_feather(
                "us_zip_data.feather",
                use_threads=True,
            )
        zipcode_df = pd.DataFrame()
        zipcode_df["zipcode"] = zip_list
        zipcode_df = pd.merge(
            zipcode_df, zip_data, left_on="zipcode", right_on="ZIP", how="left"
        )

        input_lat = zip_data[zip_data.ZIP == self.zipcode].LAT.values[0]
        input_lon = zip_data[zip_data.ZIP == self.zipcode].LNG.values[0]

        zipcode_df["distance"] = zipcode_df.apply(
            lambda x: self.calculate_distance(input_lat, input_lon, x.LAT, x.LNG),
            axis=1,
        )

        near_zip = (
            zipcode_df[(zipcode_df.distance <= 80) | (zipcode_df.distance.isna())]
            .zipcode.unique()
            .tolist()
        )

        def check_zip(near_zip, pincode):
            """Check zip code within 80km and add flag=True

            Args:
                near_zip (_type_): zipcode within 80km
                pincode (_type_): pincode that we need to check

            Returns:
                _type_: flag = True or False
            """
            flag = False
            near_zip = set(near_zip)
            pincode = set(pincode)
            pin_list = list(near_zip.intersection(pincode))
            if len(pin_list) > 0:
                flag = True
            return flag

        df["flag"] = df.pincode.apply(lambda x: check_zip(near_zip, x))

        return df

    def check_name(self, df: pd.DataFrame) -> List:
        """Match first name, last name and date of birth.

        Args:
            df (pd.DataFrame)

        Returns:
            List: List of matching user id.
        """

        result_first_name = []
        if self.first_name != "":
            result_first_name.extend(
                df[
                    df.first_name_list.apply(lambda x: self.first_name in x)
                ].user_id.values
            )
        else:
            result_first_name = []

        result_last_name = []
        if self.last_name != "":
            result_last_name.extend(
                df[
                    df.last_name_list.apply(lambda x: self.last_name in x)
                ].user_id.values
            )
        else:
            result_last_name = []

        result_dob = []
        if self.date_of_birth != "":
            result_dob.extend(
                df[df.dob_list.apply(lambda x: self.date_of_birth in x)].user_id.values
            )
        else:
            result_dob = []

        result_first_name = set(result_first_name)
        result_last_name = set(result_last_name)
        result_dob = set(result_dob)

        result_name_dob1 = result_first_name.intersection(result_last_name)
        result_name_dob = result_name_dob1.intersection(result_dob)
        return result_name_dob

    def check_street(self, df: pd.DataFrame) -> List:
        """Match street address with fuzzy match

        Args:
            df (pd.DataFrame)

        Returns:
            List: list of matching user id
        """

        street_list = list(
            set(itertools.chain(*list(df[df.flag == True].street.values)))
        )
        result_street = []
        if self.street != "":
            matched_street = process.extract(
                self.street, street_list, scorer=fuzz.token_sort_ratio, score_cutoff=80
            )
            result_address = [x[0] for x in matched_street]
            house_number = self.street.split(" ")[0]
            if len(result_address) > 0:
                for i in result_address:
                    if house_number == "PO":
                        if i.split(" ")[2] == self.street.split(" ")[2]:
                            result_street.extend(
                                df[df.street.apply(lambda x: i in x)].user_id.values
                            )

                    else:
                        if i.split(" ")[0] == house_number:
                            result_street.extend(
                                df[df.street.apply(lambda x: i in x)].user_id.values
                            )
        else:
            result_street = []
        result_street = list(set(result_street))
        return result_street

    def check_phone(self, df: pd.DataFrame) -> List:
        """Match phone muber with phone list

        Args:
            df (pd.DataFrame)

        Returns:
            List: matched user id
        """

        result_phone = []
        if self.phone != "":
            result_phone.extend(
                df[df.phone_list.apply(lambda x: self.phone in x)].user_id.values
            )
        else:
            result_phone = []
        result_phone = list(set(result_phone))
        return result_phone

    def get_matching_accounts(self, df: pd.DataFrame) -> Dict:
        """
        Finds Matched Accounts

        Args:
            df (pd.DataFrame): Main dataframe

        Returns:
            Dictionary: Dictionary of matched user IDs
        """

        result_name_dob = self.check_name(df)
        result_street = self.check_street(df)
        result_phone = self.check_phone(df)

        result_dic = {
            "Name and DOB": result_name_dob,
            "Street": result_street,
            "Phone": result_phone,
        }
        return result_dic

    def result(self, cursor) -> Dict:
        """
        Main Function to find Matched Accounts

        Returns:
            Dictionary: Dictionary of matched user IDs
        """

        df = self.read_and_process_data(cursor)
        df = self.flag(df)
        result = self.get_matching_accounts(df)

        return result


# final = matching_Algorithm(
#     first_name="Nicole",
#     last_name="Samuel",
#     date_of_birth="11-10-1985",
#     street="3654 Powell Point",
#     zipcode="80922",
#     phone="+15703698217",
# )
#
# result_2 = final.result(cursor)
# print(result_2)