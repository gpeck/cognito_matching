import os
import time
from sqlalchemy import create_engine
from urllib.parse import quote_plus as urlquote

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

def latest_cognito(cursor):

    # read congnito data
    print("Fetching latest users data")
    sql='SELECT cia.user_id, cia.data_birth_year, cia.data_birth_month, cia.data_birth_day, cia.data_name_first, cia.data_name_last, cia.data_phone_num, cia.data_address_street, cia.data_address_city, cia.data_address_postal, cia.data_address_subdivision, cia.addresses_list, cia.dob_list, cia.name_list, cia.phone_list from cognito_identity_assesment_flat cia;'
    start = time.time()
    try:
        curs = cursor
        results = curs.execute(sql)
        end = time.time()
        print("Querying time = %s ", (end - start))
        df = pd.DataFrame(results.fetchall())
        if not df.empty:
            df.columns = results.keys()
        # print("Users data: /n",df)
        return df
    except Exception as e:
        print("Failed to connect to db %s", e)

if __name__ == "__main__":

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
    latest_cognito(cursor)