from DataIngestion import DataIngestion
import sys
sys.path.append('/home/ayres/Documents/projects/use-case-gcp-etl/configs')
import databases

class SQLIngestion(DataIngestion):
    def __init__(self, spark_session):
        super().__init__(spark_session)

    def extract_data(self, db_name, table):
        
        database_properties = next((db for db in databases.databases_list if db["name"] == db_name), None)

        if database_properties:
            
            url = database_properties["url"]
            user = database_properties["user"]
            password = database_properties["password"]
            driver = database_properties["driver"]
            
            
            df_db = self.load_data("jdbc") \
                .option("url", url) \
                .option("dbtable", table) \
                .option("user", user) \
                .option("password", password) \
                .option("driver", driver) \
                .load()
            return df_db
        else:
            raise ValueError(f"database '{db_name}' not found in databases_list.")

