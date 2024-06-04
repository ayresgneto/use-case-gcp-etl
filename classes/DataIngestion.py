import pandas as pd
import sys
sys.path.append('/home/ayres/Documents/projects/use-case-gcp-etl/configs')
import databases

class DataIngestor:
    def __init__(self, format, dbname=None):
        self.format = format
        self.dbname = dbname
        

    def read_data(self):

        def read_csv(data_source):
            return pd.read_csv(data_source)
    
        def read_sql(dbname):
            for database_item_list in databases.databases_list:
                if database_item_list["name"] == dbname:
                    return database_item_list
                
        if self.format == "csv":
            data = read_csv(self.data_source)
        elif self.format == "sql":
            data = read_sql(self.dbname)
        # elif self.format == "json":
        #     data = read_json(self.data_source)
        else:
            raise ValueError(f"Formato de dados não suportado: {self.format}")

        return data

   