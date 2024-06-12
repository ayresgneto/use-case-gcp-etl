class DataIngestion(object):
    def __init__(self, spark_session):
        self.spark = spark_session
        # self.data_source = data_source
        # self.data_format = data_format

        
    def load_data(self,data_format):

        if data_format == "jdbc":      
            return self.spark.read \
                .format("jdbc") 

        # elif data_format == "csv":
        #     return self.spark.read.csv(data_source, header=True, inferSchema=True)
                
        # elif data_format == "api":
        #     import requests
        #     response = requests.get(data_source)
        #     data = response.json()
        #     return self.spark.createDataFrame(data)
        else:
            raise ValueError("Format not found!")


   