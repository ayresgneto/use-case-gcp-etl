# from google.cloud import secretmanager

# def get_secret(project_id, secret_id, version_id="latest"):
#     client = secretmanager.SecretManagerServiceClient()
#     name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
#     response = client.access_secret_version(request={"name": name})
#     return response.payload.data.decode("UTF-8")


databases_list = [
        
        {   
            "kind": "postgres",
            "name": "olist",
            "url": "jdbc:postgresql://localhost:5432/olist",
            "user": "postgres",
            "password": "1234", #get_secret("project_id", "secret_id"),
            "driver": "org.postgresql.Driver"
        },
        
       {   
            "kind": "mysql",
            "name": "xuxa",
            "url": "jdbc:mysql://localhost:5432/olist",
            "user": "mysql",
            "password": "1234", #get_secret("project_id", "secret_id"),
            "driver": "org.postgresql.Driver"
        }]
