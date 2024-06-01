from google.cloud import secretmanager

def get_secret(project_id, secret_id, version_id="latest"):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")


databases_list = [
        
        {   
            "kind": "postgres",
            "name": "database",
            "url": "jdbc:postgresql://localhost/database",
            "user": "user1",
            "password": get_secret("project_id", "secret_id"),
            "port": 5432
        }
    ]