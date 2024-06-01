REGION=europe-west1
ZONE=europe-west1-b
CLUSTER_NAME=elt-cluster
SERVICE_ACCOUNT=your_service_account_name@your-gcp-project.iam.gserviceaccount.com
BUCKET_NAME=your-dataproc-staging-bucket


gcloud dataproc clusters create ${CLUSTER_NAME} \
    --region ${REGION} \
	--zone ${ZONE} \
	--bucket $BUCKET_NAME \
	--service-account $SERVICE_ACCOUNT  \
	--optional-components=ANACONDA,JUPYTER \
    --enable-component-gateway \
    --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/connectors/connectors.sh \
    --metadata gcs-connector-version=2.2.0 \
    --metadata bigquery-connector-version=1.2.0 \
    --metadata spark-bigquery-connector-version=0.19.1