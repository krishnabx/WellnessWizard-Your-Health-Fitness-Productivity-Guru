
import pandas as pd
import boto3
from io import StringIO

from scrape import app_infos_df, fitness_app_infos_df, health_app_infos_df
from scrape import app_reviews_df, fitness_app_reviews_df, health_app_reviews_df




def upload_s3(df,i):
    s3 = boto3.client("s3",aws_access_key_id="AKIATIOAOM2VOUMBPVVI",aws_secret_access_key="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")
    csv_buf = StringIO()
    df.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)    
    s3.put_object(Bucket="adm-project-bucket", Body=csv_buf.getvalue(), Key=i)


upload_s3(app_infos_df,"app_infos_df.csv")
upload_s3(fitness_app_infos_df,"fitness_app_infos_df.csv")
upload_s3(health_app_infos_df,"health_app_infos_df.csv")


upload_s3(app_reviews_df,"app_reviews_df.csv")
upload_s3(fitness_app_reviews_df,"fitness_app_reviews_df.csv")
upload_s3(health_app_reviews_df,"health_app_reviews_df.csv")


print("6 files successfully uploaded onto your s3 bucket!")