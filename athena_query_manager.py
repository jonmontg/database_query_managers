import boto3
from botocore.exceptions import ClientError
import os
from .shared import ensure_configuration
import awswrangler as wr

def run_query(query_str, config_path="config/athena_config.yml"):
  config = ensure_configuration(config_path, "database", "output_location", "encryption", "region", "profile")
  if not config:
    print("Failed to validate configuration file.")
    return
  session = boto3.Session(profile_name=config["profile"], region_name=config["region"])
  sts_client = session.client("sts")
  try:
    sts_client.get_caller_identity()
  except ClientError as e:
    if e.response["Error"]["Code"] == "ExpiredTokenException":
      print("AWS credentials are expired. Log in again.")
      os.system("aws-azure-login --mode gui --no-prompt --profile default")
    elif e.response["Error"]["Code"] == "InvalidClientTokenId":
      print("AWS credentals are not valid. Log in again.")
      os.system("aws-azure-login --mode gui --no-prompt --profile default")
    else:
      raise e
  try:
    return wr.athena.read_sql_query(
      query_str,
      database=config["database"],
      boto3_session=session,
      s3_output=config["output_location"],
      ctas_approach=False
    )
  except Exception as e:
    raise Exception(e)
