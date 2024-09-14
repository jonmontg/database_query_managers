import boto3
import os
from lib.shared import ensure_configuration
import awswrangler as wr

class AthenaQueryManager:
  def __init__(self, config):
    self.config = ensure_configuration(config, "database", "output_location", "region", "profile")
    if self.config is None:
      raise Exception("Failed to parse configuration for AthenaQueryManager.")
    self.reset()

  def reset(self):
    self.session = boto3.Session(profile_name=self.config["profile"], region_name=self.config["region"])
    sts_client = self.session.client("sts")
    try:
      sts_client.get_caller_identity()
    except Exception as e:
      print("Exception: ", e)
      if e.response["Error"]["Code"] == "ExpiredToken":
        print("AWS credentials are expired. Log in again.")
        os.system("aws-azure-login --mode gui --no-prompt --profile default")
      elif e.response["Error"]["Code"] == "InvalidClientTokenId":
        print("AWS credentals are not valid. Log in again.")
        os.system("aws-azure-login --mode gui --no-prompt --profile default")
      else:
        raise e

  def run(self, query_str):
    return wr.athena.read_sql_query(
      query_str,
      database=self.config["database"],
      boto3_session=self.session,
      s3_output=self.config["output_location"],
      ctas_approach=False
    )
