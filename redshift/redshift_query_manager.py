import redshift_connector
import pandas as pd
from lib.shared import ensure_configuration
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)

class RedshiftQueryManager:
  def __init__(self, config_path):
    pd.set_option("display.max_columns", None)
    pd.set_option("display.max_rows", None)
    pd.set_option("display.width", None)
    self.config = ensure_configuration(config_path, "host", "database", "user", "password")
    if self.config is None:
      raise Exception("Failed to parse configuration for RedshiftQueryManager.")
    self.reset()

  def reset(self):
    self.cursor = redshift_connector.connect(**self.config).cursor()

  def run(self, query_str):
    """
    Returns a pandas dataframe of the results from running the query string. If the query fails,
    return None
    """
    self.cursor.execute(query_str)
    if self.cursor.description is None:
      print(self.cursor)
      return None
    return self.cursor.fetch_dataframe()
