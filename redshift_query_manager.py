import redshift_connector
import pandas as pd
from .lib.query_manager import *

@configured_manager("host", "database", "user", "password")
class RedshiftQueryManager(QueryManager):

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
