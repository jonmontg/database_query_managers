import redshift_connector
from .lib.query_manager import *

@configured_manager("host", "database", "user", "password", "schema")
class RedshiftQueryManager(QueryManager):

  def reset(self):
    self.cursor = redshift_connector.connect(**{key: self.config[key] for key in ("host", "database", "user", "password")}).cursor()
    self.run(f"SET search_path TO {self.config['schema']}")

  def run(self, query_str: str):
    """
    Returns a pandas dataframe of the results from running the query string. If the query fails,
    return None
    """
    self.cursor.execute(query_str)
    if self.cursor.description is None:
      return None
    return self.cursor.fetch_dataframe()
