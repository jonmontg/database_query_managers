import redshift_connector
import numpy as np
import pandas as pd
from shared import ensure_configuration
import sys
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)

class QueryManager:
  def __init__(self, config_path="config/redshift_config.yml"):
    pd.set_option("display.max_columns", None)
    pd.set_option("display.max_rows", None)
    pd.set_option("display.width", None)
    self._config = ensure_configuration(config_path, "host", "database", "user", "password")
    self._get_cursor()

  def _get_cursor(self):
    self.cursor = redshift_connector.connect(**self._config).cursor()

  def run_query(self, query_str):
    """
    Returns a pandas dataframe of the results from running the query string. If the query fails,
    return None
    """
    self.cursor.execute(query_str)
    return self.cursor.fetch_dataframe()

  def run_query_file(self, query_path, output_path):
    """
    Returns the query results from the contents of the query_path file.

    @param query_path  [str]              Path to query file
    @param output_path [str]              Path to the query output file
    """
    with open(query_path, "r") as f:
      df = self.run_query(f.read())
    df.to_csv(output_path)

  def run_command(self, command_str):
    """
    Runs the query string in the current redshift session.
    """
    self.cursor.execute(command_str)

  def console(self):
    """
    Enter a SQL console to query the database. To exit, enter "quit".
    """
    while True:
      query = input(">")
      if query.lower() == "quit":
        return
      try:
        self.cursor.execute(query)
        if self.cursor.description is None:
          continue
        print(self.cursor.fetch_dataframe())
      except Exception as e:
        print("An error occurred:", e)
        self._get_cursor()

if __name__=="__main__":
  QueryManager().console()

