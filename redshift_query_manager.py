import redshift_connector
from shared import ensure_configuration

def run_query(query_str, config_path="redshift_config.yml"):
  """
  Returns a pandas dataframe of the results from running the query string. If the query fails,
  return None
  """
  config = ensure_configuration(config_path, "host", "database", "user", "password")
  if not config:
    return
  cursor = redshift_connector.connect(**config).cursor()
  cursor.execute(query_str)
  return cursor.fetch_dataframe()

def run_query_file(query_path, output_path, config_path="redshift_config.yml"):
  """
  Returns the query results from the contents of the query_path file.

  @param query_path  [str]              Path to query file
  @param output_path [str]              Path to the query output file
  """
  with open(query_path, "r") as f:
    df = run_query(f.read(), config_path)
  df.to_csv(output_path)

if __name__=="__main__":
  run_query_file("query.txt", "flag.csv")
