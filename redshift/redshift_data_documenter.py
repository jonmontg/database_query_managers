import redshift_connector
import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)

def get_configuration():
  """
  @return Dictionary representation of the configuration.
  """
  config = {}
  default_host = "prod4x-data-warehouse.cwwu8aqhkiwx.us-east-1.redshift.amazonaws.com"
  default_port = 5439
  host = input(f"Host [{default_host}]: ")
  config["host"] = default_host if host == "" else host
  config["database"] = input("Database name (i.e. quest): ")
  config["user"] = input("User: ")
  config["password"] = input("Password: ")
  port = input(f"Port [{default_port}]: ")
  config["port"] = default_port if port == "" else port
  print(config)
  return config

class QueryManager:
  def __init__(self):
    pd.set_option("display.max_columns", None)
    pd.set_option("display.max_rows", None)
    pd.set_option("display.width", None)
    self._config = get_configuration()
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


def make_column_description(column_name, data_type, nullable, character_max_len, numeric_precision):
  if data_type == "character varying":
    display_data_type = f"varchar({character_max_len})"
  elif data_type == "integer":
    display_data_type = f"integer ({numeric_precision} byte precision)"
  elif data_type == "double precision":
    display_data_type = f"double ({numeric_precision} byte precision)"
  else:
    display_data_type = data_type
  return column_name, display_data_type, nullable

def describe_schema(schema_name, output_file, config_path="config/redshift_config.yml"):
  qm = QueryManager()
  df = qm.run_query(f"""
    SELECT t.table_name, c.column_name, c.data_type, c.is_nullable, c.character_maximum_length, c.numeric_precision
    FROM information_schema.tables as t
      JOIN information_schema.columns as c
        ON c.table_name = t.table_name
    WHERE t.table_schema = '{schema_name}' AND c.table_schema = '{schema_name}'
    AND t.table_type = 'BASE TABLE'
    ORDER BY ordinal_position
  """
  )

  version = qm.run_query("SELECT version()").version.iloc[0][:-1]

  with open(output_file, "w") as f:
    print("# LCMS Tables", file=f)
    print(f"Database: {version}\n", file=f)
    print("The Data Warehouse is updated continuously.", file=f)

    for table_name in sorted(df["table_name"].unique()):
      columns = df.query(f"table_name == '{table_name}'")
      print("", file=f)
      print(f"## {table_name}", file=f)
      print("", file=f)
      print("| Column | Data Type | Nullable | Description |", file=f)
      print("|--------|-----------|----------|-------------|", file=f)
      for row in columns.values:
        column_name, data_type, _ = make_column_description(*row[1:])
        print(f"| {column_name} | {data_type} |  | |", file=f)

if __name__=="__main__":
  describe_schema("quest", "tables.md")