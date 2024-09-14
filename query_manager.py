from athena.athena_query_manager import AthenaQueryManager
from redshift.redshift_query_manager import RedshiftQueryManager

MANAGERS = {
  "redshift": RedshiftQueryManager,
  "athena": AthenaQueryManager
}

class QueryManager:
  """
  QueryManager is a tool used to query databases. To initialize a QueryManager, you must specify
  the database type and the database configuration. The database configuration can be a yaml file
  or a dictionary of values.

  Supported databases:

  Amazon Redshift
    type: 'redshift'
    config:
      host
      database
      user
      password
      port

  Athena S3
    type: 'athena'
    config:
      database
      output_location
      region
      profile

  """
  def __init__(self, type, config):
    if type not in MANAGERS:
      raise Exception(f"Unsupported database type: {type}. Supported types are {MANAGERS.keys.join(", ")}")
    self.manager = MANAGERS[type](config)

  def run(self, query_str):
    """
    Executes the command in the database. If the command is a successful query, returns
    a pandas dataframe.
    """
    return self.manager.run(query_str)

  def console(self):
    """
     Enter a SQL console to query the database. To exit, enter "quit".
    """
    print("Enter 'quit' to exit.")
    while True:
      query = input(">")
      if query.lower() == "quit":
        return
      try:
        result = self.run(query)
        print(result)
        if result is not None:
          print(result)
      except Exception as e:
        print("An error occurred:", e)
        self.manager.reset()

if __name__=="__main__":
  qm = QueryManager("athena", "config/athena_config.yml")
  print(qm.run("select * from assays limit 1"))
