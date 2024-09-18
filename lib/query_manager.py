import pandas as pd
from .ensure_configuration import ensure_configuration

def configured_manager(*configurations):
  def configuration(cls):
    pd.set_option("display.max_columns", None)
    pd.set_option("display.max_rows", None)
    pd.set_option("display.width", None)
    cls.configurations = configurations
    return cls
  return configuration

class QueryManager(object):
  def __init__(self, config):
    self.config = ensure_configuration(config, *self.configurations)
    if self.config is None:
      raise Exception("Failed to parse configuration.")
    self.reset()

  def reset(self):
    raise NotImplemented

  def run(self, query_str):
    """
    Executes the command in the database. If the command is a successful query, returns
    a pandas dataframe.
    """
    raise NotImplemented

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
        self.reset()
