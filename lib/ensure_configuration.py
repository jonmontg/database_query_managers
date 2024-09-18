import os
import yaml

class Config(object):
  def __init__(self, obj):
    self.obj = obj

  def __enter__(self):
    if type(self.obj) == dict:
      return self.obj
    elif type(self.obj) == str:
      if not os.path.exists(self.obj):
        raise FileNotFoundError(f"File {self.obj} does not exist.")
      with open(self.obj, "r") as f:
        config = yaml.safe_load(f)
        if config is None:
          raise ValueError("Config file is empty.")
        return config
    else:
      raise ValueError(f"Configuration type is not recognized: {type(self.obj)}")

  def __exit__(self, type, value, traceback):
    pass

def ensure_configuration(config_obj, *keys):
  """
  Ensures that the configuration file exists, that it is valid YAML, and that it contains values
  for the required keys.

  @param config_obj [str or dict] path to config YAML or configuration dictionary

  @return Dictionary representation of the config file if the file exists, is valid, and contains
  the required keys. None otherwise.
  """
  with Config(config_obj) as config:
    absent = list(filter(lambda key: key not in config, keys))
    if any(absent):
      raise ValueError(f"Required item(s) missing from configuration: {', '.join(absent)}")
    return config
