import os
import yaml

def ensure_configuration(config, *keys):
  """
  Ensures that the configuration file exists, that it is valid YAML, and that it contains values
  for the required keys.

  @param config [str or dict] path to config YAML or configuration dictionary

  @return Dictionary representation of the config file if the file exists, is valid, and contains
  the required keys. None otherwise.
  """
  print("parsing")
  if type(config) == str:
    if not os.path.exists(config):
      print(f"File {config} does not exist.")
      return
    with open(config, "r") as f:
      config = yaml.safe_load(f)
      if config is None:
        print("Config file is empty.")
        return
  elif type(config) != dict:
    print("Invalid configuration. Must be a file path or a dictionary.")
  absent = list(filter(lambda key: key not in config, keys))
  for key in absent:
    print(f"Config file at {config} must contain {key}.")
  if any(absent):
    return
  return config
