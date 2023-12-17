import os
import yaml

def ensure_configuration(config_path, *keys):
  """
  Ensures that the configuration file exists, that it is valid YAML, and that it contains values
  for the required keys.

  @param config_path [str] path to config YAML.

  @return Dictionary representation of the config file if the file exists, is valid, and contains
  the required keys. None otherwise.
  """
  if not os.path.exists(config_path):
    print(f"File {config_path} does not exist.")
    return
  try:
    with open(config_path, "r") as f:
      config = yaml.safe_load(f)
      if config is None:
        print("Config file is empty.")
        return
      absent = list(filter(lambda key: key not in config, keys))
      for key in absent:
        print(f"Config file at {config_path} must contain {key}.")
      if not any(absent):
        return config
      else:
        return
  except yaml.YAMLError as e:
    print(f"The YAML file at {config_path} is invalid. Error: {e}")
    return