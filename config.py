from configparser import ConfigParser

def load_config(section):
    config = ConfigParser()
    config.read('config.ini')
    return {key: value for key, value in config.items(section)}