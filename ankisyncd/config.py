import configparser
import logging
import os
from os.path import dirname, realpath

logger = logging.getLogger("ankisyncd")

# 配置文件可能存在的路径
paths = [
    "/etc/ankisyncd/ankisyncd.conf",
    os.environ.get("XDG_CONFIG_HOME") and
        (os.path.join(os.environ['XDG_CONFIG_HOME'], "ankisyncd", "ankisyncd.conf")) or
        os.path.join(os.path.expanduser("~"), ".config", "ankisyncd", "ankisyncd.conf"),
    os.path.join(dirname(dirname(realpath(__file__))), "ankisyncd.conf"),
]

# Get values from ENV and update the config. To use this prepend `ANKISYNCD_`
# to the uppercase form of the key. E.g, `ANKISYNCD_SESSION_MANAGER` to set
# `session_manager`
def load_from_env(conf):
    print("config.py.load_from_env() 从环境变量中加载配置")
    logger.debug("Loading/overriding config values from ENV")
    for env in os.environ:
        if env.startswith('ANKISYNCD_'):
            config_key = env[10:].lower()
            conf[config_key] = os.getenv(env)
            logger.info("Setting {} from ENV".format(config_key))

def load(path=None):
    print("config.py.load() 加载配置")
    choices = paths
    parser = configparser.ConfigParser()
    if path:
        choices = [path]
    for path in choices:
        logger.debug("config.location: trying {}".format(path))
        try:
            parser.read(path)
            conf = parser['sync_app']
            logger.info("Loaded config from {}".format(path))
            load_from_env(conf)
            return conf
        except KeyError:
            pass
    raise Exception("No config found, looked for {}".format(", ".join(choices)))
