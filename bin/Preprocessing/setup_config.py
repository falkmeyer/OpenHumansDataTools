import configparser
import pandas as pd
import os


# Config Parser Example: https://alexandra-zaharia.github.io/posts/python-configuration-and-dataclasses/

class DynamicConfig:
    def __init__(self, conf):
        if not isinstance(conf, dict):
            raise TypeError(f'dict expected, found {type(conf).__name__}')

        self._raw = conf
        for key, value in self._raw.items():
            setattr(self, key, value)


class DynamicConfigIni:
    def __init__(self, conf):
        if not isinstance(conf, configparser.ConfigParser):
            raise TypeError(f'ConfigParser expected, found {type(conf).__name__}')

        self._raw = conf
        for key, value in self._raw.items():
            setattr(self, key, DynamicConfig(dict(value.items())))


class Setup_Config():
    '''
        Pass as argument a path to the config.ini file.
    '''


    def __init__(self, config_path):

        # root dir sepsis_zukunftsvorhersage of project
        self.script_root_dir = os.path.dirname(os.path.dirname(os.path.abspath( os.path.dirname( __file__ ) ) ))        

        parser = configparser.ConfigParser()
        parser.read_file(open(config_path))
        self.config = DynamicConfigIni(parser)


        # TODO: add setup validation
        #self.passed_configs = [] # At least level one check passed