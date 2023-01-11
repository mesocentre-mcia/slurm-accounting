import os.path
import logging
import re
import datetime

logger = logging.getLogger("slurm_accounting.config")

from configparser import (
    SafeConfigParser, NoOptionError, NoSectionError
)

class Config(object):
    def __init__(self, config_path):
        try:
            self.config = SafeConfigParser(allow_no_value = True)
        except TypeError:
            self.config = SafeConfigParser()

        self.config_path = config_path
        self.bootstrapFile()

    def bootstrapFile(self):
        if os.path.isfile(self.config_path):
            self.config.read(self.config_path)

    def write(self):
        file_ = open(self.config_path, "w")
        self.config.write(file_)
        file_.close()

    def has(self, section, option):
        return self.config.has_option(section, option)

    def get(self, section, option = None, defaultValue = None):
        return self.__get(self.config.get, section, option, defaultValue)

    def getint(self, section, option = None, defaultValue = None):
        return self.__get(self.config.getint, section, option, defaultValue)

    def getfloat(self, section, option = None, defaultValue = None):
        return self.__get(self.config.getfloat, section, option, defaultValue)

    def getboolean(self, section, option = None, defaultValue = None):
        return self.__get(self.config.getboolean, section, option, defaultValue)

    def getdate(self, section, option = None, defaultValue = None):
        s = self.__get(self.config.get, section, option, defaultValue)
        return datetime.datetime.strptime(s, "%Y-%m-%d")

    def __get(self, method, section, option = None, defaultValue = None):
        value = defaultValue
        try:
            if not option:
                return self.config.items(section)
            value = method(section, option)
        except NoOptionError:
            if defaultValue == None:
                logging.error("Option \"%s\" missing in section \"%s\" from configuration \"%s\"" % (option, section, self.config_path))
                return None
        except NoSectionError:
            if defaultValue == None:
                logging.error("Section missing \"%s\" from configuration \"%s\"" % (section, self.config_path))
                return None

        return value

    def set(self, section, option = None, value = ""):
        if section.lower() != "default" and not self.config.has_section(section):
            self.config.add_section(section)
        if option:
            self.config.set(section, option, value)

    def remove(self, section, option = None):
        if option:
            if not self.config.has_section(section):
                logging.error("No such section \"%s\" in file \"%s\"" % (section, self.configFilename))
            self.config.remove_option(section, option)
        else:
            self.config.remove_setcion(self, section)

    def sections(self, regexp = None):
        sections = self.config.sections()

        if regexp:
            filtered = []
            reg = re.compile(regexp)
            for s in sections:
                if reg.match(s):
                    filtered.append(s)
            sections = filtered

        return sections

    def section(self, section):
        return dict(self.config.items(section))

    def existsOrCreate(self, section, option, value):
        if self.config.has_section(section) and self.config.has_option(section, option):
            return False
        self.set(section, option, value)
        return True

def loggerConfig(logger, options):
    loglvl = getattr(logging, options.loglevel.upper())
    logger.setLevel(loglvl)
    log_handler = logging.StreamHandler()
    logfmt = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s : %(message)s", datefmt = "%Y/%m/%d %H:%M:%S")
    log_handler.setFormatter(logfmt)
    logger.addHandler(log_handler)

def find_config_file(path, conf_relpath):
    while True:
        try_path = os.path.join(path, 'etc', conf_relpath)
        if os.path.exists(try_path):
            return try_path

        d = os.path.dirname(path)

        if d == path:
            return None

        path = d
