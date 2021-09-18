from io import TextIOBase
import toml;
from more_itertools import flatten
from pathlib import Path, PurePath
from twisted.python import log
from datetime import datetime

class GenericDescriptor():
    def __set_name__(self, owner, name):
        self.public_name = name
        self.private_name = '_' + name

    def __get__(self, obj, objtype=None):
        value = getattr(obj, self.private_name)
        return value

    def __set__(self, obj, value):
        setattr(obj, self.private_name, value)

class CroesusConfig:
    __default_file__ = "Croesus.toml"
    __search_path__ = [
        Path.cwd(),
        Path(__file__).resolve().parent,
        PurePath(Path.home(), '.config'),
        Path("/opt/tnnt/config")
    ]
    server            = GenericDescriptor()
    port              = GenericDescriptor()
    ssl               = GenericDescriptor()
    channels          = GenericDescriptor()
    nick              = GenericDescriptor()
    username          = GenericDescriptor()
    bridge_bot        = GenericDescriptor()
    pwfile            = GenericDescriptor()
    logfile           = GenericDescriptor()
    clantags          = GenericDescriptor()
    irc_logdir        = GenericDescriptor()
    static_defs       = GenericDescriptor()
    scoreboard_file   = GenericDescriptor()
    server_tag        = GenericDescriptor()
    grace_days        = GenericDescriptor()
    current_year      = GenericDescriptor()
    realname          = GenericDescriptor()
    admins            = GenericDescriptor()
    remotes           = GenericDescriptor()
    webroot           = GenericDescriptor()
    botroot           = GenericDescriptor()
    nhroot            = GenericDescriptor()
    test              = GenericDescriptor()

    def __init__(self):
        self.server            = "irc.libera.chat"
        self.trigger           = "$"
        self.port              = 6697
        self.ssl               = True
        self.nick              = "Croesus"
        self.admins            = ["aoei", "K2", "Tangles"]
        self.bridge_bot        = "rld"
        self.channels          = ["#bot-test"]
        self.username          = "tnntbot"
        self.realname          = "tnnt bot"
        self.webroot           = "https://fixme.com/"
        self.botroot           = "/home/twisted"
        self.nhroot            = "/opt/nethack"
        self.pwfile            = "pw"
        self.clantags          = "clantags.json"
        self.logfile           = "bot.log"
        self.irc_logdir        = "irclogs"
        self.static_defs       = "static_defs.json"
        self.scoreboard_file   = "scoreboard.json"
        self.grace_days        = 5
        self.current_year      = datetime.now().year
        self.test              = True
        self.remotes           = []
    
    def update(self, dict_obj):
        for key, val in flatten(
            map(lambda x: iter(dict_obj[x].items()),
                iter(dict_obj.keys()))
            ):
                self.__dict__["_" + key] = val

    def from_file(self, file_path=None):
        try:
            self.update(toml.load(file_path))
        except:
            log.err(f"parsing {file_path}: failed")
            raise

    def fetch_and_update(self):
        path_join = lambda p: Path(p, self.__default_file__).resolve()
        fexists = lambda f: Path(f).resolve().exists()
        parses = lambda p: toml.load(p)
        try:
            self.update(next(map(parses,
                filter(fexists, map(path_join, iter(self.__search_path__))))))
        except:
            log.err(f"could not find config file {self.__default_file__} in search path: {self.__search_path__}")
            raise

    def fetch(self, file_path=None):
        if file_path:
            self.from_file(file_path)
        else:
            self.fetch_and_update()
