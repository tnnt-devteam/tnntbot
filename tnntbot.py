#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""

*** THIS IS THE TNNT BOT ***

tnntbot.py - a game-reporting and general services IRC bot for
              The November Nethack Tournament

Copyright (c) 2018 A. Thomson, K. Simpson
Based loosely on original code from:
deathbot.py - a game-reporting IRC bot for AceHack
Copyright (c) 2011, Edoardo Spadolini
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

1. Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in the
documentation and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""

from twisted.internet import reactor, protocol, ssl, task
from twisted.internet.protocol import Protocol, ReconnectingClientFactory
from twisted.words.protocols import irc
from twisted.python import filepath, log
from twisted.python.logfile import DailyLogFile
from twisted.application import internet, service
from datetime import datetime, timedelta
import site     # to help find botconf
import base64
import time     # for $time and rate limiting
import os       # for check path exists (dumplogs), and chmod
import stat     # for chmod mode bits
import re       # for hello, and other things.
import urllib.request, urllib.parse, urllib.error   # for dealing with NH4 variants' #&$#@ spaces in filenames.
import shelve   # for persistent $tell messages
import random   # for $rng and friends
import glob     # for matching in $whereis
import json     # for tournament scoreboard things
import resource  # for memory usage in status command

# command trigger - this should be in tnntbotconf - next time.
TRIGGER = '$'
site.addsitedir('.')
from tnntbotconf import HOST, PORT, CHANNELS, NICK, USERNAME, REALNAME, BOTDIR
from tnntbotconf import PWFILE, FILEROOT, WEBROOT, LOGROOT, ADMIN, YEAR
from tnntbotconf import SERVERTAG
try:
    from tnntbotconf import SPAMCHANNELS
except:
    SPAMCHANNELS = CHANNELS
try: from tnntbotconf import DCBRIDGE
except:
    DCBRIDGE = None
try:
    from tnntbotconf import TEST
except:
    TEST = False
try:
    from tnntbotconf import GRACEDAYS
except:
    GRACEDAYS = 5
try:
    from tnntbotconf import REMOTES
except:
    SLAVE = True
    REMOTES = {}
try:
    from tnntbotconf import MASTERS
except:
    SLAVE = False
    MASTERS = []
try:
    #from tnntbotconf import LOGBASE, IRCLOGS
    from tnntbotconf import IRCLOGS
except:
    #LOGBASE = BOTDIR + "/tnntbot.log"
    IRCLOGS = LOGROOT

# JSON configuration files are deprecated - trophy/achievement tracking removed
if not SLAVE:
    # Hardcoded game data for NetHack roles, races, aligns, genders
    NETHACK_ROLES = ["Arc", "Bar", "Cav", "Hea", "Kni", "Mon", "Pri", "Ran", "Rog", "Sam", "Tou", "Val", "Wiz"]
    NETHACK_RACES = ["Dwa", "Elf", "Gno", "Hum", "Orc"]
    NETHACK_ALIGNS = ["Cha", "Law", "Neu"]
    NETHACK_GENDERS = ["Mal", "Fem"]

    # twitter - minimalist twitter api: http://mike.verdone.ca/twitter/
    # pip install twitter
    # set TWIT to false to prevent tweeting
    #TWIT = True
    #try:
    #    from tnntbotconf import TWITAUTH
    #except:
    #    print("no TWITAUTH - twitter disabled")
    #    TWIT = False
    #try:
    #    from twitter import Twitter, OAuth
    #except:
    #    print("Unable to import from twitter module")
    #    TWIT = False

CLANTAGJSON = BOTDIR + "/clantag.json"

# Rate limiting constants
RATE_LIMIT_WINDOW = 60  # Rate limiting time window in seconds
RATE_LIMIT_COMMANDS = 60   # Commands per minute for all operations (1/second)
BURST_WINDOW = 1        # Burst protection: only 1 command per second window
ABUSE_THRESHOLD = 10    # Consecutive commands before abuse penalty
ABUSE_WINDOW = 30       # Time window for abuse detection (seconds)
ABUSE_PENALTY = 900     # Abuse penalty duration in seconds (15 minutes)
RESPONSE_RATE_LIMIT = 1   # Max penalty messages per 2 minutes to prevent spam
RESPONSE_RATE_WINDOW = 120  # Penalty message rate limit window (2 minutes)

# Pre-compiled regex patterns for better performance
RE_COLOR_FG_BG = re.compile(r'\x03\d\d,\d\d')  # fg,bg pair
RE_COLOR_FG = re.compile(r'\x03\d\d')  # fg only
RE_COLOR_END = re.compile(r'[\x1D\x03\x0f]')  # end of colour and italics
RE_DICE_CMD = re.compile(r'^\d*d\d*$')  # dice command pattern
RE_SPACE_COLOR = re.compile(r'^ [\x1D\x03\x0f]*')  # space and color codes

# some lookup tables for formatting messages
# these are not yet in conig.json
role = { "Arc": "Archeologist",
         "Bar": "Barbarian",
         "Cav": "Caveman",
         "Hea": "Healer",
         "Kni": "Knight",
         "Mon": "Monk",
         "Pri": "Priest",
         "Ran": "Ranger",
         "Rog": "Rogue",
         "Sam": "Samurai",
         "Tou": "Tourist",
         "Val": "Valkyrie",
         "Wiz": "Wizard"
       }

race = { "Dwa": "Dwarf",
         "Elf": "Elf",
         "Gno": "Gnome",
         "Hum": "Human",
         "Orc": "Orc"
       }

align = { "Cha": "Chaotic",
          "Law": "Lawful",
          "Neu": "Neutral"
        }

gender = { "Mal": "Male",
           "Fem": "Female"
         }

def safe_int_parse(s):
    """Safely parse integers, including hex values like 0x1234"""
    try:
        # Try to parse as int, supports base 10, hex (0x), octal (0o), binary (0b)
        return int(s, 0)
    except ValueError:
        # If that fails, try without base detection
        try:
            return int(s)
        except ValueError:
            return 0  # Default to 0 for invalid values

def sanitize_format_string(text):
    """Sanitize text to prevent format string injection attacks.

    Escapes curly braces that could be used in format string attacks.
    """
    if not isinstance(text, str):
        return text
    return text.replace('{', '{{').replace('}', '}}')

def fromtimestamp_int(s):
    return datetime.fromtimestamp(int(s))

def timedelta_int(s):
    return timedelta(seconds=int(s))

def isodate(s):
    return datetime.strptime(s, "%Y%m%d").date()

def fixdump(s):
    return s.replace("_",":")

xlogfile_parse = dict.fromkeys(
    ("points", "deathdnum", "deathlev", "maxlvl", "hp", "maxhp", "deaths",
     "uid", "turns", "xplevel", "exp","depth","dnum","score","amulet"), int)
xlogfile_parse.update(dict.fromkeys(
    ("conduct", "event", "carried", "flags", "achieve"), safe_int_parse))

def parse_xlogfile_line(line, delim):
    record = {}
    # User-controlled fields that need sanitization
    user_controlled_fields = {"name", "charname", "death", "role", "race",
                             "gender", "align", "bones_killed", "bones_rank",
                             "killed_uniq", "wish", "shout", "genocided_monster",
                             "shop", "shopkeeper"}
    for field in line.strip().decode(encoding='UTF-8', errors='ignore').split(delim):
        key, _, value = field.partition("=")
        if key in xlogfile_parse:
            value = xlogfile_parse[key](value)
        # Sanitize user-controlled fields to prevent format string injection
        elif key in user_controlled_fields:
            value = sanitize_format_string(value)
        record[key] = value
    return record

class DeathBotProtocol(irc.IRCClient):
    nickname = NICK
    username = USERNAME
    realname = REALNAME
    admin = ADMIN
    slaves = {}
    for r in REMOTES:
        slaves[REMOTES[r][1]] = r
    # if we're the master, include ourself on the slaves list
    if not SLAVE:
        if NICK not in slaves: slaves[NICK] = [WEBROOT,NICK,FILEROOT]
        #...and the masters list
        if NICK not in MASTERS: MASTERS += [NICK]
    try:
        with open(PWFILE, "r") as f:
            password = f.read().strip()
    except:
        password = "NotTHEPassword"
    #if TWIT:
    #   try:
    #       gibberish_that_makes_twitter_work = open(TWITAUTH,"r").read().strip().split("\n")
    #       twit = Twitter(auth=OAuth(*gibberish_that_makes_twitter_work))
    #   except Exception as e:
    #       print("Failed to auth to twitter")
    #       print(e)
    #       TWIT = False


    sourceURL = "https://github.com/tnnt-devteam/tnntbot"
    versionName = "tnntbot.py"
    versionNum = "0.1"
    # bot_start_time will be set in signedOn() for accurate uptime tracking

    dump_url_prefix = WEBROOT + "userdata/{name[0]}/{name}/"
    dump_file_prefix = FILEROOT + "dgldir/userdata/{name[0]}/{name}/"

    # tnnt runs on UTC
    os.environ["TZ"] = "UTC"
    time.tzset()
    ttime = { "start": datetime(int(YEAR),11,1,0,0,0),
              "end"  : datetime(int(YEAR),12,1,0,0,0)
            }

    chanLog = {}
    chanLogName = {}
    activity = {}
    if not SLAVE:
        scoresURL = "https://tnnt.org/leaderboards or https://tnnt.org/trophies"
        ttyrecURL = WEBROOT + "nethack/ttyrecs"
        rceditURL = WEBROOT + "nethack/rcedit"
        helpURL = sourceURL + "/blob/master/botuse.txt"
        logday = time.strftime("%d")
        for c in CHANNELS:
            activity[c] = 0
            if IRCLOGS:
                chanLogName[c] = IRCLOGS + "/" + c + time.strftime("-%Y-%m-%d.log")
                try:
                    chanLog[c] = open(chanLogName[c],'a')
                except:
                    chanLog[c] = None
                if chanLog[c]: os.chmod(chanLogName[c],stat.S_IRUSR|stat.S_IWUSR|stat.S_IRGRP|stat.S_IROTH)

    xlogfiles = {filepath.FilePath(FILEROOT+"tnnt/var/xlogfile"): ("tnnt", "\t", "tnnt/dumplog/{starttime}.tnnt.html")}
    livelogs  = {filepath.FilePath(FILEROOT+"tnnt/var/livelog"): ("tnnt", "\t")}
    # Scoreboard removed - JSON files deprecated
    try:
        with open(CLANTAGJSON) as f:
            clanTag = json.load(f)
    except:
        clanTag = {}

    # for displaying variants and server tags in colour
    displaystring = {"hdf-us"  : "\x1D\x0304US\x03\x0F",
                     "hdf-au"  : "\x1D\x0303AU\x03\x0F",
                     "hdf-eu"  : "\x1D\x0312EU\x03\x0F",
                     "hdf-test": "\x1D\x0308TS\x03\x0F",
                     "trophy"  : "\x1D\x0313Tr\x03\x0F",
                     "achieve" : "\x1D\x0305Ac\x03\x0F",
                     "clan"    : "\x1D\x0312R\x03\x0F",
                     "died"    : "\x02\x1D\x0304D\x03\x0F",
                     "quit"    : "\x02\x1D\x0308Q\x03\x0F",
                     "ascended": "\x02\x1D\x0309A\x03\x0F",
                     "escaped" : "\x02\x1D\x0310E\x03\x0F"}

    # put the displaystring for a thing in square brackets
    def displaytag(self, thing):
       return '[' + self.displaystring.get(thing,thing) + ']'

    # for !who or !players or whatever we end up calling it
    # Reduce the repetitive crap
    DGLD=FILEROOT+"dgldir/"
    INPR=DGLD+"inprogress-"
    inprog = {"tnnt" : [INPR+"tnnt/"]}

    # for !whereis
    whereis = {"tnnt": [FILEROOT+"tnnt/var/whereis/"]}

    dungeons = ["The Dungeons of Doom", "Gehennom", "The Gnomish Mines",
                "The Quest", "Sokoban", "Fort Ludios", "DevTeam Office",
                "Deathmatch Arena", "robotfindskitten", "Vlad's Tower",
                "The Elemental Planes"]

    looping_calls = None
    commands = {}

    def initStats(self, statset):
        self.stats[statset] = { "race"    : {},
                                "role"    : {},
                                "gender"  : {},
                                "align"   : {},
                                "points"  : 0,
                                "turns"   : 0,
                                "realtime": 0,
                                "games"   : 0,
                                "scum"    : 0,
                                "ascend"  : 0,
                              }

    # SASL auth nonsense required if we run on AWS
    # copied from https://github.com/habnabit/txsocksx/blob/master/examples/tor-irc.py
    # irc_CAP and irc_9xx are UNDOCUMENTED.
    def connectionMade(self):
        self.sendLine('CAP REQ :sasl')
        #self.deferred = Deferred()
        irc.IRCClient.connectionMade(self)

    def irc_CAP(self, prefix, params):
        if params[1] != 'ACK' or params[2].split() != ['sasl']:
            print('sasl not available')
            self.quit('')
        sasl_string = '{0}\0{0}\0{1}'.format(self.nickname, self.password)
        sasl_b64_bytes = base64.b64encode(sasl_string.encode(encoding='UTF-8',errors='strict'))
        self.sendLine('AUTHENTICATE PLAIN')
        self.sendLine('AUTHENTICATE ' + sasl_b64_bytes.decode('UTF-8'))

    def irc_903(self, prefix, params):
        self.sendLine('CAP END')

    def irc_904(self, prefix, params):
        print('sasl auth failed', params)
        self.quit('')
    irc_905 = irc_904

    def signedOn(self):
        self.factory.resetDelay()
        self.startHeartbeat()
        if not SLAVE:
            for c in CHANNELS:
                self.join(c)
        random.seed()

        # Track bot start time for uptime calculation
        self.starttime = time.time()

        self.logs = {}
        # boolean for whether announcements from the log are 'spam', after dumpfmt
        # true for livelogs, false for xlogfiles
        for xlogfile, (variant, delim, dumpfmt) in self.xlogfiles.items():
            self.logs[xlogfile] = (self.xlogfileReport, variant, delim, dumpfmt, False)
        for livelog, (variant, delim) in self.livelogs.items():
            self.logs[livelog] = (self.livelogReport, variant, delim, "", True)

        self.logs_seek = {}
        self.looping_calls = {}

        #stats for hourly/daily spam
        self.stats = {}
        self.initStats("hour")
        self.initStats("day")
        self.initStats("full")

        if not SLAVE:
            # work out how much hour is left
            nowtime = datetime.now()
            # add 1 hour, then subtract min, sec, usec to get exact time of next hour.
            nexthour = nowtime + timedelta(hours=1)
            nexthour -= timedelta(minutes=nexthour.minute,
                                  seconds=nexthour.second,
                                  microseconds=nexthour.microsecond)
            hourleft = (nexthour - nowtime).total_seconds() + 0.5 # start at 0.5 seconds past the hour.
            reactor.callLater(hourleft, self.startHourly)

            # round up of basic stats for milestone reporting.
            self.summaries = {}
            for s in self.slaves:
                # summary stats for each server
                self.summaries[s] = { "games"   : 0,
                                      "points"  : 0,
                                      "turns"   : 0,
                                      "realtime": 0,
                                      "ascend"  : 0 }
            # existing totals so we know when we pass a threshold
            self.summary = { "games"   : 0,
                             "points"  : 0,
                             "turns"   : 0,
                             "realtime": 0,
                             "ascend"  : 0 }
            self.milestones = { "games"   : [500, 1000, 5000, 10000, 50000, 100000],
                                "points"  : [50000000, 100000000, 500000000, 1000000000, 5000000000],
                                "turns"   : [1000000, 5000000, 10000000, 50000000, 100000000],
                                "realtime": [50, 100, 500, 1000, 5000 ], # converted to 24h days (86400s)
                                "ascend"  : [50, 100, 200, 300, 400, 500]}

        #lastgame shite
        self.lastgame = "No last game recorded"
        self.lg = {}
        self.lastasc = "No last ascension recorded"
        self.la = {}

        # streaks
        self.curstreak = {}
        self.longstreak = {}

        # ascensions (for !asc)
        # "!asc plr" will give asc stats for player.
        # "!asc" will be as above, assuming requestor's nick.
        # asc[player][role] = count;
        # asc[player][race] = count;
        # asc[player][align] = count;
        # asc[player][gender] = count;
        # assumes 3-char abbreviations for role/race/align/gender, and no overlaps.
        # for asc ratio we need total games too
        # allgames[player] = count;
        self.asc = {}
        self.allgames = {}

        # for !tell
        try:
            self.tellbuf = shelve.open(BOTDIR + "/tellmsg.db", writeback=True)
        except:
            self.tellbuf = shelve.open(BOTDIR + "/tellmsg", writeback=True, protocol=2)

        # Initialize rate limiting
        self.rate_limits = {}  # user -> list of command timestamps
        self.abuse_penalties = {}  # user -> penalty end timestamp
        self.consecutive_commands = {}  # user -> [command_time, command_time, ...]
        self.penalty_responses = {}  # user -> [timestamp, timestamp, ...]
        self.last_command_time = {}  # user -> timestamp of last command

        # Commands must be lowercase here.
        self.commands = {"ping"     : self.doPing,
                         "time"     : self.doTime,
                         "tell"     : self.takeMessage,
                         "source"   : self.doSource,
                         "lastgame" : self.multiServerCmd,
                         "lastasc"  : self.multiServerCmd,
                         "scores"   : self.doScoreboard,
                         "sb"       : self.doScoreboard,
                         "ttyrec"   : self.doTtyrec,
                         "rcedit"   : self.doRCedit,
                         "commands" : self.doCommands,
                         "help"     : self.doHelp,
                         "score"    : self.doScore,
                         "clanscore": self.doClanScore,
                         "clantag"  : self.doClanTag,
                         "status"   : self.doStatus,
                         "players"  : self.multiServerCmd,
                         "who"      : self.multiServerCmd,
                         "asc"      : self.multiServerCmd,
                         "streak"   : self.multiServerCmd,
                         "whereis"  : self.multiServerCmd,
                         "stats"    : self.multiServerCmd,
                         # these ones are for control messages between master and slaves
                         # sender is checked, so these can't be used by the public
                         # this one is a message from slave with current stats, for milestone reporting
                         "#s#"      : self.checkMilestones,
                         # query from master to slave
                         "#q#"      : self.doQuery,
                         # responses from slave to master
                         "#p#"      : self.doResponse, # 'partial' for long responses
                         "#r#"      : self.doResponse}
        # commands executed based on contents of #Q# message
        self.qCommands = {"players" : self.getPlayers,
                          "who"     : self.getPlayers,
                          "whereis" : self.getWhereIs,
                          "asc"     : self.getAsc,
                          "streak"  : self.getStreak,
                          "lastasc" : self.getLastAsc,
                          "lastgame": self.getLastGame,
                          "stats"   : self.getStats, # user requests !stats
                          "hstats"  : self.getStats, # scheduled hourly stats
                          "cstats"  : self.getStats, # cumulative day stats (6-hourly)
                          "dstats"  : self.getStats, # scheduled daily stats
                          "fstats"  : self.getStats} # scheduled final stats

        # callbacks to run when all slaves have responded
        self.callBacks = {"players" : self.outPlayers,
                          "who"     : self.outPlayers,
                          "whereis" : self.outWhereIs,
                          "asc"     : self.outAscStreak,
                          "streak"  : self.outAscStreak,
                          # TODO: timestamp these so we can report the very last one
                          # For now, use the !asc/!streak callback as it's generic enough
                          "lastasc" : self.outAscStreak,
                          "lastgame": self.outAscStreak,
                          "stats"   : self.outStats,
                          "hstats"  : self.outStats,
                          "cstats"  : self.outStats,
                          "dstats"  : self.outStats,
                          "fstats"  : self.outStats}

        # checkUsage outputs a message and returns false if input is bad
        # returns true if input is ok
        self.checkUsage ={"whereis" : self.usageWhereIs,
                          "asc"     : self.usageAsc,
                          "streak"  : self.usageStreak}

        # seek to end of livelogs
        for filepath in self.livelogs:
            with filepath.open("r") as handle:
                handle.seek(0, 2)
                self.logs_seek[filepath] = handle.tell()

        # sequentially read xlogfiles from beginning to pre-populate lastgame data.
        for filepath in self.xlogfiles:
            with filepath.open("r") as handle:
                for line in handle:
                    delim = self.logs[filepath][2]
                    game = parse_xlogfile_line(line, delim)
                    game["variant"] = self.logs[filepath][1]
                    game["dumpfmt"] = self.logs[filepath][3]
                    for line in self.logs[filepath][0](game,False):
                        pass
                self.logs_seek[filepath] = handle.tell()

        # poll logs for updates every 3 seconds
        for filepath in self.logs:
            self.looping_calls[filepath] = task.LoopingCall(self.logReport, filepath)
            self.looping_calls[filepath].start(3)

        # Additionally, keep an eye on our nick to make sure it's right.
        # Perhaps we only need to set this up if the nick was originally
        # in use when we signed on, but a 30-second looping call won't kill us
        self.looping_calls["nick"] = task.LoopingCall(self.nickCheck)
        self.looping_calls["nick"].start(30)
        # Trophy/achievement tracking removed - JSON files deprecated
        # Update local milestone summary to master every 5 minutes
        self.looping_calls["summary"] = task.LoopingCall(self.updateSummary)
        self.looping_calls["summary"].start(300)

    #def tweet(self, message):
    #    if TWIT:
    #        message = self.stripText(message)
    #        try:
    #            if TEST:
    #                 message = "[TEST] " + message
    #                 print("Not tweeting in test mode: " + message)
    #                 return
    #            self.twit.statuses.update(status=message)
    #        except Exception as e:
    #            print("Bad tweet: " + message)
    #            print(e)

    def nickCheck(self):
        # also rejoin the channel here, in case we drop off for any reason
        if not SLAVE:
            for c in CHANNELS: self.join(c)
        if (self.nickname != NICK):
            self.setNick(NICK)

    def nickChanged(self, nn):
        # catch successful changing of nick from above and identify with nickserv
        self.msg("NickServ", "identify " + nn + " " + self.password)

    def logRotate(self):
        if not IRCLOGS: return
        self.logday = time.strftime("%d")
        for c in CHANNELS:
            if self.chanLog[c]: self.chanLog[c].close()
            self.chanLogName[c] = IRCLOGS + "/" + c + time.strftime("-%Y-%m-%d.log")
            try: self.chanLog[c] = open(self.chanLogName[c],'a') # 'w' is probably fine here
            except: self.chanLog[c] = None
            if self.chanLog[c]: os.chmod(self.chanLogName[c],stat.S_IRUSR|stat.S_IWUSR|stat.S_IRGRP|stat.S_IROTH)

    def stripText(self, msg):
        # strip the colour control stuff out
        # Use pre-compiled regex patterns for better performance
        message = RE_COLOR_FG_BG.sub('', msg) # fg,bg pair
        message = RE_COLOR_FG.sub('', message) # fg only
        message = RE_COLOR_END.sub('', message) # end of colour and italics
        return message

    # Write log
    def log(self, channel, message):
        if not self.chanLog.get(channel,None): return
        message = self.stripText(message)
        if time.strftime("%d") != self.logday: self.logRotate()
        self.chanLog[channel].write(time.strftime("%H:%M ") + message + "\n")
        self.chanLog[channel].flush()

    # wrapper for "msg" that logs if msg dest is channel
    # Need to log our own actions separately as they don't trigger events
    def msgLog(self, replyto, message):
        if replyto in CHANNELS:
            self.log(replyto, "<" + self.nickname + "> " + message)
        self.msg(replyto, message)

    # Similar wrapper for describe
    def describeLog(self,replyto, message):
        if replyto in CHANNELS:
            self.log("* " + self.nickname + " " + message)
        self.describe(replyto, message)

    # Tournament announcements typically go to the channel
    # ...and to the channel log
    # ...and to twitter. announce() does this.
    # spam flag allows more verbosity in some channels
    def announce(self, message, spam = False):
        if not TEST:
            # Only announce during tournament, or short grace period following
            nowtime = datetime.now()
            game_on =  (nowtime > self.ttime["start"]) and (nowtime < (self.ttime["end"] + timedelta(days=GRACEDAYS)))
            if not game_on: return
        chanlist = CHANNELS
        if spam:
            chanlist = SPAMCHANNELS #only
        #else: # only tweet non spam
            #self.tweet(message)
        for c in chanlist:
            self.msgLog(c, message)

    # construct and send response.
    # replyto is channel, or private nick
    # sender is original sender of query
    def respond(self, replyto, sender, message):
        if (replyto.lower() == sender.lower()): #private
            self.msg(replyto, message)
        else: #channel - prepend "Nick: " to message
            self.msgLog(replyto, sender + ": " + message)

    def _checkRateLimit(self, sender, command):
        """
        Check if user is rate limited for this command.
        Returns True if command should be allowed, False if rate limited.
        """
        try:
            now = time.time()

            # Check if user is currently under abuse penalty
            if sender in self.abuse_penalties:
                if now < self.abuse_penalties[sender]:
                    return False  # Still under penalty
                else:
                    # Penalty expired, clean up
                    del self.abuse_penalties[sender]
                    if sender in self.consecutive_commands:
                        del self.consecutive_commands[sender]

            # Clean up old rate limit entries (older than 60 seconds)
            if sender in self.rate_limits:
                self.rate_limits[sender] = [
                    timestamp for timestamp in self.rate_limits[sender]
                    if now - timestamp < RATE_LIMIT_WINDOW
                ]

                # Remove empty entries
                if not self.rate_limits[sender]:
                    del self.rate_limits[sender]

            # Initialize user's rate limit tracking if needed
            if sender not in self.rate_limits:
                self.rate_limits[sender] = []

            # Check if user has exceeded rate limit
            if len(self.rate_limits[sender]) >= RATE_LIMIT_COMMANDS:
                return False  # Rate limited

            # Record this command attempt
            self.rate_limits[sender].append(now)

            # Track consecutive commands for abuse detection
            if sender not in self.consecutive_commands:
                self.consecutive_commands[sender] = []

            # Clean up old consecutive command entries
            self.consecutive_commands[sender] = [
                timestamp for timestamp in self.consecutive_commands[sender]
                if now - timestamp < ABUSE_WINDOW
            ]

            # Add this command to consecutive tracking
            self.consecutive_commands[sender].append(now)

            # Check for abuse pattern
            if len(self.consecutive_commands[sender]) >= ABUSE_THRESHOLD:
                # Apply abuse penalty
                self.abuse_penalties[sender] = now + ABUSE_PENALTY
                # Clear rate limits to prevent further commands
                if sender in self.rate_limits:
                    del self.rate_limits[sender]
                return False  # Apply penalty

            return True  # Command allowed

        except Exception as e:
            print("Rate limiting error for {}: {}".format(sender, e))
            # Fail-safe: allow command if rate limiting breaks
            return True

    def _shouldSendPenaltyMessage(self, sender):
        """Check if we should send a rate limit penalty message."""
        try:
            now = time.time()

            # Initialize penalty response tracking if needed
            if sender not in self.penalty_responses:
                self.penalty_responses[sender] = []

            # Clean up old penalty response entries
            self.penalty_responses[sender] = [
                timestamp for timestamp in self.penalty_responses[sender]
                if now - timestamp < RESPONSE_RATE_WINDOW
            ]

            # Check if user has exceeded penalty message rate limit
            if len(self.penalty_responses[sender]) >= RESPONSE_RATE_LIMIT:
                return False  # Don't send penalty message

            # Record this penalty response
            self.penalty_responses[sender].append(now)
            return True  # Send penalty message

        except Exception as e:
            print("Penalty response rate limiting error for {}: {}".format(sender, e))
            return True  # Fail-safe: allow message

    def _checkBurstProtection(self, sender, command):
        """
        Check if user is sending commands too rapidly (burst protection).
        Returns True if command should be allowed, False if it should be silently ignored.
        """
        try:
            now = time.time()

            # Check last command time
            if sender in self.last_command_time:
                time_since_last = now - self.last_command_time[sender]
                if time_since_last < BURST_WINDOW:
                    # Too fast - silently ignore
                    return False

            # Update last command time
            self.last_command_time[sender] = now
            return True

        except Exception as e:
            print("Burst protection error for {}: {}".format(sender, e))
            return True  # Fail-safe: allow command

    def generate_dumplog_url(self, game, dumpfile):
        """Generate dumplog URL, checking local storage first, then S3.

        Returns the URL if file exists in either location, or a sorry message otherwise.
        """
        # First check if file exists locally
        if os.path.exists(dumpfile):
            # File exists locally, use regular URL
            dumpurl = urllib.parse.quote(game["dumpfmt"].format(**game))
            return self.dump_url_prefix.format(**game) + dumpurl

        # File doesn't exist locally - generate S3 URL
        # S3 URL structure differs by server
        s3_base = None
        if SERVERTAG == "hdf-us":
            s3_base = "https://hdf-us.s3.amazonaws.com/dumplogs/"
        elif SERVERTAG == "hdf-eu":
            s3_base = "https://hdf-eu.s3.amazonaws.com/dumplogs/"
        elif SERVERTAG == "hdf-au":
            s3_base = "https://hdf-au.s3.amazonaws.com/dumplogs/"

        if s3_base:
            # Generate S3 URL
            dumppath = urllib.parse.quote(game["dumpfmt"].format(**game))
            # S3 path structure: dumplogs/{name[0]}/{name}/{dumppath}
            # dumppath already contains tnnt/dumplog/ prefix
            first_char = game["name"][0] if game["name"] else "a"
            s3_url = "{base}{first}/{name}/{path}".format(
                base=s3_base,
                first=first_char,  # Keep original case for first character
                name=game["name"],  # Keep original case for name
                path=dumppath
            )
            return s3_url

        # If no S3 base configured for this server, return sorry message
        return "(sorry, no dump exists for {name})".format(**game)

    def _cleanupRateLimits(self):
        """Clean up old rate limiting data to prevent memory leaks."""
        try:
            now = time.time()

            # Clean up old rate limiting entries
            users_to_clean = []
            for user in list(self.rate_limits.keys()):
                # Remove timestamps older than rate limit window
                self.rate_limits[user] = [
                    timestamp for timestamp in self.rate_limits[user]
                    if now - timestamp < RATE_LIMIT_WINDOW
                ]
                # Remove empty entries
                if not self.rate_limits[user]:
                    users_to_clean.append(user)

            for user in users_to_clean:
                del self.rate_limits[user]

            # Clean up expired abuse penalties
            expired_penalties = []
            for user in list(self.abuse_penalties.keys()):
                if now >= self.abuse_penalties[user]:
                    expired_penalties.append(user)

            for user in expired_penalties:
                del self.abuse_penalties[user]
                # Also clean up consecutive commands for expired penalties
                if user in self.consecutive_commands:
                    del self.consecutive_commands[user]

            # Clean up old consecutive command tracking
            old_consecutive = []
            for user in list(self.consecutive_commands.keys()):
                # Remove old timestamps
                self.consecutive_commands[user] = [
                    timestamp for timestamp in self.consecutive_commands[user]
                    if now - timestamp < ABUSE_WINDOW * 2  # Keep for 2x abuse window
                ]
                if not self.consecutive_commands[user]:
                    old_consecutive.append(user)

            for user in old_consecutive:
                del self.consecutive_commands[user]

            # Clean up old penalty response tracking
            old_responses = []
            for user in list(self.penalty_responses.keys()):
                self.penalty_responses[user] = [
                    timestamp for timestamp in self.penalty_responses[user]
                    if now - timestamp < RESPONSE_RATE_WINDOW
                ]
                if not self.penalty_responses[user]:
                    old_responses.append(user)

            for user in old_responses:
                del self.penalty_responses[user]

            # Clean up old burst protection data
            old_burst = []
            for user in list(self.last_command_time.keys()):
                if now - self.last_command_time[user] > 3600:  # 1 hour
                    old_burst.append(user)

            for user in old_burst:
                del self.last_command_time[user]

        except Exception as e:
            print("Error during rate limit cleanup: {}".format(e))

    # Query/Response handling
    #Q#
    def doQuery(self, sender, replyto, msgwords):
        # called when slave gets queried by master.
        # msgwords is [ #Q#, <query_id>, <orig_sender>, <command>, ... ]
        if (sender in MASTERS) and (msgwords[3] in self.qCommands):
            # sender is passed to master; msgwords[2] is passed tp sender
            self.qCommands[msgwords[3]](sender,msgwords[2],msgwords[1],msgwords[3:])
        else:
            print("Bogus slave query from " + sender + ": " + " ".join(msgwords));

    #R# / #P#
    def doResponse(self, sender, replyto, msgwords):
        # called when slave returns query response to master
        # msgwords is [ #R#, <query_id>, [server-tag], command output, ...]
        # for long resps ([ #P#, <query>, output ]) * n, finishing with #R# msg as above
        # Assumes message fragments arrive in the same order as sent. Yeah, yeah I know...
        if sender in self.slaves and msgwords[1] in self.queries:
            self.queries[msgwords[1]]["resp"][sender] = self.queries[msgwords[1]]["resp"].get(sender,"") + " ".join(msgwords[2:])
            if msgwords[0] == "#R#": self.queries[msgwords[1]]["finished"][sender] = True
            if set(self.queries[msgwords[1]]["finished"].keys()) >= set(self.slaves.keys()):
                #all slaves have responded
                self.queries[msgwords[1]]["callback"](self.queries.pop(msgwords[1]))
        else:
            print("Bogus slave response from " + sender + ": " + " ".join(msgwords));

    # As above, but timed out receiving one or more responses
    def doQueryTimeout(self, query):
        # This gets called regardless, so only process if query still exists
        if query not in self.queries: return

        noResp = []
        for i in self.slaves.keys():
            if not self.queries[query]["finished"].get(i,False):
                noResp.append(i)
        if noResp:
            print("WARNING: Query " + query + ": No response from " + self.listStuff(noResp))
        self.queries[query]["callback"](self.queries.pop(query))

    #S#
    def checkMilestones(self, sender, replyto, msgwords):
        numbers = { 1000000: "One million",
                    5000000: "Five million",
                   10000000: "Ten million",
                   50000000: "50 million",
                  100000000: "100 million",
                  500000000: "500 million",
                 1000000000: "One billion",
                 5000000000: "Five billion" }
        statnames = { "games"  : "games played",
                      "ascend" : "ascended games",
                      "points" : "nethack points scored",
                      "turns"  : "turns played",
                     "realtime": "days spent playing nethack"}
        if sender not in self.slaves:
            return
        # if this is the first time the slave has contacted us since we restarted
        # we don't want to announce anything, because we risk repeating ourselves
        FirstContact = False
        if self.summaries[sender]["games"] == 0:
            FirstContact = True
        self.summaries[sender] = json.loads(" ".join(msgwords[1:]))
        for k in list(self.milestones.keys()):
            t = 0
            for s in self.summaries:
                t += self.summaries[s][k]
            if k == "realtime": t /= 86400 # days, not seconds
            if not FirstContact:
                for m in self.milestones[k]:
                    if self.summary[k] and t >= m and self.summary[k] < m:
                        self.announce("\x02TOURNAMENT MILESTONE:\x0f {0} {1}.".format(numbers.get(m,m), statnames.get(k,k)))
            self.summary[k] = t


    # Hourly/daily/special stats
    def spamStats(self, p, stats, replyto):
        # formatting awkwardness
        # do turns and points, or time.
        stat1lst = [ "{turns} turns, {points} points. ",
                      "{d}d {h:02d}:{m:02d} gametime. "
                   ]
        stat2str = { "align"  : "alignment" } # use get() to leave unchanged if not here
        periodStr = { "hour" : "\x02Hourly Stats\x0f at %F %H:00 %Z: ",
                      "day"  : "\x02DAILY STATS\x0f AT %F %H:00 %Z: ",
                      "news" : "\x02Current Day\x0f as of %F %H:%M %Z: ",
                      "full" : "\x02FINAL TOURNAMENT STATISTICS:\x0f "
                    }
        # hourly, we report one of role/race/etc. Daily, and for news, we report them all
        if p == "hour":
            if stats["games"] - stats["scum"] < 10: return
            stat1lst = [random.choice(stat1lst)]
            # weighted. role is more interesting than gender
            stat2lst = [random.choice(["role"] * 5 + ["race"] * 3 + ["align"] * 2 + ["gender"])]
        else:
            stat2lst = ["role", "race", "align", "gender"]
        cd = self.countDown()
        if cd["event"] == "start": cd["prep"] = "to go!"
        else: cd["prep"] = "remaining."
        if replyto:
            chanlist = [replyto]
        else:
            chanlist = SPAMCHANNELS
        if stats["games"] != 0:
            # mash the realtime value into d,h,m,s
            rt = int(stats["realtime"])
            stats["s"] = int(rt%60)
            rt //= 60
            stats["m"] = int(rt%60)
            rt //= 60
            stats["h"] = int(rt%24)
            rt //= 24
            stats["d"] = int(rt)

        msg_parts = [time.strftime(periodStr[p]) + "Games: {games}, Asc: {ascend}, Scum: {scum}. ".format(**stats)]

        if stats["games"] != 0:
            # Add stat1 messages
            msg_parts.extend([stat1.format(**stats) for stat1 in stat1lst])

            # Add stat2 messages
            stat2_parts = []
            for stat2 in stat2lst:
                # Find whatever thing from the list above had the most games, and how many games it had
                maxStat2 = dict(list(zip(["name","number"],max(iter(stats[stat2].items()), key=lambda x:x[1]))))
                # convert number to % of total (non-scum) games
                maxStat2["number"] = int(round(maxStat2["number"] * 100 / (stats["games"] - stats["scum"])))
                stat2_parts.append("({number}%{name})".format(**maxStat2))
            msg_parts.append(", ".join(stat2_parts) + ", ")

        if p != "full":
            msg_parts.append("{days}d {hours:02d}:{minutes:02d} {prep}".format(**cd))

        statmsg = "".join(msg_parts)

        if p != "full":
            for c in chanlist:
                self.msgLog(c, statmsg)
        else:
            for c in chanlist:
                self.msgLog(c, statmsg)
                self.msgLog(c, "We hope you enjoyed The November Nethack Tournament.")
                self.msgLog(c, "Thank you for playing.")

    def startCountdown(self,event,time):
        self.announce("The tournament {0}s in {1}...".format(event,time),True)
        for delay in range (1,time):
            reactor.callLater(delay,self.announce,"{0}...".format(time-delay),True)

#    def testCountdown(self, sender, replyto, msgwords):
#        self.startCountdown(msgwords[1],int(msgwords[2]))

    def hourlyStats(self):
        nowtime = datetime.now()

        # Clean up old rate limiting data
        self._cleanupRateLimits()

        # special case handling for start/end
        # we are running at the top of the hour
        # so checking we are within 1 minute of start/end time is sufficient
        if abs(nowtime - self.ttime["start"]) < timedelta(minutes=1):
            self.announce("###### TNNT {0} IS OPEN! ######".format(YEAR))
        elif abs(nowtime - self.ttime["end"]) < timedelta(minutes=1):
            self.announce("###### TNNT {0} IS CLOSED! ######".format(YEAR))
            self.multiServerCmd(NICK, NICK, ["fstats"])
            return
        elif abs(nowtime + timedelta(hours=1) - self.ttime["start"]) < timedelta(minutes=1):
            reactor.callLater(3597, self.startCountdown,"start",3) # 3 seconds to the next hour
        elif abs(nowtime + timedelta(hours=1) - self.ttime["end"]) < timedelta(minutes=1):
            reactor.callLater(3597, self.startCountdown,"end",3) # 3 seconds to the next hour
        game_on =  (nowtime > self.ttime["start"]) and (nowtime < self.ttime["end"])
        if TEST: game_on = True
        if not game_on: return

        if nowtime.hour == 0:
            self.multiServerCmd(NICK, NICK, ["dstats"])
        elif nowtime.hour % 6 == 0:
            self.multiServerCmd(NICK, NICK, ["cstats"])
        else:
            self.multiServerCmd(NICK, NICK, ["hstats"])

    def startHourly(self):
        # this is scheduled to run at the first :00 after the bot starts
        # makes a looping_call to run every hour from here on.
        self.looping_calls["stats"] = task.LoopingCall(self.hourlyStats)
        self.looping_calls["stats"].start(3600)

    # Countdown timer
    def countDown(self):
        cd = {}
        for event in ("start", "end"):
            cd["event"] = event
            # add half a second for rounding (we truncate at the decimal later)
            td = (self.ttime[event] - datetime.now()) + timedelta(seconds=0.5)
            sec = int(td.seconds)
            cd["seconds"] = int(sec % 60)
            cd["minutes"] = int((sec / 60) % 60)
            cd["hours"] = int(sec / 3600)
            cd["days"] = td.days
            cd["countdown"] = td
            if td > timedelta(0):
                return cd
        return cd

    # Trophy/achievement/scoreboard methods removed - JSON files deprecated


    # implement commands here
    def doPing(self, sender, replyto, msgwords):
        self.respond(replyto, sender, "Pong! " + " ".join(msgwords[1:]))

    def doTime(self, sender, replyto, msgwords):
        timeMsg = time.strftime("%F %H:%M:%S %Z. ")
        timeLeft = self.countDown()
        if timeLeft["countdown"] <= timedelta(0):
            timeMsg += "The " + YEAR + " tournament is OVER!"
            self.respond(replyto, sender, timeMsg)
            return
        verbs = { "start" : "begins",
                  "end" : "closes"
                }
        timeMsg += YEAR + " Tournament " + verbs[timeLeft["event"]] + " in {days}d {hours:0>2}:{minutes:0>2}:{seconds:0>2}".format(**timeLeft)
        self.respond(replyto, sender, timeMsg)

    def doSource(self, sender, replyto, msgwords):
        self.respond(replyto, sender, self.sourceURL )

    def doScoreboard(self, sender, replyto, msgwords):
        self.respond(replyto, sender, self.scoresURL )

    def doTtyrec(self, sender, replyto, msgwords):
        self.respond(replyto, sender, self.ttyrecURL )

    def doRCedit(self, sender, replyto, msgwords):
        self.respond(replyto, sender, self.rceditURL )

    def doHelp(self, sender, replyto, msgwords):
        self.respond(replyto, sender, self.helpURL )

    def doScore(self, sender, replyto, msgwords):
        # Simplified - just return URL since JSON scoreboard is deprecated
        self.respond(replyto, sender, "Check the tournament scoreboard at: " + self.scoresURL)

    def doClanTag(self, sender, replyto, msgwords):
        # ClanTag functionality removed - JSON scoreboard is deprecated
        self.respond(replyto, sender, "Clan tags are no longer supported. Check the tournament scoreboard at: " + self.scoresURL)

    def doClanScore(self, sender, replyto, msgwords):
        # Simplified - just return URL since JSON scoreboard is deprecated
        self.respond(replyto, sender, "Check the tournament clan rankings at: https://tnnt.org/clans")

    def doCommands(self, sender, replyto, msgwords):
        self.respond(replyto, sender, "available commands are: help ping time tell source lastgame lastasc asc streak rcedit scores sb score ttyrec clanscore clantag whereis players who commands status" )

    def doStatus(self, sender, replyto, msgwords):
        if sender not in self.admin:
            self.respond(replyto, sender, "Admin access required.")
            return

        # Get memory usage of current process
        try:
            import resource
            mem_usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            # On Linux, ru_maxrss is in KB; on macOS it's in bytes
            if mem_usage > 1048576:  # Likely macOS (bytes)
                mem_mb = mem_usage / 1048576
            else:  # Likely Linux (KB)
                mem_mb = mem_usage / 1024
        except ImportError:
            mem_mb = "N/A"

        # Calculate uptime
        uptime_seconds = int(time.time() - self.starttime)
        uptime_days = uptime_seconds // 86400
        uptime_hours = (uptime_seconds % 86400) // 3600
        uptime_mins = (uptime_seconds % 3600) // 60

        # Count active file monitors (match beholder's specific counting)
        monitor_count = 0
        for v in self.xlogfiles:
            monitor_count += 1  # Each xlogfile is one monitor
        for v in self.livelogs:
            monitor_count += 1  # Each livelog is one monitor

        # Count queries in queue
        query_count = len(self.queries) if hasattr(self, 'queries') else 0

        # Count cached messages
        msg_count = len(self.tellbuf) if hasattr(self, 'tellbuf') else 0

        # Count rate limited users
        rate_limit_count = len(self.rate_limits) if hasattr(self, 'rate_limits') else 0

        # Count users under abuse penalty
        abuse_penalty_count = len(self.abuse_penalties) if hasattr(self, 'abuse_penalties') else 0

        # Build status message
        status_parts = []
        status_parts.append("Status: {} on {}".format(NICK, SERVERTAG))
        status_parts.append("Uptime: {}d {}h {}m".format(uptime_days, uptime_hours, uptime_mins))
        if mem_mb != "N/A":
            status_parts.append("Memory: {:.1f}MB".format(mem_mb))
        status_parts.append("Monitors: {}".format(monitor_count))
        status_parts.append("Queries: {}".format(query_count))
        status_parts.append("Messages: {}".format(msg_count))
        status_parts.append("RateLimit: {}".format(rate_limit_count))
        if abuse_penalty_count > 0:
            status_parts.append("AbusePenalty: {}".format(abuse_penalty_count))

        self.respond(replyto, sender, " | ".join(status_parts))

    def takeMessage(self, sender, replyto, msgwords):
        if len(msgwords) < 3:
            self.respond(replyto, sender, TRIGGER + "tell <recipient> <message> (leave a message for someone)")
            return
        willDo = [ "Will do, {0}!",
                   "I'm on it, {0}.",
                   "No worries, {0}, I've got this!",
                   "{1} shall be duly informed at the first opportunity, {0}." ]

        rcpt = msgwords[1].split(":")[0] # remove any trailing colon - could check for other things here.
        message = " ".join(msgwords[2:])
        if (replyto == sender): #this was a privmsg
            forwardto = rcpt # so we pass a privmsg
            # and mark it so rcpt knows it was sent privately
            message = "[private] " + message
        else: # !tell on channel
            forwardto = replyto # so pass to channel
        if not self.tellbuf.get(rcpt.lower(),False):
            self.tellbuf[rcpt.lower()] = []
        self.tellbuf[rcpt.lower()].append((forwardto,sender,time.time(),message))
        self.tellbuf.sync()
        # Sanitize sender and recipient names to prevent format string injection
        safe_sender = sanitize_format_string(sender)
        safe_rcpt = sanitize_format_string(rcpt)
        self.msgLog(replyto,random.choice(willDo).format(safe_sender,safe_rcpt))

    def msgTime(self, stamp):
        # Timezone handling is not great, but the following seems to work.
        # assuming TZ has not changed between leaving & taking the message.
        return datetime.fromtimestamp(stamp).strftime("%Y-%m-%d %H:%M") + time.strftime(" %Z")

    def checkMessages(self, user, CHANNEL):
        # this runs every time someone speaks on the channel,
        # so return quickly if there's nothing to do
        # but first... deal with the "bonus" colours and leading @ symbols of discord users
        if user[0] == '@':
            plainuser = self.stripText(user).lower()
            if not self.tellbuf.get(plainuser,None):
                plainuser = plainuser[1:] # strip the leading @ and try again (below)
        else:
            plainuser = user.lower()
        if not self.tellbuf.get(plainuser,None): return
        nicksfrom = []
        if len(self.tellbuf[plainuser]) > 2 and user[0] != '@':
            for (forwardto,sender,ts,message) in self.tellbuf[plainuser]:
                if forwardto.lower() != user.lower(): # don't add sender to list if message was private
                    if sender not in nicksfrom: nicksfrom += [sender]
                self.respond(user,user, "Message from " + sender + " at " + self.msgTime(ts) + ": " + message)
            # "tom" "tom and dick" "tom, dick, and harry"
            # Sanitize all nicknames to prevent format string injection
            safe_nicks = [sanitize_format_string(nick) for nick in nicksfrom]
            fromstr = ""
            for (i,n) in enumerate(safe_nicks):
                # first item
                if (i == 0):
                    fromstr = n
                # last item
                elif (i == len(safe_nicks)-1):
                    if (i > 1): fromstr += "," # oxford comma :P
                    fromstr += " and " + n
                # middle items
                else:
                   fromstr += ", " + n

            if fromstr: # don't say anything if all messages were private
                self.respond(CHANNEL, user, "Messages from " + fromstr + " have been forwarded to you privately.");

        else:
            for (forwardto,sender,ts,message) in self.tellbuf[plainuser]:
                self.respond(forwardto, user, "Message from " + sender + " at " + self.msgTime(ts) + ": " + message)
        del self.tellbuf[plainuser]
        self.tellbuf.sync()

    QUERY_ID = 0 # just use a sequence number for now
    def newQueryId(self):
        self.QUERY_ID += 1
        return str(self.QUERY_ID)

    queries = {}

    def forwardQuery(self,sender,replyto,msgwords,callback):
        # [Here]
        # Store a query reference locally, indexed by a unique identifier
        # Store a callback function for when everyone responds to the query.
        # forward the query tagged with the ID to the slaves.
        # Queue up a timeout callback to handle slave(s) not responding
        # [elsewhere]
        # record query responses, and call callback when all received (or timeout)
        # This all becomes easier if we just treat ourself (master) as one of the slaves
        TIMEOUT = 5 # move this to config later
        q = self.newQueryId()
        self.queries[q] = {}
        self.queries[q]["callback"] = callback
        self.queries[q]["replyto"] = replyto
        self.queries[q]["sender"] = sender
        self.queries[q]["resp"] = {}
        self.queries[q]["finished"] = {}
        message = "#Q# " + " ".join([q,sender] + msgwords)

        for sl in list(self.slaves.keys()):
            if TEST: print("forwardQuery: " + sl + " " + message)
            self.msg(sl,message)
        reactor.callLater(TIMEOUT, self.doQueryTimeout, q)

    # Multi-server command entry point (forwards query to slaves)
    def multiServerCmd(self, sender, replyto, msgwords):
        if msgwords[0] in self.checkUsage:
            if not self.checkUsage[msgwords[0]](sender, replyto, msgwords):
                return
        if self.slaves:
            self.forwardQuery(sender, replyto, msgwords, self.callBacks.get(msgwords[0],None))

    def blowChunks(self, line, n):
        # split line into a list of chunks of max n chars
        # https://stackoverflow.com/questions/9475241/split-string-every-nth-character
        return [line[i:i+n] for i in range(0, len(line), n)]

    # !stats (or server generated hstats, etc)
    def getStats(self, master, sender, query, msgwords):
        statPeriod = { "stats" : "day", "cstats" : "day", "dstats": "day", "hstats": "hour", "fstats": "full" }
        statType = { "stats" : "news", "cstats" : "news" } # so far today...
        period = statPeriod[msgwords[0]]
        p = statType.get(msgwords[0],period)
        response = p + " " + json.dumps(self.stats[period])
        respChunks = self.blowChunks(response, 200)
        lastChunk = respChunks.pop()
        while respChunks:
            self.msg(master, "#P# " + query + " " + respChunks.pop(0))
        self.msg(master, "#R# " + query + " " + lastChunk)
        if msgwords[0] == "stats": return # don't init any stats
        self.initStats("hour")
        if msgwords[0] == "hstats": return # don't init day/full stats
        if msgwords[0] == "cstats": return # don't init day/full stats
        self.initStats("day")

    def outStats(self, q):
        aggStats = {}
        for r in q["resp"]:
            statType, statJson = q["resp"][r].split(' ', 1)
            stat = json.loads(statJson)
            for item in ["games", "scum", "turns", "points", "realtime", "ascend"]:
                aggStats[item] = aggStats.get(item,0) + stat.get(item,0)
            for rrga in ["role", "race", "gender", "align"]:
                if rrga not in aggStats: aggStats[rrga] = {}
                for rrga_item in stat[rrga]:
                    aggStats[rrga][rrga_item] = aggStats[rrga].get(rrga_item,0) + stat[rrga][rrga_item]
        replyto = None
        if statType == "news": replyto = q["replyto"]
        self.spamStats(statType, aggStats, replyto)

    # !players - respond to forwarded query and actually pull the info
    def getPlayers(self, master, sender, query, msgwords):
        players = []
        for var in list(self.inprog.keys()):
            for inpdir in self.inprog[var]:
                for inpfile in glob.iglob(inpdir + "*.ttyrec"):
                    # /stuff/crap/PLAYER:shit:garbage.ttyrec
                    # we want AFTER last '/', BEFORE 1st ':'
                    players.append(inpfile.split("/")[-1].split(":")[0])
        if players:
            plrvar = " ".join(players) + " "
        else:
            plrvar = "No current players"
        response = "#R# " + query + " " + self.displaytag(SERVERTAG) + " " + plrvar
        self.msg(master, response)

    # !players callback. Actually print the output.
    def outPlayers(self,q):
        outmsg = " | ".join(list(q["resp"].values()))
        self.respond(q["replyto"],q["sender"],outmsg)

    def usageWhereIs(self, sender, replyto, msgwords):
        if (len(msgwords) != 2):
            self.respond(replyto, sender, TRIGGER + msgwords[0] + " <player> - finds a player in the dungeon.")
            return False
        return True

    def getWhereIs(self, master, sender, query, msgwords):
        ammy = ["", " (with Amulet)"]

        # Validate player name to prevent path traversal
        player_name = msgwords[1]
        if "/" in player_name or ".." in player_name or "\\" in player_name:
            self.msg(master, "#R# " + query + " " + self.displaytag(SERVERTAG)
                     + " Invalid player name.")
            return

        # look for inrpogress file first, only report active games
        for var in list(self.inprog.keys()):
            for inpdir in self.inprog[var]:
                for inpfile in glob.iglob(inpdir + "*.ttyrec"):
                    plr = inpfile.split("/")[-1].split(":")[0]
                    if plr.lower() == msgwords[1].lower():
                        for widir in self.whereis[var]:
                            for wipath in glob.iglob(widir + "*.whereis"):
                                if wipath.split("/")[-1].lower() == (msgwords[1] + ".whereis").lower():
                                    plr = wipath.split("/")[-1].split(".")[0] # Correct case
                                    with open(wipath, "rb") as f:
                                        wirec = parse_xlogfile_line(f.read(),":")

                                    self.msg(master, "#R# " + query
                                             + " " + self.displaytag(SERVERTAG) + " " + plr
                                             + " : ({role} {race} {gender} {align}) T:{turns} ".format(**wirec)
                                             + self.dungeons[wirec["dnum"]]
                                             + " level: " + str(wirec["depth"])
                                             + ammy[wirec["amulet"]])
                                    return

                        self.msg(master, "#R# " + query + " "
                                                + self.displaytag(SERVERTAG)
                                                + " " + plr + " "
                                                + ": No details available")
                        return
        self.msg(master, "#R# " + query + " " + self.displaytag(SERVERTAG)
                                        + " " + msgwords[1]
                                        + " is not currently playing on this server.")

    def outWhereIs(self,q):
        player = ''
        msgs = []
        for server in q["resp"]:
            if " is not currently playing" in q["resp"][server]:
                player = q["resp"][server].split(" ")[1]
            else:
                msgs += [q["resp"][server]]
        outmsg = " | ".join(msgs)
        if not outmsg: outmsg = player + " is not playing."
        self.respond(q["replyto"],q["sender"],outmsg)

    def usageAsc(self, sender, replyto, msgwords):
        if len(msgwords) < 3:
            return True
        return False

    def getAsc(self, master, sender, query, msgwords):
        if len(msgwords) == 2:
            PLR = msgwords[1]
        else:
            PLR = sender
        if not PLR: return # bogus input, should have been handled in usage check above
        plr = PLR.lower()
        stats = ""
        totasc = 0
        if not plr in self.asc:
            repl = self.displaytag(SERVERTAG) + " No ascensions for " + PLR
            if plr in self.allgames:
                repl += " in " + str(self.allgames[plr]) + " games"
            repl += "."
            self.msg(master,"#R# " + query + " " + repl)
            return
        role_stats = []
        race_stats = []
        align_stats = []
        gender_stats = []

        for role in NETHACK_ROLES:
             if role in self.asc[plr]:
                totasc += self.asc[plr][role]
                role_stats.append(str(self.asc[plr][role]) + "x" + role)

        for race in NETHACK_RACES:
            if race in self.asc[plr]:
                race_stats.append(str(self.asc[plr][race]) + "x" + race)

        for alig in NETHACK_ALIGNS:
            if alig in self.asc[plr]:
                align_stats.append(str(self.asc[plr][alig]) + "x" + alig)

        for gend in NETHACK_GENDERS:
            if gend in self.asc[plr]:
                gender_stats.append(str(self.asc[plr][gend]) + "x" + gend)

        stats = " ".join(role_stats) + ", " + " ".join(race_stats) + ", " + " ".join(align_stats) + ", " + " ".join(gender_stats) + "."
        self.msg(master, "#R# " + query + " " + self.displaytag(SERVERTAG)
                         + " " + PLR
                         + " has ascended "
                         + str(totasc) + " times in "
                         + str(self.allgames[plr])
                         + " games ({:0.2f}%):".format((100.0 * totasc)
                                               / self.allgames[plr])
                         + stats)
        return

    def outAscStreak(self,q):
        msgs = []
        for server in q["resp"]:
            if q["resp"][server].split(' ')[0] == 'No':
                # If they all say "No streaks for bob", that becomes the eventual output
                fallback_msg = q["resp"][server]
            else:
               msgs += [q["resp"][server]]
        outmsg = " | ".join(msgs)
        if not outmsg: outmsg = fallback_msg
        self.respond(q["replyto"],q["sender"],outmsg)

    def usageStreak(self, sender, replyto, msgwords):
        if len(msgwords) > 2: return False
        return True

    def streakDate(self,stamp):
        return datetime.fromtimestamp(float(stamp)).strftime("%Y-%m-%d")

    def getStreak(self, master, sender, query, msgwords):
        if len(msgwords) == 2:
            PLR = msgwords[1]
        else:
            PLR = sender
        if not PLR: return # bogus input, handled by usage check.
        plr = PLR.lower()
        (lstart,lend,llength) = self.longstreak.get(plr,(0,0,0))
        (cstart,cend,clength) = self.curstreak.get(plr,(0,0,0))

        reply_parts = ["#R#", query]

        if llength == 0:
            reply = " ".join(reply_parts) + " No streaks for " + PLR + "."
            self.msg(master,reply)
            return

        reply_parts.extend([self.displaytag(SERVERTAG), PLR])
        reply_parts.append("Max: {} ({} - {})".format(
            llength, self.streakDate(lstart), self.streakDate(lend)))

        if clength > 0:
            if cstart == lstart:
                reply_parts.append("(current)")
            else:
                reply_parts.append(". Current: {} (since {})".format(
                    clength, self.streakDate(cstart)))

        reply = " ".join(reply_parts) + "."
        self.msg(master,reply)
        return

    def getLastGame(self, master, sender, query, msgwords):
        if (len(msgwords) >= 2): #player specified
            plr = msgwords[1].lower()
            dl = self.lg.get(plr,False)
            if not dl:
                self.msg(master, "#R# " + query +
                                 " No last game for " + msgwords[1] + ".")
                return
            self.msg(master, "#R# " + query + " " + self.displaytag(SERVERTAG) + " " + dl)
            return
        # no player
        self.msg(master, "#R# " + query + " " + self.displaytag(SERVERTAG) + " " + self.lastgame)

    def getLastAsc(self, master, sender, query, msgwords):
        if (len(msgwords) >= 2):  #player specified
            plr = msgwords[1].lower()
            dl = self.la.get(plr,False)
            if not dl:
                self.msg(master, "#R# " + query +
                                 " No last ascension for " + msgwords[1] + ".")
                return
            self.msg(master, "#R# " + query + " " + self.displaytag(SERVERTAG) + " " + dl)
            return
        self.msg(master, "#R# " + query + " " + self.displaytag(SERVERTAG) + " " + self.lastasc)

    # Listen to the chatter
    def privmsg(self, sender, dest, message):
        # Extract both nick and hostmask for rate limiting
        sender_full = sender
        sender = sender.partition("!")[0]
        sender_host = sender_full.partition("!")[2]  # user@host part for rate limiting
        if SLAVE and sender not in MASTERS: return
        if (dest in CHANNELS): #public message
            self.log(dest, "<"+sender+"> " + message)
            replyto = dest
            if (sender == DCBRIDGE):
                message = message.partition("<")[2] #everything after the first <
                sender,x,message = message.partition(">") #everything remaining before/after the first >
                message = RE_SPACE_COLOR.sub('', message) # everything after the first space and any colour codes
                if len(sender) == 0: return
        else: #private msg
            replyto = sender
        # Message checks next.
        self.checkMessages(sender, dest)
        # ignore other channel noise unless !command
        if (message[0] != TRIGGER):
            if (dest in CHANNELS): return
        else: # pop the '!'
            message = message[1:]
        msgwords = message.strip().split(" ")
        if RE_DICE_CMD.match(msgwords[0]):
            self.rollDice(sender, replyto, msgwords)
            return
        if self.commands.get(msgwords[0].lower(), False):
            command = msgwords[0].lower()

            # Skip rate limiting for internal commands (#q#, #r#, #p#)
            if not command.startswith('#'):
                # Apply burst protection (use host for rate limiting)
                if not self._checkBurstProtection(sender_host, command):
                    return  # Silently ignore burst commands

                # Apply rate limiting (use host for rate limiting)
                if not self._checkRateLimit(sender_host, command):
                    # Check if we should send a penalty message
                    if not self._shouldSendPenaltyMessage(sender_host):
                        return  # Silently ignore to prevent penalty message spam

                    # Provide specific error message based on penalty type
                    if hasattr(self, 'abuse_penalties') and sender_host in self.abuse_penalties:
                        remaining = int(self.abuse_penalties[sender_host] - time.time())
                        self.respond(replyto, sender, "Abuse penalty active: {}m {}s remaining. (Triggered by spamming consecutive commands)".format(remaining//60, remaining%60))
                    else:
                        self.respond(replyto, sender, "Rate limit exceeded. Please wait before using {} again.".format(TRIGGER + command))
                    return

            self.commands[command](sender, replyto, msgwords)
            return
        if dest not in CHANNELS and sender in self.slaves: # game announcement from slave
            spam = False
            if msgwords[0] == "SPAM:":
                msgwords = msgwords[1:]
                spam = True
            self.announce(" ".join(msgwords), spam)

    #other events for logging
    def action(self, doer, dest, message):
        if (dest in CHANNELS):
            doer = doer.split('!', 1)[0]
            self.log(dest, "* " + doer + " " + message)

    def userRenamed(self, oldName, newName):
        self.log(CHANNELS[0], "-!- " + oldName + " is now known as " + newName) # fix channel

    def noticed(self, user, channel, message):
        if (channel in CHANNELS):
            user = user.split('!')[0]
            self.log(channel, "-" + user + ":" + channel + "- " + message)

    def modeChanged(self, user, channel, set, modes, args):
        if (set): s = "+"
        else: s = "-"
        user = user.split('!')[0]
        if args[0]:
            self.log(channel, "-!- mode/" + channel + " [" + s + modes + " " + " ".join(list(args)) + "] by " + user)
        else:
            self.log(channel, "-!- mode/" + channel + " [" + s + modes + "] by " + user)

    def userJoined(self, user, channel):
        #(user,details) = user.split('!')
        #self.log("-!- " + user + " [" + details + "] has joined " + channel)
        self.log( channel, "-!- " + user + " has joined " + channel)

    def userLeft(self, user, channel):
        #(user,details) = user.split('!')
        #self.log("-!- " + user + " [" + details + "] has left " + channel)
        self.log(channel, "-!- " + user + " has left " + channel)

    def userQuit(self, user, quitMsg):
        #(user,details) = user.split('!')
        #self.log("-!- " + user + " [" + details + "] has quit [" + quitMsg + "]")
        self.log(CHANNELS[0], "-!- " + user + " has quit [" + quitMsg + "]")

    def userKicked(self, kickee, channel, kicker, message):
        kicker = kicker.split('!')[0]
        kickee = kickee.split('!')[0]
        self.log(channel, "-!- " + kickee + " was kicked from " + channel + " by " + kicker + " [" + message + "]")

    def topicUpdated(self, user, channel, newTopic):
        user = user.split('!')[0]
        self.log(channel, "-!- " + user + " changed the topic on " + channel + " to: " + newTopic)


    ### Xlog/livelog event processing
    def startscummed(self, game):
        return game["death"].lower() in ["quit", "escaped"] and game["points"] < 1000

    # shortgame tracks consecutive games < 100 turns
    # we report a summary of these rather than individually
    shortgame = {}

    def xlogfileReport(self, game, report = True):
        # lowercased name is used for lookups
        lname = game["name"].lower()
        # "allgames" for a player even counts scummed games
        if not lname in self.allgames:
            self.allgames[lname] = 0
        self.allgames[lname] += 1
        scumbag = self.startscummed(game)

        # collect hourly/daily stats for games that actually ended within the period
        etime = fromtimestamp_int(game["endtime"])
        ntime = datetime.now()
        et = {}
        nt = {}
        et["hour"] = datetime(etime.year,etime.month,etime.day,etime.hour)
        et["day"] = datetime(etime.year,etime.month,etime.day)
        nt["hour"] = datetime(ntime.year,ntime.month,ntime.day,ntime.hour)
        nt["day"] = datetime(ntime.year,ntime.month,ntime.day)
        for period in ["hour","day","full"]:
            if period == "full" or et[period] == nt[period]:
                self.stats[period]["games"] += 1
                if scumbag:
                    self.stats[period]["scum"] += 1
                else: # only count non-scums in rrga stats
                    for rrga in ["role","race","gender","align"]:
                        self.stats[period][rrga][game[rrga]] = self.stats[period][rrga].get(game[rrga],0) + 1
                for tp in ["turns","points","realtime"]:
                    self.stats[period][tp] += int(game[tp])
                if game["death"] == "ascended":
                    self.stats[period]["ascend"] += 1

        dumplog = game.get("dumplog",False)
        # Need to figure out the dump path before messing with the name below
        dumpfile = (self.dump_file_prefix + game["dumpfmt"]).format(**game)

        # Generate dumplog URL using new method that checks both local and S3
        if TEST:
            # In test mode, always generate a URL
            dumpurl = urllib.parse.quote(game["dumpfmt"].format(**game))
            dumpurl = self.dump_url_prefix.format(**game) + dumpurl
        else:
            # In production, use the new method that checks both local and S3
            dumpurl = self.generate_dumplog_url(game, dumpfile)
        self.lg[lname] = dumpurl
        self.lastgame = dumpurl

        if game["death"][0:8] in ("ascended"):
            # append dump url to report for ascensions
            game["ascsuff"] = "\n" + dumpurl
            # !lastasc stats.
            self.la[lname] = dumpurl
            self.lastasc = dumpurl

            # !asc stats
            if not lname in self.asc: self.asc[lname] = {}
            if not game["role"]   in self.asc[lname]: self.asc[lname][game["role"]]   = 0
            if not game["race"]   in self.asc[lname]: self.asc[lname][game["race"]]   = 0
            if not game["gender"] in self.asc[lname]: self.asc[lname][game["gender"]] = 0
            if not game["align"]  in self.asc[lname]: self.asc[lname][game["align"]]  = 0
            self.asc[lname][game["role"]]   += 1
            self.asc[lname][game["race"]]   += 1
            self.asc[lname][game["gender"]] += 1
            self.asc[lname][game["align"]]  += 1

            # streaks
            (cs_start, cs_end, cs_length) = self.curstreak.get(lname,
                                                      (game["starttime"],0,0))
            cs_end = game["endtime"]
            cs_length += 1
            self.curstreak[lname] = (cs_start, cs_end, cs_length)
            (ls_start, ls_end, ls_length) = self.longstreak.get(lname, (0,0,0))
            if cs_length > ls_length:
                self.longstreak[lname] = self.curstreak[lname]

        else:   # not ascended - kill off any streak
            game["ascsuff"] = ""
            if lname in self.curstreak:
                del self.curstreak[lname]
        # end of statistics gathering

        game["shortsuff"] = ""
        if game["turns"] < 100:
            self.shortgame[game["name"]] = self.shortgame.get(game["name"],0) + 1
            if report and self.shortgame[game["name"]] % 100 == 0:
                yield("{0} has {1} consecutive games less than 100 turns.".format(game["name"], self.shortgame[game["name"]]))
            return
        elif game["name"] in self.shortgame:
            if self.shortgame[game["name"]] == 1:
                # extremely verbose wording is reqiured here because otherwise people somehow think that the
                # bot knows how many other factors contributed to the player's death. That would actually be
                # pretty cool, but that info is not in the xlogfile.
                game["shortsuff"] = " (and one other game not reported)"
            else:
                game["shortsuff"] = " (and {0} other games not reported)".format(self.shortgame[game["name"]])
            del self.shortgame[game["name"]]

        if (not report): return # we're just reading through old entries at startup
        if scumbag: return # must break streak even on scum games

        # start of actual reporting
        if "while" in game and game["while"] != "":
            game["death"] += (", while " + game["while"])

        if game["death"] in ("quit", "escaped", "ascended"):
            END = self.displaytag(game["death"])
        else: END = self.displaytag("died")

        yield (END + ": {name} ({role} {race} {gender} {align}), "
                   "{points} points, {turns} turns, {death}{shortsuff}{ascsuff}").format(**game)

    def livelogReport(self, event):
        if event.get("charname", False):
            if event.get("player", False):
                if event["player"] != event["charname"]:
                    event["player"] = "{charname} ({player})".format(**event)
            else:
                event["player"] = event["charname"]

        if "historic_event" in event and "message" not in event:
            if event["historic_event"].endswith("."):
                event["historic_event"] = event["historic_event"][:-1]
            event["message"] = event["historic_event"]

        if "message" in event:
            yield ("{player} ({role} {race} {gender} {align}) "
                   "{message}, on T:{turns}").format(**event)
        elif "wish" in event:
            yield ("{player} ({role} {race} {gender} {align}) "
                   'wished for "{wish}", on T:{turns}').format(**event)
        elif "shout" in event:
            yield ("{player} ({role} {race} {gender} {align}) "
                   'shouted "{shout}", on T:{turns}').format(**event)
        elif "bones_killed" in event:
            if not event.get("bones_rank",False): # fourk does not have bones rank so use role instead
                event["bones_rank"] = event["bones_role"]
            yield ("{player} ({role} {race} {gender} {align}) "
                   "killed the {bones_monst} of {bones_killed}, "
                   "the former {bones_rank}, on T:{turns}").format(**event)
        elif "killed_uniq" in event:
            yield ("{player} ({role} {race} {gender} {align}) "
                   "killed {killed_uniq}, on T:{turns}").format(**event)
        elif "defeated" in event: # fourk uses this instead of killed_uniq.
            yield ("{player} ({role} {race} {gender} {align}) "
                   "defeated {defeated}, on T:{turns}").format(**event)
        # more 1.3d shite
        elif "genocided_monster" in event:
            if event.get("dungeon_wide","yes") == "yes":
                event["genoscope"] = "dungeon wide";
            else:
                event["genoscope"] = "locally";
            yield ("{player} ({role} {race} {gender} {align}) "
                   "genocided {genocided_monster} {genoscope} on T:{turns}").format(**event)
        elif "shoplifted" in event:
            yield ("{player} ({role} {race} {gender} {align}) "
                   "stole {shoplifted} zorkmids of merchandise from the {shop} of"
                   " {shopkeeper} on T:{turns}").format(**event)
        elif "killed_shopkeeper" in event:
            yield ("{player} ({role} {race} {gender} {align}) "
                   "killed {killed_shopkeeper} on T:{turns}").format(**event)

    def connectionLost(self, reason=None):
        if self.looping_calls is None: return
        for call in self.looping_calls.values():
            call.stop()

    def updateSummary(self):
        # send most up-to-date full stats to master for milestone tracking
        # called every time a game ends, and on a timer in case master restarted.
        for master in MASTERS:
            self.msg(master, "#S# " + json.dumps({k: self.stats["full"][k] for k in ('games', 'ascend', 'points', 'turns', 'realtime')}))

    def logReport(self, filepath):
        with filepath.open("r") as handle:
            handle.seek(self.logs_seek[filepath])

            for line in handle:
                delim = self.logs[filepath][2]
                game = parse_xlogfile_line(line, delim)
                game["dumpfmt"] = self.logs[filepath][3]
                spam = self.logs[filepath][4]
                for line in self.logs[filepath][0](game):
                    line = self.displaytag(SERVERTAG) + " " + line
                    if SLAVE:
                        if spam:
                            line = "SPAM: " + line
                        for master in MASTERS:
                            self.msg(master, line)
                    else:
                        self.announce(line,spam)
                self.updateSummary()

            self.logs_seek[filepath] = handle.tell()

class DeathBotFactory(ReconnectingClientFactory):
    def startedConnecting(self, connector):
        print('Started to connect.')

    def buildProtocol(self, addr):
        print('Connected.')
        print('Resetting reconnection delay')
        self.resetDelay()
        p = DeathBotProtocol()
        p.factory = self
        return p

    def clientConnectionLost(self, connector, reason):
        print('Lost connection.  Reason:', reason)
        ReconnectingClientFactory.clientConnectionLost(self, connector, reason)

    def clientConnectionFailed(self, connector, reason):
        print('Connection failed. Reason:', reason)
        ReconnectingClientFactory.clientConnectionFailed(self, connector,
                                                         reason)

if __name__ == '__main__':
    # initialize logging
    #log.startLogging(DailyLogFile.fromFullPath(LOGBASE))

    # create factory protocol and application
    f = DeathBotFactory()

    # connect factory to this host and port
    reactor.connectSSL(HOST, PORT, f, ssl.ClientContextFactory())

    # run bot
    reactor.run()
