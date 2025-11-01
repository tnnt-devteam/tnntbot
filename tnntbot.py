#!/usr/bin/env python3
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
import requests  # for GitHub API
import xml.etree.ElementTree as ET  # for parsing GitHub Atom feeds

# command trigger - this should be in tnntbotconf - next time.
TRIGGER = '$'
site.addsitedir('.')
from tnntbotconf import HOST, PORT, CHANNELS, NICK, USERNAME, REALNAME, BOTDIR
from tnntbotconf import PWFILE, FILEROOT, WEBROOT, LOGROOT, ADMIN, YEAR
from tnntbotconf import SERVERTAG

# GitHub configuration (optional)
try:
    from tnntbotconf import ENABLE_GITHUB, GITHUB_REPOS
except ImportError:
    ENABLE_GITHUB = False
    GITHUB_REPOS = []

# TNNT API configuration
TNNT_API_BASE = "http://127.0.0.1:8000/api"  # Use localhost for same server
TNNT_API_HEADERS = {"Host": "tnnt.org"}  # Required for Django ALLOWED_HOSTS
try:
    from tnntbotconf import SPAMCHANNELS
except ImportError:
    SPAMCHANNELS = CHANNELS
try:
    from tnntbotconf import DCBRIDGE
except ImportError:
    DCBRIDGE = None
try:
    from tnntbotconf import TEST
except ImportError:
    TEST = False
try:
    from tnntbotconf import GRACEDAYS
except ImportError:
    GRACEDAYS = 5

try:
    from tnntbotconf import ANNOUNCE_AFTER_DB_REBUILD
except ImportError:
    ANNOUNCE_AFTER_DB_REBUILD = True  # Default to announcing for backwards compatibility
try:
    from tnntbotconf import REMOTES
except ImportError:
    SLAVE = True
    REMOTES = {}
try:
    from tnntbotconf import MASTERS
except ImportError:
    SLAVE = False
    MASTERS = []
try:
    #from tnntbotconf import LOGBASE, IRCLOGS
    from tnntbotconf import IRCLOGS
except ImportError:
    #LOGBASE = BOTDIR + "/tnntbot.log"
    IRCLOGS = LOGROOT

# JSON configuration files are deprecated - trophy/achievement tracking removed
if not SLAVE:
    # Hardcoded game data for NetHack roles, races, aligns, genders
    NETHACK_ROLES = ["Arc", "Bar", "Cav", "Hea", "Kni", "Mon", "Pri", "Ran", "Rog", "Sam", "Tou", "Val", "Wiz"]
    NETHACK_RACES = ["Dwa", "Elf", "Gno", "Hum", "Orc"]
    NETHACK_ALIGNS = ["Cha", "Law", "Neu"]
    NETHACK_GENDERS = ["Mal", "Fem"]

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

# Time constants
SECONDS_PER_MINUTE = 60
SECONDS_PER_HOUR = 3600
SECONDS_PER_DAY = 86400
LOG_POLL_INTERVAL = 3  # seconds between log file checks
NICK_CHECK_INTERVAL = 30  # seconds between nick checks
SUMMARY_UPDATE_INTERVAL = 300  # seconds between summary updates (5 minutes)
STALE_BURST_TIMEOUT = 3600  # 1 hour before removing burst protection data

# Game thresholds
SCUM_THRESHOLD = 1000  # points below which quit/escape is considered scum
SHORT_GAME_TURNS = 100  # turns below which games are batched
SHORT_GAME_BATCH_SIZE = 100  # report every N short games

# Pre-compiled regex patterns for better performance
RE_COLOR_FG_BG = re.compile(r'\x03\d\d,\d\d')  # fg,bg pair
RE_COLOR_FG = re.compile(r'\x03\d\d')  # fg only
RE_COLOR_END = re.compile(r'[\x1D\x03\x0f]')  # end of colour and italics
RE_DICE_CMD = re.compile(r'^\d*d\d*$')  # dice command pattern
RE_SPACE_COLOR = re.compile(r'^ [\x1D\x03\x0f]*')  # space and color codes

# Custom dict class for shelve fallback
class DictWithSync(dict):
    """Dict subclass that supports sync() method for shelve compatibility.

    When the shelve database fails to open, we fall back to an in-memory
    dict. This class provides a no-op sync() method so the same code can
    work with both shelve objects and the in-memory fallback.
    """
    def sync(self):
        """No-op sync method for in-memory dict fallback."""
        pass

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
    except (IOError, OSError) as e:
        print(f"Warning: Could not read password file {PWFILE}: {e}")
        password = "NotTHEPassword"

    sourceURL = "https://github.com/tnnt-devteam/tnntbot"
    versionName = "tnntbot.py"
    versionNum = "0.1"
    # bot_start_time will be set in signedOn() for accurate uptime tracking

    # Croesus reaction messages (bot speaks AS Croesus)
    croesus_player_wins = [
        "{player} has defeated me! How dare you!",
        "{player} strikes me down. I'll remember this...",
        "Well done, {player}. You've bested me!",
        "{player} has proven stronger than me!",
        "I fall before {player}!",
        "Impressive, {player}. I didn't stand a chance.",
        "{player} loots my vault and walks away victorious!",
        "I have been slain by {player}. Brutal!",
    ]

    croesus_croesus_wins = [
        "I claim {player}! Muahahaha!",
        "I have been avenged! RIP {player}.",
        "{player} learned not to mess with me the hard way.",
        "{player} underestimated me. Fatal mistake.",
        "I defend my vault from {player}!",
        "{player} won't be stealing from me today... or ever.",
        "Another greedy adventurer falls to me. RIP {player}.",
        "I add {player} to my collection of failed thieves.",
    ]

    dump_url_prefix = f"{WEBROOT}userdata/{{name[0]}}/{{name}}/"
    dump_file_prefix = f"{FILEROOT}dgldir/userdata/{{name[0]}}/{{name}}/"

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
        ttyrecURL = f"{WEBROOT}nethack/ttyrecs"
        dumplogURL = f"{WEBROOT}nethack/dumplogs"
        irclogURL = f"{WEBROOT}nethack/irclogs/tnnt"
        rceditURL = f"{WEBROOT}nethack/rcedit"
        helpURL = f"{sourceURL}/blob/main/botuse.md"
        logday = time.strftime("%d")
        for c in CHANNELS:
            activity[c] = 0
            if IRCLOGS:
                chanLogName[c] = f"{IRCLOGS}/{c}{time.strftime('-%Y-%m-%d.log')}"
                try:
                    chanLog[c] = open(chanLogName[c],'a')
                except (IOError, OSError) as e:
                    print(f"Warning: Could not open log file {chanLogName[c]}: {e}")
                    chanLog[c] = None
                if chanLog[c]: os.chmod(chanLogName[c],stat.S_IRUSR|stat.S_IWUSR|stat.S_IRGRP|stat.S_IROTH)

    xlogfiles = {filepath.FilePath(FILEROOT+"tnnt/var/xlogfile"): ("tnnt", "\t", "tnnt/dumplog/{starttime}.tnnt.html")}
    livelogs  = {filepath.FilePath(FILEROOT+"tnnt/var/livelog"): ("tnnt", "\t")}
    # Scoreboard removed - JSON files deprecated
    try:
        with open(CLANTAGJSON) as f:
            clanTag = json.load(f)
    except (IOError, OSError) as e:
        # File doesn't exist or can't be read - normal for fresh install
        clanTag = {}
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in {CLANTAGJSON}: {e}")
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
       return f'[{self.displaystring.get(thing,thing)}]'

    # for !who or !players or whatever we end up calling it
    # Reduce the repetitive crap
    DGLD = f"{FILEROOT}dgldir/"
    INPR = f"{DGLD}inprogress-"
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

    def _initializeStats(self):
        """Initialize statistics tracking for hourly/daily/full periods."""
        self.stats = {}
        self.initStats("hour")
        self.initStats("day")
        self.initStats("full")

    def _scheduleMasterTasks(self):
        """Schedule master-specific periodic tasks."""
        # work out how much hour is left
        nowtime = datetime.now()
        # add 1 hour, then subtract min, sec, usec to get exact time of next hour.
        nexthour = nowtime + timedelta(hours=1)
        nexthour -= timedelta(minutes=nexthour.minute,
                              seconds=nexthour.second,
                              microseconds=nexthour.microsecond)
        hourleft = (nexthour - nowtime).total_seconds() + 0.5 # start at 0.5 seconds past the hour.
        reactor.callLater(hourleft, self.startHourly)

    def _scheduleAPIPolling(self):
        """Schedule API polling to run every 5 minutes at :00:30, :05:30, :10:30, :15:30, etc."""
        # Do an initial fetch after 30 seconds to populate data quickly
        reactor.callLater(30, self._initialAPIFetch)

        nowtime = datetime.now()
        # Calculate next 5-minute mark
        current_minute = nowtime.minute
        minutes_to_next_5 = (5 - (current_minute % 5)) % 5
        if minutes_to_next_5 == 0 and nowtime.second >= 30:
            # If we're already past :X5:30, go to next 5-minute mark
            minutes_to_next_5 = 5

        # Calculate time until next :X5:30
        next_poll = nowtime + timedelta(minutes=minutes_to_next_5)
        next_poll = next_poll.replace(second=30, microsecond=0)
        next_poll -= timedelta(minutes=next_poll.minute % 5)  # Ensure we're at :00, :05, :10, :15, etc.

        seconds_until_next = (next_poll - nowtime).total_seconds()
        if seconds_until_next <= 0:
            seconds_until_next += 300  # Add 5 minutes if somehow negative

        print(f"TNNT API: Scheduling regular polling to start in {seconds_until_next:.1f} seconds (at {next_poll.strftime('%H:%M:%S')})")
        reactor.callLater(seconds_until_next, self.startAPIPolling)

    def _initialAPIFetch(self):
        """Do an initial API fetch to populate data quickly after startup"""
        print("TNNT API: Performing initial data fetch...")
        self.checkTNNTAPI()

    def _initializeMilestones(self):
        """Initialize milestone tracking for tournament announcements."""
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

    def _initializeGameTracking(self):
        """Initialize game tracking data structures."""
        # lastgame tracking
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
            self.tellbuf = shelve.open(f"{BOTDIR}/tellmsg.db", writeback=True)
        except Exception as e:
            # Fallback to older format if .db fails
            try:
                self.tellbuf = shelve.open(f"{BOTDIR}/tellmsg", writeback=True, protocol=2)
            except Exception as e2:
                print(f"Error: Could not open tell message database: {e2}")
                # Create an in-memory fallback so bot doesn't crash
                self.tellbuf = DictWithSync()

    def _initializeGitHub(self):
        """Initialize GitHub monitoring data structures."""
        # For GitHub monitoring
        self.seen_github_commits = {}  # repo -> set of commit IDs
        self.github_initialized = False
        self.github_repos = []
        if ENABLE_GITHUB and GITHUB_REPOS:
            self.github_repos = GITHUB_REPOS
            # Initialize seen commits for each repo
            for repo_config in self.github_repos:
                repo_key = repo_config["repo"]
                self.seen_github_commits[repo_key] = set()

        # TNNT API monitoring for achievements/trophies/rankings
        self.api_initialized = False
        self.player_achievements = {}  # player -> set of achievement names
        self.player_trophies = {}  # player -> set of trophy names
        self.clan_trophies = {}  # clan -> set of trophy names
        self.clan_rankings = {}  # clan -> rank position
        self.player_scores = {}  # player -> {wins, total_games, ratio}
        self.clan_scores = {}  # clan -> {wins, total_games, ratio}
        self.recently_cleared_players = set()  # Players cleared due to database wipe

    def _initializeRateLimiting(self):
        """Initialize rate limiting data structures."""
        self.rate_limits = {}  # user -> list of command timestamps
        self.abuse_penalties = {}  # user -> penalty end timestamp
        self.consecutive_commands = {}  # user -> [command_time, command_time, ...]
        self.penalty_responses = {}  # user -> [timestamp, timestamp, ...]
        self.last_command_time = {}  # user -> timestamp of last command

    def _initializeCommands(self):
        """Initialize command handlers and callbacks."""
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
                         "dumplog"  : self.doDumplog,
                         "irclog"   : self.doIRClog,
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

    def _initializeLogReading(self):
        """Initialize log file reading and seek to appropriate positions."""
        # seek to end of livelogs
        for filepath in self.livelogs:
            try:
                with filepath.open("r") as handle:
                    handle.seek(0, 2)
                    self.logs_seek[filepath] = handle.tell()
            except (IOError, OSError) as e:
                print(f"Warning: Could not seek to end of livelog {filepath}: {e}")
                self.logs_seek[filepath] = 0

        # sequentially read xlogfiles from beginning to pre-populate lastgame data.
        for filepath in self.xlogfiles:
            try:
                with filepath.open("r") as handle:
                    for line in handle:
                        try:
                            delim = self.logs[filepath][2]
                            game = parse_xlogfile_line(line, delim)
                            game["variant"] = self.logs[filepath][1]
                            game["dumpfmt"] = self.logs[filepath][3]
                            for line in self.logs[filepath][0](game,False):
                                pass
                        except Exception as e:
                            print("Warning: Error processing xlogfile line during startup: {e}")
                            continue
                    self.logs_seek[filepath] = handle.tell()
            except (IOError, OSError) as e:
                print(f"Warning: Could not read xlogfile {filepath}: {e}")
                self.logs_seek[filepath] = 0

    def _startMonitoringTasks(self):
        """Start periodic monitoring tasks."""
        # poll logs for updates
        for filepath in self.logs:
            self.looping_calls[filepath] = task.LoopingCall(self.logReport, filepath)
            self.looping_calls[filepath].start(LOG_POLL_INTERVAL)

        # Additionally, keep an eye on our nick to make sure it's right.
        # Perhaps we only need to set this up if the nick was originally
        # in use when we signed on, but a 30-second looping call won't kill us
        self.looping_calls["nick"] = task.LoopingCall(self.nickCheck)
        self.looping_calls["nick"].start(NICK_CHECK_INTERVAL)
        # Check GitHub for new commits (every minute)
        if not SLAVE and ENABLE_GITHUB and self.github_repos:
            self.looping_calls["github"] = task.LoopingCall(self.checkGitHub)
            # Add initial delay to ensure bot is fully connected before first check
            self.looping_calls["github"].start(60, now=False)  # 1 minute interval, don't run immediately
        # Schedule TNNT API polling for every 5 minutes at :00:30, :05:30, :10:30, :15:30, etc.
        if not SLAVE:
            self._scheduleAPIPolling()
        # Update local milestone summary to master every 5 minutes
        self.looping_calls["summary"] = task.LoopingCall(self.updateSummary)
        self.looping_calls["summary"].start(SUMMARY_UPDATE_INTERVAL)

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
        sasl_string = f'{self.nickname}\0{self.nickname}\0{self.password}'
        sasl_b64_bytes = base64.b64encode(sasl_string.encode(encoding='UTF-8',errors='strict'))
        self.sendLine('AUTHENTICATE PLAIN')
        self.sendLine(f'AUTHENTICATE {sasl_b64_bytes.decode("UTF-8")}')

    def irc_903(self, prefix, params):
        self.sendLine('CAP END')

    def irc_904(self, prefix, params):
        print('sasl auth failed', params)
        self.quit('')
    irc_905 = irc_904

    def _initializeConnection(self):
        """Initialize connection-related settings after signing on."""
        self.factory.resetDelay()
        self.startHeartbeat()
        if not SLAVE:
            for c in CHANNELS:
                self.join(c)
        random.seed()
        # Track bot start time for uptime calculation
        self.starttime = time.time()

    def _initializeLogs(self):
        """Initialize log monitoring configuration."""
        self.logs = {}
        # boolean for whether announcements from the log are 'spam', after dumpfmt
        # true for livelogs, false for xlogfiles
        for xlogfile, (variant, delim, dumpfmt) in self.xlogfiles.items():
            self.logs[xlogfile] = (self.xlogfileReport, variant, delim, dumpfmt, False)
        for livelog, (variant, delim) in self.livelogs.items():
            self.logs[livelog] = (self.livelogReport, variant, delim, "", True)

        self.logs_seek = {}
        self.looping_calls = {}

    def signedOn(self):
        self._initializeConnection()
        self._initializeLogs()

        self._initializeStats()
        if not SLAVE:
            self._scheduleMasterTasks()
            self._initializeMilestones()

        self._initializeGameTracking()
        self._initializeGitHub()
        self._initializeRateLimiting()

        self._initializeCommands()

        self._initializeLogReading()
        self._startMonitoringTasks()

    def nickCheck(self):
        # also rejoin the channel here, in case we drop off for any reason
        if not SLAVE:
            for c in CHANNELS: self.join(c)
        if (self.nickname != NICK):
            self.setNick(NICK)

    def nickChanged(self, nn):
        # catch successful changing of nick from above and identify with nickserv
        self.msg("NickServ", f"identify {nn} {self.password}")

    def logRotate(self):
        if not IRCLOGS: return
        self.logday = time.strftime("%d")
        for c in CHANNELS:
            if self.chanLog[c]: self.chanLog[c].close()
            self.chanLogName[c] = f"{IRCLOGS}/{c}{time.strftime('-%Y-%m-%d.log')}"
            try:
                self.chanLog[c] = open(self.chanLogName[c],'a') # 'w' is probably fine here
            except (IOError, OSError) as e:
                print(f"Warning: Could not rotate log file {self.chanLogName[c]}: {e}")
                self.chanLog[c] = None
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
        self.chanLog[channel].write(f"{time.strftime('%H:%M')} {message}\n")
        self.chanLog[channel].flush()

    # wrapper for "msg" that logs if msg dest is channel
    # Need to log our own actions separately as they don't trigger events
    def msgLog(self, replyto, message):
        if replyto in CHANNELS:
            self.log(replyto, f"<{self.nickname}> {message}")
        self.msg(replyto, message)

    # Similar wrapper for describe
    def describeLog(self,replyto, message):
        if replyto in CHANNELS:
            self.log(f"* {self.nickname} {message}")
        self.describe(replyto, message)

    # Tournament announcements typically go to the channel
    # ...and to the channel log
    # spam flag allows more verbosity in some channels
    def announce(self, message, spam = False, strict_tournament_time = False, early_start_hours = 0):
        if not TEST:
            # Check if we should announce based on tournament timing
            nowtime = datetime.now()
            # Calculate effective start time (may be earlier for clan registrations)
            effective_start = self.ttime["start"] - timedelta(hours=early_start_hours)

            if strict_tournament_time:
                # Strict mode: only during official tournament (no grace period)
                # Used for API announcements (achievements, trophies, clan rankings)
                # Can start early if early_start_hours is specified (for clan registrations)
                game_on = (nowtime > effective_start) and (nowtime < self.ttime["end"])
            else:
                # Normal mode: tournament plus grace period
                # Used for game events (deaths, ascensions)
                game_on = (nowtime > effective_start) and (nowtime < (self.ttime["end"] + timedelta(days=GRACEDAYS)))
            if not game_on: return
        chanlist = CHANNELS
        if spam:
            chanlist = SPAMCHANNELS #only
        for c in chanlist:
            self.msgLog(c, message)

    # construct and send response.
    # replyto is channel, or private nick
    # sender is original sender of query
    def respond(self, replyto, sender, message):
        try:
            if (replyto.lower() == sender.lower()): #private
                self.msg(replyto, message)
            else: #channel - prepend "Nick: " to message
                self.msgLog(replyto, sender + ": " + message)
        except Exception as e:
            print(f"Error sending response to {replyto}: {e}")

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
            print(f"Rate limiting error for {sender}: {e}")
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
            print(f"Penalty response rate limiting error for {sender}: {e}")
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
            print(f"Burst protection error for {sender}: {e}")
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
                if now - self.last_command_time[user] > STALE_BURST_TIMEOUT:
                    old_burst.append(user)

            for user in old_burst:
                del self.last_command_time[user]

        except Exception as e:
            print(f"Error during rate limit cleanup: {e}")

    # Query/Response handling
    #Q#
    def doQuery(self, sender, replyto, msgwords):
        # called when slave gets queried by master.
        # msgwords is [ #Q#, <query_id>, <orig_sender>, <command>, ... ]
        if (sender in MASTERS) and (msgwords[3] in self.qCommands):
            # sender is passed to master; msgwords[2] is passed tp sender
            self.qCommands[msgwords[3]](sender,msgwords[2],msgwords[1],msgwords[3:])
        else:
            print(f"Bogus slave query from {sender}: {' '.join(msgwords)}")

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
            print(f"Bogus slave response from {sender}: {' '.join(msgwords)}")

    # As above, but timed out receiving one or more responses
    def doQueryTimeout(self, query):
        # This gets called regardless, so only process if query still exists
        if query not in self.queries: return

        noResp = []
        for i in self.slaves.keys():
            if not self.queries[query]["finished"].get(i,False):
                noResp.append(i)
        if noResp:
            print(f"WARNING: Query {query}: No response from {self.listStuff(noResp)}")
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
            if k == "realtime": t /= SECONDS_PER_DAY # days, not seconds
            if not FirstContact:
                for m in self.milestones[k]:
                    if self.summary[k] and t >= m and self.summary[k] < m:
                        self.announce(f"\x02TOURNAMENT MILESTONE:\x0f {numbers.get(m,m)} {statnames.get(k,k)}.")
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
                maxStat2 = dict(zip(["name","number"], max(stats[stat2].items(), key=lambda x:x[1])))
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
        self.announce(f"The tournament {event}s in {time}...",True)
        for delay in range (1,time):
            reactor.callLater(delay,self.announce,f"{time-delay}...",True)

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
            self.announce(f"###### TNNT {YEAR} IS OPEN! ######")
        elif abs(nowtime - self.ttime["end"]) < timedelta(minutes=1):
            self.announce(f"###### TNNT {YEAR} IS CLOSED! ######")
            self.multiServerCmd(NICK, NICK, ["fstats"])
            return
        elif abs(nowtime + timedelta(hours=1) - self.ttime["start"]) < timedelta(minutes=1):
            reactor.callLater(SECONDS_PER_HOUR - 3, self.startCountdown,"start",3) # 3 seconds to the next hour
        elif abs(nowtime + timedelta(hours=1) - self.ttime["end"]) < timedelta(minutes=1):
            reactor.callLater(SECONDS_PER_HOUR - 3, self.startCountdown,"end",3) # 3 seconds to the next hour
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
        self.looping_calls["stats"].start(SECONDS_PER_HOUR)

    def startAPIPolling(self):
        """Start the API polling loop - runs every 5 minutes from first scheduled time"""
        # Run the first check
        self.checkTNNTAPI()
        # Schedule to run every 5 minutes from now on
        self.looping_calls["api"] = task.LoopingCall(self.checkTNNTAPI)
        self.looping_calls["api"].start(300)  # 300 seconds = 5 minutes

    # Countdown timer
    def countDown(self):
        cd = {}
        for event in ("start", "end"):
            cd["event"] = event
            # add half a second for rounding (we truncate at the decimal later)
            td = (self.ttime[event] - datetime.now()) + timedelta(seconds=0.5)
            sec = int(td.seconds)
            cd["seconds"] = int(sec % 60)
            cd["minutes"] = int((sec / SECONDS_PER_MINUTE) % SECONDS_PER_MINUTE)
            cd["hours"] = int(sec / SECONDS_PER_HOUR)
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
            timeMsg += f"The {YEAR} tournament is OVER!"
            self.respond(replyto, sender, timeMsg)
            return
        verbs = { "start" : "begins",
                  "end" : "closes"
                }
        timeMsg += (f"{YEAR} Tournament {verbs[timeLeft['event']]} in {timeLeft['days']}d "
                   f"{timeLeft['hours']:0>2}:{timeLeft['minutes']:0>2}:{timeLeft['seconds']:0>2}")
        self.respond(replyto, sender, timeMsg)

    def doSource(self, sender, replyto, msgwords):
        self.respond(replyto, sender, self.sourceURL )

    def doScoreboard(self, sender, replyto, msgwords):
        self.respond(replyto, sender, self.scoresURL )

    def doTtyrec(self, sender, replyto, msgwords):
        self.respond(replyto, sender, self.ttyrecURL )

    def doDumplog(self, sender, replyto, msgwords):
        self.respond(replyto, sender, self.dumplogURL )

    def doIRClog(self, sender, replyto, msgwords):
        self.respond(replyto, sender, self.irclogURL )

    def doRCedit(self, sender, replyto, msgwords):
        self.respond(replyto, sender, self.rceditURL )

    def doHelp(self, sender, replyto, msgwords):
        self.respond(replyto, sender, self.helpURL )

    def doScore(self, sender, replyto, msgwords):
        # Check if player name provided
        if len(msgwords) < 2:
            # Show top 5 players if no name specified
            if not self.player_scores:
                self.respond(replyto, sender, f"Scoreboard data not yet loaded. Check: {self.scoresURL}")
                return

            # Sort players by wins then by name
            sorted_players = sorted(self.player_scores.items(),
                                  key=lambda x: (-x[1]["wins"], x[0]))[:5]

            response = "Top 5 players: "
            for i, (name, data) in enumerate(sorted_players, 1):
                clan_text = f" ({data['clan']})" if data['clan'] else ""
                response += f"#{i} {name}{clan_text}: {data['wins']} wins ({data['ratio']}) | "
            response = response.rstrip(" | ")
            self.respond(replyto, sender, response)
        else:
            # Look up specific player
            player_name = " ".join(msgwords[1:])

            if player_name not in self.player_scores:
                # Try fetching directly from API if not in cache
                try:
                    r = requests.get(f"{TNNT_API_BASE}/players/{player_name}/",
                                   headers=TNNT_API_HEADERS, timeout=5)
                    if r.status_code == 200:
                        data = r.json()
                        clan_text = f" (clan: {data['clan']})" if data.get('clan') else ""
                        response = (f"{player_name}{clan_text}: {data['wins']} wins out of "
                                  f"{data['total_games']} games ({data['ratio']}) | Z-score: {data['zscore']}")
                        self.respond(replyto, sender, response)
                    else:
                        self.respond(replyto, sender, f"Player '{player_name}' not found. Check: {self.scoresURL}")
                except Exception:
                    self.respond(replyto, sender, f"Error fetching player data. Check: {self.scoresURL}")
            else:
                # Use cached data
                data = self.player_scores[player_name]
                clan_text = f" (clan: {data['clan']})" if data['clan'] else ""

                # Find player's rank
                sorted_players = sorted(self.player_scores.items(),
                                      key=lambda x: (-x[1]["wins"], x[0]))
                rank = next((i for i, (n, _) in enumerate(sorted_players, 1) if n == player_name), "?")

                response = f"#{rank} {player_name}{clan_text}: {data['wins']} wins out of {data['total_games']} games ({data['ratio']})"
                self.respond(replyto, sender, response)

    def doClanTag(self, sender, replyto, msgwords):
        # ClanTag functionality removed - JSON scoreboard is deprecated
        self.respond(replyto, sender, f"Clan tags are no longer supported. Check the tournament scoreboard at: {self.scoresURL}")

    def doClanScore(self, sender, replyto, msgwords):
        # Check if clan name provided
        if len(msgwords) < 2:
            # Show top 5 clans if no name specified
            if not self.clan_scores:
                self.respond(replyto, sender, "Clan data not yet loaded. Check: https://tnnt.org/clans")
                return

            # Sort clans by rank
            sorted_clans = sorted(self.clan_scores.items(),
                                key=lambda x: x[1]["rank"])[:5]

            response = "Top 5 clans: "
            for name, data in sorted_clans:
                response += f"#{data['rank']} {name}: {data['wins']} wins ({data['ratio']}) | "
            response = response.rstrip(" | ")
            self.respond(replyto, sender, response)
        else:
            # Look up specific clan
            clan_name = " ".join(msgwords[1:])

            if clan_name not in self.clan_scores:
                # Try fetching directly from API if not in cache
                try:
                    r = requests.get(f"{TNNT_API_BASE}/clans/{clan_name}/",
                                   headers=TNNT_API_HEADERS, timeout=5)
                    if r.status_code == 200:
                        data = r.json()
                        member_count = len(data.get('members', []))
                        response = (f"{clan_name}: {data['wins']} wins out of {data['total_games']} games "
                                  f"({data['ratio']}) | Members: {member_count}")
                        self.respond(replyto, sender, response)
                    else:
                        self.respond(replyto, sender, f"Clan '{clan_name}' not found. Check: https://tnnt.org/clans")
                except Exception:
                    self.respond(replyto, sender, "Error fetching clan data. Check: https://tnnt.org/clans")
            else:
                # Use cached data
                data = self.clan_scores[clan_name]
                response = f"#{data['rank']} {clan_name}: {data['wins']} wins out of {data['total_games']} games ({data['ratio']})"
                self.respond(replyto, sender, response)

    def doCommands(self, sender, replyto, msgwords):
        commands_list = ("$help $ping $time $tell $source $lastgame $lastasc $asc $streak $rcedit "
                        "$scores $sb $score $ttyrec $dumplog $irclog $clanscore $clantag $whereis "
                        "$players $who $commands $status")
        self.respond(replyto, sender, f"available commands are: {commands_list}")

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
        uptime_days = uptime_seconds // SECONDS_PER_DAY
        uptime_hours = (uptime_seconds % SECONDS_PER_DAY) // SECONDS_PER_HOUR
        uptime_mins = (uptime_seconds % SECONDS_PER_HOUR) // SECONDS_PER_MINUTE

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
        status_parts.append(f"Status: {NICK} on {SERVERTAG}")
        status_parts.append(f"Uptime: {uptime_days}d {uptime_hours}h {uptime_mins}m")
        if mem_mb != "N/A":
            status_parts.append(f"Memory: {mem_mb:.1f}MB")
        status_parts.append(f"Monitors: {monitor_count}")
        status_parts.append(f"Queries: {query_count}")
        status_parts.append(f"Messages: {msg_count}")
        status_parts.append(f"RateLimit: {rate_limit_count}")
        if abuse_penalty_count > 0:
            status_parts.append(f"AbusePenalty: {abuse_penalty_count}")

        # GitHub monitoring status
        if hasattr(self, 'seen_github_commits') and not SLAVE and ENABLE_GITHUB:
            if self.github_repos:
                # Count total commits across all repos
                total_commits = sum(len(commits) for commits in self.seen_github_commits.values())
                repo_count = len(self.github_repos)
                status_parts.append(f"GitHub: {total_commits} commits tracked across {repo_count} repos")
        self.respond(replyto, sender, " | ".join(status_parts))

    # GitHub monitoring via Atom feed
    def checkGitHub(self):
        """Check GitHub repos for new commits via Atom feed and announce them"""
        if SLAVE:
            return  # Only master bot monitors GitHub
        if not self.github_repos:
            return  # GitHub monitoring not configured
        all_new_commits = []  # Collect commits from all repos
        for repo_config in self.github_repos:
            new_commits = self._checkGitHubRepo(repo_config)
            all_new_commits.extend(new_commits)
        # Announce all new commits with delays to prevent flood kicks
        for i, (msg, repo, short_hash, author) in enumerate(all_new_commits):
            # Schedule message with one second delay between each
            delay = i * 1.0
            for channel in SPAMCHANNELS:
                reactor.callLater(delay, self.msgLog, channel, msg)
            # Debug log
            print(f"GitHub: New commit in {repo}: {short_hash} by {author} (delayed {delay}s)")
        # Mark as initialized only after ALL repos have been checked
        if not self.github_initialized:
            self.github_initialized = True

    def _checkGitHubRepo(self, repo_config):
        """Check a single GitHub repo for new commits"""
        repo = repo_config["repo"]
        branch = repo_config.get("branch", "master")
        new_commits = []  # Collect new commits to return
        try:
            # GitHub Atom feed for commits on specified branch
            url = f"https://github.com/{repo}/commits/{branch}.atom"
            headers = {"User-Agent": "TNNT IRC Bot/1.0"}
            r = requests.get(url, headers=headers, timeout=10)
            if r.status_code != 200:
                print(f"GitHub Atom feed for {repo} returned status {r.status_code}")
                return new_commits
            # Parse the Atom feed
            root = ET.fromstring(r.text)
            # GitHub uses Atom format
            ns = {'atom': 'http://www.w3.org/2005/Atom'}
            entries = root.findall('atom:entry', ns)
            # Reverse entries to process oldest first (Atom feed is newest first)
            for entry in reversed(entries):
                # Get commit details
                title_elem = entry.find('atom:title', ns)
                link_elem = entry.find('atom:link', ns)
                id_elem = entry.find('atom:id', ns)
                author_elem = entry.find('atom:author/atom:name', ns)
                # Get commit ID from the id tag (format: tag:github.com,2008:Grit::Commit/SHA)
                commit_id = None
                if id_elem is not None and id_elem.text:
                    # Extract SHA from the id
                    parts = id_elem.text.split('/')
                    if parts:
                        commit_id = parts[-1]
                # Extract commit details
                title = title_elem.text.strip() if title_elem is not None and title_elem.text else ""
                # Replace newlines and excess whitespace with single spaces
                title = ' '.join(title.split()) if title else ""
                link = link_elem.get('href', '') if link_elem is not None else ""
                author = author_elem.text if author_elem is not None else "unknown"
                # Check if we've seen this commit before for this repo
                if commit_id and title and commit_id not in self.seen_github_commits[repo]:
                    self.seen_github_commits[repo].add(commit_id)
                    # Only announce if this is a recent check (not first run)
                    if hasattr(self, "github_initialized") and self.github_initialized:
                        # Sanitize title - remove format string placeholders
                        title = sanitize_format_string(title)
                        # Format message like botifico with IRC colors:
                        # - 12 (Light Blue) for repository name
                        # - 07 (Orange) for username
                        # - 03 (Dark Green) for commit hash
                        # - 13 (Pink/Magenta) for URLs
                        repo_name = repo.split('/')[-1]  # Get repo name from owner/repo
                        short_hash = commit_id[:7] if commit_id else "unknown"
                        # Format: [RepoName] author hash - Commit message URL
                        msg = f"[\x0312{repo_name}\x03] \x0307{author}\x03 \x0303{short_hash}\x03 - {title} \x0313{link}\x03"
                        # Add to list for return (msg, repo_name, short_hash, author)
                        new_commits.append((msg, repo_name, short_hash, author))
            # Clean up old commits to prevent memory growth
            # Keep only the 50 most recent commit IDs per repo
            if len(self.seen_github_commits[repo]) > 50:
                # Convert to list and keep newest 50
                commit_list = list(self.seen_github_commits[repo])
                self.seen_github_commits[repo] = set(commit_list[-50:])
            return new_commits
        except requests.exceptions.Timeout:
            print(f"Timeout checking GitHub Atom feed for {repo}")
            return new_commits
        except requests.exceptions.RequestException as e:
            print(f"Error fetching GitHub Atom feed for {repo}: {e}")
            return new_commits
        except ET.ParseError as e:
            print(f"Error parsing GitHub Atom XML for {repo}: {e}")
            return new_commits
        except Exception as e:
            print(f"Unexpected error checking GitHub: {e}")
            return new_commits

    # TNNT API monitoring for scoreboard functionality
    def checkTNNTAPI(self):
        """Check TNNT API for achievement/trophy/ranking changes"""
        if SLAVE:
            return  # Only master bot monitors API

        try:
            # Fetch current scoreboard data
            r = requests.get(f"{TNNT_API_BASE}/scoreboard/", headers=TNNT_API_HEADERS, timeout=10)
            if r.status_code != 200:
                print(f"TNNT API scoreboard returned status {r.status_code}")
                return

            data = r.json()

            # Collect all announcements to send with delays
            all_announcements = []

            # Track current players to detect removals
            current_players = set()

            # Process player data
            for player_data in data.get("players", []):
                player_name = player_data["name"]
                current_players.add(player_name)

                # Store player scores for $score command
                self.player_scores[player_name] = {
                    "wins": player_data["wins"],
                    "total_games": player_data["total_games"],
                    "ratio": player_data["ratio"],
                    "clan": player_data.get("clan", None)
                }

                # Check for new achievements (requires separate API call)
                player_announcements = self._checkPlayerAchievements(player_name)
                if self.api_initialized:
                    all_announcements.extend(player_announcements)

            # Clear data for players no longer in scoreboard (e.g., after database wipe)
            if self.api_initialized:
                removed_players = set(self.player_achievements.keys()) - current_players
                if removed_players:
                    print(f"TNNT API: {len(removed_players)} players no longer in scoreboard, marking as cleared")
                    # Track these players as recently cleared so we can announce when they return
                    self.recently_cleared_players.update(removed_players)
                    # Clear the stored data
                    for player_name in removed_players:
                        self.player_achievements.pop(player_name, None)
                        self.player_trophies.pop(player_name, None)
                        self.player_scores.pop(player_name, None)

            # Process clan data and check for ranking changes
            new_clan_rankings = {}
            for idx, clan_data in enumerate(data.get("clans", []), 1):
                clan_name = clan_data["name"]
                new_clan_rankings[clan_name] = idx

                # Store clan scores for $clanscore command
                self.clan_scores[clan_name] = {
                    "wins": clan_data["wins"],
                    "total_games": clan_data["total_games"],
                    "ratio": clan_data["ratio"],
                    "rank": idx
                }

                # Check for new clan registration
                if self.api_initialized and clan_name not in self.clan_rankings:
                    # New clan registered
                    msg = f"[{self.displaystring['clan']}] New clan registered - {clan_name}"
                    all_announcements.append((msg, "clan", clan_name, "new"))
                    print(f"TNNT API: New clan registered - {clan_name}")

                # Check for ranking changes
                elif self.api_initialized and clan_name in self.clan_rankings:
                    old_rank = self.clan_rankings[clan_name]
                    # Only announce ranking changes if the clan has at least 1 win
                    # (0-win rankings are purely alphabetical and not meaningful)
                    if old_rank != idx and clan_data["wins"] > 0:
                        # Clan ranking changed!
                        if idx < old_rank:
                            # Improved ranking
                            ascensions = clan_data["wins"]
                            msg = f"[{self.displaystring['clan']}] Clan {clan_name} moves up to position #{idx} with {ascensions} ascensions."
                        else:
                            # Dropped ranking
                            msg = f"[{self.displaystring['clan']}] Clan {clan_name} drops to position #{idx}."

                        all_announcements.append((msg, "clan", clan_name, f"{old_rank}->{idx}"))
                        print(f"TNNT API: Clan ranking change - {clan_name}: {old_rank} -> {idx}")

            # Update stored rankings
            self.clan_rankings = new_clan_rankings

            # Send all announcements with delays to prevent flood kicks
            for i, announcement in enumerate(all_announcements):
                msg = announcement[0]
                # Schedule message with 1 second delay between each
                delay = i * 1.0
                # Check if this is a clan registration announcement (starts 24 hours early)
                is_clan_registration = len(announcement) >= 4 and announcement[3] == "new"
                early_hours = 24 if is_clan_registration else 0
                # Use announce() method with strict tournament time (no grace period for API events)
                reactor.callLater(delay, self.announce, msg, True, True, early_hours)
                # Debug log
                if len(announcement) >= 3:
                    print(f"TNNT API: Scheduling announcement #{i+1} (delay {delay}s): {announcement[1]} - {announcement[2]}")

            # Mark as initialized after first successful fetch
            if not self.api_initialized:
                self.api_initialized = True
                print(f"TNNT API: Initialized with {len(self.player_scores)} players and {len(self.clan_scores)} clans")

        except requests.exceptions.Timeout:
            print("Timeout checking TNNT API")
        except requests.exceptions.RequestException as e:
            print(f"Error fetching TNNT API: {e}")
        except Exception as e:
            print(f"Unexpected error checking TNNT API: {e}")

    def _checkPlayerAchievements(self, player_name):
        """Check for new achievements and trophies for a specific player
        Returns a list of announcement tuples (message, type, player, details)
        """
        announcements = []
        try:
            # Fetch player details including trophies
            r = requests.get(f"{TNNT_API_BASE}/players/{player_name}/",
                           headers=TNNT_API_HEADERS, timeout=10)
            if r.status_code != 200:
                return announcements  # Player might not exist or API error

            player_data = r.json()

            # Check if this player was recently cleared (database rebuild scenario)
            was_recently_cleared = player_name in self.recently_cleared_players
            if was_recently_cleared:
                print(f"TNNT API: Player {player_name} returned after being cleared")
                self.recently_cleared_players.discard(player_name)
                # Check if we should suppress announcements for database rebuilds
                if not ANNOUNCE_AFTER_DB_REBUILD:
                    print(f"TNNT API: Suppressing re-announcements for {player_name} (ANNOUNCE_AFTER_DB_REBUILD=False)")

            # Check for new trophies
            current_trophies = set(t["name"] for t in player_data.get("trophies", []))
            # Announce if: player was tracked before OR (was recently cleared with trophies AND announcements enabled)
            if player_name in self.player_trophies or (was_recently_cleared and current_trophies and ANNOUNCE_AFTER_DB_REBUILD):
                if player_name in self.player_trophies:
                    new_trophies = current_trophies - self.player_trophies[player_name]
                else:
                    # Player was cleared, treat all trophies as new (only if ANNOUNCE_AFTER_DB_REBUILD is True)
                    new_trophies = current_trophies
                if new_trophies:
                    count = len(new_trophies)
                    trophy_list = list(new_trophies)

                    if count == 1:
                        msg = f"[{self.displaystring['trophy']}] {player_name} now has {trophy_list[0]}."
                    elif count == 2:
                        msg = f"[{self.displaystring['trophy']}] {player_name} now has {trophy_list[0]} and {trophy_list[1]}."
                    elif count == 3:
                        msg = f"[{self.displaystring['trophy']}] {player_name} now has {trophy_list[0]}, {trophy_list[1]}, and {trophy_list[2]}."
                    elif count == 4:
                        msg = f"[{self.displaystring['trophy']}] {player_name} now has {trophy_list[0]}, {trophy_list[1]}, {trophy_list[2]}, and {trophy_list[3]}."
                    else:
                        msg = f"[{self.displaystring['trophy']}] {player_name} now has {count} new trophies."

                    announcements.append((msg, "trophy", player_name, str(new_trophies)))
                    print(f"TNNT API: New trophies - {player_name}: {new_trophies}")
            self.player_trophies[player_name] = current_trophies

            # Fetch achievements
            r = requests.get(f"{TNNT_API_BASE}/players/{player_name}/achievements/",
                           headers=TNNT_API_HEADERS, timeout=10)
            if r.status_code != 200:
                return announcements

            achievements = r.json()
            current_achievements = set(a["name"] for a in achievements)

            # Announce if: player was tracked before OR (was recently cleared with achievements AND announcements enabled)
            if player_name in self.player_achievements or (was_recently_cleared and current_achievements and ANNOUNCE_AFTER_DB_REBUILD):
                if player_name in self.player_achievements:
                    new_achievements = current_achievements - self.player_achievements[player_name]
                else:
                    # Player was cleared, treat all achievements as new (only if ANNOUNCE_AFTER_DB_REBUILD is True)
                    new_achievements = current_achievements
                if new_achievements:
                    count = len(new_achievements)
                    achievement_list = list(new_achievements)

                    if count == 1:
                        msg = f"[{self.displaystring['achieve']}] {player_name} just earned {achievement_list[0]}."
                    elif count == 2:
                        msg = f"[{self.displaystring['achieve']}] {player_name} just earned {achievement_list[0]} and {achievement_list[1]}."
                    elif count == 3:
                        msg = f"[{self.displaystring['achieve']}] {player_name} just earned {achievement_list[0]}, {achievement_list[1]}, and {achievement_list[2]}."
                    elif count == 4:
                        msg = f"[{self.displaystring['achieve']}] {player_name} just earned {achievement_list[0]}, {achievement_list[1]}, {achievement_list[2]}, and {achievement_list[3]}."
                    else:
                        msg = f"[{self.displaystring['achieve']}] {player_name} just earned {count} new achievements."

                    announcements.append((msg, "achievement", player_name, str(new_achievements)))
                    print(f"TNNT API: New achievements - {player_name}: {new_achievements}")

            self.player_achievements[player_name] = current_achievements

        except Exception as e:
            # Silently ignore individual player errors to not spam logs
            pass

        return announcements

    def takeMessage(self, sender, replyto, msgwords):
        if len(msgwords) < 3:
            self.respond(replyto, sender, f"{TRIGGER}tell <recipient> <message> (leave a message for someone)")
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
                self.respond(user,user, f"Message from {sender} at {self.msgTime(ts)}: {message}")
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
                self.respond(CHANNEL, user, f"Messages from {fromstr} have been forwarded to you privately.");

        else:
            for (forwardto,sender,ts,message) in self.tellbuf[plainuser]:
                self.respond(forwardto, user, f"Message from {sender} at {self.msgTime(ts)}: {message}")
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
        message = f"#Q# {' '.join([q, sender] + msgwords)}"

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
            self.msg(master, f"#P# {query} {respChunks.pop(0)}")
        self.msg(master, f"#R# {query} {lastChunk}")
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
        response = f"#R# {query} {self.displaytag(SERVERTAG)} {plrvar}"
        self.msg(master, response)

    # !players callback. Actually print the output.
    def outPlayers(self,q):
        outmsg = " :: ".join(list(q["resp"].values()))
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
            self.msg(master, f"#R# {query} {self.displaytag(SERVERTAG)} Invalid player name.")
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
                sender = sender.split(" ")[0] # Extract just username before space
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
                        msg = (f"Abuse penalty active: {remaining//60}m {remaining%60}s remaining. "
                               "(Triggered by spamming consecutive commands)")
                        self.respond(replyto, sender, msg)
                    else:
                        self.respond(replyto, sender, f"Rate limit exceeded. Please wait before using {TRIGGER}{command} again.")
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
        return game["death"].lower() in ["quit", "escaped"] and game["points"] < SCUM_THRESHOLD

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
        if game["turns"] < SHORT_GAME_TURNS:
            self.shortgame[game["name"]] = self.shortgame.get(game["name"],0) + 1
            if report and self.shortgame[game["name"]] % SHORT_GAME_BATCH_SIZE == 0:
                yield("{0} has {1} consecutive games less than {2} turns.".format(game["name"], self.shortgame[game["name"]], SHORT_GAME_TURNS))
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

        # Special reaction if player was killed by the bot's namesake
        if "Croesus" in game.get("death", ""):
            yield random.choice(self.croesus_croesus_wins).format(player=game.get("name", "Someone"))

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
            # Special reaction if the bot's namesake is killed
            if event.get("killed_uniq") == "Croesus":
                yield random.choice(self.croesus_player_wins).format(player=event.get("player", "Someone"))
        elif "defeated" in event: # fourk uses this instead of killed_uniq.
            yield ("{player} ({role} {race} {gender} {align}) "
                   "defeated {defeated}, on T:{turns}").format(**event)
            # Special reaction if the bot's namesake is defeated
            if event.get("defeated") == "Croesus":
                yield random.choice(self.croesus_player_wins).format(player=event.get("player", "Someone"))
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
        try:
            summary_data = {k: self.stats["full"][k] for k in ('games', 'ascend', 'points', 'turns', 'realtime')}
            for master in MASTERS:
                self.msg(master, f"#S# {json.dumps(summary_data)}")
        except Exception as e:
            print(f"Error sending summary update: {e}")

    def logReport(self, filepath):
        try:
            with filepath.open("r") as handle:
                handle.seek(self.logs_seek[filepath])

                for line in handle:
                    try:
                        delim = self.logs[filepath][2]
                        game = parse_xlogfile_line(line, delim)
                        game["dumpfmt"] = self.logs[filepath][3]
                        spam = self.logs[filepath][4]
                        for line in self.logs[filepath][0](game):
                            line = f"{self.displaytag(SERVERTAG)} {line}"
                            if SLAVE:
                                if spam:
                                    line = f"SPAM: {line}"
                                for master in MASTERS:
                                    self.msg(master, line)
                            else:
                                self.announce(line,spam)
                        self.updateSummary()
                    except Exception as e:
                        print(f"Error processing log line from {filepath}: {e}")
                        # Continue processing other lines
                        continue

                self.logs_seek[filepath] = handle.tell()
        except (IOError, OSError) as e:
            print("Error reading log file {}: {}".format(filepath, e))
            # Don't update seek position on read error

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
