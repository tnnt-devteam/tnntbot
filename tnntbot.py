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
from twisted.words.protocols import irc
from twisted.python import filepath
from twisted.application import internet, service
from datetime import datetime, timedelta
import time     # for !time
import ast      # for conduct/achievement bitfields - not really used
import os       # for check path exists (dumplogs), and chmod
import stat     # for chmod mode bits
import re       # for hello, and other things.
import urllib   # for dealing with NH4 variants' #&$#@ spaces in filenames.
import shelve   # for persistent !tell messages
import random   # for !rng and friends
import glob     # for matching in !whereis
import json     # for tournament scoreboard things

from tnnt.botconf import HOST, PORT, CHANNELS, NICK, USERNAME, REALNAME, BOTDIR
from tnnt.botconf import PWFILE, FILEROOT, WEBROOT, ADMIN, YEAR
from tnnt.botconf import SERVERTAG
try: from tnnt.botconf import SPAMCHANELS
except: SPAMCHANNELS = CHANNELS
try: from tnnt.botconf import DCBRIDGE
except: DCBRIDGE = None
try: from tnnt.botconf import TEST
except: TEST = False
try: from tnnt.botconf import GRACEDAYS
except: GRACEDAYS = 5
try:
    from tnnt.botconf import REMOTES
except:
    SLAVE = True #if we have no slaves, we (probably) are the slave
    REMOTES = {}
try:
    from tnnt.botconf import MASTERS
except:
    SLAVE = False #if we have no master we (definitely) are the master
    MASTERS = []
try:
    from tnnt.botconf import LOGROOT
except:
    LOGROOT = None

# config.json is where all the tournament trophies, achievements, other stuff are defined.
# it's mainly used for driving the official scoreboard but we use it here too.
TWIT = False
if not SLAVE:
    try: from tnnt.botconf import CONFIGJSON
    except: CONFIGJSON = "config.json" # assume current directory

    # slurp the whole shebang into a big-arse dict.
    # need to parse out the comments. Thses must start with '# ' or '#-'
    # because my regexp is dumb
    config = json.loads(re.sub('#[ -].*','',open(CONFIGJSON).read()))

    # scoreboard.json is the output from the scoreboard script that tracks achievements and trophies
    try: from tnnt.botconf import SCOREBOARDJSON
    except: SCOREBOARDJSON = "scoreboard.json" # assume current directory

    # twitter - minimalist twitter api: http://mike.verdone.ca/twitter/
    # pip install twitter
    # set TWIT to false to prevent tweeting
    TWIT = True
    try:
        from tnnt.botconf import TWITAUTH
    except:
        print "no TWITAUTH - twitter disabled"
        TWIT = False
    try:
        from twitter import Twitter, OAuth
    except:
        print "Unable to import from twitter module"
        TWIT = False

CLANTAGJSON = BOTDIR + "/clantag.json"

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
    ("conduct", "event", "carried", "flags", "achieve"), ast.literal_eval))
#xlogfile_parse["starttime"] = fromtimestamp_int
#xlogfile_parse["curtime"] = fromtimestamp_int
#xlogfile_parse["endtime"] = fromtimestamp_int
#xlogfile_parse["realtime"] = timedelta_int
#xlogfile_parse["deathdate"] = xlogfile_parse["birthdate"] = isodate
#xlogfile_parse["dumplog"] = fixdump

def parse_xlogfile_line(line, delim):
    record = {}
    for field in line.strip().split(delim):
        key, _, value = field.partition("=")
        if key in xlogfile_parse:
            value = xlogfile_parse[key](value)
        record[key] = value
    return record

#def xlogfile_entries(fp):
#    if fp is None: return
#    with fp.open("rt") as handle:
#        for line in handle:
#            yield parse_xlogfile_line(line)

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
        password = open(PWFILE, "r").read().strip()
    except:
        password = "NotTHEPassword"
    if TWIT:
       try:
           gibberish_that_makes_twitter_work = open(TWITAUTH,"r").read().strip().split("\n")
           twit = Twitter(auth=OAuth(*gibberish_that_makes_twitter_work))
       except Exception as e:
           print "Failed to auth to twitter"
           print e
           TWIT = False


    sourceURL = "https://github.com/tnnt-devteam/tnntbot"
    versionName = "tnntbot.py"
    versionNum = "0.1"

    dump_url_prefix = WEBROOT + "userdata/{name[0]}/{name}/"
    dump_file_prefix = FILEROOT + "dgldir/userdata/{name[0]}/{name}/"

    # tnnt runs on UTC
    os.environ["TZ"] = ":UTC"
    ttime = { "start": datetime(int(YEAR),11,01,00,00,00),
              "end"  : datetime(int(YEAR),12,01,00,00,00)
            }

    chanLog = {}
    chanLogName = {}
    activity = {}
    if not SLAVE:
        scoresURL = "https://www.hardfought.org/tnnt/trophies.html or https://www.hardfought.org/tnnt/clans.html"
        rceditURL = WEBROOT + "nethack/rcedit"
        helpURL = sourceURL + "/blob/master/botuse.txt"
        logday = time.strftime("%d")
        for c in CHANNELS:
            activity[c] = 0
            if LOGROOT:
                chanLogName[c] = LOGROOT + c + time.strftime("-%Y-%m-%d.log")
                try:
                    chanLog[c] = open(chanLogName[c],'a')
                except:
                    chanLog[c] = None
                if chanLog[c]: os.chmod(chanLogName[c],stat.S_IRUSR|stat.S_IWUSR|stat.S_IRGRP|stat.S_IROTH)

    xlogfiles = {filepath.FilePath(FILEROOT+"tnnt/var/xlogfile"): ("tnnt", "\t", "tnnt/dumplog/{starttime}.tnnt.txt")}
    livelogs  = {filepath.FilePath(FILEROOT+"tnnt/var/livelog"): ("tnnt", "\t")}
    scoreboard = {}
    try:
        clanTag = json.load(open(CLANTAGJSON))
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

    dungeons = ["The Dungeons of Doom","Gehennom","The Gnomish Mines","The Quest",
                "Sokoban","Fort Ludios","DevTeam's Office","Vlad's Tower","The Elemental Planes"]

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
            print 'sasl not available'
            self.quit('')
        sasl = ('{0}\0{0}\0{1}'.format(self.nickname, self.password)).encode('base64').strip()
        self.sendLine('AUTHENTICATE PLAIN')
        self.sendLine('AUTHENTICATE ' + sasl)

    def irc_903(self, prefix, params):
        self.sendLine('CAP END')

    def irc_904(self, prefix, params):
        print 'sasl auth failed', params
        self.quit('')
    irc_905 = irc_904

    def signedOn(self):
        self.factory.resetDelay()
        self.startHeartbeat()
        if not SLAVE:
            for c in CHANNELS:
                self.join(c)
        random.seed()

        self.logs = {}
        # boolean for whether announcements from the log are 'spam', after dumpfmt
        # true for livelogs, false for xlogfiles
        for xlogfile, (variant, delim, dumpfmt) in self.xlogfiles.iteritems():
            self.logs[xlogfile] = (self.xlogfileReport, variant, delim, dumpfmt, False)
        for livelog, (variant, delim) in self.livelogs.iteritems():
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
        self.tellbuf = shelve.open(BOTDIR + "/tellmsg.db", writeback=True)
        # for !setmintc
        self.plr_tc = shelve.open(BOTDIR + "/plrtc.db", writeback=True)

        # Commands must be lowercase here.
        self.commands = {"ping"     : self.doPing,
                         "time"     : self.doTime,
                         "tell"     : self.takeMessage,
                         "source"   : self.doSource,
                         "lastgame" : self.multiServerCmd,
                         "lastasc"  : self.multiServerCmd,
                         "scores"   : self.doScoreboard,
                         "sb"       : self.doScoreboard,
                         "rcedit"   : self.doRCedit,
                         "commands" : self.doCommands,
                         "help"     : self.doHelp,
                         "score"    : self.doScore,
                         "clanscore": self.doClanScore,
                         "clantag"  : self.doClanTag,
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
        # 1 minute looping call for trophies and achievements.
        self.looping_calls["trophy"] = task.LoopingCall(self.checkScoreboard)
        self.looping_calls["trophy"].start(30)
        # Call it now to seed the trophy dict.
        self.checkScoreboard()
        # Update local milestone summary to master every 5 minutes
        self.looping_calls["summary"] = task.LoopingCall(self.updateSummary)
        self.looping_calls["summary"].start(300)

    def tweet(self, message):
        if TWIT:
            message = self.stripText(message)
            try:
                if TEST:
                     message = "[TEST] " + message
                     print "Not tweeting in test mode: " + message
                     return
                self.twit.statuses.update(status=message)
            except Exception as e:
                print "Bad tweet: " + message
                print e

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
        if not LOGROOT: return
        self.logday = time.strftime("%d")
        for c in CHANNELS:
            if self.chanLog[c]: self.chanLog[c].close()
            self.chanLogName[c] = LOGROOT + c + time.strftime("-%Y-%m-%d.log")
            try: self.chanLog[c] = open(self.chanLogName[c],'a') # 'w' is probably fine here
            except: self.chanLog[c] = None
            if self.chanLog[c]: os.chmod(self.chanLogName[c],stat.S_IRUSR|stat.S_IWUSR|stat.S_IRGRP|stat.S_IROTH)

    def stripText(self, msg):
        # strip the colour control stuff out
        # This can probably all be done with a single RE but I have a headache.
        message = re.sub(r'\x03\d\d,\d\d', '', msg) # fg,bg pair
        message = re.sub(r'\x03\d\d', '', message) # fg only
        message = re.sub(r'[\x1D\x03\x0f]', '', message) # end of colour and italics
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
        else: # only tweet non spam
            self.tweet(message)
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

    # Query/Response handling
    #Q#
    def doQuery(self, sender, replyto, msgwords):
        # called when slave gets queried by master.
        # msgwords is [ #Q#, <query_id>, <orig_sender>, <command>, ... ]
        if (sender in MASTERS) and (msgwords[3] in self.qCommands):
            # sender is passed to master; msgwords[2] is passed tp sender
            self.qCommands[msgwords[3]](sender,msgwords[2],msgwords[1],msgwords[3:])
        else:
            print "Bogus slave query from " + sender + ": " + " ".join(msgwords);

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
            print "Bogus slave response from " + sender + ": " + " ".join(msgwords);

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
                     "realtime": "days spent playning nethack"}
        if sender not in self.slaves:
            return
        # if this is the first time the slave has contacted us since we restarted
        # we don't want to announce anything, because we risk repeating ourselves
        FirstContact = False
        if self.summaries[sender]["games"] == 0:
            FirstContact = True
        self.summaries[sender] = json.loads(" ".join(msgwords[1:]))
        for k in self.milestones.keys():
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

        statmsg = time.strftime(periodStr[p]) + "Games: {games}, Asc: {ascend}, Scum: {scum}. ".format(**stats)
        if stats["games"] != 0:
            for stat1 in stat1lst:
                statmsg += stat1.format(**stats)
            for stat2 in stat2lst:
                # Find whatever thing from the list above had the most games, and how many games it had
                maxStat2 = dict(zip(["name","number"],max(stats[stat2].iteritems(), key=lambda x:x[1])))
                # Expand the Rog->Rogue, Fem->Female, etc
                #maxStat2["name"] = dict(role.items() + race.items() + gender.items() + align.items()).get(maxStat2["name"],maxStat2["name"])
                # convert number to % of total (non-scum) games
                maxStat2["number"] = int(round(maxStat2["number"] * 100 / (stats["games"] - stats["scum"])))

                statmsg += "({number}%{name}), ".format(**maxStat2)
        if p != "full":
            statmsg += "{days}d {hours:02d}:{minutes:02d} {prep}".format(**cd)
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

    # Trohy/achievement reporting
    def listStuff(self, theList):
        # make a string from a list, like "this, that, and the other thing"
        listStr = ""
        for (i,n) in enumerate(theList):
            # first item
            if (i == 0):
                listStr = str(n)
            # last item
            elif (i == len(theList)-1):
                if (i > 1): listStr += "," # oxford
                listStr += " and " + str(n)
            # middle items
            else:
                listStr += ", " + str(n)
        return listStr

    def listTrophies(self,trophies):
        tlist = []
        for t in trophies:
            tlist += [config["trophies"][str(t)]["title"]]
        return self.listStuff(tlist)

    def listAchievements(self, achievements, maxCount):
        if len(achievements) > maxCount:
            return str(len(achievements)) + " new achievements"
        alist = []
        for a in achievements:
            alist += [config["achievements"][str(a)]["title"]]
        return self.listStuff(alist)

    def checkScoreboard(self):
        if SLAVE: return
        # this chokes down the whole json file output by the scoreboard system,
        # Makes some comparisons,
        # and reports anything interesting that has changed.
        prevScoreboard = {}
        if self.scoreboard: prevScoreboard = self.scoreboard
        try:
            self.scoreboard = json.load(open(SCOREBOARDJSON))
        except:
            print "Failed to load scoreboard from " + SCOREBOARDJSON
            self.scoreboard = prevScoreboard
            return

        if not prevScoreboard: return
        if "all" not in self.scoreboard["players"]: return # scoreboard is empty at the start
        prevGreatFoo = prevScoreboard["trophies"]["players"].get("greatfoo",{})
        for player in self.scoreboard["players"]["all"]:
            currTrophies = self.scoreboard["players"]["all"][player].get("trophies",[])
            try: prevTrophies = prevScoreboard["players"]["all"][player].get("trophies",[])
            except: prevTrophies = [] # Player won't be in prev, if it's their 1st game
            newTrophies = []
            for t in currTrophies:
                if t not in prevTrophies and t["trophy"] != "noscum": # noscum trophy will be spammy
                    newTrophies += [t["trophy"]]
            if newTrophies:
                self.announce(self.displaytag("trophy") + " "
                              + str(self.scoreboard["players"]["all"][player]["name"])
                              + " now has " + self.listTrophies(newTrophies) + "!")
            currAch = self.scoreboard["players"]["all"][player].get("achievements",[])
            try: prevAch = prevScoreboard["players"]["all"][player].get("achievements",[])
            except: prevAch = []
            newAch = []
            for a in currAch:
                if a not in prevAch:
                    newAch += [a]
            if newAch:
                alist = self.listAchievements(newAch, 4)
                if alist == "Shafted":
                    alist = " just got " + alist
                else:
                    alist = " just earned " + alist
                self.announce(self.displaytag("achieve") + " "
                              + str(self.scoreboard["players"]["all"][player]["name"])
                              + alist + ".", True)

        # report clan ranking changes
        # this assumes clan["n"] is the index to the clan list and it never changes
        for clan in self.scoreboard["clans"]["all"]:
            if len(prevScoreboard["clans"]["all"]) <= int(clan["n"]):
                self.announce(self.displaytag("clan") + " New clan registered - "
                              + str(clan["name"]) + "!")
            elif "rank" in clan and prevScoreboard["clans"]["all"][int(clan["n"])].get("rank",0) > clan["rank"]:
                self.announce(self.displaytag("clan") + " Clan "
                              + str(clan["name"])
                              + " moves to ranking position "
                              + str(clan["rank"]) + "!")


    # implement commands here
    def doPing(self, sender, replyto, msgwords):
        self.respond(replyto, sender, "Pong! " + " ".join(msgwords[1:]))

    def doTime(self, sender, replyto, msgwords):
        timeMsg = time.strftime("%F %H:%M:%S %Z. ")
        timeLeft = self.countDown()
        if timeLeft["countdown"] <= timedelta(0):
            timeMsg += "The " + YEAR + " tournament is OVER!"
            self.respond(replyto, sender, timeMsg)
            retur
        verbs = { "start" : "begins",
                  "end" : "closes"
                }
        timeMsg += YEAR + " Tournament " + verbs[timeLeft["event"]] + " in {days}d {hours:0>2}:{minutes:0>2}:{seconds:0>2}".format(**timeLeft)
        self.respond(replyto, sender, timeMsg)

    def doSource(self, sender, replyto, msgwords):
        self.respond(replyto, sender, self.sourceURL )

    def doScoreboard(self, sender, replyto, msgwords):
        self.respond(replyto, sender, self.scoresURL )

    def doRCedit(self, sender, replyto, msgwords):
        self.respond(replyto, sender, self.rceditURL )

    def doHelp(self, sender, replyto, msgwords):
        self.respond(replyto, sender, self.helpURL )

    def doScore(self, sender, replyto, msgwords):
        if len(msgwords) > 2:
            self.respond(replyto, sender, "!" + msgwords[0]
                         + " - get tournament score and ranking of yourself or another player")
            return
        if len(msgwords) == 2:
            # accommodate the '\' clan tags that players add in irc.
            PLR = msgwords[1].split("\\")[0]
        else:
            PLR = sender
        plr = PLR.lower()
        # case insensitive search
        player = None
        for p in self.scoreboard["players"]["all"].keys():
            if plr == p.lower():
                player = p
                break
        if not player:
            self.respond(replyto, sender, "Can't find player {0} on the scoreboard.".format(PLR))
            return
        score = int(self.scoreboard["players"]["all"][player]["score"])
        rank = int(self.scoreboard["players"]["all"][player]["rank"])
        self.respond(replyto, sender, str(player) + " - Score: {0} - Rank: {1}".format(score, rank))

    def doClanTag(self, sender, replyto, msgwords):
        # msgwords[1] is the desired tag, msgwords[the rest] is the clan name as it appears in the scoreboard
        # case is ignored for searching, but correct case is stored in the table for faster lookup later.
        if len(msgwords) < 3:
            self.respond(replyto, sender, "!" + msgwords[0] + " <tag> <clan name> - assigns a shorthand tag to a clan for use with !clanscore")
            return
        if msgwords[1].lower() in [clan["name"].lower() for clan in self.scoreboard["clans"]["all"]]:
            self.respond(replyto, sender, msgwords[1] + " is already the name of a clan.") # people will be smartarses
            return
        for clan in self.scoreboard["clans"]["all"]:
            if clan["name"].lower() == " ".join(msgwords[2:]).lower():
                self.clanTag[msgwords[1].lower()] = {"n": int(clan["n"]), "name": str(clan["name"])}
                self.respond(replyto, sender, "Clan Tag {0} assigned to {1}".format(msgwords[1],str(clan["name"])))
                with open(CLANTAGJSON, 'w') as f:
                    json.dump(self.clanTag, f)
                return
        self.respond(replyto, sender, "Can't find a clan named {0} on the scoreboard".format(" ".join(msgwords[2:])))

    def doClanScore(self, sender, replyto, msgwords):
        tryClan, name, score, rank = '', '', 0, 0
        # the hard part is working out what clan we need to look up
        if len(msgwords) > 1:
            tryClan = " ".join(msgwords[1:])
        else:
            splitNick = sender.split("\\")
            if len(splitNick) > 1:
                tryClan = splitNick[1]
            else:
                # look up clan of player(sender)
                for clan in self.scoreboard["clans"]["all"]:
                    # fugly case-insensitive search
                    if sender.lower() in " ".join(clan["players"]).lower().split(" "):
                        name, score, rank = [clan[x] for x in ["name","score","rank"]]
                        break
        if not name:
            if not tryClan:
                self.respond(replyto, sender, "Could not get clan membership for " + sender + ".")
                return
            if tryClan.lower() in self.clanTag:
                clan = self.scoreboard["clans"]["all"][self.clanTag[tryClan.lower()]["n"]]
                name, score, rank = [clan[x] for x in ["name","score","rank"]]
            else:
                for clan in self.scoreboard["clans"]["all"]:
                    if clan["name"].lower() == tryClan.lower():
                        name, score, rank = [clan[x] for x in ["name","score","rank"]]
        if name:
            self.respond(replyto, sender, str(name) + " - Score: {0} - Rank: {1}".format(int(score),int(rank)))
        else:
            self.respond(replyto, sender, "Can't find clan {0}".format(tryClan))

    def doCommands(self, sender, replyto, msgwords):
        self.respond(replyto, sender, "available commands are !help !ping !time !tell !source !lastgame !lastasc !asc !streak !rcedit !scores !sb !score !clanscore !clantag !whereis !players !who !commands" )

    def takeMessage(self, sender, replyto, msgwords):
        if len(msgwords) < 3:
            self.respond(replyto, sender, "!tell <recipient> <message> (leave a message for someone)")
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
        self.msgLog(replyto,random.choice(willDo).format(sender,rcpt))

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
            fromstr = ""
            for (i,n) in enumerate(nicksfrom):
                # first item
                if (i == 0):
                    fromstr = n
                # last item
                elif (i == len(nicksfrom)-1):
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
        # [elsewhere]
        # record query responses, and call callback when all received (or timeout)
        # This all becomes easier if we just treat ourself (master) as one of the slaves
        q = self.newQueryId()
        self.queries[q] = {}
        self.queries[q]["callback"] = callback
        self.queries[q]["replyto"] = replyto
        self.queries[q]["sender"] = sender
        self.queries[q]["resp"] = {}
        self.queries[q]["finished"] = {}
        message = "#Q# " + " ".join([q,sender] + msgwords)

        for sl in self.slaves.keys():
            if TEST: print "forwardQuery: " + sl + " " + message
            self.msg(sl,message)

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
        plrvar = ""
        for var in self.inprog.keys():
            for inpdir in self.inprog[var]:
                for inpfile in glob.iglob(inpdir + "*.ttyrec"):
                    # /stuff/crap/PLAYER:shit:garbage.ttyrec
                    # we want AFTER last '/', BEFORE 1st ':'
                    plrvar += inpfile.split("/")[-1].split(":")[0] + " "
        if len(plrvar) == 0:
            plrvar = "No current players"
        response = "#R# " + query + " " + self.displaytag(SERVERTAG) + " " + plrvar
        self.msg(master, response)

    # !players callback. Actually print the output.
    def outPlayers(self,q):
        outmsg = " | ".join(q["resp"].values())
        self.respond(q["replyto"],q["sender"],outmsg)

    def usageWhereIs(self, sender, replyto, msgwords):
        if (len(msgwords) != 2):
            self.respond(replyto, sender, "!" + msgwords[0] + " <player> - finds a player in the dungeon." + replytag)
            return False
        return True

    def getWhereIs(self, master, sender, query, msgwords):
        ammy = ["", " (with Amulet)"]
        # look for inrpogress file first, only report active games
        for var in self.inprog.keys():
            for inpdir in self.inprog[var]:
                for inpfile in glob.iglob(inpdir + "*.ttyrec"):
                    plr = inpfile.split("/")[-1].split(":")[0]
                    if plr.lower() == msgwords[1].lower():
                        for widir in self.whereis[var]:
                            for wipath in glob.iglob(widir + "*.whereis"):
                                if wipath.split("/")[-1].lower() == (msgwords[1] + ".whereis").lower():
                                    plr = wipath.split("/")[-1].split(".")[0] # Correct case
                                    wirec = parse_xlogfile_line(open(wipath, "r").read().strip(),":")

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
        for role in config["nethack"]["roles"]:
             role = role.title() # capitalise the first letter
             if role in self.asc[plr]:
                totasc += self.asc[plr][role]
                stats += " " + str(self.asc[plr][role]) + "x" + role
        stats += ", "
        for race in config["nethack"]["races"]:
            race = race.title()
            if race in self.asc[plr]:
                stats += " " + str(self.asc[plr][race]) + "x" + race
        stats += ", "
        for alig in config["nethack"]["aligns"]:
            if alig in self.asc[plr]:
                stats += " " + str(self.asc[plr][alig]) + "x" + alig
        stats += ", "
        for gend in config["nethack"]["genders"]:
            if gend in self.asc[plr]:
                stats += " " + str(self.asc[plr][gend]) + "x" + gend
        stats += "."
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
        reply = "#R# " + query + " "
        (lstart,lend,llength) = self.longstreak.get(plr,(0,0,0))
        (cstart,cend,clength) = self.curstreak.get(plr,(0,0,0))
        if llength == 0:
            reply += "No streaks for " + PLR + "."
            self.msg(master,reply)
            return
        reply += self.displaytag(SERVERTAG) + " " + PLR
        reply += " Max: " + str(llength) + " (" + self.streakDate(lstart) \
                          + " - " + self.streakDate(lend) + ")"
        if clength > 0:
            if cstart == lstart:
                reply += "(current)"
            else:
                reply += ". Current: " + str(clength) + " (since " \
                                       + self.streakDate(cstart) + ")"
        reply += "."
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
        sender = sender.partition("!")[0]
        if SLAVE and sender not in MASTERS: return
        if (dest in CHANNELS): #public message
            self.log(dest, "<"+sender+"> " + message)
            replyto = dest
            if (sender == DCBRIDGE and message[0] == '<'):
                msgparts = message[1:].split('> ')
                sender = msgparts[0]
                message = "> ".join(msgparts[1:]) # in case there's more "> " in the message
        else: #private msg
            replyto = sender
        # Message checks next.
        self.checkMessages(sender, dest)
        # ignore other channel noise unless !command
        if (message[0] != '!'):
            if (dest in CHANNELS): return
        else: # pop the '!'
            message = message[1:]
        msgwords = message.strip().split(" ")
        if re.match(r'^\d*d\d*$', msgwords[0]):
            self.rollDice(sender, replyto, msgwords)
            return
        if self.commands.get(msgwords[0].lower(), False):
            self.commands[msgwords[0].lower()](sender, replyto, msgwords)
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
        dumpurl = "(sorry, no dump exists for {name})".format(**game)
        if TEST or os.path.exists(dumpfile): # dump files may not exist on test system
            # quote only the game-specific part, not the prefix.
            # Otherwise it quotes the : in https://
            # assume the rest of the url prefix is safe.
            dumpurl = urllib.quote(game["dumpfmt"].format(**game))
            dumpurl = self.dump_url_prefix.format(**game) + dumpurl
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

        yield (END + ": {name} ({role}-{race}-{gender}-{align}), "
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
        for call in self.looping_calls.itervalues():
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

if __name__ == "__builtin__":
    f = protocol.ReconnectingClientFactory()
    f.protocol = DeathBotProtocol
    application = service.Application("DeathBot")
    deathservice = internet.SSLClient(HOST, PORT, f,
                                      ssl.ClientContextFactory())
    deathservice.setServiceParent(application)
