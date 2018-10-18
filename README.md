# tnntbot
IRC/Twitter Announce Bot for TNNT hosted on hardfought.org, based on hardfought's main irc bot "Beholder", with some functions pulled from the "NotTheOracle" bot we used for the 2017 /dev/null/tribute tournament https://github.com/NHTangles/NotTheOracle
Can run distributed master/slave network to report and aggregate stats from multiple servers (run an instance on each server and configure as described below)

Edit botconf.py with local settings (see comments in botconf.py.example)

Run with twistd, as follows:
 twistd -y tnntbot.py

Commands:

!ping

!time (gives remaining time to start/end of tournament)

!stats (basic stats of games played in the current day)

!tell (leave a message for another irc user)
