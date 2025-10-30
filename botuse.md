# TNNT IRC Bot - Command Reference

This is the TNNT IRC bot. Code is based on Beholder of #hardfought.

Currently the bot's name is **Croesus**, and it lives in the channel **#tnnt** on **libera**.

The bot reports on games being played in the tournament, including ascensions (wins), deaths (losses), and other significant game events. It also announces tournament scoreboard events by polling the TNNT API every 5 minutes for achievements, trophies, and clan ranking changes.

All commands use the `$` prefix (e.g., `$ping`, `$score`, `$whereis`).

---

## Utility Commands

| Command | Description |
|---------|-------------|
| `$ping` | Check if bot is alive. |
| `$time` | Display current time on server and time remaining in or until tournament. |
| `$tell <nick> <message>` | Forward a message when the recipient becomes active. |
| `$source` | Link to bot source code on GitHub. |
| `$help` | Link to this command reference. |
| `$commands` | List all available bot commands. |

---

## Game Server Commands

| Command | Description |
|---------|-------------|
| `$lastgame [player]` | Display link to dumplog of last game ended. |
| `$lastasc [player]` | Display link to dumplog for last ascended game. |
| `$asc [player]` | Show ascension stats for a player. |
| `$streak [player]` | Show ascension streak stats for a player. |
| `$whereis <player>` | Give info about a player's current game. |
| `$who` / `$players` | List players currently playing. |
| `$stats` | Display tournament statistics. |

---

## Tournament Scoreboard Commands

### `$score [player]`
Show tournament scoreboard position.
- **Without args**: displays top 5 players.
- **With player name**: shows rank, wins, games, ratio, and clan.

### `$clanscore [clan]`
Show clan scoreboard position.
- **Without args**: displays top 5 clans.
- **With clan name**: shows rank, wins, games, and ratio.

### `$clantag <tag> <clan>`
**[DEPRECATED]** Clan tags no longer supported.

---

## Resource Links

| Command | Description |
|---------|-------------|
| `$scores` / `$sb` | Links to leaderboards and trophies pages. |
| `$ttyrec` | Link to ttyrecs directory. |
| `$dumplog` | Link to dumplogs directory. |
| `$irclog` | Link to IRC logs. |
| `$rcedit` | Link to RC file editor. |

---

## Admin Commands

| Command | Description |
|---------|-------------|
| `$status` | Display bot health and monitoring statistics (admin-only). |
