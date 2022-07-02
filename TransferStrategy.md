Transfer Logic

Problem to solve: I need to connect a 247Sports (Master) record with AA, AllConf, NFL and NCAA data where college and year don't match.

Things to try:
[DONE AND UPDATED] - Review standardized positions 
[DONE AND UPDATED] - Create source dataframe: 247Sports with playername, year, position, positiongroup
[DONE AND UPDATE] - then block on standardized position group
- Block on master year < target year and target year within 5 of master year
- - Can't seem to block so instead I'm trying to score based on the field difference - which is causing long compare times.  I've tried to 
- Compare PlayerName using Winkler or Levenstein
- Review data