CREATE VIEW "SummarizedNCAAData-v2" AS
SELECT 
		a.ID,
		a.College,
		min(a.Year) as Year,
		a.PlayerName,
		a.Position,
		sum(a.NCAAGamesPlayed) as NCAAGamesPlayed,
		sum(a.NCAAGamesStarted) as NCAAGamesStarted
		
	FROM 
		SourcedPlayers as a

	WHERE
		a.KeyDataSet = 5
		and a.PlayerName <> ''

GROUP BY a.ID;