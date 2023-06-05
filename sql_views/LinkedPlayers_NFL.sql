CREATE VIEW "LinkedPlayers_NFL" AS
SELECT DISTINCT
		a.ID,
		a.IDYR,
		a.College,
		n.conference,
		a.Year,
		a.PlayerName,
		a.HighSchool,
		a.City,
		a.State,
		coalesce(l.StandardizedPosition, m.StandardizedPosition, a.Position) as Position,
		a.Height,
		a.Weight,
		a.CompositeRating,
		coalesce(a.CompositeStars, 1) as 'CompositeStars',
		a.CompositeNationalRank,
		a.CompositePositionRank,
		a.CompositeStateRank,
		a.Rating247,
		coalesce(a.Stars247, 1) as 'Stars247',
		a.NationalRank247,
		a.PositionRank247,
		a.StateRank247,
		coalesce(a.StarsRivals, 1) as 'StarsRivals',
		a.NationalRankRivals,
		a.PositionRankRivals,
		a.StateRankRivals,
		a.NFLDraftRound,
		a.NFLDraftPick,
		a.NFLAllProFirstTeam,
		a.NFLProBowl,
		a.NFLYearsAsStarter,
		a.NFLGamesPlayed,
		MIN(g.AllConferenceTeam) as AllConferenceTeam_Best,
		i.NCAAGamesPlayed,
		i.NCAAGamesStarted,
		k.AllAmericanBest
		
	FROM 
		SourcedPlayers as a
		
		--All Conference
		LEFT JOIN RecordLinks as f
			ON a.ID = f.TargetID and f.KeyDataSet = 4 and f.TargetKeyDataSet = 3
		LEFT JOIN SourcedPlayers as g
			ON f.MasterID = g.ID and g.KeyDataSet = 4 and g.Year >= 2004
		
		--NCAA
		LEFT JOIN RecordLinks as h
			ON a.ID = h.TargetID and h.KeyDataSet = 5 and h.TargetKeyDataSet = 3
		LEFT JOIN 'SummarizedNCAAData-v2' as i
			ON h.MasterID = i.ID
		
		--All American
		LEFT JOIN RecordLinks as j
			ON a.ID = j.TargetID and j.KeyDataSet = 6 and j.TargetKeyDataSet = 3
		LEFT JOIN SourcedPlayers as k
			ON j.MasterID = k.ID and k.KeyDataSet = 6 and k.Year >= 2004
		
		LEFT JOIN Positions as l
			ON a.Position = l.Position
		LEFT JOIN Positions as m
			ON a.Position = l.Position
		LEFT JOIN Schools as n
			ON a.college = n.'nfl-ref'

	WHERE
		a.KeyDataSet = 3
		and a.ID not in (select masterID from RecordLinks where KeyDataSet = 3 and TargetKeyDataSet in (1,2))
		and a.year > 2005
	GROUP BY
		a.ID;