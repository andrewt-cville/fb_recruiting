CREATE VIEW "LinkedPlayers_NCAA" AS
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
		MIN(a.AllConferenceTeam) as AllConferenceTeam_Best,
		i.NCAAGamesPlayed,
		i.NCAAGamesStarted,
		k.AllAmericanBest
		
	FROM 
		'SummarizedNCAAData-v2' as i
		
		left join SourcedPlayers as a
			ON a.ID = i.ID and a.KeyDataSet = 5
		
		--All American
		LEFT JOIN RecordLinks as j
			ON i.ID = j.TargetID and j.KeyDataSet = 6 and j.TargetKeyDataSet = 5
		LEFT JOIN SourcedPlayers as k
			ON j.MasterID = k.ID and k.KeyDataSet = 6 and k.Year >= 2004
		
		LEFT JOIN Positions as l
			ON a.Position = l.Position
		LEFT JOIN Positions as m
			ON a.Position = l.Position
		LEFT JOIN Schools as n
			ON a.college = n.wikipedia

	WHERE
		i.ID not in (select masterID from RecordLinks where KeyDataSet = 5)
	GROUP BY
		a.ID;