CREATE VIEW "LinkedPlayers_AA" AS
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
		a.NCAAGamesPlayed,
		a.NCAAGamesStarted,
		a.AllAmericanBest
		
	FROM 
		SourcedPlayers as a
		
		LEFT JOIN Positions as l
			ON a.Position = l.Position
		LEFT JOIN Positions as m
			ON a.Position = l.Position
		LEFT JOIN Schools as n
			ON a.college = n.wikipedia

	WHERE
		a.KeyDataSet = 6 
		and a.ID not in (select masterID from RecordLinks where KeyDataSet = 6)
		and a.Year >= 2004
	GROUP BY
		a.ID;