CREATE VIEW "LinkedPlayers_Rivals" AS
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
		e.NFLDraftRound,
		e.NFLDraftPick,
		e.NFLAllProFirstTeam,
		e.NFLProBowl,
		e.NFLYearsAsStarter,
		e.NFLGamesPlayed,
		MIN(g.AllConferenceTeam) as AllConferenceTeam_Best,
		i.NCAAGamesPlayed,
		i.NCAAGamesStarted,
		k.AllAmericanBest
		
	FROM 
		SourcedPlayers as a

		--NFL Draft		
		LEFT JOIN RecordLinks as d
			ON a.IDYR = d.TargetID and d.KeyDataSet = 3 and d.TargetKeyDataSet = 2
		LEFT JOIN SourcedPlayers as e
			ON d.MasterID = e.ID and e.KeyDataSet = 3 and e.Year >= 2005
		
		--All Conference
		LEFT JOIN RecordLinks as f
			ON a.IDYR = f.TargetID and f.KeyDataSet = 4 and f.TargetKeyDataSet = 2
		LEFT JOIN SourcedPlayers as g
			ON f.MasterID = g.ID and g.KeyDataSet = 4 and g.year >= 2004
		
		--NCAA
		LEFT JOIN RecordLinks as h
			ON a.IDYR = h.TargetID and h.KeyDataSet = 5 and h.TargetKeyDataSet = 2
		LEFT JOIN 'SummarizedNCAAData-v2' as i
			ON h.MasterID = i.ID
		
		--All American
		LEFT JOIN RecordLinks as j
			ON a.IDYR = j.TargetID and j.KeyDataSet = 6 and j.TargetKeyDataSet = 2
		LEFT JOIN SourcedPlayers as k
			ON j.MasterID = k.ID and k.KeyDataSet = 6 and k.Year >= 2004
		
		LEFT JOIN Positions as l
			ON a.Position = l.Position
		LEFT JOIN Positions as m
			ON a.Position = l.Position
		LEFT JOIN Schools as n
			ON a.college = n.rivals

	WHERE
		a.KeyDataSet = 2
		and a.IDYR not in (select masterID from RecordLinks where KeyDataSet = 2)
		and a.Year <= 2016
	GROUP BY
		a.ID;