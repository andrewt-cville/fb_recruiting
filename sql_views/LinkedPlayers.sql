CREATE VIEW "LinkedPlayers" AS
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
		coalesce(c.StarsRivals, 1) as 'StarsRivals',
		c.NationalRankRivals,
		c.PositionRankRivals,
		c.StateRankRivals,
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

		--Rivals
		LEFT JOIN RecordLinks as b
			ON a.IDYR = b.MasterID
		LEFT JOIN SourcedPlayers as c
			ON b.TargetID = c.IDYR 
				and (b.KeyDataSet = 2 and b.TargetKeyDataSet = 1) and c.KeyDataSet = 2
		
		--NFL
		LEFT JOIN RecordLinks as d
			ON a.IDYR = d.TargetID and d.KeyDataSet = 3 and d.TargetKeyDataSet = 1
		LEFT JOIN SourcedPlayers as e
			ON d.MasterID = e.ID and e.KeyDataSet = 3 and e.Year >= 2005
	
		--All Conference	
		LEFT JOIN RecordLinks as f
			ON a.IDYR = f.TargetID and f.KeyDataSet = 4 and d.TargetKeyDataSet = 1
		LEFT JOIN SourcedPlayers as g
			ON f.MasterID = g.ID and g.KeyDataSet = 4 and g.Year >= 2004
		
		--NCAA
		LEFT JOIN RecordLinks as h
			ON a.IDYR = h.TargetID and h.KeyDataSet = 5 and h.TargetKeyDataSet = 1
		LEFT JOIN 'SummarizedNCAAData-v2' as i
			ON h.MasterID = i.ID
		
		--All American
		LEFT JOIN RecordLinks as j
			ON a.IDYR = j.TargetID and j.KeyDataSet = 6 and d.TargetKeyDataSet = 1
		LEFT JOIN SourcedPlayers as k
			ON j.MasterID = k.ID and k.KeyDataSet = 6 and k.Year >= 2004
		
		LEFT JOIN Positions as l
			ON a.Position = l.Position
		LEFT JOIN Positions as m
			ON c.Position = l.Position
		LEFT JOIN Schools as n
			ON a.college = n.id

	WHERE
		a.KeyDataSet = 1
		and a.Year <= 2016
	GROUP BY
		a.ID;