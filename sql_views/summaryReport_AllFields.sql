CREATE VIEW 'SummaryReport_AllFields' AS
select
	coalesce(IDYR, ID) as PID,
	max(HornbeakRating) as 'HornbeakRating',
	CASE
		WHEN (IDYR is not null) THEN 'Recruiting Service'
		ELSE 'Other'
	END as 'Source',
	CASE
		WHEN (NFLDraftPick is not null) THEN 1
		ELSE 0
	END as 'NFLDrafted',
	Stars247,
	(Stars247 - HornbeakRating) as 'delta247',
	CASE
		WHEN (Stars247 - HornbeakRating) >= 0 THEN 1
		ELSE 0
	END as 'success247',
	StarsRivals,
	(StarsRivals - HornbeakRating) as 'deltaRivals',
	CASE
		WHEN (StarsRivals - HornbeakRating) >= 0 THEN 1
		ELSE 0
	END as 'successRivals',
	CompositeStars,
	(CompositeStars - HornbeakRating) as 'deltaComposite',
	CASE
		WHEN (CompositeStars - HornbeakRating) >= 0 THEN 1
		ELSE 0
	END as 'successComposite',
	College,
  	conference,
  	Year,
  	PlayerName,
  	HighSchool,
  	City,
  	State,
  	Position,
  	Height,
  	Weight,
  	CompositeRating,
  	CompositeStars,
  	CompositeNationalRank,
  	CompositePositionRank,
  	CompositeStateRank,
  	Rating247,
  	Stars247,
  	NationalRank247,
  	PositionRank247,
  	StateRank247,
  	StarsRivals,
  	NationalRankRivals,
  	PositionRankRivals,
  	StateRankRivals,
  	NFLDraftRound,
  	NFLDraftPick,
  	NFLAllProFirstTeam,
  	NFLProBowl,
  	NFLYearsAsStarter,
  	NFLGamesPlayed,
  	AllConferenceTeam_Best,
  	NCAAGamesPlayed,
  	NCAAGamesStarted,
	AllAmericanBest
FROM
	HornbeakRatings
GROUP BY PID;