CREATE VIEW "SummaryReport" AS
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
	END as 'successComposite'
FROM
	HornbeakRatings
GROUP BY PID;