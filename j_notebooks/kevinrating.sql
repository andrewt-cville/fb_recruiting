---- Five Stars -----
 CREATE VIEW "KevinFiveStars" AS
select
a.*,
5 as 'KevinRating'
from X_LinkedPLayers as a
left join Positions as b
	on a."Position" = b."Position"
where
(b.StandardizedPosition not in ('P', 'K')
and a.AllAmerican_Best in (0,1)) 
OR (NFLDraftRound = 1)
ORDER BY Year;

------Four Stars--------
CREATE VIEW "KevinFourStars" AS
select
a.*,
4 as 'KevinRating'
from X_LinkedPLayers as a
left join Positions as b
	on a."Position" = b."Position"

where
(b.StandardizedPosition not in ('P', 'K')
and a.AllConferenceTeam_Best in (1,2)
and a.conference in ('acc', 'bigten', 'bigtwelve', 'sec', 'pactwelve'))
OR
(b.StandardizedPosition not in ('P', 'K')
and NFLDraftRound > 1)
OR
(b.StandardizedPosition in ('P', 'K')
and (AllConferenceTeam_Best in (1) OR AllAmerican_Best in (0,1)))

and a.ID not in (select ID from KevinFiveStars)

ORDER BY Year;
---------Three Stars-------------
CREATE VIEW "KevinThreeStars" AS
select
a.*,
3 as 'KevinRating'
from X_LinkedPLayers as a
left join Positions as b
	on a."Position" = b."Position"

where
a.NCAAGamesPlayed >= 25
and a.ID not in (select ID from KevinFiveStars)
and a.ID not in (select ID from KevinFourStars)
ORDER BY Year;
------------Two Stars--------------
CREATE VIEW "KevinTwoStars" AS
select
a.*,
2 as 'KevinRating'
from X_LinkedPLayers as a
left join Positions as b
	on a."Position" = b."Position"

where
a.NCAAGamesPlayed >= 6 and a.NCAAGamesPlayed <= 24 and a.NCAAGamesStarted < 10

ORDER BY Year;
-------------One Star-----------
CREATE VIEW "KevinOneStar" AS
select
a.*,
1 as 'KevinRating'
from X_LinkedPLayers as a
left join Positions as b
	on a."Position" = b."Position"

where
(a.NCAAGamesPlayed <= 5 OR a.NCAAGamesPlayed is null)

ORDER BY Year;