from sql_templates_base import *


# Returns either ranked or unranked 247 rows
# Implemented to limit what was fuzzy matching
def get_query_all247(limit, ranked):
    columns = ['a.IDYR', 'a.ID', 'a.PlayerName', 'a.Year', 'a.College', 'a.HighSchool', 'a.City', 'a.State', 'b.KeyPositionGroup', 'b.StandardizedPosition', 'a.Height', 'a.Weight']

    params = {
        'Select': '\n  , '.join(columns),
        'Ranked': ranked,
        'Limit': limit
    }

    template  = '''
        SELECT
                {{ Select | sqlsafe}}
        FROM
            SourcedPlayers as a
            INNER JOIN Positions as b
                ON a.Position = b.Position
        WHERE
            a.KeyDataSet = 1
            and a.Year > 2013
            {% if Limit %}
                {% if Ranked %}
                    and a.Rating247 <> 0
                {% else %}
                    and a.Rating247 is null
                {% endif %}
            {% endif %}
    '''

    return apply_sql_template(template, params)

def get_query_Unlinked247():
    columns = ['a.IDYR', 'a.ID', 'a.PlayerName', 'a.Year', 'a.College', 'a.HighSchool', 'a.City', 'a.State', 'b.KeyPositionGroup', 'b.StandardizedPosition', 'a.Height', 'a.Weight']

    params = {
        'Select': '\n  , '.join(columns)
    }

    template  = '''
        SELECT
                {{ Select | sqlsafe}}
        FROM
            SourcedPlayers as a
            INNER JOIN Positions as b
                ON a.Position = b.Position
        WHERE
            a.KeyDataSet = 1
            and a.IDYR not in (select TargetID from RecordLinks where KeyDataSet = 1)
    '''

    return apply_sql_template(template,params)

def get_query_UnlinkedRivals():
    columns = ['a.IDYR', 'a.ID', 'a.PlayerName', 'a.Year', 'a.College', 'a.HighSchool', 'a.City', 'a.State', 'b.KeyPositionGroup', 'b.StandardizedPosition', 'a.Height', 'a.Weight']

    params = {
        'Select': '\n  , '.join(columns)
    }

    template  = '''
        SELECT
                {{ Select | sqlsafe}}
        FROM
            SourcedPlayers as a
            INNER JOIN Positions as b
                ON a.Position = b.Position
        WHERE
            a.KeyDataSet = 2
            and a.IDYR not in (select MasterID from RecordLinks where KeyDataSet = 2)
            and a.IDYR not in (select TargetID from RecordLinks where TargetKeyDataSet = 2)
    '''

    return apply_sql_template(template,params)

def get_query_UnlinkedNFL(year):
    columns = ['a.ID', 'a.PlayerName', 'a.Year', 'a.College', 'b.KeyPositionGroup', 'b.StandardizedPosition']

    params = {
        'Select': '\n  , '.join(columns),
        'Year': year
    }

    template  = '''
        SELECT
                {{ Select | sqlsafe}}
        FROM
            SourcedPlayers as a
            INNER JOIN Positions as b
                ON a.Position = b.Position
        WHERE
            a.KeyDataSet = 3
            and a.ID not in (select MasterID from RecordLinks where KeyDataSet = 3)
            and a.ID not in (select TargetID from RecordLinks where TargetKeyDataSet = 3)
            and a.Year >= {{ Year | sqlsafe }}
    '''

    return apply_sql_template(template, params)

def get_query_UnlinkedAllConference(limit, year):
    #columns = ['a.ID', 'a.PlayerName', 'a.AllConferenceTeam', 'a.Year', 'a.College', 'b.KeyPositionGroup', 'b.StandardizedPosition']
    columns = ['a.ID', 'a.PlayerName', 'a.Year', 'a.College', 'b.KeyPositionGroup', 'b.StandardizedPosition']
    params = {
        'Select': '\n  , '.join(columns),
        'Limit': limit,
        'Year': year
    }

    template  = '''
        SELECT
                {{ Select | sqlsafe}}
        FROM
            SourcedPlayers as a
            INNER JOIN Positions as b
                ON a.Position = b.Position
        WHERE
            a.KeyDataSet = 4
            and a.ID not in (select MasterID from RecordLinks where KeyDataSet = 4)
            and a.ID not in (select TargetID from RecordLinks where TargetKeyDataSet = 4)
            {% if Limit %}
                and a.Year >= {{ Year | sqlsafe}}
            {% endif %}
    '''

    return apply_sql_template(template, params)

def get_query_UnlinkedNCAA(fuzzy):
    columns = ['a.ID', 'a.PlayerName',  'min(a.Year) as Year', 'a.College', 'b.KeyPositionGroup', 'b.StandardizedPosition']

    params = {
        'Select': '\n  , '.join(columns),
        'Fuzzy': fuzzy
    }

    template  = '''
        SELECT
                {{ Select | sqlsafe}}
        FROM
            SourcedPlayers as a
            INNER JOIN Positions as b
                ON a.Position = b.Position
        WHERE
            a.KeyDataSet = 5
            and a.PlayerName is not ''
            and a.ID not in (select MasterID from RecordLinks where KeyDataSet = 5)
            and a.ID not in (select TargetID from RecordLinks where TargetKeyDataSet = 5)
            {% if Fuzzy %}
                and a.NCAAGamesPlayed <> 0
            {% endif %}
            and a.Year = 2021
        GROUP BY 
            a.ID;
    '''

    return apply_sql_template(template, params)

def get_query_UnlinkedAllAmerican():
    columns = ['a.ID', 'a.PlayerName',  'min(a.Year) as Year', 'a.College']

    params = {
        'Select': '\n  , '.join(columns)
    }

    template  = '''
        SELECT
            {{ Select | sqlsafe}}
        FROM
            SourcedPlayers as a
        WHERE
            a.KeyDataSet = 6
            and a.ID not in (select MasterID from RecordLinks where KeyDataSet = 6)
        GROUP BY 
            a.ID;
    '''

    return apply_sql_template(template, params)

def get_query_KevinRating(ranking):
    columns = ['a.ID', 'a.PlayerName',  'min(a.Year) as Year', 'a.College']

    params = {
        'Ranking': ranking
    }

    template  = '''
        SELECT
            a.*,
            {{ Ranking }} as "HornbeakRating"
        FROM
            X_LinkedPLayers as a
                left join Positions as b
	                on a."Position" = b."Position"
        WHERE
            {% if Ranking == 5 %}
                (b.StandardizedPosition not in ('P', 'K')
                and a.AllAmericanBest = 1) 
                OR (NFLDraftRound = 1)
            {% elif Ranking == 4 %}
                (b.StandardizedPosition not in ('P', 'K')
                and a.AllConferenceTeam_Best in (1,2)
                and a.conference in ('acc', 'bigten', 'bigtwelve', 'sec', 'pactwelve'))
                OR
                (b.StandardizedPosition not in ('P', 'K')
                and NFLDraftRound > 1)
                OR
                (b.StandardizedPosition in ('P', 'K')
                and (AllConferenceTeam_Best in (1) OR AllAmericanBest=1))
            {% elif Ranking == 3 %}
                a.NCAAGamesPlayed >= 25
            {% elif Ranking == 2 %}
                a.NCAAGamesPlayed >= 6 
                and a.NCAAGamesPlayed <= 24 
                and a.NCAAGamesStarted < 10
            {% elif Ranking == 1 %}
                (a.NCAAGamesPlayed <= 5 
                OR a.NCAAGamesPlayed is null)
            {% endif %}
        ORDER BY 
            a.Year;
    '''

    return apply_sql_template(template, params)

def get_query_Schools():
    params = {}

    template = '''
        select * from Schools
    '''
    return (apply_sql_template(template, params))

def insert_query_RecordLinks():
    
    params = {}

    template = '''
        INSERT INTO RecordLinks_DevB(MasterID, TargetID, KeyDataSet, TargetKeyDataSet, KeyLinkType, LinkConfidence, Transfer,UpdDate)
            VALUES (?,?,?,?,?,?,?,?)
            '''  
    
    return(apply_sql_template(template, params))

def update_query_Schools(id, wiki):
    
    params = {
        'ID': id,
        'Wikipedia': wiki
    }

    template = '''
        UPDATE Schools SET wikipedia = {{ Wikipedia }} WHERE id = {{ ID }}
            '''  
    
    return(apply_sql_template(template, params))

## Sankey Queries ##
def getCountbyStars(college, maxYear, drafted):
    params = {
        'college' : college,
        'maxYear' : maxYear,
        'drafted' : drafted
    }

    template = '''
        SELECT 
            CompositeStars,
            Count(*) as PlayerCount
        FROM
            SummaryReport_AllFields
        WHERE
            College = {{ college }}
            and year <= {{ maxYear }}
            and NFLDrafted = {{ drafted }}
        GROUP BY
            CompositeStars
            '''
    
    return(apply_sql_template(template, params))

def getNFLGamesbyDraftStatus(college, maxYear, drafted):
    params = {
        'college' : college,
        'maxYear' : maxYear,
        'drafted' : drafted
    }

    template = '''
        SELECT 
            Count(*) as PlayerCount
        FROM
            SummaryReport_AllFields
        WHERE
            College = {{ college }}
            and year <= {{ maxYear }}
            and NFLDrafted = {{ drafted }}
            and NFLGamesPlayed > 0
            '''
    
    return(apply_sql_template(template, params))

def insert_query_GameData():
    
    params = {}

    template = '''
        INSERT INTO GameData(SeasonYear, GameDate, HomeScore, AwayScore, AlternateSite, BowlGame, ID, HomeSchool, AwaySchool, UpdDate)
            VALUES (?,?,?,?,?,?,?,?,?,?)
            '''  
    
    return(apply_sql_template(template, params))

def insert_query_SeasonData():
    
    params = {}

    template = '''
        INSERT INTO SeasonData(ID, SeasonYear, School, Win, Loss, Tie, UpdDate)
            VALUES (?,?,?,?,?,?,?)
            '''  
    
    return(apply_sql_template(template, params))

def getHornbeakByYearBySchool(college, year):
    params = {
        'college' : college,
        'year' : year
    }

    template = '''
                select 
                    HornbeakRating, count(*), HornbeakRating * count(*) as Sum
                from SourcedPlayers as sp
                    JOIN HornbeakRatings as hr
                        ON sp.ID = hr.ID
                where 
                    sp.KeyDataSet = 5 
                    and sp.College = {{ college }} 
                    and sp.Year = {{ year }} 
                group by HornbeakRating
            '''
    
    return(apply_sql_template(template, params))

def getLinks247(recruitMaxYear, nflMinYear, acMinYear, aaMinYear):   
    params = {
        'recruitMaxYear' : recruitMaxYear,
        'nflMinYear' : nflMinYear,
        'acMinYear' : acMinYear,
        'aaMinYear' : aaMinYear
    }

    template = '''
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
                ON d.MasterID = e.ID and e.KeyDataSet = 3 and e.Year >= {{ nflMinYear }}
        
            --All Conference	
            LEFT JOIN RecordLinks as f
                ON a.IDYR = f.TargetID and f.KeyDataSet = 4 and d.TargetKeyDataSet = 1
            LEFT JOIN SourcedPlayers as g
                ON f.MasterID = g.ID and g.KeyDataSet = 4 and g.Year >= {{ acMinYear }}
            
            --NCAA
            LEFT JOIN RecordLinks as h
                ON a.IDYR = h.TargetID and h.KeyDataSet = 5 and h.TargetKeyDataSet = 1
            LEFT JOIN "NCAA-Summarized" as i
                ON h.MasterID = i.ID
            
            --All American
            LEFT JOIN RecordLinks as j
                ON a.IDYR = j.TargetID and j.KeyDataSet = 6 and d.TargetKeyDataSet = 1
            LEFT JOIN SourcedPlayers as k
                ON j.MasterID = k.ID and k.KeyDataSet = 6 and k.Year >= {{ aaMinYear }}
            
            LEFT JOIN Positions as l
                ON a.Position = l.Position
            LEFT JOIN Positions as m
                ON c.Position = l.Position
            LEFT JOIN Schools as n
                ON a.college = n.id

        WHERE
            a.KeyDataSet = 1
            and a.Year <= {{ recruitMaxYear }}
        GROUP BY
            a.ID;
    '''

    return(apply_sql_template(template, params))

def getLinksRivals(recruitMaxYear, nflMinYear, acMinYear, aaMinYear):   
    params = {
        'recruitMaxYear' : recruitMaxYear,
        'nflMinYear' : nflMinYear,
        'acMinYear' : acMinYear,
        'aaMinYear' : aaMinYear
    }

    template = '''
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
                    ON d.MasterID = e.ID and e.KeyDataSet = 3 and e.Year >= {{ nflMinYear }}
                
                --All Conference
                LEFT JOIN RecordLinks as f
                    ON a.IDYR = f.TargetID and f.KeyDataSet = 4 and f.TargetKeyDataSet = 2
                LEFT JOIN SourcedPlayers as g
                    ON f.MasterID = g.ID and g.KeyDataSet = 4 and g.year >= {{ acMinYear }}
                
                --NCAA
                LEFT JOIN RecordLinks as h
                    ON a.IDYR = h.TargetID and h.KeyDataSet = 5 and h.TargetKeyDataSet = 2
                LEFT JOIN "NCAA-Summarized" as i
                    ON h.MasterID = i.ID
                
                --All American
                LEFT JOIN RecordLinks as j
                    ON a.IDYR = j.TargetID and j.KeyDataSet = 6 and j.TargetKeyDataSet = 2
                LEFT JOIN SourcedPlayers as k
                    ON j.MasterID = k.ID and k.KeyDataSet = 6 and k.Year >= {{ aaMinYear }}
                
                LEFT JOIN Positions as l
                    ON a.Position = l.Position
                LEFT JOIN Positions as m
                    ON a.Position = l.Position
                LEFT JOIN Schools as n
                    ON a.college = n.rivals

            WHERE
                a.KeyDataSet = 2
                and a.IDYR not in (select masterID from RecordLinks where KeyDataSet = 2)
                and a.Year <= {{ recruitMaxYear }}
            GROUP BY
                a.ID;
    '''
    return(apply_sql_template(template, params))

def getLinksNFL(nflMinYear, acMinYear, aaMinYear):   
    params = {
        'nflMinYear' : nflMinYear,
        'acMinYear' : acMinYear,
        'aaMinYear' : aaMinYear
    }

    template = '''
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
                ON f.MasterID = g.ID and g.KeyDataSet = 4 and g.Year >= {{ acMinYear }}
            
            --NCAA
            LEFT JOIN RecordLinks as h
                ON a.ID = h.TargetID and h.KeyDataSet = 5 and h.TargetKeyDataSet = 3
            LEFT JOIN "NCAA-Summarized" as i
                ON h.MasterID = i.ID
            
            --All American
            LEFT JOIN RecordLinks as j
                ON a.ID = j.TargetID and j.KeyDataSet = 6 and j.TargetKeyDataSet = 3
            LEFT JOIN SourcedPlayers as k
                ON j.MasterID = k.ID and k.KeyDataSet = 6 and k.Year >= {{ aaMinYear }}
            
            LEFT JOIN Positions as l
                ON a.Position = l.Position
            LEFT JOIN Positions as m
                ON a.Position = l.Position
            LEFT JOIN Schools as n
                ON a.college = n.'nfl-ref'

        WHERE
            a.KeyDataSet = 3
            and a.ID not in (select masterID from RecordLinks where KeyDataSet = 3 and TargetKeyDataSet in (1,2))
            and a.year >= {{ nflMinYear }}
        GROUP BY
            a.ID;
    '''
    return(apply_sql_template(template, params))

def getLinksAC(acMinYear, aaMinYear):   
    params = {
        'acMinYear' : acMinYear,
        'aaMinYear' : aaMinYear
    }

    template = '''
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
            SourcedPlayers as a
            
            --NCAA
            LEFT JOIN RecordLinks as h
                ON a.ID = h.TargetID and h.KeyDataSet = 5 and h.TargetKeyDataSet = 4
            LEFT JOIN "NCAA-Summarized" as i
                ON h.MasterID = i.ID
            
            --All American
            LEFT JOIN RecordLinks as j
                ON a.ID = j.TargetID and j.KeyDataSet = 6 and j.TargetKeyDataSet = 4
            LEFT JOIN SourcedPlayers as k
                ON j.MasterID = k.ID and k.KeyDataSet = 6 and k.Year >= {{ aaMinYear }}
            
            LEFT JOIN Positions as l
                ON a.Position = l.Position
            LEFT JOIN Positions as m
                ON a.Position = l.Position
            LEFT JOIN Schools as n
                ON a.college = n.wikipedia

        WHERE
            a.KeyDataSet = 4
            and a.ID not in (select masterID from RecordLinks where KeyDataSet = 4)
            and a.Year >= {{ acMinYear }}
        GROUP BY
            a.ID;
    '''
    return(apply_sql_template(template, params))

def getLinksNCAA(aaMinYear):   
    params = {
        'aaMinYear' : aaMinYear
    }

    template = '''
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
		'NCAA-Summarized' as i
		
		left join SourcedPlayers as a
			ON a.ID = i.ID and a.KeyDataSet = 5
		
		--All American
		LEFT JOIN RecordLinks as j
			ON i.ID = j.TargetID and j.KeyDataSet = 6 and j.TargetKeyDataSet = 5
		LEFT JOIN SourcedPlayers as k
			ON j.MasterID = k.ID and k.KeyDataSet = 6 and k.Year >= {{ aaMinYear }}
		
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
    '''
    return(apply_sql_template(template, params))

def getLinksAA(aaMinYear):   
    params = {
        'aaMinYear' : aaMinYear
    }

    template = '''
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
		and a.Year >= {{ aaMinYear }}
	GROUP BY
		a.ID;
    '''
    return(apply_sql_template(template, params))