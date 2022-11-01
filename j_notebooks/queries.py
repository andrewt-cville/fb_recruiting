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

def insert_query_RecordLinks():
    
    params = {}

    template = '''
        INSERT INTO RecordLinks_DevB(MasterID, TargetID, KeyDataSet, TargetKeyDataSet, KeyLinkType, LinkConfidence, Transfer)
            VALUES (?,?,?,?,?,?,?)
            '''  
    
    return(apply_sql_template(template, params))