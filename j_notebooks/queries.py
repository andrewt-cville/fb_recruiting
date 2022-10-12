from sql_templates_base import *


# Returns either ranked or unranked 247 rows
# Implemented to limit what was fuzzy matching
def get_query_Transfers247(ranked):
    columns = ['a.IDYR', 'a.ID', 'a.PlayerName', 'a.Year', 'a.College', 'a.HighSchool', 'a.City', 'a.State', 'b.KeyPositionGroup', 'b.StandardizedPosition', 'a.Height', 'a.Weight']

    params = {
        'Select': '\n  , '.join(columns),
        'Ranked': ranked
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
            {% if Ranked %}
                and a.Rating247 <> 0
            {% else %}
                and a.Rating247 is null
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
            and a.Year > {{ Year | sqlsafe }}
    '''

    return apply_sql_template(template, params)

def get_query_UnlinkedAllConference(limit, year):
    columns = ['a.ID', 'a.PlayerName', 'a.AllConferenceTeam', 'a.Year', 'a.College', 'b.KeyPositionGroup', 'b.StandardizedPosition']

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
                and a.Year > {{ Year }}
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