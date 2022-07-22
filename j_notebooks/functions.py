from bs4 import BeautifulSoup
from bs4.builder import TreeBuilder
from bs4 import UnicodeDammit
import requests
import lxml
import time
import os
import io
import json
import core_constants as cc
import string
import csv
import cleantext as clean
import pandas as pd
import sqlite3 as sql
import traceback
import recordlinkage
from recordlinkage.base import BaseCompareFeature


# ---------------------------------------------------------------------------------------------------------------------------------------
# Common Functions
# ---------------------------------------------------------------------------------------------------------------------------------------

## If there are multiple files for any given dataset, then this function will combine all of those
## records into a single dicti
def mergeSourceFiles (source, outputDir, sourceFiles):
    recordList = []
    for file in sourceFiles[source]:  
        file = json.loads(open(outputDir + file, "r", encoding="utf-8").read())
        for record in file:
            recordList.append(record)
    return recordList

## Clean up dirty names
def mungeID(playerString):
    return ''.join(e for e in playerString if e.isalnum()).lower().replace("jr.", "").replace("st.", "state") 

#Unique ID generator
def createNewID (fieldList, thisDict, fieldAgg, ifAlternate = False, fieldName = 'ID'):
    finalID= ''
    for i in thisDict:
        #if not (ifAlternate):
        #    try:
        #        i['displayName'] = i['PlayerName']
        #    except:
        #        print('Error with Alternate')
        #        print(i)
        for idx, val in enumerate(fieldList):
            if (type(i[val]) is list):
                try:
                    i[val]= mungeID(i[val][0])
                except:
                    print(i)
                if (len(fieldList) -1 == idx):
                    finalID += str(i[val]).strip('[]').strip("''")
            elif (type(val) is not list):
                try:
                    i[val] = mungeID(str(i[val]))
                except Exception as e:
                    traceback.print_exc()
                if (len(fieldList) - 1 == idx):
                    finalID += str(i[val])
                elif (len(fieldList) - 2 == idx):
                    finalID += str(i[val]) + fieldAgg
                else:
                    finalID = str(i[val]) + fieldAgg
        i[fieldName] = finalID
        finalID=''

#this is used by NCAA but really should be leveraged everywhere
def requestPage (url):
    headers={'user-agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36'}
    r = requests.get(url, headers=headers)
    request = {}
    request['status_code'] = r.status_code
    request['reason'] = r.reason
    request['text'] = r.text
    return request

def searchID(identifier, dataList):
    return [element for element in dataList if (element['ID'] == identifier)]

# TO DO!!!!!! CLEAN THESE PLEASE
def search(name, team, dataList):
    return [element for element in dataList if (element['name'] == name and element['team'] == team)]

def searchAllConf(name, team, dataList):
    return [element for element in dataList if (element['PlayerName'] == name and element['College'] == team)]

def searchAllAmerican(name, team, dataList):
    return [element for element in dataList if (element['player'] == name and element['school'] == team)]

def connDBAndReturnDF(SQL):
    conn = sql.connect(cc.databaseName)
    SQL_Query = pd.read_sql_query(SQL, conn)
    df = pd.DataFrame(SQL_Query)

    return df

def connAndWriteDB(df, table):
    conn = sql.connect(cc.databaseName)
    c = conn.cursor()

    df.to_sql(table, conn, if_exists='replace', index = False)

def writeToSourcedPlayers(dataset, columns, query, keydataset):
    conn = sql.connect(cc.databaseName)
    c = conn.cursor()
    
    finalPlayers = []
    for player in dataset:
        finalPlayer = []
        for column in columns:
            if (column == 'KeyDataSet'):
                finalPlayer.append(keydataset)
            elif (column in player.keys()):
                finalPlayer.append(player[column])
            else:
                finalPlayer.append(None)
        c.execute(query, finalPlayer)
        conn.commit()
    
    conn.close()

    return 'DB Write is done'

def getKeyDataset(value):
    conn = sql.connect(cc.databaseName)
    conn.row_factory = sql.Row
    c = conn.cursor()

    query = '''SELECT KeyDataSet FROM DataSet where DataSet = ?''' 
    vars = [value]
    c.execute(query, vars)

    result = (c.fetchall())[0]

    return result[0]

def clearDB(dataset):
    conn = sql.connect(cc.databaseName)
    c = conn.cursor()

    sourcedPlayersQuery = ''' DELETE FROM SourcedPlayers WHERE KeyDataSet =  ''' + str(getKeyDataset(dataset))
    c.execute(sourcedPlayersQuery)
    conn.commit()

    recordLinksQuery = ''' DELETE FROM RecordLinks WHERE KeyDataSet =  ''' + str(getKeyDataset(dataset))
    c.execute(recordLinksQuery)
    conn.commit()

    return 'Success'

# ---------------------------------------------------------------------------------------------------------------------------------------
# Literal Linking Functions
# ---------------------------------------------------------------------------------------------------------------------------------------
def literalLinking(dataset):
    conn = sql.connect(cc.databaseName)
    c = conn.cursor()
    print("Connected to SQLite")

    ## Get the KeyDataSet
    keyDataset = getKeyDataset(dataset)
    ## Load all of the IDs
    dataset_tuple = [keyDataset]
    
    if(keyDataset == 2):
        fetchIds = c.execute('SELECT a.IDYR, b.IDYR from SourcedPlayers a inner join SourcedPlayers b on (a.IDYR = b.IDYR and b.KeyDataSet = 1) where a.KeyDataSet = ?', dataset_tuple)
    elif(keyDataset == 5):
        fetchIds = c.execute('SELECT DISTINCT a.ID, b.IDYR from SummarizedNCAAData a inner join SourcedPlayers b on (a.ID = b.ID and b.KeyDataSet = 1)')
    else:
        fetchIds = c.execute('SELECT a.ID, b.IDYR from SourcedPlayers a inner join SourcedPlayers b on (a.ID = b.ID and b.KeyDataSet = 1) where a.KeyDataSet = ?', dataset_tuple)

    records = c.fetchall()
    
    ## Insert records into the RecordLinks table
    for record in records:
        #below you are hardcoding the KeyLinkType - this should probably be a lookup so it doesn't break in the future
        #if i'm working with All American data, as an example, then the MasterID is going to be the data source's unique ID and target will be 247
        sqlite_insert_query = """INSERT INTO RecordLinks
                            (MasterID, TargetID, KeyDataSet, KeyLinkType, LinkConfidence) 
                            VALUES 
                            (?,?,?,2,1);"""
        data_tuple = [record[0],record[1],keyDataset]
        count = c.execute(sqlite_insert_query, data_tuple)
        conn.commit()
# ---------------------------------------------------------------------------------------------------------------------------------------
# Fuzzy Matching Functions
# ---------------------------------------------------------------------------------------------------------------------------------------

def queryBuilderFM(KeyDataSet, DataSet):
    fuzzyFields = cc.fuzzyFields[DataSet]
    query = '''SELECT '''
    count = len(fuzzyFields)
    i = 1
    for field in fuzzyFields:
        if i == count:
            query = query + field
        else:
           query = query + field + ', '
        i = i + 1
    if (KeyDataSet == 3):
        query = query + ''' FROM UnlinkedNFL '''
    elif (KeyDataSet == 4):
        query = query + ''' FROM UnlinkedAllConference '''
    elif (KeyDataSet == 5):
        query = query + ''' FROM UnlinkedNCAA '''
    elif (KeyDataSet == 6):
        query = query + ''' FROM UnlinkedAllAmerican '''
    else:
        query = query + ''' FROM SourcedPlayers where KeyDataSet = ''' + str(KeyDataSet)
    return query
    

def doFuzzyMatching (source, target):
    

    # Create Dataframes

    ###### Source Dataframe
    ######## I'm setting the key equal to the target dataset - that way we don't compare IDYR to ID
    ######## HEY when you changed target vs source being 247, I think you broke the linked players view.  those joins need to be revisited!
    key = getKeyDataset(source)
    #tkey = getKeyDataset(target)
    SQL = queryBuilderFM(key, source)
    if (key in (1,2)):
        df_source = (connDBAndReturnDF(SQL)).set_index('IDYR')
        df_source.index.name = source + '_IDYR'
        df_source['IDYR'] = df_source.index.get_level_values(0)
    else:
        df_source = (connDBAndReturnDF(SQL)).set_index('ID')
        df_source.index.name = source + '_ID'
        df_source['ID'] = df_source.index.get_level_values(0)
    
    ###### Target Dataframe
    key = getKeyDataset(target)

    SQL = queryBuilderFM(key, target)
    if (key in (1,2)):
        df_target = (connDBAndReturnDF(SQL)).set_index('IDYR')
        df_target.index.name = target + '_IDYR'
        df_target['IDYR'] = df_target.index.get_level_values(0)
    else:
        df_target = (connDBAndReturnDF(SQL)).set_index('ID')
        df_target.index.name = target + '_ID'
        df_target['ID'] = df_target.index.get_level_values(0)
    
    #Create Blockers & Build Candidate Links - !!changing blockers from target to source
    indexer = recordlinkage.BlockIndex(on=cc.blockers[source])
    candidate_links = indexer.index(df_source, df_target)
    
    #changing this from target being NFL to target being 247 for a test
    ######## HEY when you changed target vs source being 247, I think you broke the linked players view.  those joins need to be revisited!
    targetFuzzy = cc.fuzzyFields[source]
    sumFields = []
    c = recordlinkage.Compare()
    if 'IDYR' in targetFuzzy:
        c.exact('IDYR', 'IDYR', label='IDYR')
        sumFields.append('IDYR')
    if 'ID' in targetFuzzy:
        c.string('ID', 'ID', label='ID')
        sumFields.append('ID')
    if 'PlayerName' in targetFuzzy:
        c.string('PlayerName', 'PlayerName', method='damerau_levenshtein', label='PlayerName')
        sumFields.append('PlayerName')
    if 'City' in targetFuzzy:
        c.string('City', 'City', label='City')
        sumFields.append('City')
    if 'State' in targetFuzzy:
        c.exact('State', 'State', label='State')
        sumFields.append('State')
    if 'HighSchool' in targetFuzzy:
        c.string('HighSchool', 'HighSchool', label='HighSchool')
        sumFields.append('HighSchool')
    if 'Position' in targetFuzzy:
        c.exact('Position', 'Position', label='Position', agree_value=.25)
        sumFields.append('Position')
    if 'Year' in targetFuzzy:
        c.exact('Year', 'Year', label='Year', agree_value=.25)
        sumFields.append('Year')

    try:
        features = c.compute(candidate_links, df_source, df_target)
    except KeyError as e:
        print(e)
    
    sum = 0
    for field in sumFields:
        sum = sum + features[field]

    features['sum'] = sum / len(sumFields)

    filteredList = []
    noMatch = []

    features.insert(0, 'sourceID', features.index.get_level_values(0))
    features.insert(1, 'targetID', features.index.get_level_values(1))

    for idx, data in features.groupby(level=0):
        data = data.loc[data['sum'].idxmax()]
        if (data['ID'] == 1):
            filteredList.append(data)
        #NFL was set to .72 threshold
        #NCAA was set to .41864
        #AllConf was set to .8347 and .75 for annotations
        #AllAmerican was set to .831 and .72 for annotations
        elif (data['ID'] != 1 and data['sum'] > .4186):
        #elif (data['ID'] != 1):
            filteredList.append(data)
        else:
            noMatch.append(data)

    dfFinal = pd.DataFrame()
    dfFinal = dfFinal.append(filteredList)
    dfFinal.to_csv("results.csv")

    return dfFinal

# ---------------------------------------------------------------------------------------------------------------------------------------
# Transfer Functions - Year 
# ---------------------------------------------------------------------------------------------------------------------------------------

class YearNFL(BaseCompareFeature):

    def _compute_vectorized(self, s1, s2):
        """Compare years

        College players can only get drafted after 3 years but usually within 5
        """
        sim = ((s1 == s2 + 2) | (s1 == s2 + 3) | (s1 == s2 + 4) | (s1 == s2 + 5) | (s1 == s2 + 6)).astype(float)

        return sim

# ---------------------------------------------------------------------------------------------------------------------------------------
# 247Sports Specific Functions
# ---------------------------------------------------------------------------------------------------------------------------------------

def get_247Teams (conference, schoolsJSON, years, headers, sleepyTime=4):
    for y in years:
        for school in schoolsJSON:
            if (school['conference'][0] == conference):
                filenameString = cc.get_htmlDir('247', conference, 'teams') + school['247'] + "_" + y + ".html"
                if (os.path.isfile(filenameString)):
                    print(filenameString + ' already exists.')
                else:
                    url = cc.create_url247('team', school['247'], y)
                    r = requests.get(url, headers=headers)
                    with open(filenameString, "w") as write_file:
                        write_file.write(r.text)
                    print(school['247'] + ': ' + str(y))
                    time.sleep(sleepyTime)

def get_247PlayerProfiles (conference, teamDirectory, headers, sleepyTime=4):
    for file in os.listdir(teamDirectory):
        gameSoup = BeautifulSoup(open(teamDirectory + file, "r", encoding='windows-1252').read(), 'lxml')
        team = file.split('_')[0]
        y = (file.split('_')[1]).split('.')[0]
        for x in gameSoup.find_all("li", class_="ri-page__list-item"):
            player_status = ""
            if (x.find("p", class_="commit-date") is not None):
                player_status = x.find("p", class_="commit-date").text
            if (player_status.strip() == 'Enrolled' or player_status.strip() == 'Signed'):
                #URL
                if (x.find("a", class_="ri-page__name-link") is not None):
                    name = x.find("a", class_="ri-page__name-link").text
                    filenameString = "..//html//247//" + conference + "//recruits//" + name + "_" + team + "_firsthop_" + y + ".html"
                    if (os.path.isfile(filenameString)):
                        print(filenameString + ' already exists.')
                    else:
                        url = "https:" + x.find("a", class_="ri-page__name-link")['href']
                        req = requests.get(url, headers=headers)
                        with open(filenameString, "w") as write_file:
                            write_file.write(req.text)
                        print(team + ': ' + name + '-' + y)
                        time.sleep(sleepyTime)  

def get_247ProspectProfiles (conference, playerDirectory, headers, sleepyTime=3):
    count = 1
    for file in os.listdir(playerDirectory):
        gameSoup = BeautifulSoup(open(playerDirectory + file, "r", encoding='utf-8').read(), 'lxml')
        fileName = file.split('_')
        playerName = fileName[0]
        team = fileName[1]
        year = fileName[3].split('.')[0]
        filenameString = cc.get_htmlDir('247', conference, 'prospects') + playerName + "_" + team + "_secondhop_" + year + ".html"
        if not (os.path.isfile(filenameString)):
            if (gameSoup.find("a", class_="view-profile-link") is not None):
                prospectLink = gameSoup.find("a", class_="view-profile-link")['href']
                req = requests.get(prospectLink, headers=headers)
                filePath = "..//html//247//" + conference + "//prospects//" + playerName + "_" + team + "_secondhop_" + year + ".html"
                cc.save_html(filePath, req.text)
                time.sleep(sleepyTime)
        if (count % 100 == 0):
            print('Ive processed ' + str(count) + ' files.')
        count = count + 1

def process_247Sports(prospectDirectory, teamDirectory):
    all_recruits = []

    for file in os.listdir(teamDirectory):
        try:
            gameSoup = BeautifulSoup(open(teamDirectory + file, "r", encoding='windows-1252').read(), 'lxml')
        except:
            print(file)
        team = file.split('_')[0]
        y = (file.split('_')[1]).split('.')[0]
        for x in gameSoup.find_all("li", class_="ri-page__list-item"):
            #append player to sqlinsert list
            player={}
            player_status = ""
            if (x.find("p", class_="commit-date") is not None):
                player_status = x.find("p", class_="commit-date").text
            if (player_status.strip() == 'Enrolled' or player_status.strip() == 'Signed'):
                player['College'] = team
                player['Year'] = int(y)
                #Name
                if (x.find("a", class_="ri-page__name-link") is not None):
                    player['PlayerName'] = x.find("a", class_="ri-page__name-link").text
                #School/CityState
                if (x.find("span", class_="meta") is not None):
                    locationInfoRaw = x.find("span", class_="meta")
                    locationInfoList = locationInfoRaw.text.split("(")
                    player['HighSchool'] = (locationInfoList[0].strip())
                    cityState = locationInfoList[1].split(', ')
                    if (len(cityState) > 1):
                        #print(cityState)
                        state = cityState[1].strip()
                        #print(state.rstrip(')'))
                        player['City'] = (cityState[0].strip())
                        player['State'] = (state.rstrip(')'))
                    else:
                        player['City'] = "None"
                        player['State'] = "None"
                #Position
                if (x.find("div", class_="position") is not None):
                    player['Position'] = ((x.find("div", class_="position").text).strip())
                #Height/Weight
                if (x.find("div", class_="metrics") is not None):
                    heightWeight = x.find("div", class_="metrics").text.strip()
                    height = (heightWeight.split(' / '))[0]
                    inchHeightPre = height.split('-')
                    if (inchHeightPre[0] != ''):
                        player['Height'] = int(inchHeightPre[0])*12 + float(inchHeightPre[1])
                    else:
                        player['Height']  = None
                    player['Weight'] = (heightWeight.split(' / '))[1]
                    if (player['Weight'] == '-'):
                        player['Weight'] = None
                    else:
                        player['Weight'] = int(player['Weight'])
                #Getting composite rankings from the class page because of the prospect link, in some cases,
                #actually goes to the JUCO page and as a result the JUCO rankings.
                
                #Composite Rating
                if (x.select('span[class*="scorer"]') is not None):
                    player['CompositeRating'] = (x.find("span", class_="score").text)
                #Composite Stars
                ratingChildren = x.select('span[class*="icon-starsolid yellow"]')
                i = 0
                for child in ratingChildren:
                    i = i + 1
                if (i == 0):
                    player['CompositeStars'] = None
                else:
                    player['CompositeStars'] = (i)
                #Composite National Rank
                if (x.select('a[class*="natrank"]') is not None):
                    player['CompositeNationalRank'] = ((x.find("a", class_="natrank").text).strip())
                    if (player['CompositeNationalRank'] == 'NA'):
                        player['CompositeNationalRank'] = None
                    else:
                        player['CompositeNationalRank'] = int(player['CompositeNationalRank'])
                #Composite Position Rank
                if (x.select('a[class*="posrank"]') is not None):
                    player['CompositePositionRank'] = ((x.find("a", class_="posrank").text).strip())
                    if (player['CompositePositionRank'] == 'NA'):
                        player['CompositePositionRank'] = None
                    else:
                        player['CompositePositionRank'] = int(player['CompositePositionRank'])
                #Composite State Rank
                if (x.select('a[class*="sttrank"]') is not None):
                    player['CompositeStateRank'] = ((x.find("a", class_="sttrank").text).strip())
                    if (player['CompositeStateRank'] == 'NA'):
                        player['CompositeStateRank'] = None
                    else:
                        player['CompositeStateRank'] = int(player['CompositeStateRank'])
                ## We are going to get 247 rankings data from the prospect page, but nothing else
                prospectFile = prospectDirectory + player['PlayerName'] + "_" + player['College'] + "_secondhop_" + str(player['Year']) + ".html"
                if (os.path.isfile(prospectFile)):
                    prospectSoup = BeautifulSoup(open(prospectFile, "r", encoding='utf8').read(), 'lxml')
                    # The recent crawl turned out a weird prospect file, so this catches any
                    # section whose class CONTAINS rankings section
                    ratingsSection = prospectSoup.select('section[class*="rankings-section"]')
                    count = 0
                    for rating in ratingsSection:
                        if (count == 1):
                            #247 Rating
                            if (rating.select('div[class*="rank-block"]') is not None):
                                player['Rating247'] = (rating.select('div[class*="rank-block"]')[0].text).strip()
                                if (player['Rating247'] == 'N/A'):
                                    player['Rating247'] = None
                            #Stars
                            ratingChildren = []
                            for ratingChild in rating.select('span[class*="icon-starsolid yellow"]'):
                                ratingChildren.append(ratingChild)
                            i = 0
                            for child in ratingChildren:
                                i = i + 1
                            if (i == 0):
                                player['Stars247'] = None
                            else:
                                player['Stars247'] = (i)
                            #247 Rankings are stupid
                            ratingValues = rating.find_all("li", class_=None)
                            if (len(ratingValues) >= 3):
                                player['NationalRank247'] = ratingValues[0].find("strong").text
                                if (player['NationalRank247'] == 'N/A'):
                                    player['NationalRank247'] = None
                                else:
                                    player['NationalRank247'] = int(player['NationalRank247'])
                                player['PositionRank247'] = ratingValues[1].find("strong").text
                                if (player['PositionRank247'] == 'N/A'):
                                    player['PositionRank247'] = None
                                else:
                                    player['PositionRank247'] = int(player['PositionRank247'])
                                player['StateRank247'] = ratingValues[2].find("strong").text   
                                if (player['StateRank247'] == 'N/A'):
                                    player['StateRank247'] = None
                                else:
                                    player['StateRank247'] = int(player['StateRank247'])
                            elif (len(ratingValues) == 2):
                                if (ratingValues[0].find("strong") is not None):
                                    player['PositionRank247'] = ratingValues[0].find("strong").text
                                    if (player['PositionRank247'] == 'N/A'):
                                        player['PositionRank247'] = None
                                    else:
                                        player['PositionRank247'] = int(player['PositionRank247'])
                                if (ratingValues[1].find("strong") is not None):
                                    player['StateRank247'] = ratingValues[1].find("strong").text
                                    if (player['StateRank247'] == 'N/A'):
                                        player['StateRank247'] = None
                                    else:
                                        player['StateRank247'] = int(player['StateRank247'])
                            else:
                                print("Error: " + player['PlayerName'])
                        count += 1
            if player:
                duplicate = False
                for i in all_recruits:
                    if (player['PlayerName'].lower().replace(".","").replace(" ", "").replace("'", "") == i['PlayerName'].lower().replace(".","").replace(" ", "").replace("'", "") ):
                        if (player['Year'] == i['Year']):
                            duplicate = True
                
                if (not duplicate):
                    all_recruits.append(player)  
    return all_recruits

def toDB_247Sports():
    inputDirectory = '..//scrapedData//'
    dataset = 'sports247'
    dataset_yr = 'sports247_yr'

    ## Load the id config
    idConfig = json.loads(open('..//config//idConfig.json', "r").read())

    ## Load the source file dict
    sourceFiles = json.loads(open('..//config//sourceFiles.json', "r").read())
    
    sports247Data = mergeSourceFiles(dataset, inputDirectory, sourceFiles)

    createNewID(idConfig[dataset], sports247Data, '_')
    createNewID(idConfig[dataset_yr], sports247Data, '_', False, 'IDYR')

    columns = ['ID', 'IDYR', 'KeyDataSet', 'PlayerName', 'College', 
        'Year', 'HighSchool', 'City', 'State', 'Position', 'Height', 'Weight', 
        'CompositeRating', 'CompositeStars', 'CompositeNationalRank', 'CompositeStateRank', 
        'CompositePositionRank', 'Rating247', 'Stars247', 'NationalRank247', 'StateRank247', 'PositionRank247']

    query = ''' INSERT INTO SourcedPlayers(ID, IDYR, KeyDataSet, PlayerName, College, 
        Year, HighSchool, City, State, Position, Height, Weight, 
        CompositeRating, CompositeStars, CompositeNationalRank, CompositeStateRank, 
        CompositePositionRank, Rating247, Stars247, NationalRank247, StateRank247, PositionRank247)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
    
    writeToSourcedPlayers(sports247Data, columns, query, 1)

def sports247_SourcedPlayers():
    SQL = '''SELECT * from Sports247'''
    df = connDBAndReturnDF(SQL)
    df['KeyDataSet'] = 1

    connAndWriteDB(df, 'SourcedPlayers')

    return 'DB Write is done'

# ---------------------------------------------------------------------------------------------------------------------------------------
# Rivals Specific Functions
# ---------------------------------------------------------------------------------------------------------------------------------------

def get_Rivals(conference, schoolsJSON, years, headers, sleepyTime=4):
    for y in years:
        for school in schoolsJSON:
            if ('rivals' in school.keys()):
                teamFile = "..//html//rivals//" + conference + "//teams//" + school['rivals'] + "_" + y + ".html"
                # TO-DO - problem here is that if you have the team file, but not all of the player files - it won't fetch those.  so need to rethink this a bit. 
                if not (os.path.isfile(teamFile)):
                    if (school['conference'][0] == conference):
                        url = 'https://{}.rivals.com/commitments/football/{}'.format(school['rivals'],y)
                        r = requests.get(url, headers=headers)
                        gameSoup = BeautifulSoup(r.text, 'lxml')
                        with open(teamFile, "w") as write_file:
                            write_file.write(r.text)
                        print(school['rivals'] + ": " + str(y))
                        print('------------------------------')
                        commitments = gameSoup.find("rv-commitments")['prospects']

                        for commit in json.loads(commitments):
                            if not (os.path.exists("..//html//rivals//" + conference + "//recruits//" + str(commit['id']) + "_" + school['rivals'] + "_" + y + ".html")):
                                #go to player page
                                recruitRequest = requests.get(commit['url'], headers=headers)
                                with open("..//html//rivals//" + conference + "//recruits//" + str(commit['id']) + "_" + school['rivals'] + "_" + y + ".html", "w") as write_file:
                                    write_file.write(recruitRequest.text)
                                print(commit['id'])
                                time.sleep(sleepyTime)

#Rivals specific schools check due to their naming philosophy
def checkSchools(recruitSchool, conference, schoolsJSON):
    
    #clean recruitSchool
    #recruitSchoolCleaned = recruitSchool.lower().replace(" ", "").replace("&","")

    for school in schoolsJSON:
        if (conference in school['conference']):
            if ('rivalsDisplay' in school.keys() and recruitSchool == school['rivalsDisplay']):
                return school['id']

#The below is used to cycle through all conferences maintaing the proper encoding for the different data sets. Not sure what to do with this yet, but don't want to lose it.
""" conferences = cc.get_availableConferences()

#conferences = ['sunbelt']
for conf in conferences:
    print ("working on - " + conf)
    conference = conf

    years = cc.get_defYears()
    headers= cc.get_header()
    schoolsList = cc.get_schoolsList()
    teamDirectory = cc.get_htmlDir('rivals', conference, 'teams')
    playerDirectory = cc.get_htmlDir('rivals', conference, 'recruits')
    if (conf == 'acc' or conf == 'pactwelve'):
        cc.save_records('scrapedData', 'rivals_' + conference, fx.process_Rivals(playerDirectory, conference, schoolsList, 'utf-8'))
    else:
        cc.save_records('scrapedData', 'rivals_' + conference, fx.process_Rivals(playerDirectory, conference, schoolsList, 'windows-1252'))"""

def process_Rivals(recruitDir, conference, schoolsJSON, encode):
    all_recruits = []
    #error_files = [] 
    for file in os.listdir(recruitDir):

        player = {}
        #get file contents and soup it
        recruitSoup = BeautifulSoup(io.open(recruitDir + file, "r", encoding=encode).read(), 'lxml')

        #find the magical html attr
        if (recruitSoup.find("div", class_="profile-block") is not None):
            recruitInfoJson = recruitSoup.find("div", class_="profile-block")['ng-init']
            #this is harsh - but i'm removing an init() and a trailing id which always seems to be x characters long
            recruitInfo = json.loads(recruitInfoJson[5:-57])

            #player info
            #rawSchool is helpful for troubleshooting rivals school names
            player['CollegeRaw'] = recruitInfo['school_name']
            player['College'] = checkSchools(recruitInfo['school_name'],conference, schoolsJSON)
            player['Year'] = str(recruitInfo['recruit_year'])
            player['PlayerName'] = recruitInfo['full_name']
            player['City'] = recruitInfo['city']
            player['State'] = recruitInfo['state_abbreviation']
            player['HighSchool'] = recruitInfo['highschool_name']        
            player['Position'] = recruitInfo['position_group_abbreviation']
            player['Height'] = recruitInfo['height']
            player['Weight'] = recruitInfo['weight']
            player['StarsRivals'] = recruitInfo['stars']
            player['NationalRankRivals'] = recruitInfo['national_rank']
            player['PositionRankRivals'] = recruitInfo['position_rank']
            player['StateRankRivals'] = recruitInfo['state_rank']

            if player:
                duplicate = False
                for i in all_recruits:
                    if (player['PlayerName'].lower().replace(".","").replace(" ", "").replace("'", "") == i['PlayerName'].lower().replace(".","").replace(" ", "").replace("'", "") ):
                        if (player['Year'] == i['Year']):
                            duplicate = True
                
                if (not duplicate):
                    all_recruits.append(player)  
        #else:
            #error_files.append(file)
    return all_recruits

def toDB_Rivals():
    inputDirectory = '..//scrapedData//'
    dataset = 'rivals'
    dataset_yr = 'rivals_yr'

    ## Load the id config
    idConfig = json.loads(open('..//config//idConfig.json', "r").read())

    ## Load the source file dict
    sourceFiles = json.loads(open('..//config//sourceFiles.json', "r").read())
    
    rivalsData = mergeSourceFiles(dataset, inputDirectory, sourceFiles)
    createNewID(idConfig[dataset], rivalsData, '_')
    createNewID(idConfig[dataset_yr], rivalsData, '_', False, 'IDYR')

    columns = ['ID', 'IDYR', 'KeyDataSet', 'PlayerName', 'College', 'CollegeRaw',
        'Year', 'HighSchool', 'City', 'State', 'Position', 'Height', 'Weight', 
        'StarsRivals', 'NationalRankRivals', 'StateRankRivals', 'PositionRankRivals']

    query = ''' INSERT INTO SourcedPlayers(ID, IDYR, KeyDataSet, PlayerName, College, CollegeRaw,
        Year, HighSchool, City, State, Position, Height, Weight, 
        StarsRivals, NationalRankRivals, StateRankRivals, PositionRankRivals)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
    
    writeToSourcedPlayers(rivalsData, columns, query, 2)

    return 'DB Write is done'

# ---------------------------------------------------------------------------------------------------------------------------------------
# NCAA Specific Functions
# ---------------------------------------------------------------------------------------------------------------------------------------

def get_NCAA(schoolsList, ncaaDates, sleepyTime=6):
    urlDict = []
    for x in schoolsList:
        if ('ncaa' in x.keys()):
            thisUrl = {}
            thisUrl['team'] = x['id']
            thisUrl['conference'] = x['conference'][0]
            thisUrl['ncaa'] = 'http://stats.ncaa.org/team/' + x['ncaa'] + '/roster/'
            urlDict.append(thisUrl)

    for x in urlDict:
        for year in ncaaDates:
            filename = "..//html//ncaa//" + x['conference'] + "//rosters//" + x['team'] + "_" + year['year'] + ".html"
            if not(os.path.isfile(filename)):
                request = requestPage(x['ncaa'] + year['id'])
                if (request['status_code'] == 200):
                    with open(filename, "w", encoding="utf-8") as write_file:
                        write_file.write(request['text'])
                time.sleep(sleepyTime)

def process_NCAA(conferences):
    playerData = []
    for conf in conferences:
        rosterDir= "..//html//ncaa//" + conf + "//rosters//"
        for file in os.listdir(rosterDir):
            try:
                gameSoup = BeautifulSoup(open(rosterDir + file, "r").read(), 'lxml')
                for x in gameSoup.find_all("tr", class_=""): 
                    count = 0
                    player={}
                    player['College'] = file.split("_")[0]
                    player['Year'] = file.split("_")[1].split(".")[0]
                    for z in x.find_all("td"):
                        if (count == 1):
                            player['PlayerName'] = z.text.strip()
                        if (count == 2):
                            player['Position'] = z.text.strip()
                        if (len(x.find_all("td")) == 6):
                            if (count == 4):
                                player['NCAAGamesPlayed'] = z.text.strip()
                            if (count == 5):
                                player['NCAAGamesStarted'] = z.text.strip()
                        else:
                            if (count == 5):
                                player['NCAAGamesPlayed'] = z.text.strip()
                            if (count == 6):
                                player['NCAAGamesStarted'] = z.text.strip()
                        count = count + 1
                    playerData.append(player)
            except:
                print(file)
    
    for player in playerData:
        newName = player['PlayerName'].split(',')
        player['PlayerName'] = newName[1].strip() + ' ' + newName[0].strip()
    
    return playerData

def toDB_NCAA():
    inputDirectory = '..//scrapedData//'
    dataset = 'ncaa'

    ## Load the id config
    idConfig = json.loads(open('..//config//idConfig.json', "r").read())

    ## Load the source file dict
    sourceFiles = json.loads(open('..//config//sourceFiles.json', "r").read())
    ncaaData = json.loads(open(inputDirectory + sourceFiles['ncaa'][0], "r", encoding="utf-8").read())

    createNewID(idConfig[dataset], ncaaData, '_', True)

    columns = ['ID', 'KeyDataSet', 'PlayerName', 'College', 
        'Year', 'Position',
        'NCAAGamesPlayed', 'NCAAGamesStarted']

    query = ''' INSERT INTO SourcedPlayers(ID, KeyDataSet, PlayerName, College, 
        Year, Position, 
        NCAAGamesPlayed, NCAAGamesStarted)
        VALUES (?,?,?,?,?,?,?,?)'''
    
    writeToSourcedPlayers(ncaaData, columns, query, 5)

    return 'DB Write is done'

# ---------------------------------------------------------------------------------------------------------------------------------------
# All Conference Specific Functions
# ---------------------------------------------------------------------------------------------------------------------------------------

def get_WikipediaAllConf(pageTitles, headers, years, sleepyTime = 5):

    for page in pageTitles:
        for y in years:
            url = 'https://en.wikipedia.org/wiki/{}_{}'.format(y,page[1])
            r = requests.get(url, headers=headers)
            with open("..//html//wikipedia//allconference//" + page[0] + "//" + y + ".html", "w", encoding="utf-8") as write_file:
                write_file.write(r.text)
            time.sleep(sleepyTime)

def process_wikiConferences(teamDir):
    all_players = []
    for folder in teamDir:
        for file in os.listdir(folder):
            wikiSoup = BeautifulSoup(open(folder + file, "r", encoding="utf-8").read(), 'lxml')
            y = file.split('.')[0]
            firsth3 = wikiSoup.h3
            targetedWiki = firsth3.find_all_next(string = True)
            quitKeyword = 'Key'
            removeKeywords = ['[', 'edit', ']']
            positionKeywords = ['Quaterbacks', 'Running backs', 'Fullbacks', 'Wide receivers', 'Receivers', 'Centers', 'Guards', 'Tackles', 'Tight ends', 'Defensive linemen', 'Defensive ends', 'Defensive tackles', 'Linebackers', 'Defensive backs', 'Cornerbacks', 'Safeties', 'Kickers', 'Punter', 'Punters', 'All purpose/return specialist', 'All-purpose / Return specialists','Return specialist']
            position = 'Quaterbacks'
            playerName = ''
            player = {}
            for row in targetedWiki:
                row = row.lstrip()
                row = row.replace('†', '').replace('*', '').replace('#', '')
                #print (row)
                if (', ' != row[0:2] and '(' in row):
                    try:
                        playerName = row.split(',', 1)[0]
                        college = (row.split(',', 1)[1]).split('(', 1)[0].lstrip().rstrip()
                        allConfTeam = [int(char) for char in row if char.isdigit()]
                        player['PlayerName'] = playerName
                        player['Position'] = position
                        player['College'] = college
                        player['AllConferenceTeam'] = min(allConfTeam)
                        player['Year'] = y
                        all_players.append(player)
                        player = {}
                    except:
                        print('ERROR - Couldn"t write record without link: ' + row + ' Year: ' + y)
                elif (row == quitKeyword):
                    break
                elif (row in positionKeywords):
                    position = row
                elif ('(' not in row):
                    playerName = row
                elif (row not in removeKeywords):
                    try:
                        college = row.split('(', 1)[0].replace(', ', '').rstrip()
                        allConfTeam = [int(char) for char in row if char.isdigit()]
                        player['PlayerName'] = playerName
                        player['Position'] = position
                        player['College'] = college
                        player['AllConferenceTeam'] = min(allConfTeam)
                        player['Year'] = y
                        all_players.append(player)
                        player = {}
                    except:
                        print('ERROR - Couldn"t write record: ' + row + ' Year:' + y)
                else:
                    print('ERROR - No Conditional Match: ' + row + ' Year:' + y)
    return all_players

def get_csvAllConf(filename):
    csvFile = csv.DictReader(open(filename))
    finalList = []
    for record in csvFile:
        finalList.append(record)
    
    return finalList

def process_csvAllConf(records):
    for record in records:
        try:
            record['PlayerName'] = record['First Name'] + ' ' + record['Last Name']
            record['College'] = record['School']
            record['Position'] = record['POSITION']
            record['Year'] = record['Year']

            if (len(record['Team']) > 1 ):
                record['Team'] = record['Team'][0]
            record['AllConferenceTeam'] = record['Team']

        except Exception as e:
            print (e)
            print (record)

    for record in records:
        del record['School']
        del record['First Name']
        del record['Last Name']
        del record['Team']
        del record['POSITION']
    
    return records

def toDB_AllConference():
    inputDirectory = '..//scrapedData//'
    dataset = 'allConf'

    ## Load the id config
    idConfig = json.loads(open('..//config//idConfig.json', "r").read())

    ## Load the source file dict
    sourceFiles = json.loads(open('..//config//sourceFiles.json', "r").read())
    
    #allConfData = mergeSourceFiles(dataset, inputDirectory, sourceFiles)
    allConfData = mergeSourceFiles('allConf', inputDirectory, sourceFiles)

    createNewID(idConfig[dataset], allConfData, '_')
    allConf = []
    for x in allConfData:
        if (len(searchID(x['ID'], allConf)) == 0):
            try:
                playerList = searchAllConf(x['PlayerName'], x['College'], allConfData)
                finalPlayer = {}
                finalPlayer['ID'] = x['ID']
                finalPlayer['AllConferenceTeam'] = x['AllConferenceTeam']
                finalPlayer['PlayerName'] = x['PlayerName']
                finalPlayer['College'] = x['College']
                finalPlayer['Year'] = x['Year']
                finalPlayer['Position'] = x['Position']
            
            except Exception as e:
                print(e)
                print(playerList)
                print (x)
            
            allConf.append(finalPlayer)
    #finalList = []
    #for record in allConf:
    #    finalList.append(record['ID'])
    #print(len(list(set(finalList))))
    columns = ['ID', 'KeyDataSet', 'AllConferenceTeam', 'PlayerName', 'College', 'Year', 'Position']

    query = ''' INSERT INTO SourcedPlayers (ID, KeyDataSet, AllConferenceTeam, PlayerName, College, Year, Position)
        VALUES (?,?,?,?,?,?,?)'''
    
    # Changing temporarily from 4 to 10 so I can test the code
    writeToSourcedPlayers(allConf, columns, query, 4)

    return 'DB Write is done'
    
# ---------------------------------------------------------------------------------------------------------------------------------------
# Sports Reference - NFL Draft Specific Functions
# NOTE: we don't keep the html files locally for this dataset, so get/process are one step
# ---------------------------------------------------------------------------------------------------------------------------------------

def normalizeNFLCollege(recruitSchool, schoolsJSON):
    college = ""
    for school in schoolsJSON:
        if ('nfl-ref' in school.keys()):
            if (recruitSchool == school['nfl-ref']):
                college = school['id']
    
    if college is not None:
        return college
    else:
        return recruitSchool

def handle_nflData(years, headers, schoolsJSON, sleepyTime=10):
    all_picks = []
    for y in years:
        url = 'https://www.pro-football-reference.com/years/{}/draft.htm'.format(y)
        r = requests.get(url, headers=headers)
        draftSoup = BeautifulSoup(r.text, 'lxml')
        draftTableSoup = draftSoup.find("table", class_="stats_table")
        tableBodySoup = draftTableSoup.find("tbody")
        for x in tableBodySoup.find_all("tr"):
            if (x.find("td", attrs={"data-stat":"draft_pick"}) is not None):
                draftPick = []
                draftPick.append(y)
                draftPick.append(x.find("th", attrs={"data-stat":"draft_round"}).text)
                draftPick.append(x.find("td", attrs={"data-stat":"draft_pick"}).text)
                draftPick.append(x.find("td", attrs={"data-stat":"team"}).text)       
                draftPick.append(x.find("td", attrs={"data-stat":"player"}).text)
                draftPick.append(x.find("td", attrs={"data-stat":"pos"}).text)
                draftPick.append(x.find("td", attrs={"data-stat":"all_pros_first_team"}).text)
                draftPick.append(x.find("td", attrs={"data-stat":"pro_bowls"}).text)
                draftPick.append(x.find("td", attrs={"data-stat":"years_as_primary_starter"}).text)
                draftPick.append(x.find("td", attrs={"data-stat":"g"}).text)
                #This all needs to be cleaned but it's for normalizing the college name
                drafteeSchool = x.find("td", attrs={"data-stat":"college_id"}).text
                alphanum = [character for character in drafteeSchool if character.isalnum()]
                alphanum = ("".join(alphanum)).lower()
                draftPick.append(normalizeNFLCollege(alphanum, schoolsJSON))
                #draftPick.append(x.find("td", attrs={"data-stat":"college_id"}).text)
                all_picks.append(draftPick)
        time.sleep(sleepyTime)
    
    final_nflDraft = []
    nfl_keys = ['Year', 'NFLDraftRound', 'NFLDraftPick', 'NFLDraftTeam', 'PlayerName', 'Position', 'NFLAllProFirstTeam', 'NFLProBowl', 'NFLYearsAsStarter', 'NFLGamesPlayed', 'College']

    for list in all_picks:
        newdict = {nfl_keys[i]: list[i] for i in range(len(nfl_keys))}
        final_nflDraft.append(newdict)
        newdict = {}
    
    return final_nflDraft

def toDB_NFLDraft():
    inputDirectory = '..//scrapedData//'
    dataset = 'nflData'

    ## Load the id config
    idConfig = json.loads(open('..//config//idConfig.json', "r").read())

    ## Load the source file dict
    sourceFiles = json.loads(open('..//config//sourceFiles.json', "r").read())
    nflData = json.loads(open(inputDirectory + sourceFiles['nflData'][0], "r", encoding="utf-8").read())

    createNewID(idConfig[dataset], nflData, '_')

    columns = ['ID', 'KeyDataSet', 'PlayerName', 'College',
        'Year', 'Position',
        'NFLDraftRound', 'NFLDraftPick', 'NFLDraftTeam', 'NFLAllProFirstTeam', 'NFLProBowl', 'NFLYearsAsStarter', 'NFLGamesPlayed' ]

    query = ''' INSERT INTO SourcedPlayers(ID, KeyDataSet, PlayerName, College,
        Year, Position,
        NFLDraftRound, NFLDraftPick, NFLDraftTeam, NFLAllProFirstTeam, NFLProBowl, NFLYearsAsStarter, NFLGamesPlayed)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)'''
    
    writeToSourcedPlayers(nflData, columns, query, 3)

    return 'DB Write is done'

# ---------------------------------------------------------------------------------------------------------------------------------------
# Wikipedia - All American Specific Functions
# NOTE: we don't keep the html files locally for this dataset, so get/process are one step
# ---------------------------------------------------------------------------------------------------------------------------------------

def handle_allAmerican(years, headers, sleepyTime=5):
    all_players = []
    for y in years: 
        url = 'https://en.wikipedia.org/wiki/{}_College_Football_All-America_Team'.format(y)
        r = requests.get(url, headers=headers)
        aaSoup = BeautifulSoup(r.text, 'lxml')
        for x in aaSoup.find_all("li", class_=""):
            try:
                player = []
                if ((",") in x.text and ("(") in x.text and (")") in x.text and ("{") not in x.text \
                    and ("Archived") not in x.text and ("Gridiron") not in x.text and ("edited") not in x.text \
                    and ("all-purpose") not in x.text and ("Football") not in x.text and ("Foundation") not in x.text):
                    playerInfo = x.text.split(",", 1)
                    #year
                    player.append(int(y))
                    #Name
                    player.append(playerInfo[0])
                    playerInfo[1] = playerInfo[1].replace("(Fla.)", "FL")
                    playerLocationAwards = playerInfo[1].split("(",1)
                    #School
                    if ("CONSENSUS" in x.text):
                        playerLocationAwards[0] = playerLocationAwards[0].replace("-- CONSENSUS --", "")
                        player.append(playerLocationAwards[0])
                    elif ("UNANIMOUS" in x.text):
                        playerLocationAwards[0] = playerLocationAwards[0].replace("-- UNANIMOUS --", "")
                        player.append(playerLocationAwards[0])
                    else:
                        player.append(playerLocationAwards[0].strip())
                    #Awards String
                    awardString = playerLocationAwards[1]
                    #coaches (AFCA)
                    if ("AFCA" in awardString):
                        player.append(1)
                    else:
                        player.append(0)
                    #associated press (AP)
                    if ("AP" in awardString):
                        player.append(1)
                    else:
                        player.append(0)
                    #writers (FWAA)
                    if ("FWAA" in awardString):
                        player.append(1)
                    else:
                        player.append(0)
                    #sporting news (TSN)
                    if ("TSN" in awardString):
                        player.append(1)
                    else:
                        player.append(0)
                    #walter camp (WCFF)
                    if ("WCFF" in awardString):
                        player.append(1)
                    else:
                        player.append(0)
                    all_players.append(player)
            except:
                print(x)
    time.sleep(sleepyTime)

    final_aaSelections = []
    aa_keys = ['Year', 'PlayerName', 'College', 'AllAmericanAFCA', 'AllAmericanAP', 'AllAmericanFWAA', 'AllAmericanTSN', 'AllAmericanWCFF']

    for list in all_players:
        newdict = {aa_keys[i]: list[i] for i in range(len(aa_keys))}
        final_aaSelections.append(newdict)
        newdict = {}
    
    return final_aaSelections

def toDB_AllAmerican():
    inputDirectory = '..//scrapedData//'
    dataset = 'allAmerican'

    ## Load the id config
    idConfig = json.loads(open('..//config//idConfig.json', "r").read())

    ## Load the source file dict
    sourceFiles = json.loads(open('..//config//sourceFiles.json', "r").read())
    aaData = json.loads(open(inputDirectory + sourceFiles['allAmerican'][0], "r", encoding="utf-8").read())

    createNewID(idConfig[dataset], aaData, '_')

    columns = ['ID', 'KeyDataSet', 'PlayerName', 'College',
        'Year',
        'AllAmericanAFCA', 'AllAmericanAP', 'AllAmericanFWAA', 'AllAmericanTSN', 'AllAmericanWCFF']

    query = ''' INSERT INTO SourcedPlayers(ID, KeyDataSet, PlayerName, College,
        Year,
        AllAmericanAFCA, AllAmericanAP, AllAmericanFWAA, AllAmericanTSN, AllAmericanWCFF)
        VALUES (?,?,?,?,?,?,?,?,?,?)'''
    
    writeToSourcedPlayers(aaData, columns, query, 6)

    return 'DB Write is done'