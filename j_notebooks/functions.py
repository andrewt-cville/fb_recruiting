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
def createNewID (fieldList, thisDict, fieldAgg, ifAlternate = False):
    finalID= ''
    for i in thisDict:
        if not (ifAlternate):
            try:
                i['displayName'] = i['playerName']
            except:
                print(i)
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
                    i[val] = mungeID(i[val])
                except:
                    print(i)
                if (len(fieldList) - 1 == idx):
                    finalID += i[val]
                elif (len(fieldList) - 2 == idx):
                    finalID += i[val] + fieldAgg
                else:
                    finalID = i[val] + fieldAgg
        i['ID'] = finalID
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
    return [element for element in dataList if (element['playerName'] == name and element['school'] == team)]

def searchAllAmerican(name, team, dataList):
    return [element for element in dataList if (element['player'] == name and element['school'] == team)]

def connAndWriteDB(df, table):
    conn = sql.connect(cc.databaseName)
    c = conn.cursor()

    df.to_sql(table, conn, if_exists='replace', index = False)
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
                player['school'] = team
                player['year'] = y
                #Name
                if (x.find("a", class_="ri-page__name-link") is not None):
                    player['playerName'] = x.find("a", class_="ri-page__name-link").text
                #School/CityState
                if (x.find("span", class_="meta") is not None):
                    locationInfoRaw = x.find("span", class_="meta")
                    locationInfoList = locationInfoRaw.text.split("(")
                    player['highSchool'] = (locationInfoList[0].strip())
                    cityState = locationInfoList[1].split(', ')
                    if (len(cityState) > 1):
                        #print(cityState)
                        state = cityState[1].strip()
                        #print(state.rstrip(')'))
                        player['city'] = (cityState[0].strip())
                        player['state'] = (state.rstrip(')'))
                    else:
                        player['city'] = "None"
                        player['state'] = "None"
                #Position
                if (x.find("div", class_="position") is not None):
                    player['position'] = ((x.find("div", class_="position").text).strip())
                #Height/Weight
                if (x.find("div", class_="metrics") is not None):
                    heightWeight = x.find("div", class_="metrics").text.strip()
                    height = (heightWeight.split(' / '))[0]
                    inchHeightPre = height.split('-')
                    if (inchHeightPre[0] != ''):
                        inchHeight = int(inchHeightPre[0])*12 + float(inchHeightPre[1])
                    else:
                        inchHeight = '0.0'
                    weight = (heightWeight.split(' / '))[1]
                    if (weight == '-'):
                        weight = '0'
                    player['height'] = (inchHeight)
                    player['weight'] = (weight)
                #Getting composite rankings from the class page because of the prospect link, in some cases,
                #actually goes to the JUCO page and as a result the JUCO rankings.
                
                #Composite Rating
                if (x.select('span[class*="scorer"]') is not None):
                    player['compRating'] = (x.find("span", class_="score").text)
                #Composite Stars
                ratingChildren = x.select('span[class*="icon-starsolid yellow"]')
                i = 0
                for child in ratingChildren:
                    i = i + 1
                player['compStars'] = (i)
                #Composite National Rank
                if (x.select('a[class*="natrank"]') is not None):
                    player['nationalRank'] = ((x.find("a", class_="natrank").text).strip())
                #Composite Position Rank
                if (x.select('a[class*="posrank"]') is not None):
                    player['positionRank'] = ((x.find("a", class_="posrank").text).strip())
                #Composite State Rank
                if (x.select('a[class*="sttrank"]') is not None):
                    player['stateRank'] = ((x.find("a", class_="sttrank").text).strip())
                
                ## We are going to get 247 rankings data from the prospect page, but nothing else
                prospectFile = prospectDirectory + player['playerName'] + "_" + player['school'] + "_secondhop_" + player['year'] + ".html"
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
                                player['247Rating'] = (rating.select('div[class*="rank-block"]')[0].text)
                            #Stars
                            ratingChildren = []
                            for ratingChild in rating.select('span[class*="icon-starsolid yellow"]'):
                                ratingChildren.append(ratingChild)
                            i = 0
                            for child in ratingChildren:
                                i = i + 1
                            player['247Stars'] = (i)
                            #247 Rankings are stupid
                            ratingValues = rating.find_all("li", class_=None)
                            if (len(ratingValues) >= 3):
                                player['247nationalRank'] = ratingValues[0].find("strong").text
                                player['247positionRank'] = ratingValues[1].find("strong").text
                                player['247stateRank'] = ratingValues[2].find("strong").text   
                            elif (len(ratingValues) == 2):
                                if (ratingValues[0].find("strong") is not None):
                                    player['247positionRank'] = ratingValues[0].find("strong").text
                                if (ratingValues[1].find("strong") is not None):
                                    player['247stateRank'] = ratingValues[1].find("strong").text
                            else:
                                print("Error: " + player['playerName'])
                        count += 1
            if player:
                duplicate = False
                for i in all_recruits:
                    if (player['playerName'].lower().replace(".","").replace(" ", "").replace("'", "") == i['playerName'].lower().replace(".","").replace(" ", "").replace("'", "") ):
                        if (player['year'] == i['year']):
                            duplicate = True
                
                if (not duplicate):
                    all_recruits.append(player)  
    return all_recruits

def summarize_247Sports():
    inputDirectory = '..//scrapedData//'
    dataset = 'sports247'

    ## Load the id config
    idConfig = json.loads(open('..//config//idConfig.json', "r").read())

    ## Load the source file dict
    sourceFiles = json.loads(open('..//config//sourceFiles.json', "r").read())
    
    sports247Data = mergeSourceFiles(dataset, inputDirectory, sourceFiles)
    createNewID(idConfig[dataset], sports247Data, '_')
    final247 = []
    for record in sports247Data:
        finalRecord = {}
        finalRecord['ID'] = record['ID']
        finalRecord['playerName'] = record['displayName']
        finalRecord['year'] = record['year']
        finalRecord['college'] = record['school']
        finalRecord['highSchool'] = record['highSchool']
        finalRecord['homeCity'] = record['city']
        finalRecord['homeState'] = record['state']
        finalRecord['position'] = record['position']
        finalRecord['height'] = record['height']
        finalRecord['weight'] = record['weight']
        finalRecord['comp_stars'] = record['compStars']
        finalRecord['comp_rating'] = record['compRating']
        finalRecord['comp_natlRank'] = record['nationalRank']
        finalRecord['comp_posRank'] = record['positionRank']
        finalRecord['comp_stateRank'] = record['stateRank']
        if ('247Rating' in record.keys()):
            finalRecord['247_rating'] = record['247Rating']
            finalRecord['247_stars'] = record['247Stars']
        if ('247positionRank' in record.keys()):
            finalRecord['247_positionRank'] = record['247positionRank'] 
        if ('247stateRank' in record.keys()):
            finalRecord['247_stateRank'] = record['247stateRank']
        final247.append(finalRecord)

    return final247

def toDB_247Sports(df):
    inputDirectory = '..//scrapedData//'
    dataset = 'sports247'

    ## Load the id config
    idConfig = json.loads(open('..//config//idConfig.json', "r").read())

    ## Load the source file dict
    sourceFiles = json.loads(open('..//config//sourceFiles.json', "r").read())
    
    sports247Data = mergeSourceFiles(dataset, inputDirectory, sourceFiles)
    createNewID(idConfig[dataset], sports247Data, '_')

    connAndWriteDB(df, cc.table247)

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

def process_Rivals(recruitDir, conference, schoolsJSON):
    all_recruits = []
    #error_files = [] 
    for file in os.listdir(recruitDir):

        player = {}
        #get file contents and soup it
        recruitSoup = BeautifulSoup(io.open(recruitDir + file, "r", encoding='windows-1252').read(), 'lxml')

        #find the magical html attr
        if (recruitSoup.find("div", class_="profile-block") is not None):
            recruitInfoJson = recruitSoup.find("div", class_="profile-block")['ng-init']
            #this is harsh - but i'm removing an init() and a trailing id which always seems to be x characters long
            recruitInfo = json.loads(recruitInfoJson[5:-57])

            #player info
            #rawSchool is helpful for troubleshooting rivals school names
            player['rawSchool'] = recruitInfo['school_name']
            player['school'] = checkSchools(recruitInfo['school_name'],conference, schoolsJSON)
            player['year'] = str(recruitInfo['recruit_year'])
            player['playerName'] = recruitInfo['full_name']
            player['city'] = recruitInfo['city']
            player['state'] = recruitInfo['state_abbreviation']
            player['highSchool'] = recruitInfo['highschool_name']        
            player['position'] = recruitInfo['position_group_abbreviation']
            player['height'] = recruitInfo['height']
            player['weight'] = recruitInfo['weight']
            player['stars'] = recruitInfo['stars']
            player['nationalRank'] = recruitInfo['national_rank']
            player['positionRank'] = recruitInfo['position_rank']
            player['stateRank'] = recruitInfo['state_rank']

            if player:
                duplicate = False
                for i in all_recruits:
                    if (player['playerName'].lower().replace(".","").replace(" ", "").replace("'", "") == i['playerName'].lower().replace(".","").replace(" ", "").replace("'", "") ):
                        if (player['year'] == i['year']):
                            duplicate = True
                
                if (not duplicate):
                    all_recruits.append(player)  
        #else:
            #error_files.append(file)
    return all_recruits

def summarize_Rivals():
    inputDir = '..//scrapedData//'
    dataset = 'rivals'

    ## Load the id config
    idConfig = json.loads(open('..//config//idConfig.json', "r").read())

    ## Load the source file dict
    sourceFiles = json.loads(open('..//config//sourceFiles.json', "r").read())

    rivalsData = mergeSourceFiles(dataset, inputDir, sourceFiles)

    createNewID(idConfig[dataset], rivalsData, '_')
    finalRivals = []
    for record in rivalsData:
        try:
            finalRecord = {}
            finalRecord['ID'] = record['ID']
            finalRecord['rivals_stars'] = record['stars']
            finalRecord['rivals_natlRank'] = record['nationalRank']
            finalRecord['rivals_posRank'] = record['positionRank']
            finalRecord['rivals_stateRank'] = record['stateRank']
            finalRivals.append(finalRecord)
        except:
            print(record)
    
    return finalRivals

def toDB_Rivals():
    inputDirectory = '..//scrapedData//'
    dataset = 'rivals'

    ## Load the id config
    idConfig = json.loads(open('..//config//idConfig.json', "r").read())

    ## Load the source file dict
    sourceFiles = json.loads(open('..//config//sourceFiles.json', "r").read())
    
    rivalsData = mergeSourceFiles(dataset, inputDirectory, sourceFiles)
    createNewID(idConfig[dataset], rivalsData, '_')

    df = pd.DataFrame(rivalsData)

    connAndWriteDB(df, cc.tableRivals)

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
            gameSoup = BeautifulSoup(open(rosterDir + file, "r", encoding='utf-8').read(), 'lxml')
            for x in gameSoup.find_all("tr", class_=""): 
                count = 0
                player={}
                player['team'] = file.split("_")[0]
                player['year'] = file.split("_")[1].split(".")[0]
                for z in x.find_all("td"):
                    if (count == 1):
                        player['name'] = z.text.strip()
                    elif (count == 2):
                        player['position'] = z.text.strip()
                    elif (count == 4):
                        player['gamesPlayed'] = z.text.strip()
                    elif (count == 5):
                        player['gamesStarted'] = z.text.strip()
                    count = count + 1
                playerData.append(player)
    
    for player in playerData:
        newName = player['name'].split(',')
        player['name'] = newName[1].strip() + ' ' + newName[0].strip()
    
    return playerData

def summarize_NCAA():
    inputDir = '..//scrapedData//'
    sourceFiles = json.loads(open('..//config//sourceFiles.json', "r").read())
    ncaaData = json.loads(open(inputDir + sourceFiles['ncaa'][0], "r", encoding="utf-8").read())
    idConfig = json.loads(open('..//config//idConfigLink.json', "r").read())

    createNewID(idConfig['ncaa'], ncaaData, '_', True)

    finalOutput = []
    for x in ncaaData:
        if (len(searchID(x['ID'], finalOutput)) == 0):
            playerList = search(x['name'], x['team'], ncaaData)
            finalPlayer = {}
            finalPlayer['ID'] = x['ID']
            finalPlayer['name'] = x['name']
            finalPlayer['team'] = x['team']
            gamesPlayed = 0
            gamesStarted = 0
            year = 2021
            for y in playerList:
                gamesPlayed = gamesPlayed + int(y['gamesPlayed'])
                gamesStarted = gamesStarted + int(y['gamesStarted'])
                if (int(y['year']) < int(year)):
                    year = y['year']
            finalPlayer['gamesPlayed'] = gamesPlayed
            finalPlayer['gamesStarted'] = gamesStarted
            finalPlayer['year'] = year
            finalOutput.append(finalPlayer)
    
    for record in finalOutput:
        record['ncaa_gamesPlayed'] = record['gamesPlayed']
        record['ncaa_gamesStarted'] = record['gamesStarted']
        del record['gamesPlayed']
        del record['gamesStarted']
        del record['year']
        del record['name']
        del record['team']
        if ('position' in record.keys()):
            del record['position']
    
    return(finalOutput)

def toDB_NCAA():
    inputDirectory = '..//scrapedData//'
    dataset = 'ncaa'

    ## Load the id config
    idConfig = json.loads(open('..//config//idConfigLink.json', "r").read())

    ## Load the source file dict
    sourceFiles = json.loads(open('..//config//sourceFiles.json', "r").read())
    ncaaData = json.loads(open(inputDirectory + sourceFiles['ncaa'][0], "r", encoding="utf-8").read())

    createNewID(idConfig[dataset], ncaaData, '_', True)

    df = pd.DataFrame(ncaaData)

    connAndWriteDB(df, cc.tableNCAA)

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

def process_WikipediaBigTenBigTwelve(teamDir):
    all_players = []
    for folder in teamDir:
        for file in os.listdir(folder):
            wikiSoup = BeautifulSoup(open(folder + file, "r", encoding="utf-8").read(), 'lxml')
            y = file.split('.')[0]
            
            for x in wikiSoup.find_all("li", class_=""):
                if ((",") in x.text and ("(") in x.text and (")") in x.text and ("{") not in x.text \
                    and ("Archived") not in x.text and ("Gridiron") not in x.text and ("edited") not in x.text \
                    and ("all-purpose") not in x.text and ("Football") not in x.text and ("Coaches")):
                    player = {}
                    playerInfo = x.text.split(",", 1)
                    #year
                    player['year'] = y
                    #name
                    player['playerName'] = playerInfo[0]
                    #schoolAndAwardsFX
                    playerSAF = playerInfo[1].split("(", 1)
                    if(len(playerSAF) > 1):
                        #school
                        player['school'] = playerSAF[0].strip()
                        #AwardsFX
                        playerAwards = playerSAF[1].split(";")
                        if ("Coaches" in playerAwards[0] and len(playerAwards)> 1):
                            playerCoaches = playerAwards[0].split("-", 1)
                            player['coaches'] = playerCoaches[1][:1]           
                        elif ("Coaches" in playerAwards[0] and len(playerAwards) == 1):
                            playerCoaches = playerAwards[0].split("-", 1)
                            player['coaches'] = playerCoaches[1][:-1]
                            player['media'] = "0"
                        elif ("Media" in playerAwards[0]):
                            playerMedia = playerAwards[0].split("-", 1)
                            player['coaches'] = "0"
                            player['media'] = playerMedia[1][:1]
                        if (len(playerAwards) > 1 and ("Media" in playerAwards[1])):
                            playerMedia = playerAwards[1].split("-", 1)
                            player['media'] = playerMedia[1][:1]
                        else:
                            player['media'] = "0"
                        all_players.append(player)
    
    for player in all_players:
        if (len(player['coaches']) == 3):
            player['coaches'] == player['coaches'][0]
        if (len(player['media']) == 3):
            player['media'] == player['media'][0]
        
        player['team'] = max(player['coaches'], player['media'])

    for player in all_players:
        del player['coaches']
        del player['media']
    
    return all_players

def process_WikipediaSEC(teamDir):
    all_players = []
    for file in os.listdir(teamDir):
        wikiSoup = BeautifulSoup(open(teamDir + file, "r", encoding="utf-8").read(), 'lxml')
        y = file.split('.')[0]
        for x in wikiSoup.find_all("li", class_=""):
            if ((",") in x.text and ("(") in x.text and (")") in x.text and ("{") not in x.text \
                and ("Archived") not in x.text and ("^") not in x.text and ("Gridiron") not in x.text \
                and ("edited") not in x.text \
                and ("all-purpose") not in x.text and ("Football") not in x.text and ("Coaches")):
                player = {}
                playerInfo = x.text.split(",", 1)
                #year
                player['year'] = y
                #name
                player['playerName'] = playerInfo[0]
                #schoolAndAwardsFX
                playerSAF = playerInfo[1].split("(", 1)
                if(len(playerSAF) > 1):
                    #school
                    player['school'] = playerSAF[0].strip()
                    #AwardsFX
                    if ("," in playerSAF[1]):
                        playerAwards = playerSAF[1].split(",")
                    if (";" in playerSAF[1]):
                        playerAwards = playerSAF[1].split(",")
                    if ("AP" in playerAwards[0] and len(playerAwards)> 1):
                        playerCoaches = playerAwards[0].split("-", 1)
                        player['coaches'] = playerCoaches[1][:1]              
                    elif ("AP" in playerAwards[0] and len(playerAwards) == 1):
                        playerCoaches = playerAwards[0].split("-", 1)
                        player['coaches'] = playerCoaches[1][:-1] 
                        player['media'] = "0"
                    elif ("Coaches" in playerAwards[0]):
                        playerMedia = playerAwards[0].split("-", 1)
                        player['coaches'] = "0"
                        player['media'] = playerMedia[1][:1]
                    if (len(playerAwards) > 1 and ("Coaches" in playerAwards[1])):
                        playerMedia = playerAwards[1].split("-", 1)
                        player['media'] = playerMedia[1][:1]
                    else:
                        player['media'] = "0"
                    all_players.append(player)   

    for player in all_players:
        if (len(player['coaches']) == 3):
            player['coaches'] == player['coaches'][0]
        if (len(player['media']) == 3):
            player['media'] == player['media'][0]
    
        player['team'] = max(player['coaches'], player['media'])

    for player in all_players:
        del player['coaches']
        del player['media']
    
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
            record['playerName'] = record['First Name'] + ' ' + record['Last Name']
            record['school'] = record['School']
            record['position'] = record['POSITION']
            record['year'] = record['Year']
            record['team'] = record['Team']
            if (len(record['team']) > 1 ):
                record['team'] = record['team'][0]
        except:
            print (record)

    for record in records:
        del record['School']
        del record['POSITION']
        del record['First Name']
        del record['Last Name']
        del record['Year']
        del record['Team']
    
    return records

def summarize_allConf():
    inputDirectory = '..//scrapedData//'
    dataset = 'allConf'

    ## Load the id config
    idConfig = json.loads(open('..//config//idConfigLink.json', "r").read())

    ## Load the source file dict
    sourceFiles = json.loads(open('..//config//sourceFiles.json', "r").read())
    
    allConfData = mergeSourceFiles(dataset, inputDirectory, sourceFiles)
    createNewID(idConfig[dataset], allConfData, '_')
    finalOutput = []
    for x in allConfData:
        if (len(searchID(x['ID'], finalOutput)) == 0):
            try:
                playerList = searchAllConf(x['playerName'], x['school'], allConfData)
                finalPlayer = {}
                finalPlayer['ID'] = x['ID']
                finalPlayer['team'] = x['team']
            
            except:
                print (x)
            
            finalOutput.append(finalPlayer)
    
    return finalOutput

def toDB_AllConference():
    inputDirectory = '..//scrapedData//'
    dataset = 'allConf'

    ## Load the id config
    idConfig = json.loads(open('..//config//idConfigLink.json', "r").read())

    ## Load the source file dict
    sourceFiles = json.loads(open('..//config//sourceFiles.json', "r").read())
    allConfData = mergeSourceFiles(dataset, inputDirectory, sourceFiles)

    createNewID(idConfig[dataset], allConfData, '_')

    df = pd.DataFrame(allConfData)

    connAndWriteDB(df, cc.tableAllConference)

    return 'DB Write is done'

# ---------------------------------------------------------------------------------------------------------------------------------------
# Sports Reference - NFL Draft Specific Functions
# NOTE: we don't keep the html files locally for this dataset, so get/process are one step
# ---------------------------------------------------------------------------------------------------------------------------------------

def handle_nflData(years, headers, sleepyTime=10):
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
                draftPick.append(x.find("td", attrs={"data-stat":"college_id"}).text)
                all_picks.append(draftPick)
        time.sleep(sleepyTime)
    
    final_nflDraft = []
    nfl_keys = ['year', 'draft_round', 'draft_pick', 'team', 'playerName', 'pos', 'all_pros_first_team', 'pro_bowls', 'years_as_primary_starter', 'g', 'college_id']

    for list in all_picks:
        newdict = {nfl_keys[i]: list[i] for i in range(len(nfl_keys))}
        final_nflDraft.append(newdict)
        newdict = {}
    
    return final_nflDraft

def summarize_nflDraft ():
    inputDirectory = '..//scrapedData//'
    sourceFiles = json.loads(open('..//config//sourceFiles.json', "r").read())
    nflData = json.loads(open(inputDirectory + sourceFiles['nflData'][0], "r", encoding="utf-8").read())
    idConfig = json.loads(open('..//config//idConfigLink.json', "r").read())

    createNewID(idConfig['nflData'], nflData, '_', True)

    for x in nflData:
        x['draft_year'] = x['year']
    
    for record in nflData:
        del record['year']
        del record['playerName']
        del record['college_id']

    return nflData

def toDB_NFLDraft():
    inputDirectory = '..//scrapedData//'
    dataset = 'nflData'

    ## Load the id config
    idConfig = json.loads(open('..//config//idConfigLink.json', "r").read())

    ## Load the source file dict
    sourceFiles = json.loads(open('..//config//sourceFiles.json', "r").read())
    nflData = json.loads(open(inputDirectory + sourceFiles['nflData'][0], "r", encoding="utf-8").read())

    createNewID(idConfig[dataset], nflData, '_')

    df = pd.DataFrame(nflData)

    connAndWriteDB(df, cc.tableNFLDraft)

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
                    player.append(y)
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
                        player.append("1")
                    else:
                        player.append("0")
                    #associated press (AP)
                    if ("AP" in awardString):
                        player.append("1")
                    else:
                        player.append("0")
                    #writers (FWAA)
                    if ("FWAA" in awardString):
                        player.append("1")
                    else:
                        player.append("0")
                    #sporting news (TSN)
                    if ("TSN" in awardString):
                        player.append("1")
                    else:
                        player.append("0")
                    #walter camp (WCFF)
                    if ("WCFF" in awardString):
                        player.append("1")
                    else:
                        player.append("0")
                    all_players.append(player)
            except:
                print(x)
    time.sleep(sleepyTime)

    final_aaSelections = []
    aa_keys = ['year', 'playerName', 'school', 'afca', 'ap', 'fwaa', 'tsn', 'wcff']

    for list in all_players:
        newdict = {aa_keys[i]: list[i] for i in range(len(aa_keys))}
        final_aaSelections.append(newdict)
        newdict = {}
    
    return final_aaSelections

def summarize_allAmerican():
    inputDirectory = '..//scrapedData//'
    sourceFiles = json.loads(open('..//config//sourceFiles.json', "r").read())
    aaData = json.loads(open(inputDirectory + sourceFiles['allAmerican'][0], "r", encoding="utf-8").read())
    idConfig = json.loads(open('..//config//idConfigLink.json', "r").read())

    createNewID(idConfig['allAmerican'], aaData, '_', True)

    finalOutput = []
    for x in aaData:
        if (len(searchID(x['ID'], finalOutput)) == 0):
            playerList = searchAllConf(x['playerName'], x['school'], aaData)
            finalPlayer = {}
            finalPlayer['ID'] = x['ID']
            finalPlayer['playerName'] = x['playerName']
            finalPlayer['school'] = x['school']
            year = 2021
            afca = ap = fwaa = tsn = wcff = 0
            for y in playerList:
                afca = afca + int(y['afca'])
                ap = ap + int(y['ap'])
                fwaa = fwaa + int(y['fwaa'])
                tsn = tsn + int(y['tsn'])
                wcff = wcff + int(y['wcff'])
                if (int(y['year']) < int(year)):
                    year = y['year']
            finalPlayer['afca'] = afca
            finalPlayer['ap'] = ap
            finalPlayer['fwaa'] = fwaa
            finalPlayer['tsn'] = tsn
            finalPlayer['wcff'] = wcff
            finalPlayer['year'] = year
            finalOutput.append(finalPlayer)

    for record in finalOutput:
        del record['year']
        del record['playerName']
        del record['school']
    
    return finalOutput

def toDB_AllAmerican():
    inputDirectory = '..//scrapedData//'
    dataset = 'allAmerican'

    ## Load the id config
    idConfig = json.loads(open('..//config//idConfigLink.json', "r").read())

    ## Load the source file dict
    sourceFiles = json.loads(open('..//config//sourceFiles.json', "r").read())
    aaData = json.loads(open(inputDirectory + sourceFiles['allAmerican'][0], "r", encoding="utf-8").read())

    createNewID(idConfig[dataset], aaData, '_')

    df = pd.DataFrame(aaData)

    connAndWriteDB(df, cc.tableAllAmerican)

    return 'DB Write is done'