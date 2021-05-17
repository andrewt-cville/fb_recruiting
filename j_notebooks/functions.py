from bs4 import BeautifulSoup
import requests
import lxml
import time
import os
import io
import json
import core_constants as cc

#Common Functions

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
def createNewID (fieldList, thisDict, fieldAgg):
    finalID= ''
    for i in thisDict:
        i['displayName'] = i['playerName']
        for idx, val in enumerate(fieldList):
            if (type(i[val]) is list):
                i[val]= mungeID(i[val][0])
                if (len(fieldList) -1 == idx):
                    finalID += str(i[val]).strip('[]').strip("''")
            elif (type(val) is not list):
                i[val] = mungeID(i[val])
                if (len(fieldList) - 1 == idx):
                    finalID += i[val]
                else:
                    finalID = i[val] + fieldAgg
        i['ID'] = finalID
        finalID=''

#247Sports Specific Functions

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
            print('Ive processed ' + count + ' files.')
        count = count + 1

def process_247Sports(prospectDirectory, teamDirectory):
    all_recruits = []

    for file in os.listdir(teamDirectory):
        gameSoup = BeautifulSoup(open(teamDirectory + file, "r", encoding='utf-8').read(), 'lxml')
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

# Rivals Specific Functions
def get_Rivals(conference, schoolsJSON, years, headers, sleepyTime=4):
    all_recruits = []
    for y in years:
        for school in schoolsJSON:
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
                    x = json.loads(commitments)

                for commit in x:
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
    #print (recruitSchoolCleaned)
    for school in schoolsJSON:
        if (conference in school['conference']):
            #print('rivals: ' + school['rivals']))
            #print('recruit: ' + recruitSchoolCleaned)
            if ('rivalsDisplay' in school.keys() and recruitSchool == school['rivalsDisplay']):
                return school['id']

def process_Rivals(recruitDir, conference, schoolsJSON):
    all_recruits = []
    #error_files = [] 
    for file in os.listdir(recruitDir):

        player = {}
        #get file contents and soup it
        recruitSoup = BeautifulSoup(io.open(recruitDir + file, "r", encoding='utf-8').read(), 'lxml')

        #find the magical html attr
        if (recruitSoup.find("div", class_="profile-block") is not None):
            recruitInfoJson = recruitSoup.find("div", class_="profile-block")['ng-init']
            #this is harsh - but i'm removing an init() and a trailing id which always seems to be x characters long
            recruitInfo = json.loads(recruitInfoJson[5:-57])

            #player info
            #change the rivals school to the proper id
            #for school in schools:
            #    print(school['rivals'] + " and " + recruitInfo['school_name'] )
            #    if (recruitInfo['school_name'] == school['rivals']):
            #        player['school'] = school['id']
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

            all_recruits.append(player)
        #else:
            #error_files.append(file)
    return all_recruits