from bs4 import BeautifulSoup
import requests
import lxml
import time
import os
import core_constants as cc

#247 Functions

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
    for file in os.listdir(playerDirectory):
        print(file)
        gameSoup = BeautifulSoup(open(playerDirectory + file, "r", encoding='utf-8').read(), 'lxml')
        fileName = file.split('_')
        playerName = fileName[0]
        team = fileName[1]
        year = fileName[3].split('.')[0]
        filenameString = cc.get_htmlDir('247', conference, 'prospects') + playerName + "_" + team + "_secondhop_" + year + ".html"
        if (os.path.isfile(filenameString)):
            print("file exists")
        else:
            if (gameSoup.find("a", class_="view-profile-link") is not None):
                prospectLink = gameSoup.find("a", class_="view-profile-link")['href']
                req = requests.get(prospectLink, headers=headers)
                filePath = "..//html//247//" + conference + "//prospects//" + playerName + "_" + team + "_secondhop_" + year + ".html"
                cc.save_html(filePath, req.text)
                time.sleep(sleepyTime)

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