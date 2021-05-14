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

def get_247ProspectProfiles (conference, playerDirectory, headers):
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
                time.sleep(3) 