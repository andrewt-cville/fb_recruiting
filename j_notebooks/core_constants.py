import requests
import json

def get_defYears():
    return ['2002', '2003', '2004', '2005', '2006', '2007', '2008','2009','2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019', '2020']

def get_header():
    return {'user-agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36'}

def get_schoolsList():
    return json.loads(open('..//config//schools.json', "r").read())

def create_url247(level, school_id, year):
    if (level == 'team'):
        return 'https://247sports.com/college/{}/Season/{}-Football/Commits/'.format(school_id,year)
    else:
        print('ERROR: appropriate levels are team, prospect and recruit')

def get_htmlDir(source, conference, level):
    return "..//html//{}//{}//{}//".format(source, conference, level)

def get_availableConferences():
    return ['bigten', 'bigtwelve', 'acc', 'sec', 'pactwelve', 'american', 'independents', 'cusa', 'mac']

def save_files(filePath, filePersist):
    with open(filePath, "w") as write_file:
        json.dump(filePersist, write_file)

def save_html(filePath, reqText):
    with open(filePath, "w") as write_file:
        write_file.write(reqText)

def save_records(folder, filename, listPersist):
    with open("..//{}//{}.json".format(folder, filename), "w") as write_file:
        json.dump(listPersist, write_file)
