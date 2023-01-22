def normalizeAACollege(recruitSchool, schoolsJSON):
    college = ""
    for school in schoolsJSON:
        if ('wikipedia' in school.keys()):
            recruitSchool = recruitSchool.lower().lstrip().rstrip()
            if (recruitSchool == school['wikipedia']):
                college = school['id']
                break
    if college != '':
        return college
    else:
        return recruitSchool

def checkSchools(recruitSchool, conference, schools = getSchools()):
    for school in schools:
        if (conference in school['conference']):
            if ('rivalsDisplay' in school.keys() and recruitSchool == school['rivalsDisplay']):
                return school['id']

#EXCERPT from get_NCAA()
for x in schoolsList:
    if ('ncaa' in x.keys()):
        thisUrl = {}
        thisUrl['team'] = x['id']
        thisUrl['conference'] = x['conference']
        thisUrl['ncaa'] = 'http://stats.ncaa.org/team/' + x['ncaa'] + '/roster/'
        urlDict.append(thisUrl)

def normalizeNFLCollege(recruitSchool, schoolsJSON):
    college = ""
    for school in schoolsJSON:
        if ('nfl-ref' in school.keys()):
            if (recruitSchool == school['nfl-ref']):
                college = school['id']
    
    if college != '':
        return college
    else:
        return recruitSchool

def normalizeAACollege(recruitSchool, schoolsJSON):
    college = ""
    for school in schoolsJSON:
        if ('wikipedia' in school.keys()):
            recruitSchool = recruitSchool.lower().lstrip().rstrip()
            if (recruitSchool == school['wikipedia']):
                college = school['id']
                break
    if college != '':
        return college
    else:
        return recruitSchool