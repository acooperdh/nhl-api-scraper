import pandas as pd 
import requests as r

seasons = [
    "20222023",
    "20232024",
    "20242025"
]

def get_skaters(season):
    df = r.get(f'https://api.nhle.com/stats/rest/en/skater/bios?limit=-1&start=0&cayenneExp=seasonId={season}')
    df = df.json()
    df = pf.json_normalize(df, "data")
    # rename 'skaterFullName' to 'name' and select and order columns 
    df = df.rename(columns={"skaterFullName":"Name"})
    df = df[['birthDate','draftOverall','draftYear','height','nationalityCode','playerId','shootsCatches','weight','positionCode','Name']]
    return df

def get_goalies(season):
    df = r.get(f'https://api.nhle.com/stats/rest/en/goalies/bios?limit=-1&start=0&cayenneExp=seasonId={season}')
    df = df.json()
    df = pf.json_normalize(df, "data")
    # rename 'skaterFullName' to 'name' and select and order columns 
    df = df.rename(columns={"goalieFullName":"Name"})
    df['positionCode'] = 'G'
    df = df[['birthDate','draftOverall','draftYear','height','nationalityCode','playerId','shootsCatches','weight','positionCode','Name']]
    return df

def get_players(season):
    df = pd.concat(get_skaters(season), get_goalies(season))
    df['Season'] = season
    # reorder columns so they start with the season
    df = df[['Season','birthDate','draftOverall','draftYear','height','nationalityCode','playerId','shootsCatches','weight','positionCode','Name']]
    # convert datatypes to avoid floats
    df = df.convert_dtypes()
    df = df.drop_duplicates(subset=['playerId'])
    return df

def get_teams():
    df = r.get('https://api.nhle.com/stats/rest/en/team')
    df = df.json()
    df = pd.json_normalize(df, "data")
    df = df.convert_dtypes()
    return df

def get_schedules(season, teams):
    return None

def get_schedule(season, team):
    url = f'https://api-web.nhle.com/v1/club-schedule-season/{team['triCode']}/{season}'
    res = r.get(url)
    df = res.json()
    df = pd.json_normalize(df, "games")
    return df
