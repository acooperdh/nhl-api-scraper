import pandas as pd
from pandas import DataFrame
import requests as r

seasons = ["20222023", "20232024", "20242025"]


def get_skaters(season: str) -> DataFrame:
    df = r.get(
        f"https://api.nhle.com/stats/rest/en/skater/bios?limit=-1&start=0&cayenneExp=seasonId={season}"
    )
    df = df.json()
    df = pd.json_normalize(df, "data")
    # rename 'skaterFullName' to 'name' and select and order columns
    df = df.rename(columns={"skaterFullName": "Name"})
    df = df[
        [
            "birthDate",
            "draftOverall",
            "draftYear",
            "height",
            "nationalityCode",
            "playerId",
            "shootsCatches",
            "weight",
            "positionCode",
            "Name",
        ]
    ]
    return df


def get_goalies(season: str) -> DataFrame:
    df = r.get(
        f"https://api.nhle.com/stats/rest/en/goalies/bios?limit=-1&start=0&cayenneExp=seasonId={season}"
    )
    df = df.json()
    df = pd.json_normalize(df, "data")
    # rename 'skaterFullName' to 'name' and select and order columns
    df = df.rename(columns={"goalieFullName": "Name"})
    df["positionCode"] = "G"
    df = df[
        [
            "birthDate",
            "draftOverall",
            "draftYear",
            "height",
            "nationalityCode",
            "playerId",
            "shootsCatches",
            "weight",
            "positionCode",
            "Name",
        ]
    ]
    return df


def get_players(season: str):
    df = pd.concat(get_skaters(season), get_goalies(season))
    df["Season"] = season
    # reorder columns so they start with the season
    df = df[
        [
            "Season",
            "birthDate",
            "draftOverall",
            "draftYear",
            "height",
            "nationalityCode",
            "playerId",
            "shootsCatches",
            "weight",
            "positionCode",
            "Name",
        ]
    ]
    # convert datatypes to avoid floats
    df = df.convert_dtypes()
    df = df.drop_duplicates(subset=["playerId"])
    return df


def get_teams() -> DataFrame:
    df = r.get("https://api.nhle.com/stats/rest/en/team")
    df = df.json()
    df = pd.json_normalize(df, "data")
    df = df.convert_dtypes()
    return df


def get_schedule(season: str, team_tri_code: str) -> DataFrame:
    url = f"https://api-web.nhle.com/v1/club-schedule-season/{team_tri_code}/{season}"
    res = r.get(url)
    df = res.json()
    df = pd.json_normalize(df, "games")
    return df


def get_schedules(season: str, teams: DataFrame) -> DataFrame:
    schedule = []
    for triCode in teams["triCode"]:
        schedule.append(get_schedule(triCode))
    schedule = pd.concat(schedule, ignore_index=True)
    schedule = schedule.drop_duplicates(subset=["id"])
    # select and order columns and convert datatypes to avoid floats
    schedule = schedule[
        [
            "id",
            "season",
            "gameType",
            "gameDate",
            "startTimeUTC",
            "gameState",
            "awayTeam.abbrev",
            "awayTeam.score",
            "homeTeam.abbrev",
            "homeTeam.score",
            "gameOutcome.lastPeriodType",
        ]
    ]
    schedule = schedule.convert_dtype()

    # remove preseason and playoff games
    no_preseason = schedule["gameType"].isin([2, 3])
    schedule = schedule[no_preseason]
    schedule = schedule.sort_values(by=["id"])
    return schedule


def get_pbp(game_id: str) -> list:
    pbp = []
    url = f"https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play"
    res = r.get(url)
    data = res.json()
    data = pd.json_normalize(data, "plays")
    data["GameID"] = game_id
    pbp.append(data)
    return pbp


def get_pbp_for_season(season: str, schedule: DataFrame) -> None:
    return None


def get_pbp_for_season(season, schedule):
    return None
