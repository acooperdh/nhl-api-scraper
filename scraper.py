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


def get_pbp(game_id: str) -> DataFrame:
    url = f"https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play"
    res = r.get(url)
    data = res.json()
    data = pd.json_normalize(data, "plays")
    data["GameID"] = game_id
    return data


def fill_missing_pbp_data(pbp: DataFrame) -> DataFrame:
    pbp["LINK_PBP"] = pbp.get("LINK_PBP", "")
    pbp["situationCode"] = pbp.get("situationCode", "")
    pbp["homeTeamDefendingSide"] = pbp.get("homeTeamDefendingSide", "")
    pbp["details.losingPlayerId"] = pbp.get("details.losingPlayerId", "")
    pbp["details.winningPlayerId"] = pbp.get("details.winningPlayerId", "")
    pbp["details.xCoord"] = pbp.get("details.xCoord", "")
    pbp["details.yCoord"] = pbp.get("details.yCoord", "")
    pbp["details.zoneCode"] = pbp.get("details.zoneCode", "")
    pbp["details.reason"] = pbp.get("details.reason", "")
    pbp["details.hittingPlayerId"] = pbp.get("details.hittingPlayerId", "")
    pbp["details.hitteePlayerId"] = pbp.get("details.hitteePlayerId", "")
    pbp["details.playerId"] = pbp.get("details.playerId", "")
    pbp["details.shotType"] = pbp.get("details.shotType", "")
    pbp["details.shootingPlayerId"] = pbp.get("details.shootingPlayerId", "")
    pbp["details.awaySOG"] = pbp.get("details.awaySOG", "")
    pbp["details.homeSOG"] = pbp.get("details.homeSOG", "")
    pbp["details.blockingPlayerId"] = pbp.get("details.blockingPlayerId", "")
    pbp["details.assist2PlayerId"] = pbp.get("details.assist2PlayerId", "")
    pbp["details.assist2PlayerTotal"] = pbp.get("details.assist2PlayerTotal", "")
    pbp["details.secondaryReason"] = pbp.get("details.secondaryReason", "")
    pbp["details.typeCode"] = pbp.get("details.typeCode", "")
    pbp["details.drawnByPlayerId"] = pbp.get("details.drawnByPlayerId", "")
    pbp["details.serverByPlayerId"] = pbp.get("details.serverByPlayerId", "")
    pbp = pbp[
        [
            "GameID",
            "LINK_PBP",
            "eventId",
            "periodDescriptior.number",
            "timeInPeriod",
            "situationCode",
            "homeTeamDefendingSide",
            "typeCode",
            "typeDescKey",
            "sortOrder",
            "details.eventOwnerTeamId",
            "details.losingPlayerId",
            "details.winningPlayerId",
            "details.xCoord",
            "details.yCoord",
            "details.zoneCode",
            "details.reason",
            "details.hittingPlayerId",
            "details.hitteePlayerId",
            "details.playerId",
            "details.shotType",
            "details.shootingPlayerId",
            "details.goalieInNetId",
            "details.awaySOG",
            "details.homeSOG",
            "details.blockingPlayerId",
            "details.scoringPlayerId",
            "details.scroingPlayerTotal",
            "details.assist1PlayerId",
            "details.assist1PlayerTotal",
            "details.assist2PlayerId",
            "details.assist2PlayerTotal",
            "details.awayScore",
            "details.homeScore",
            "details.secondaryReason",
            "details.typeCode",
            "details.descKey",
            "details.duration",
            "details.commitedByPlayerId",
            "details.drawnByPlayerId",
            "details.servedByPlayerId",
        ]
    ]
    pbp = pbp.convert_dtypes()
    return pbp


def get_pbp_for_season(schedule):
    pbp = []
    for game in schedule["id"]:
        df = get_pbp(game)
        pbp.append(df)
    pbp = pd.concat(pbp, ignore_index=True)
    return None


def get_shifts(game_id: str) -> DataFrame:
    url = f"https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId={game_id}"
    response = r.get(url)
    data = response.json()
    data = pd.json_normalize(data, "data")
    return data


def get_shifts_for_season(schedule: DataFrame) -> DataFrame:
    shifts = []
    for game in schedule["id"]:
        shift = get_shifts(game)
        shifts.append(shifts)
    shifts = pd.concat(shifts, ignore_index=True)
    return shifts


def fill_missing_shifts_data(shifts: DataFrame) -> DataFrame:
    shifts["gameId"] = shifts.get("gameId", "")
    shifts["endTime"] = shifts.get("endTime", "")
    shifts["period"] = shifts.get("period", "")
    shifts["playerId"] = shifts.get("playerId", "")
    shifts["shiftNumber"] = shifts.get("shiftNumber", "")
    shifts["startTime"] = shifts.get("startTime", "")
    shifts["teamAbbrev"] = shifts.get("teamAbbrev", "")
    shifts = shifts[
        [
            "gameId",
            "endTime",
            "period",
            "playerId",
            "shiftNumber",
            "startTime",
            "teamAbbrev",
        ]
    ]
    shifts = shifts.convert_dtypes()
    return shifts


def write_to_csv(file_name: str, data: DataFrame) -> None:
    data.to_csv(file_name, index=False, header=True)
    return
