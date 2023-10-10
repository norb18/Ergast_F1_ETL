import urllib.request, json, requests, pandas as pd
import time
import duckdb as ddb
import pyodbc

response = requests.api.get("https://ergast.com/api/f1/2020/drivers.json")

def get_response(url: str):
    response = requests.api.get(f'https://ergast.com/api/f1/{url}.json')
    return response

def handle_drivers(api_endpoint):
    url = f'https://ergast.com/api/f1/{api_endpoint}.json'
    response = requests.api.get(url)
    json_drivers = response.json()
    drivers_df = pd.json_normalize(json_drivers['MRData']['DriverTable']['Drivers'])
    return drivers_df

def handle_standingList(api_endpoint):
    url = f'https://ergast.com/api/f1/{api_endpoint}.json'
    response = requests.api.get(url)
    json_standinglist = response.json()
    standings_df = pd.json_normalize(json_standinglist['MRData']['StandingsTable']['StandingsLists'][0]['DriverStandings'])
    return standings_df

def handle_races(api_endpoint):
    url = f'https://ergast.com/api/f1/{api_endpoint}.json'
    response = requests.api.get(url)
    json_races = response.json()
    races_df = pd.json_normalize(json_races['MRData']['RaceTable']['Races'])
    return races_df

def handle_circuits(api_endpoint):
    url = f'https://ergast.com/api/f1/{api_endpoint}.json'
    response = requests.api.get(url)
    json_circuits = response.json()
    circuits_df = pd.json_normalize(json_circuits['MRData']['CircuitTable']['Circuits'])
    return circuits_df