import streamlit as st
import pymongo

def init_connection():
    return pymongo.MongoClient("mongodb://localhost:32768/")

mongoClient = init_connection()


#Read data fro streamlit.
@st.cache_data(ttl=600)
def read_data_races():
    db = mongoClient["local"]
    races = db['Races'].find()
    races = list(races)
    return races

races = read_data_races()

@st.cache_data(ttl=600)
def read_data_drivers():
    db = mongoClient["local"]
    drivers = db['Drivers'].find()
    drivers = list(drivers)
    return drivers

drivers = read_data_drivers()

@st.cache_data(ttl=600)
def read_data_standings():
    db = mongoClient["local"]
    standings = db['StandingList'].find()
    standings = list(standings)
    return standings

standings = read_data_standings()

st.write('Showing data for races.')
st.table(races )
st.write('Showing chart for standings.')
st.bar_chart(standings, y='position',x=['Driver']['permanentNumber'])