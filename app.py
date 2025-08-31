import streamlit as st
import pandas as pd
from pymongo import MongoClient

# MongoDB connection
MONGO_URI = "mongodb+srv://ragbhuvan_db_user:Bhuvanrag123@cluster0.vhjf9di.mongodb.net/census_database"
client = MongoClient(MONGO_URI)
db = client["census_database"]
collection = db["fact_census"]

# Load data into a DataFrame
data = list(collection.find({}, {"_id": 0}))  # exclude Mongo _id
df = pd.DataFrame(data)

st.title("Census Dashboard (MongoDB Atlas)")
st.dataframe(df.head())

