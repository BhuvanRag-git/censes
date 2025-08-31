import streamlit as st
import pandas as pd
from pymongo import MongoClient

# -------------------------
# MongoDB Connection
# -------------------------
MONGO_URI = "mongodb+srv://ragbhuvan_db_user:Bhuvanrag123@cluster0.vhjf9di.mongodb.net/census_database"
client = MongoClient(MONGO_URI)
db = client["census_data"]
collection = db["fact_census"]

st.set_page_config(page_title="Census Dashboard", layout="wide")

st.title("📊 Census Dashboard (MongoDB Atlas)")

# Helper to run aggregation and return DataFrame
def run_query(pipeline):
    docs = list(collection.aggregate(pipeline))
    if not docs:
        return pd.DataFrame()
    return pd.DataFrame(docs)


# -------------------------
# Query 1: Total population of each district
# -------------------------
st.subheader("1️⃣ Total population of each district")
df1 = run_query([
    {"$group": {"_id": "$District name", "Total_Population": {"$sum": "$Population"}}}
])
st.dataframe(df1)


# -------------------------
# Query 2: Literate males and females in each district
# -------------------------
st.subheader("2️⃣ Literate males and females in each district")
df2 = run_query([
    {"$group": {
        "_id": "$District name",
        "Literate_Males": {"$sum": "$Male_Literate"},
        "Literate_Females": {"$sum": "$Female_Literate"}
    }}
])
st.dataframe(df2)


# -------------------------
# Query 3: Percentage of workers in each district
# -------------------------
st.subheader("3️⃣ Percentage of workers in each district")
df3 = run_query([
    {"$group": {
        "_id": "$District name",
        "Workers": {"$sum": "$Workers"},
        "Population": {"$sum": "$Population"}
    }},
    {"$project": {
        "Percentage_Workers": {"$multiply": [{"$divide": ["$Workers", "$Population"]}, 100]}
    }}
])
st.dataframe(df3)


# -------------------------
# Query 4: Gender ratio (Females per 1000 Males)
# -------------------------
st.subheader("4️⃣ Gender ratio in each district (Females per 1000 Males)")
df4 = run_query([
    {"$group": {
        "_id": "$District name",
        "Males": {"$sum": "$Male"},
        "Females": {"$sum": "$Female"}
    }},
    {"$project": {
        "Gender_Ratio": {"$multiply": [{"$divide": ["$Females", "$Males"]}, 1000]}
    }}
])
st.dataframe(df4)


# -------------------------
# Query 5: Literacy rate in each district
# -------------------------
st.subheader("5️⃣ Literacy rate in each district")
df5 = run_query([
    {"$group": {
        "_id": "$District name",
        "Literate": {"$sum": "$Literate"},
        "Population": {"$sum": "$Population"}
    }},
    {"$project": {
        "Literacy_Rate": {"$multiply": [{"$divide": ["$Literate", "$Population"]}, 100]}
    }}
])
st.dataframe(df5)


# -------------------------
# Query 6: Rural vs Urban households
# -------------------------
st.subheader("6️⃣ Rural vs Urban households in each district")
df6 = run_query([
    {"$group": {
        "_id": "$District name",
        "Rural_Households": {"$sum": "$Rural_Households"},
        "Urban_Households": {"$sum": "$Urban_Households"}
    }}
])
st.dataframe(df6)


# -------------------------
# Query 7: Age distribution (0-29, 30-49, 50+)
# -------------------------
st.subheader("7️⃣ Age distribution in each district")
df7 = run_query([
    {"$group": {
        "_id": "$District name",
        "Young_and_Adult": {"$sum": "$Age_Group_0_29"},
        "Middle_Aged": {"$sum": "$Age_Group_30_49"},
        "Senior_Citizen": {"$sum": "$Age_Group_50"}
    }}
])
st.dataframe(df7)


# -------------------------
# Query 8: Households with LPG or PNG
# -------------------------
st.subheader("8️⃣ Households with LPG/PNG")
df8 = run_query([
    {"$group": {
        "_id": "$District name",
        "LPG_Households": {"$sum": "$LPG_or_PNG_Households"}
    }}
])
st.dataframe(df8)


# -------------------------
# Query 9: Households with Internet
# -------------------------
st.subheader("9️⃣ Households with Internet")
df9 = run_query([
    {"$group": {
        "_id": "$District name",
        "Internet_Households": {"$sum": "$Households_with_Internet"}
    }}
])
st.dataframe(df9)


# -------------------------
# Query 10: Households with TV, Computer, Phone & Vehicle
# -------------------------
st.subheader("🔟 Households with TV, Computer, Phone & Vehicle")
df10 = run_query([
    {"$group": {
        "_id": "$District name",
        "HH_TV_PC_Phone_Vehicle": {"$sum": "$Households_with_TV_Computer_Laptop_Telephone_mobile_phone_and_Scooter_Car"}
    }}
])
st.dataframe(df10)


# -------------------------
# Query 11: Dilapidated houses
# -------------------------
st.subheader("1️⃣1️⃣ Dilapidated households")
df11 = run_query([
    {"$group": {
        "_id": "$District name",
        "HH_Dilapidated": {"$sum": "$Condition_of_occupied_census_houses_Dilapidated_Households"}
    }}
])
st.dataframe(df11)


# -------------------------
# Query 12: Households with Latrine facilities
# -------------------------
st.subheader("1️⃣2️⃣ Households with latrine facilities")
df12 = run_query([
    {"$group": {
        "_id": "$District name",
        "Flush_Other": {"$sum": "$Type_of_latrine_facility_Flush_pour_flush_latrine_connected_to_other_system_Households"}
    }}
])
st.dataframe(df12)


# -------------------------
# Query 13: Drinking water from wells, handpumps, borewells
# -------------------------
st.subheader("1️⃣3️⃣ Drinking water sources (Well/Handpump/Borewell)")
df13 = run_query([
    {"$group": {
        "_id": "$District name",
        "Well": {"$sum": "$Main_source_of_drinking_water_Un_covered_well_Households"},
        "Handpump": {"$sum": "$Main_source_of_drinking_water_Handpump_Tubewell_Borewell_Households"}
    }}
])
st.dataframe(df13)


# -------------------------
# Query 14: Households with electricity
# -------------------------
st.subheader("1️⃣4️⃣ Households with electricity")
df14 = run_query([
    {"$group": {
        "_id": "$District name",
        "Electricity_Households": {"$sum": "$Housholds_with_Electric_Lighting"}
    }}
])
st.dataframe(df14)


# -------------------------
# Query 15: Population by religion
# -------------------------
st.subheader("1️⃣5️⃣ Population by religion in each district")
df15 = run_query([
    {"$group": {
        "_id": "$District name",
        "Hindus": {"$sum": "$Hindus"},
        "Muslims": {"$sum": "$Muslims"},
        "Christians": {"$sum": "$Christians"},
        "Sikhs": {"$sum": "$Sikhs"},
        "Buddhists": {"$sum": "$Buddhists"},
        "Jains": {"$sum": "$Jains"}
    }}
])
st.dataframe(df15)


# -------------------------
# Query 16: Education levels
# -------------------------
st.subheader("1️⃣6️⃣ Education distribution in each district")
df16 = run_query([
    {"$group": {
        "_id": "$District name",
        "Below_Primary": {"$sum": "$Below_Primary_Education"},
        "Primary": {"$sum": "$Primary_Education"},
        "Secondary": {"$sum": "$Secondary_Education"},
        "Graduate": {"$sum": "$Graduate_Education"}
    }}
])
st.dataframe(df16)


# -------------------------
# Query 17: Household size distribution
# -------------------------
st.subheader("1️⃣7️⃣ Household size distribution")
df17 = run_query([
    {"$group": {
        "_id": "$District name",
        "HH_1_2": {"$sum": "$Household_size_1_to_2_persons"},
        "HH_3_5": {"$sum": "$Household_size_3_to_5_persons"},
        "HH_6_8": {"$sum": "$Household_size_6_8_persons"},
        "HH_9+": {"$sum": "$Household_size_9_persons_and_above_Households"}
    }}
])
st.dataframe(df17)


# -------------------------
# Query 18: Ownership of houses
# -------------------------
st.subheader("1️⃣8️⃣ Ownership of houses")
df18 = run_query([
    {"$group": {
        "_id": "$District name",
        "Owned": {"$sum": "$Ownership_Owned_Households"},
        "Rented": {"$sum": "$Ownership_Rented_Households"}
    }}
])
st.dataframe(df18)


# -------------------------
# Query 19: Power parity distribution
# -------------------------
st.subheader("1️⃣9️⃣ Power parity distribution")
df19 = run_query([
    {"$group": {
        "_id": "$District name",
        "Below_45k": {"$sum": "$Power_Parity_Less_than_Rs_45000"},
        "Rs_45k_90k": {"$sum": "$Power_Parity_Rs_45000_90000"},
        "Rs_90k_150k": {"$sum": "$Power_Parity_Rs_90000_150000"},
        "Above_5L": {"$sum": "$Power_Parity_Above_Rs_545000"}
    }}
])
st.dataframe(df19)

