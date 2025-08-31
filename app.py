import streamlit as st
import pandas as pd
from pymongo import MongoClient

# -------------------------------
# MongoDB Connection
# -------------------------------
MONGO_URI = "mongodb+srv://ragbhuvan_db_user:Bhuvanrag123@cluster0.vhjf9di.mongodb.net/census_database"
client = MongoClient(MONGO_URI)
db = client["census_database"]
collection = db["census_data"]

st.set_page_config(page_title="Census Dashboard", layout="wide")

st.title("📊 Census Dashboard (MongoDB Atlas)")

# -------------------------------
# Helper Function
# -------------------------------
def run_query(pipeline):
    data = list(collection.aggregate(pipeline))
    if data:
        return pd.DataFrame(data)
    return pd.DataFrame()


# -------------------------------
# Queries
# -------------------------------

# 1. Total population of each district
st.subheader("1️⃣ Total population of each district")
df1 = run_query([
    {"$group": {"_id": "$District", "Total_Population": {"$sum": "$Population"}}},
    {"$sort": {"Total_Population": -1}}
])
st.dataframe(df1)

# 2. Literate males and females in each district
st.subheader("2️⃣ Literate males and females in each district")
df2 = run_query([
    {"$group": {
        "_id": "$District",
        "Literate_Males": {"$sum": "$Literate_Male"},
        "Literate_Females": {"$sum": "$Literate_Female"}
    }}
])
st.dataframe(df2)

# 3. Percentage of workers in each district
st.subheader("3️⃣ Percentage of workers in each district")
df3 = run_query([
    {"$project": {
        "_id": "$District",
        "Percentage_Workers": {
            "$multiply": [{"$divide": ["$Workers", "$Population"]}, 100]
        }
    }}
])
st.dataframe(df3)

# 4. Gender ratio (Males vs Females)
st.subheader("4️⃣ Gender ratio (Males vs Females)")
df4 = run_query([
    {"$project": {
        "_id": "$District",
        "Gender_Ratio": {"$divide": ["$Female", "$Male"]}
    }}
])
st.dataframe(df4)

# 5. SC/ST population in each district
st.subheader("5️⃣ SC/ST population in each district")
df5 = run_query([
    {"$group": {
        "_id": "$District",
        "SC_Population": {"$sum": "$SC"},
        "ST_Population": {"$sum": "$ST"}
    }}
])
st.dataframe(df5)

# 6. Worker categories
st.subheader("6️⃣ Worker categories in each district")
df6 = run_query([
    {"$project": {
        "_id": "$District",
        "Cultivator_Workers": "$Cultivator_Workers",
        "Agricultural_Workers": "$Agricultural_Workers",
        "Household_Workers": "$Household_Workers",
        "Other_Workers": "$Other_Workers"
    }}
])
st.dataframe(df6)

# 7. Religion distribution
st.subheader("7️⃣ Religion distribution in each district")
df7 = run_query([
    {"$project": {
        "_id": "$District",
        "Hindus": "$Hindus",
        "Muslims": "$Muslims",
        "Christians": "$Christians",
        "Sikhs": "$Sikhs",
        "Buddhists": "$Buddhists",
        "Jains": "$Jains",
        "Others": "$Others_Religions",
        "Not_Stated": "$Religion_Not_Stated"
    }}
])
st.dataframe(df7)

# 8. Households with internet/computer
st.subheader("8️⃣ Households with Internet and Computers")
df8 = run_query([
    {"$project": {
        "_id": "$District",
        "Households_Internet": "$Households_with_Internet",
        "Households_Computer": "$Households_with_Computer"
    }}
])
st.dataframe(df8)

# 9. Rural vs Urban households
st.subheader("9️⃣ Rural vs Urban households")
df9 = run_query([
    {"$project": {
        "_id": "$District",
        "Households_Rural": "$Households_Rural",
        "Households_Urban": "$Households_Urban"
    }}
])
st.dataframe(df9)

# 10. Education levels
st.subheader("🔟 Education distribution in each district")
df10 = run_query([
    {"$project": {
        "_id": "$District",
        "Primary": "$Primary_Education",
        "Middle": "$Middle_Education",
        "Secondary": "$Secondary_Education",
        "Higher": "$Higher_Education",
        "Graduate": "$Graduate_Education"
    }}
])
st.dataframe(df10)

# 11. Age group distribution
st.subheader("1️⃣1️⃣ Age group distribution")
df11 = run_query([
    {"$project": {
        "_id": "$District",
        "Young_and_Adult": "$Young_and_Adult",
        "Middle_Aged": "$Middle_Aged",
        "Senior_Citizen": "$Senior_Citizen"
    }}
])
st.dataframe(df11)

# 12. Vehicle ownership
st.subheader("1️⃣2️⃣ Vehicle ownership in households")
df12 = run_query([
    {"$project": {
        "_id": "$District",
        "Bicycle": "$Households_with_Bicycle",
        "Car": "$Households_with_Car_Jeep_Van",
        "Scooter": "$Households_with_Scooter_Motorcycle_Moped"
    }}
])
st.dataframe(df12)

# 13. Housing condition
st.subheader("1️⃣3️⃣ Housing conditions")
df13 = run_query([
    {"$project": {
        "_id": "$District",
        "Dilapidated_Houses": "$Condition_of_occupied_census_houses_Dilapidated_Households",
        "Separate_Kitchen": "$Households_with_separate_kitchen_Cooking_inside_house"
    }}
])
st.dataframe(df13)

# 14. Sanitation facilities
st.subheader("1️⃣4️⃣ Sanitation facilities")
df14 = run_query([
    {"$project": {
        "_id": "$District",
        "Latrine_within": "$Having_latrine_facility_within_the_premises_Total_Households",
        "No_Latrine": "$Not_having_latrine_facility_within_the_premises_Alternative_source_Open_Households"
    }}
])
st.dataframe(df14)

# 15. Drinking water sources
st.subheader("1️⃣5️⃣ Drinking water sources")
df15 = run_query([
    {"$project": {
        "_id": "$District",
        "Tapwater": "$Main_source_of_drinking_water_Tapwater_Households",
        "Handpump": "$Main_source_of_drinking_water_Handpump_Tubewell_Borewell_Households",
        "River_Canal": "$Main_source_of_drinking_water_River_Canal_Households"
    }}
])
st.dataframe(df15)

# 16. Household sizes
st.subheader("1️⃣6️⃣ Household sizes")
df16 = run_query([
    {"$project": {
        "_id": "$District",
        "1_to_2": "$Household_size_1_to_2_persons",
        "3_to_5": "$Household_size_3_to_5_persons",
        "6_to_8": "$Household_size_6_8_persons_Households",
        "9_plus": "$Household_size_9_persons_and_above_Households"
    }}
])
st.dataframe(df16)

# 17. Married couples
st.subheader("1️⃣7️⃣ Married couples in households")
df17 = run_query([
    {"$project": {
        "_id": "$District",
        "Couples_1": "$Married_couples_1_Households",
        "Couples_2": "$Married_couples_2_Households",
        "Couples_3_plus": "$Married_couples_3_or_more_Households"
    }}
])
st.dataframe(df17)

# 18. Household lighting
st.subheader("1️⃣8️⃣ Households with electric lighting")
df18 = run_query([
    {"$project": {
        "_id": "$District",
        "Lighting": "$Housholds_with_Electric_Lighting"
    }}
])
st.dataframe(df18)

# 19. Income/Power Parity
st.subheader("1️⃣9️⃣ Household Power Parity")
df19 = run_query([
    {"$project": {
        "_id": "$District",
        "Below_45k": "$Power_Parity_Less_than_Rs_45000",
        "45k_90k": "$Power_Parity_Rs_45000_90000",
        "90k_150k": "$Power_Parity_Rs_90000_150000",
        "Above_545k": "$Power_Parity_Above_Rs_545000"
    }}
])
st.dataframe(df19)
