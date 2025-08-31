import streamlit as st
from pymongo import MongoClient
import pandas as pd

# ---------------- MongoDB Connection ----------------
MONGO_URI = "mongodb+srv://ragbhuvan_db_user:Bhuvanrag123@cluster0.vhjf9di.mongodb.net/census_database"
client = MongoClient(MONGO_URI)
db = client["census_databse"]   # change if needed
collection = db["fact_census"]

st.title("📊 Census Dashboard (MongoDB Atlas)")

# Helper to run pipelines
def run_pipeline(pipeline):
    return pd.DataFrame(list(collection.aggregate(pipeline)))

# ---------------- Queries ----------------

# 1. Total population of each district
st.subheader("1️⃣ Total population of each district")
pipeline = [{"$group": {"_id": "$district", "total_population": {"$sum": "$population"}}}]
st.dataframe(run_pipeline(pipeline))

# 2. Literate males and females in each district
st.subheader("2️⃣ Literate males and females in each district")
pipeline = [{"$group": {"_id": "$district",
                        "literate_males": {"$sum": "$literate_male"},
                        "literate_females": {"$sum": "$literate_female"}}}]
st.dataframe(run_pipeline(pipeline))

# 3. Percentage of workers in each district
st.subheader("3️⃣ Percentage of workers in each district")
pipeline = [{"$group": {"_id": "$district",
                        "total_pop": {"$sum": "$population"},
                        "workers": {"$sum": {"$add": ["$workers_male", "$workers_female"]}}}},
            {"$project": {"percentage_workers": {"$multiply": [{"$divide": ["$workers", "$total_pop"]}, 100]}}}]
st.dataframe(run_pipeline(pipeline))

# 4. Households with LPG/PNG
st.subheader("4️⃣ Households with LPG/PNG as cooking fuel")
pipeline = [{"$group": {"_id": "$district", "lpg_households": {"$sum": "$lpg_png"}}}]
st.dataframe(run_pipeline(pipeline))

# 5. Religious composition
st.subheader("5️⃣ Religious composition of each district")
pipeline = [{"$group": {"_id": "$district",
                        "hindus": {"$sum": "$hindu"},
                        "muslims": {"$sum": "$muslim"},
                        "christians": {"$sum": "$christian"},
                        "others": {"$sum": "$others"}}}]
st.dataframe(run_pipeline(pipeline))

# 6. Households with internet
st.subheader("6️⃣ Households with internet access in each district")
pipeline = [{"$group": {"_id": "$district", "internet_households": {"$sum": "$internet"}}}]
st.dataframe(run_pipeline(pipeline))

# 7. Educational attainment distribution
st.subheader("7️⃣ Educational attainment distribution by district")
pipeline = [{"$group": {"_id": "$district",
                        "below_primary": {"$sum": "$edu_below_primary"},
                        "primary": {"$sum": "$edu_primary"},
                        "middle": {"$sum": "$edu_middle"},
                        "secondary": {"$sum": "$edu_secondary"}}}]
st.dataframe(run_pipeline(pipeline))

# 8. Households with transport facilities
st.subheader("8️⃣ Transport access in each district")
pipeline = [{"$group": {"_id": "$district",
                        "bicycles": {"$sum": "$transport_bicycle"},
                        "cars": {"$sum": "$transport_car"},
                        "radios": {"$sum": "$transport_radio"},
                        "tvs": {"$sum": "$transport_tv"}}}]
st.dataframe(run_pipeline(pipeline))

# 9. Condition of census houses
st.subheader("9️⃣ Condition of occupied census houses")
pipeline = [{"$group": {"_id": "$district",
                        "dilapidated": {"$sum": "$house_dilapidated"},
                        "separate_kitchen": {"$sum": "$house_separate_kitchen"},
                        "bathing_facility": {"$sum": "$house_bathing"},
                        "latrine_facility": {"$sum": "$house_latrine"}}}]
st.dataframe(run_pipeline(pipeline))

# 10. Household size distribution
st.subheader("🔟 Household size distribution in each district")
pipeline = [{"$group": {"_id": "$district",
                        "1_person": {"$sum": "$hh_size_1"},
                        "2_persons": {"$sum": "$hh_size_2"},
                        "3to5_persons": {"$sum": "$hh_size_3to5"}}}]
st.dataframe(run_pipeline(pipeline))

# 11. Total households in each state
st.subheader("1️⃣1️⃣ Total households in each state")
pipeline = [{"$group": {"_id": "$state", "total_households": {"$sum": "$total_households"}}}]
st.dataframe(run_pipeline(pipeline))

# 12. Households with latrine within premises
st.subheader("1️⃣2️⃣ Households with latrine facility in each state")
pipeline = [{"$group": {"_id": "$state", "latrine_within": {"$sum": "$latrine_within"}}}]
st.dataframe(run_pipeline(pipeline))

# 13. Average household size
st.subheader("1️⃣3️⃣ Average household size in each state")
pipeline = [{"$group": {"_id": "$state",
                        "total_people": {"$sum": "$population"},
                        "total_households": {"$sum": "$total_households"}}},
            {"$project": {"avg_hh_size": {"$divide": ["$total_people", "$total_households"]}}}]
st.dataframe(run_pipeline(pipeline))

# 14. Owned vs Rented
st.subheader("1️⃣4️⃣ Owned vs Rented households in each state")
pipeline = [{"$group": {"_id": "$state",
                        "owned": {"$sum": "$owned"},
                        "rented": {"$sum": "$rented"}}}]
st.dataframe(run_pipeline(pipeline))

# 15. Types of latrine facilities
st.subheader("1️⃣5️⃣ Types of latrine facilities in each state")
pipeline = [{"$group": {"_id": "$state",
                        "pit_latrine": {"$sum": "$latrine_pit"},
                        "flush_latrine": {"$sum": "$latrine_flush"}}}]
st.dataframe(run_pipeline(pipeline))

# 16. Drinking water near premises
st.subheader("1️⃣6️⃣ Drinking water near premises in each state")
pipeline = [{"$group": {"_id": "$state", "drinking_water_near": {"$sum": "$drinking_water_near"}}}]
st.dataframe(run_pipeline(pipeline))

# 17. Average household income distribution
st.subheader("1️⃣7️⃣ Average household income distribution in each state")
pipeline = [{"$group": {"_id": "$state", "avg_income": {"$avg": "$avg_income"}}}]
st.dataframe(run_pipeline(pipeline))

# 18. Married couples with different household sizes
st.subheader("1️⃣8️⃣ Married couples with household sizes in each state")
pipeline = [{"$group": {"_id": "$state", "married_couples": {"$sum": "$married_couples"}}}]
st.dataframe(run_pipeline(pipeline))

# 19. Overall literacy rate
st.subheader("1️⃣9️⃣ Overall literacy rate in each state")
pipeline = [{"$group": {"_id": "$state",
                        "literate": {"$sum": {"$add": ["$literate_male", "$literate_female"]}},
                        "population": {"$sum": "$population"}}},
            {"$project": {"literacy_rate": {"$multiply": [{"$divide": ["$literate", "$population"]}, 100]}}}]
st.dataframe(run_pipeline(pipeline))

