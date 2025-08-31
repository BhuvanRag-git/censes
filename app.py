import streamlit as st
import mysql.connector as mysql
import pandas as pd

# MySQL connection
def get_connection():
    return mysql.connect(
        user="root",
        password="",   # update if your MySQL has password
        host="localhost",  # or your server IP
        database="census_database"
    )

st.title("?? Census Dashboard")

queries = {
    "Total Population by District": """
        SELECT district, SUM(Total_Population) as population
        FROM fact_census
        GROUP BY district;
    """,
    "Literate Males and Females by District": """
        SELECT district, SUM(Literate_Male) as male, SUM(Literate_Female) as female
        FROM fact_census
        GROUP BY district;
    """,
    "Households with Internet Access by District": """
        SELECT district, SUM(Households_with_Internet) as households_with_internet
        FROM fact_census
        GROUP BY district;
    """
}

choice = st.sidebar.selectbox("Select a Query", list(queries.keys()))

if st.button("Run Query"):
    conn = get_connection()
    df = pd.read_sql(queries[choice], conn)
    st.dataframe(df)
