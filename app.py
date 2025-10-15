import streamlit as st
import pandas as pd
import mysql.connector

# Title and Sidebar
st.title("Financial Dataset Dashboard")
st.sidebar.header("Choose data to view:")
report_category = st.sidebar.selectbox("Select Report to View:", ["Loans by Region", "Orders by Bank", "Transactions by Operation"])
filter_option = st.sidebar.selectbox("Filter by:", ["None", "Region", "Bank", "Operation"])

# Placeholder for the main report
st.write("### Category of Reports")
st.write("#### Specific Report")
st.write("Report will be displayed here.")

def get_db_connection():
    return mysql.connector.connect(
        host="localhost",  # Use "localhost" only
        port=3307,         # Specify the port separately
        user="warehouse_user",
        password="rootpass",
        database="warehouse_db"
    )

def fetch_data(query):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute(query)
    data = cursor.fetchall()
    conn.close()
    return pd.DataFrame(data)

if report_category == "Loans by Region":
    query = """
    SELECT dist.region AS region_name,
           fl.status AS loan_status,
           COUNT(*) AS num_loans
    FROM FactLoan fl
    JOIN DimClientAccount ca ON fl.clientAcc_id = ca.clientAcc_id
    JOIN DimDistrict dist ON ca.distCli_id = dist.district_id
    GROUP BY dist.region, fl.status
    ORDER BY dist.region, fl.status;
    """
    data = fetch_data(query)
    st.write(data)

elif report_category == "Orders by Bank":
    query = """
    SELECT fo.bank_to AS bank_name,
           COUNT(*) AS num_orders
    FROM FactOrder fo
    WHERE fo.bank_to IS NOT NULL
    GROUP BY fo.bank_to
    ORDER BY num_orders DESC;
    """
    data = fetch_data(query)
    st.write(data)

elif report_category == "Transactions by Operation":
    query = """
    SELECT ft.operation AS operation_type,
           COUNT(*) AS num_operations
    FROM FactTrans ft
    GROUP BY ft.operation
    ORDER BY num_operations DESC;
    """
    data = fetch_data(query)
    st.write(data)