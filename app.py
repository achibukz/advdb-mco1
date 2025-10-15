import streamlit as st
import pandas as pd
import mysql.connector

# Set page config for wider layout
st.set_page_config(layout="wide")

# Title and Sidebar
st.title("Financial Dataset Dashboard")
st.sidebar.header("Choose data to view:")
report_category = st.sidebar.selectbox("Select Report to View:", ["Loan Value All-Throughout", "Orders by Bank", "Transactions by Operation"])

# Dynamic filter based on report category
if report_category == "Loan Value All-Throughout":
    filter_option = st.sidebar.selectbox("Filter by:", ["All Years", "1993", "1994", "1995", "1996", "1997", "1998"])
else:
    filter_option = st.sidebar.selectbox("Filter by:", ["None", "Region", "Bank", "Operation"])

st.sidebar.markdown("---")  # Adds a horizontal line for separation
st.sidebar.markdown("Balcita, Bukuhan, Cu, Dimaunahan")
st.sidebar.markdown("STADVDB S17 | Group 12")

# Placeholder for the main report
st.write("Description here idk")

def get_db_connection():
    return mysql.connector.connect(
        host="localhost", 
        port=3307,         
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

if report_category == "Loan Value All-Throughout":
    if filter_option == "All Years":
        # Show yearly average loan data
        query = """
        SELECT d.year,
               ROUND(AVG(fl.amount),2) AS avg_loan,
               COUNT(*) AS loan_count
        FROM FactLoan fl
        JOIN DimDate d ON fl.date_id = d.date_id
        GROUP BY d.year
        ORDER BY d.year;
        """
        data = fetch_data(query)
        st.subheader("Average Loan Amount by Year")
        
        # Display line chart
        if not data.empty:
            # Convert avg_loan to numeric (in case it's returned as string)
            data['avg_loan'] = pd.to_numeric(data['avg_loan'])
            
            # Create chart with year as index and avg_loan as values
            chart_data = data.set_index('year')['avg_loan']
            st.line_chart(chart_data)
    else:
        # Drill down into specific year by month
        selected_year = int(filter_option)
        query = f"""
        SELECT d.month,
               ROUND(AVG(fl.amount), 2) AS avg_loan,
               COUNT(*) AS loan_count
        FROM FactLoan fl
        JOIN DimDate d ON fl.date_id = d.date_id
        WHERE d.year = {selected_year}
        GROUP BY d.month
        ORDER BY d.month;
        """
        data = fetch_data(query)
        st.subheader(f"Average Loan Amount by Month for {selected_year}")
        
        # Display line chart
        if not data.empty:
            # Convert avg_loan to numeric (in case it's returned as string)
            data['avg_loan'] = pd.to_numeric(data['avg_loan'], errors='coerce')
            
            # Convert month numbers to month names
            month_names = {
                1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
                7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'
            }
            data['month_name'] = data['month'].map(month_names)
            
            # Create chart with month names as index and avg_loan as values
            # Use Categorical to preserve month order
            data['month_name'] = pd.Categorical(
                data['month_name'],
                categories=['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                           'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'],
                ordered=True
            )
            data = data.sort_values('month_name')
            chart_data = data.set_index('month_name')['avg_loan']
            st.line_chart(chart_data)
            
            # # Display the data table below the chart
            # st.write("Detailed Data:")
            # st.write(data)

# Filler Data (No real report queries yet)
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