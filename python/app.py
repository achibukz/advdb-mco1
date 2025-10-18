import streamlit as st
import pandas as pd
import mysql.connector
import altair as alt

# ------------------------------------------------
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
# ------------------------------------------------

# Set page config for wider layout
st.set_page_config(layout="wide")

# Title and Sidebar
st.title("Financial Reports Dashboard")
st.sidebar.header("Choose data to view:")
report_category = st.sidebar.selectbox("Select Report to View:", 
                ["Loan Amount Trend",
                 "Location Net Cash Flow", 
                 "Number of Payments and Total Amount", 
                 ])

# Dynamic filter based on report category
if report_category == "Loan Amount Trend":
    filter_option = st.sidebar.selectbox("Year:", ["All Years", "1993", "1994", "1995", "1996", "1997", "1998"])
elif report_category == "Location Net Cash Flow":
    filter_option = st.sidebar.selectbox("Region:", ["All Regions", "Prague", "central Bohemia", "south Bohemia", "west Bohemia", "north Bohemia", "east Bohemia", "south Moravia", "north Moravia"])   
elif report_category == "Number of Payments and Total Amount":
    filter_option = st.sidebar.selectbox("Year:", ["All Years", "1993", "1994", "1995", "1996", "1997", "1998"])
    filter_option2 = st.sidebar.selectbox("Card Type:", ["All Cards", "Junior", "Classic", "Gold"])
 

st.sidebar.markdown("---")  # Adds a horizontal line for separation
st.sidebar.markdown("Balcita, Bukuhan, Cu, Dimaunahan")
st.sidebar.markdown("STADVDB S17 | Group 12")

# Placeholder for the main report
st.write("Description here idk")

# REPORT 1
if report_category == "Loan Amount Trend":
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
            
            # Create Altair chart with no scientific notation
            chart = alt.Chart(data).mark_line(point=True).encode(
                x=alt.X('year:O', axis=alt.Axis(labelAngle=0), title='Year'),
                y=alt.Y('avg_loan:Q', 
                       title='Average Loan Amount',
                       axis=alt.Axis(format='~s')),
                tooltip=[
                    alt.Tooltip('year:O', title='Year'),
                    alt.Tooltip('avg_loan:Q', title='Average Loan', format=',.2f'),
                    alt.Tooltip('loan_count:Q', title='Loan Count', format=',')
                ]
            ).properties(
                height=400
            )
            st.altair_chart(chart, use_container_width=True)
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
            
            # Use Categorical to preserve month order
            data['month_name'] = pd.Categorical(
                data['month_name'],
                categories=['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                           'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'],
                ordered=True
            )
            data = data.sort_values('month_name')
            
            # Create Altair chart with no scientific notation
            chart = alt.Chart(data).mark_line(point=True).encode(
                x=alt.X('month_name:N', 
                       axis=alt.Axis(labelAngle=0), 
                       title='Month',
                       sort=['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']),
                y=alt.Y('avg_loan:Q', 
                       title='Average Loan Amount',
                       axis=alt.Axis(format='~s')),
                tooltip=[
                    alt.Tooltip('month_name:N', title='Month'),
                    alt.Tooltip('avg_loan:Q', title='Average Loan', format=',.2f'),
                    alt.Tooltip('loan_count:Q', title='Loan Count', format=',')
                ]
            ).properties(
                height=400
            )
            st.altair_chart(chart, use_container_width=True)

# REPORT 2 - Location Net Cash Flow
elif report_category == "Location Net Cash Flow":
    if filter_option == "All Regions":
        st.markdown("### No Region Selected")
        st.info("Please select a specific region to view district net cash flow data.")
    else:
        # Query to get net cash flow by district for selected region
        query = f"""
        SELECT dist.district_name,
               ROUND(SUM(ft.amount), 2) AS net_cash
        FROM FactTrans ft
        JOIN DimClientAccount ca ON ft.clientAcc_id = ca.clientAcc_id
        JOIN DimDistrict dist ON ca.distAcc_id = dist.district_id
        WHERE dist.region = '{filter_option}'
        GROUP BY dist.district_name
        ORDER BY net_cash DESC;
        """
        
        data = fetch_data(query)
        
        if not data.empty:
            # Convert to numeric
            data['net_cash'] = pd.to_numeric(data['net_cash'], errors='coerce')
            
            # Display chart title
            st.subheader(f"Net Cash Flow by District - {filter_option}")
            
            # Create horizontal bar chart with Altair
            chart = alt.Chart(data).mark_bar().encode(
                x=alt.X('net_cash:Q', 
                       title='Net Cash Flow',
                       axis=alt.Axis(format='~s')),
                y=alt.Y('district_name:N', 
                       title='District',
                       sort='-x'),  # Sort by net_cash descending
                tooltip=[
                    alt.Tooltip('district_name:N', title='District'),
                    alt.Tooltip('net_cash:Q', title='Net Cash Flow', format=',.2f')
                ]
            ).properties(
                height=max(400, len(data) * 25)  # Dynamic height based on number of districts
            )
            
            st.altair_chart(chart, use_container_width=True)
            
            # Display data table below
            st.write("Detailed Data:")
            st.dataframe(data, use_container_width=True)
        else:
            st.warning(f"No data available for {filter_option}.")

# REPORT 3 - Number of Payments and Total Amount
elif report_category == "Number of Payments and Total Amount":
    # Check if both filters are "All"
    if filter_option == "All Years" and filter_option2 == "All Cards":
        st.markdown("### No Filters Selected")
        st.info("Please select a specific year or card type to view payment data.")
    else:
        # Build dynamic query based on filters
        query = """
        SELECT 
            dd.year,
            dc.type,
            ROUND(SUM(fl.payments) / 1000, 2) AS total_payments_thousands
        FROM DimDate dd
        JOIN FactLoan fl ON dd.date_id = fl.date_id
        JOIN DimCard dc ON dd.date_id = dc.date_id
        """
        
        # Add WHERE clause based on filters
        conditions = []
        if filter_option != "All Years":
            conditions.append(f"dd.year = {filter_option}")
        if filter_option2 != "All Cards":
            conditions.append(f"dc.type = '{filter_option2}'")
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        # Add GROUP BY based on what's being filtered
        group_by_fields = []
        if filter_option == "All Years":
            group_by_fields.append("dd.year")
        if filter_option2 == "All Cards":
            group_by_fields.append("dc.type")
        
        if group_by_fields:
            query += " GROUP BY " + ", ".join(group_by_fields)
            query += " ORDER BY " + group_by_fields[0]
        
        # Fetch data
        data = fetch_data(query)
        
        if not data.empty:
            # Convert to numeric
            data['total_payments_thousands'] = pd.to_numeric(data['total_payments_thousands'], errors='coerce')
            
            # Create dynamic x-axis label
            x_label_parts = []
            if filter_option != "All Years":
                x_label_parts.append(filter_option)
            if filter_option2 != "All Cards":
                x_label_parts.append(filter_option2)
            x_axis_label = " | ".join(x_label_parts) if x_label_parts else "Category"
            
            # Determine x-axis field and create label column
            if filter_option == "All Years" and filter_option2 != "All Cards":
                # Grouped by year (showing different years for one card type)
                data['x_label'] = data['year'].astype(str)
                x_field = 'year'
                chart_title = f"Total Payments by Year - {filter_option2} Card"
            elif filter_option != "All Years" and filter_option2 == "All Cards":
                # Grouped by card type (showing different card types for one year)
                data['x_label'] = data['type']
                x_field = 'type'
                chart_title = f"Total Payments by Card Type - {filter_option}"
            else:
                # Single bar (specific year and card type)
                data['x_label'] = f"{filter_option} | {filter_option2}"
                x_field = 'x_label'
                chart_title = f"Total Payments - {filter_option} | {filter_option2}"
            
            # Display chart title
            st.subheader(chart_title)
            
            # Create Altair bar chart
            chart = alt.Chart(data).mark_bar().encode(
                x=alt.X('x_label:N', 
                       title=x_axis_label,
                       axis=alt.Axis(labelAngle=0)),
                y=alt.Y('total_payments_thousands:Q', 
                       title='Total Payments (Thousands)',
                       axis=alt.Axis(format='~s')),
                tooltip=[
                    alt.Tooltip('x_label:N', title='Category'),
                    alt.Tooltip('total_payments_thousands:Q', title='Total Payments (Thousands)', format=',.2f')
                ]
            ).properties(
                height=400
            )
            
            st.altair_chart(chart, use_container_width=True)
            
            # Display data table below
            st.write("Detailed Data:")
            st.dataframe(data, use_container_width=True)
        else:
            st.warning("No data available for the selected filters.")

# SAMPLE ONLY 
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