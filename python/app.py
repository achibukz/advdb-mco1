import streamlit as st
import pandas as pd
import altair as alt

# Import database functions from separate config file
from db_config import fetch_data

# Fetch district names for dropdown (cached to avoid repeated queries)
@st.cache_data
def get_districts():
    query = "SELECT DISTINCT district_name FROM DimDistrict ORDER BY district_name;"
    df = fetch_data(query)
    return ["None Selected"] + df['district_name'].tolist()
# ------------------------------------------------

# Set page config for wider layout
st.set_page_config(layout="wide")

# Custom CSS to adjust sidebar width
st.markdown(
    """
    <style>
        [data-testid="stSidebar"][aria-expanded="true"]{
            min-width: 380px;
        }
        [data-testid="stSidebar"][aria-expanded="false"]{
            min-width: 380px;
            margin-left: -380px;
        }
    </style>
    """,
    unsafe_allow_html=True
)

# Title and Sidebar
st.title("Financial Reports Dashboard")
st.sidebar.header("Choose data to view:")
report_category = st.sidebar.selectbox("Select Report to View:", 
                ["Loan Amount Trend",
                 "Location Net Cash Flow", 
                 "Number of Payments and Total Amount",
                 "Transaction Types and Volume by District",
                 "Loan Status and Loan Volume by Region"
                 ])

# Dynamic filter based on report category
if report_category == "Loan Amount Trend":
    filter_option = st.sidebar.selectbox("Year:", ["All Years", "1993", "1994", "1995", "1996", "1997", "1998"])
elif report_category == "Location Net Cash Flow":
    filter_option = st.sidebar.selectbox("Region:", ["No Region Selected", "Prague", "central Bohemia", "south Bohemia", "west Bohemia", "north Bohemia", "east Bohemia", "south Moravia", "north Moravia"])   
elif report_category == "Number of Payments and Total Amount":
    filter_option = st.sidebar.selectbox("Year:", ["All Years", "1993", "1994", "1995", "1996", "1997", "1998"])
    filter_option2 = st.sidebar.selectbox("Card Type:", ["All Cards", "Junior", "Classic", "Gold"])
elif report_category == "Transaction Types and Volume by District":
    districts = get_districts()
    filter_option = st.sidebar.selectbox("District:", districts)
elif report_category == "Loan Status and Loan Volume by Region":
    filter_option = None
    filter_option2 = None

st.sidebar.markdown("---")  # Adds a horizontal line for separation
st.sidebar.markdown("Balcita, Bukuhan, Cu, Dimaunahan")
st.sidebar.markdown("STADVDB S17 | Group 12")

# Placeholder for the main report
st.write("Version 1.3.2")

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
            st.altair_chart(chart, width='stretch')
            
            # Display data table below
            st.write("Detailed Data:")
            st.dataframe(data, use_container_width=True)
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
            st.altair_chart(chart, width='stretch')

            st.write("Detailed Data:")
            st.dataframe(data, use_container_width=True)

# REPORT 2 - Location Net Cash Flow
elif report_category == "Location Net Cash Flow":
    if filter_option == "No Region Selected":
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
            
            st.altair_chart(chart, width='stretch')
            
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
            
            st.altair_chart(chart, width='stretch')
            
            # Display data table below
            st.write("Detailed Data:")
            st.dataframe(data, use_container_width=True)
        else:
            st.warning("No data available for the selected filters.")

# REPORT 4 - Loan Status and Loan Volume by Region
elif report_category == "Loan Status and Loan Volume by Region":
    # Query to get loan status breakdown by region
    query = """
    SELECT 
        dd.region,
        SUM(CASE WHEN fl.status = 'A' THEN 1 ELSE 0 END) AS finished_no_problems,
        SUM(CASE WHEN fl.status = 'B' THEN 1 ELSE 0 END) AS finished_pending_payments,
        SUM(CASE WHEN fl.status = 'C' THEN 1 ELSE 0 END) AS active_ok,
        SUM(CASE WHEN fl.status = 'D' THEN 1 ELSE 0 END) AS active_in_debt,
        SUM(CASE WHEN fl.status IN ('A', 'B') THEN 1 ELSE 0 END) AS total_completed,
        SUM(CASE WHEN fl.status IN ('C', 'D') THEN 1 ELSE 0 END) AS total_ongoing,
        COUNT(fl.loan_id) AS total_loans
    FROM FactLoan fl
    JOIN DimClientAccount dca ON fl.clientAcc_id = dca.clientAcc_id
    JOIN DimDistrict dd ON dca.distCli_id = dd.district_id
    GROUP BY dd.region
    ORDER BY total_loans DESC;
    """
    
    data = fetch_data(query)
    
    if not data.empty:
        # Convert numeric columns
        numeric_cols = ['finished_no_problems', 'finished_pending_payments', 
                       'active_ok', 'active_in_debt', 'total_completed', 
                       'total_ongoing', 'total_loans']
        for col in numeric_cols:
            data[col] = pd.to_numeric(data[col], errors='coerce')
        
        st.subheader("Loan Status and Volume by Region")
        
        # Reshape data for stacked bar chart
        # Create a long-form dataframe for Altair
        chart_data = pd.DataFrame()
        for _, row in data.iterrows():
            region = row['region']
            chart_data = pd.concat([chart_data, pd.DataFrame({
                'region': [region, region, region, region],
                'status': ['Finished - No Problems', 'Finished - Pending Payments', 
                          'Active - OK', 'Active - In Debt'],
                'count': [row['finished_no_problems'], row['finished_pending_payments'],
                         row['active_ok'], row['active_in_debt']]
            })], ignore_index=True)
        
        # Define color scheme for loan statuses
        color_scale = alt.Scale(
            domain=['Finished - No Problems', 'Finished - Pending Payments', 
                   'Active - OK', 'Active - In Debt'],
            range=['#2ecc71', '#f39c12', '#3498db', '#e74c3c']
        )
        
        # Create horizontal stacked bar chart
        chart = alt.Chart(chart_data).mark_bar().encode(
            x=alt.X('count:Q', 
                   title='Number of Loans',
                   axis=alt.Axis(format='~s')),
            y=alt.Y('region:N', 
                   title='Region',
                   sort=alt.EncodingSortField(field='count', op='sum', order='descending')),
            color=alt.Color('status:N', 
                          title='Loan Status',
                          scale=color_scale,
                          legend=alt.Legend(orient='bottom')),
            tooltip=[
                alt.Tooltip('region:N', title='Region'),
                alt.Tooltip('status:N', title='Status'),
                alt.Tooltip('count:Q', title='Count', format=',')
            ]
        ).properties(
            height=400
        )
        
        st.altair_chart(chart, width='stretch')
        
        # Display summary statistics
        st.write("Summary Statistics:")
        summary_data = data[['region', 'finished_no_problems', 'finished_pending_payments', 
                            'active_ok', 'active_in_debt', 'total_completed', 
                            'total_ongoing', 'total_loans']].copy()
        summary_data.columns = ['Region', 'Finished (No Problems)', 'Finished (Pending)', 
                               'Active (OK)', 'Active (In Debt)', 'Total Completed', 
                               'Total Ongoing', 'Total Loans']
        st.dataframe(summary_data, use_container_width=True)
    else:
        st.warning("No loan data available.")


# REPORT 5 - Transaction Types and Volume by District
elif report_category == "Transaction Types and Volume by District":
    if filter_option == "None Selected":
        st.markdown("### No District Selected")
        st.info("Please select a specific district to view transaction type distribution.")
    else:
        # Direct aggregation query - works with connection pooling and cloud deployment
        # Single statement compatible with st.connection() and Streamlit's caching
        query = f"""
        SELECT 
            dd.district_name,
            dd.region,
            SUM(CASE WHEN ft.operation = 'Credit in Cash' THEN 1 ELSE 0 END) AS credit_in_cash,
            SUM(CASE WHEN ft.operation = 'Collection from Another Bank' THEN 1 ELSE 0 END) AS collection_from_bank,
            SUM(CASE WHEN ft.operation = 'Withdrawal in Cash' THEN 1 ELSE 0 END) AS withdrawal_in_cash,
            SUM(CASE WHEN ft.operation = 'Remittance to Another Bank' THEN 1 ELSE 0 END) AS remittance_to_bank,
            SUM(CASE WHEN ft.operation = 'Credit Card Withdrawal' THEN 1 ELSE 0 END) AS credit_card_withdrawal,
            COUNT(ft.trans_id) AS total_transactions,
            ROUND(AVG(ft.amount), 2) AS avg_transaction_amount,
            ROUND(SUM(ft.amount), 2) AS total_money_transferred
        FROM FactTrans ft
        JOIN DimClientAccount dca ON ft.clientAcc_id = dca.clientAcc_id
        JOIN DimDistrict dd ON dca.distCli_id = dd.district_id
        WHERE dd.district_name = '{filter_option}'
        GROUP BY dd.district_id, dd.district_name, dd.region;
        """
        
        # Use standard fetch_data - works with st.connection() and cloud deployment
        data = fetch_data(query)
        
        if not data.empty and len(data) > 0:
            row = data.iloc[0]
            
            # Display summary information
            st.subheader(f"Transaction Distribution - {filter_option}")
            st.write(f"**Region:** {row['region']}")
            st.write(f"**Total Transactions:** {int(row['total_transactions']):,}")
            st.write(f"**Average Transaction Amount:** {row['avg_transaction_amount']:,.2f}")
            st.write(f"**Total Money Transferred:** {row['total_money_transferred']:,.2f}")
            
            st.markdown("---")
            
            # Prepare data for pie chart
            operations_data = pd.DataFrame({
                'Operation Type': [
                    'Credit in Cash',
                    'Collection from Another Bank',
                    'Withdrawal in Cash',
                    'Remittance to Another Bank',
                    'Credit Card Withdrawal'
                ],
                'Count': [
                    int(row['credit_in_cash']),
                    int(row['collection_from_bank']),
                    int(row['withdrawal_in_cash']),
                    int(row['remittance_to_bank']),
                    int(row['credit_card_withdrawal'])
                ]
            })
            
            # Filter out operations with 0 count
            operations_data = operations_data[operations_data['Count'] > 0]
            
            if not operations_data.empty:
                # Calculate percentage for each operation
                operations_data['Percentage'] = (operations_data['Count'] / operations_data['Count'].sum() * 100).round(2)
                
                # Create pie chart with Altair
                chart = alt.Chart(operations_data).mark_arc(innerRadius=50).encode(
                    theta=alt.Theta('Count:Q', stack=True),
                    color=alt.Color('Operation Type:N', 
                                   legend=alt.Legend(
                                       title="Transaction Type",
                                       labelLimit=300,  # Increase label character limit
                                       titleLimit=300   # Increase title character limit
                                   )),
                    tooltip=[
                        alt.Tooltip('Operation Type:N', title='Transaction Type'),
                        alt.Tooltip('Count:Q', title='Number of Transactions', format=','),
                        alt.Tooltip('Percentage:Q', title='Percentage', format='.2f')
                    ]
                ).properties(
                    height=500,
                    title=f"Transaction Type Distribution"
                )
                
                st.altair_chart(chart, width='stretch')
                
                # Display operations breakdown table
                st.write("Transaction Breakdown:")
                operations_data['Percentage_Display'] = operations_data['Percentage'].astype(str) + '%'
                display_data = operations_data[['Operation Type', 'Count', 'Percentage_Display']].copy()
                display_data.columns = ['Operation Type', 'Count', 'Percentage']
                st.dataframe(display_data, use_container_width=True)
            else:
                st.warning("No transaction operations recorded for this district.")
        else:
            st.warning(f"No data available for {filter_option}.")

