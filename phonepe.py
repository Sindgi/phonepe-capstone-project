import streamlit as st
from PIL import Image
import os
import json
from streamlit_option_menu import option_menu
import pandas as pd
from sqlalchemy import create_engine
import pymysql
import plotly.express as px

#To clone the Github Pulse repository use the following code
#Repo.clone_from("Clone Url", "Your working directory")
# Repo.clone_from("https://github.com/PhonePe/pulse.git", "Project_2_PhonepePulse/Phonepe_data/data")


phn = Image.open(r"C:\Users\Admin\PycharmProjects\pythonProject\phone\phonepe.jpg")
st.set_page_config(page_title="PhonePe Pulse", page_icon=phn, layout="wide")
# Define the path to the main directory containing all the data
main_path = r"C:\Users\Admin\PycharmProjects\phone\data"

# Define the list of states
states = ["andaman-&-nicobar-islands", "andhra-pradesh", "arunachal-pradesh", "assam", "bihar", "chandigarh",
          "chhattisgarh", "dadra-&-nagar-haveli-&-daman-&-diu", "delhi", "goa", "gujarat", "haryana",
          "himachal-pradesh", "jammu-&-kashmir", "jharkhand", "karnataka", "kerala", "ladakh", "lakshadweep",
          "madhya-pradesh", "maharashtra", "manipur", "meghalaya", "mizoram", "nagaland", "odisha", "puducherry",
          "punjab", "rajasthan", "sikkim", "tamil-nadu", "telangana", "tripura", "uttar-pradesh", "uttarakhand",
          "west-bengal"]

# Initialize empty lists for dataframes
aggregated_trans_df_list = []
aggregated_user_df_list = []
map_trans_df_list = []
map_user_df_list = []
top_trans_df_list = []
top_user_df_list = []

# Loop through each state
for state in states:

    # Loop through each year
    for year in range(2018, 2023):

        # Loop through each quarter
        for quarter in range(1, 5):

            # Define the file path for the aggregated data
            agg_file_path = os.path.join(main_path, "aggregated", "transaction", "country", "india", "state", state,
                                         str(year), f"{quarter}.json")

            # Check if the aggregated data file exists
            if os.path.exists(agg_file_path):

                # Load the aggregated data
                with open(agg_file_path, "r") as f:
                    data = json.load(f)

                # Extract the relevant data from the JSON object
                from_time = data["data"]["from"]
                to_time = data["data"]["to"]
                transactions = data["data"]["transactionData"]

                # Loop through each transaction type and payment instrument
                for transaction in transactions:
                    transaction_name = transaction["name"]
                    payment_instruments = transaction["paymentInstruments"]

                    for payment_instrument in payment_instruments:
                        payment_type = payment_instrument["type"]
                        payment_count = payment_instrument["count"]
                        payment_amount = payment_instrument["amount"]

                        # Add the data to the aggregated dataframe list
                        aggregated_trans_df_list.append(
                            [state, year, quarter, transaction_name, payment_type, payment_count, payment_amount])

            # Define the file path for the user data
            user_file_path = os.path.join(main_path, "aggregated", "user", "country", "india", "state", state,
                                          str(year), f"{quarter}.json")

            # Check if the user data file exists
            if os.path.exists(user_file_path):

                # Load the user data
                with open(user_file_path, "r") as f:
                    data = json.load(f)

                # Extract the relevant data from the JSON object
                reg_users = data["data"]["aggregated"]["registeredUsers"]
                users_by_device = data["data"]["usersByDevice"]

                # Loop through each device brand and add the data to the user dataframe list
                if users_by_device is not None:
                    for device in users_by_device:
                        user_brand = device['brand']
                        count_users = device['count']
                        percentage_user = device['percentage']

                        aggregated_user_df_list.append(
                            [state, year, quarter, reg_users, user_brand, count_users, percentage_user])

# next folder
# Loop through each quarter
for state in states:
    for year in range(2018, 2023):
        for quarter in range(1, 5):

            # Define the file path for the aggregated data
            agg_file_path = os.path.join(main_path, "map", "transaction", "hover", "country", "india", "state", state,
                                         str(year), f"{quarter}.json")

            # Check if the aggregated data file exists
            if os.path.exists(agg_file_path):

                # Load the aggregated data
                with open(agg_file_path, "r") as f:
                    data = json.load(f)

                # Extract the relevant data from the JSON object
                hover_data = data["data"]["hoverDataList"]

                # Loop through each transaction type and payment instrument
                for hover in hover_data:
                    hover_name = hover["name"]
                    hover_metrics = hover["metric"]

                    for met in hover_metrics:
                        metric_type = met["type"]
                        metric_count = met["count"]
                        metric_amount = met["amount"]

                        # Add the data to the aggregated dataframe list
                        map_trans_df_list.append(
                            [state, year, quarter, hover_name, metric_type, metric_count, metric_amount])

            # Define the file path for the user data
            user_file_path = os.path.join(main_path, "map", "user", "hover", "country", "india", "state", state,
                                          str(year), f"{quarter}.json")

            # Check if the user data file exists
            if os.path.exists(user_file_path):
                # Load the user data
                with open(user_file_path, "r") as f:
                    data = json.load(f)

                # Extract the relevant data from the JSON object
                registered_users = data["data"]["hoverData"]

                # Loop through each device brand and add the data to the user dataframe list
            if registered_users is not None:
                for dis_t in registered_users:
                    district_name = dis_t
                    regis_user = registered_users[dis_t]["registeredUsers"]

                    map_user_df_list.append([state, year, quarter, district_name, regis_user])

# third folder
# Loop through each quarter
for state in states:

    for year in range(2018, 2023):

        for quarter in range(1, 5):

            # Define the file path for the aggregated data
            top_file_path = os.path.join(main_path, "top", "transaction", "country", "india", "state", state, str(year),
                                         f"{quarter}.json")

            # Check if the aggregated data file exists
            if os.path.exists(top_file_path):

                # Load the aggregated data
                with open(top_file_path, "r") as f:
                    data = json.load(f)
                    if not data:
                        continue
                    # Extract the relevant data from the JSON object
                    districts = data["data"]["districts"]

                    # Loop through each district and metric
                    for distr in districts:
                        entity_name = distr["entityName"]
                        metr = distr["metric"]
                        t_ype = metr["type"]
                        c_ount = metr["count"]
                        a_mount = metr["amount"]

                        # Add the data to the aggregated dataframe list
                        top_trans_df_list.append([state, year, quarter, entity_name, t_ype, c_ount, a_mount])

            # Map pincode to district
            # Define the file path for the user data
            top_user_file_path = os.path.join(main_path, "top", "user", "country", "india", "state", state, str(year),
                                              f"{quarter}.json")

            # Check if the user data file exists
            if os.path.exists(top_user_file_path):

                # Load the user data
                with open(top_user_file_path, "r") as f:
                    data = json.load(f)
                    if not data:
                        continue

                    # Extract the relevant data from the JSON object
                    district_data = data["data"]["districts"]
                    pincode_data = data['data']['pincodes']

                    pincode_to_district = {}

                    for pincode in pincode_data:
                        if pincode['name'] in ['744103', '744101', '744105', '744102', '744104', '744112', '744107',
                                               '744211']:
                            pincode_to_district[pincode['name']] = 'south andaman'
                        elif pincode['name'] == '744202':
                            pincode_to_district[pincode['name']] = 'north and middle andaman'
                        elif pincode['name'] == '744301':
                            pincode_to_district[pincode['name']] = 'nicobars'

                    for district in district_data:
                        district_name = district['name']
                        district_registered_users = district['registeredUsers']

                        for pincode in pincode_data:
                            pincode_name = pincode['name']
                            registered_users = pincode['registeredUsers']

                            if pincode_name in pincode_to_district:
                                district_name = pincode_to_district[pincode_name]

                            top_user_df_list.append(
                                [state, year, quarter, pincode_name, registered_users, district_name,
                                 district_registered_users])

df_aggregated_transaction=pd.DataFrame(aggregated_trans_df_list,columns=["state","year","quarter","transaction_name","payment_type","payment_count","payment_amount"])
df_aggregated_user=pd.DataFrame(aggregated_user_df_list,columns=["state","year","quarter","reg_users","user_brand","count_users","percentage_user"])
df_map_transaction=pd.DataFrame(map_trans_df_list,columns=["state","year","quarter","district","metric_type","metric_count","metric_amount"])
df_map_user=pd.DataFrame(map_user_df_list,columns=["state","year","quarter","district","registeredusers"])
df_top_transaction=pd.DataFrame(top_trans_df_list,columns=["state","year","quarter","entity_name","t_ype","c_ount","a_mount"])
df_top_user=pd.DataFrame(top_user_df_list,columns=["state","year","quarter","pincode_name","registered_users","district_name","district_registered_users"])



# df_top_user
# Create a connection to the MySQL database
mydb = pymysql.connect(
  host="127.0.0.1",
  user="root",
  password="qwerty123",
  database="book"
)
# Create a cursor object
cursor = mydb.cursor()

# Create a connection engine to the database
engine = create_engine('mysql://root:qwerty123@127.0.0.1:3306/book')

df_aggregated_transaction.to_sql(name='aggregated_transaction',con=engine,if_exists='append',index=False)
df_aggregated_user.to_sql(name='aggregated_user',con=engine,if_exists='append',index=False)
df_map_transaction.to_sql(name='map_transaction',con=engine,if_exists='append',index=False)
df_map_user.to_sql(name='map_user',con=engine,if_exists='append',index=False)
df_top_transaction.to_sql(name='top_transaction',con=engine,if_exists='append',index=False)
df_top_user.to_sql(name='top_user',con=engine,if_exists='append',index=False)


#CREATING CONNECTION WITH SQL SERVER
#connection = sqlite3.connect("phone.db")
#cursor = connection.cursor()
#df_aggregated_transaction.to_sql('aggregated_transaction', con=engine, if_exists='replace')
#df_aggregated_user.to_sql('aggregated_user', con=engine, if_exists='replace')
#df_map_trans.to_sql('map_transaction', con=engine, if_exists='replace')
#df_map_user.to_sql('map_user', con=engine, if_exists='replace')
#df_top_transaction.to_sql('top_transaction', con=engine, if_exists='replace')
#df_top_user.to_sql('top_user', con=engine, if_exists='replace')
with st.sidebar:
    color = 'cyan'  # Replace with the desired color value
    st.markdown("<span style='color:{}'>Phonepe-Pulse</span>".format(color), unsafe_allow_html=True)
    st.write("---")
    SELECT = option_menu(
        menu_title = None,
        options = ["About","Search","Home","Basic insights","Contact"],
        icons =["bar-chart","search","house","toggles","at"],
        default_index=2,
        orientation="vertical",
        styles={
            "container": {"padding": "0!important", "background-color": "green","size":"cover"},
            "icon": {"color": "white", "font-size": "16px"},
            "nav-link": {"font-size": "16px", "text-align": "center", "margin": "-2px", "--hover-color": "#6F36AD"},
            "nav-link-selected": {"background-color": "#6F36AD"},}
        )

if SELECT == "Basic insights":
    st.title("BASIC INSIGHTS")
    st.write("----")
    st.subheader("Let's know some basic insights about the data")
    options = ["--select--","Top 10 states based on year and amount of transaction","Least 10 states based on type and amount of transaction",
               "Top 10 mobile brands based on percentage of transaction","Top 10 Registered-users based on States and District(pincode)",
               "Top 10 Districts based on states and amount of transaction","Least 10 Districts based on states and amount of transaction",
               "Least 10 registered-users based on Districts and states","Top 10 transactions_type based on states and transaction_amount"]
    select = st.selectbox("Select the option",options)
    if select=="Top 10 states based on year and amount of transaction":
        cursor.execute("SELECT DISTINCT state,sum(a_mount) as total_amount,year,quarter FROM top_transaction GROUP BY state,year,quarter ORDER BY total_amount DESC LIMIT 10");
        df = pd.DataFrame(cursor.fetchall(),columns=['state','total_amount','year','quarter'])
        col1,col2 = st.columns(2)
        with col1:
            st.write(df)
        with col2:
            st.title("Top 10 states based on year and amount of transaction")
            st._arrow_bar_chart(data=df,x="state",y="total_amount")
    elif select=="Least 10 states based on type and amount of transaction":
        cursor.execute("SELECT DISTINCT state,sum(a_mount) as total_amount,year,quarter FROM top_transaction GROUP BY state,year,quarter ORDER BY total_amount ASC LIMIT 10");
        df = pd.DataFrame(cursor.fetchall(),columns=['state','total_amount','year','quarter'])
        col1,col2 = st.columns(2)
        with col1:
            st.write(df)
        with col2:
            st.title("Least 10 states based on type and amount of transaction")
            st._arrow_bar_chart_chart(data=df,x="state",y="total_amount")
    elif select=="Top 10 mobile brands based on percentage of transaction":
        cursor.execute("SELECT DISTINCT user_brand,sum(percentage_user) as percent_user FROM aggregated_user GROUP BY user_brand ORDER BY percent_user DESC LIMIT 10");
        df = pd.DataFrame(cursor.fetchall(),columns=['user_brand','percent_user'])
        col1,col2 = st.columns(2)
        with col1:
            st.write(df)
        with col2:
            st.title("Top 10 mobile brands based on percentage of transaction")
            st._arrow_bar_chart(data=df,x="user_brand",y="percent_user")
    elif select=="Top 10 Registered-users based on States and District(pincode)":
        cursor.execute("SELECT DISTINCT state,district_name,max(district_registered_users) as register_user FROM top_user GROUP BY state,district_name ORDER BY register_user DESC LIMIT 10");
        df = pd.DataFrame(cursor.fetchall(),columns=['state','district_name','district_registered_users'])
        col1,col2 = st.columns(2)
        with col1:
            st.write(df)
        with col2:
            st.title("Top 10 Registered-users based on States and District(pincode)")
            st._arrow_bar_chart(data=df,x="state",y="district_registered_users")
    elif select=="Top 10 Districts based on states and amount of transaction":
        cursor.execute("SELECT DISTINCT state,district,max(metric_amount) as amount FROM map_transaction GROUP BY state,district ORDER BY amount DESC LIMIT 10");
        df = pd.DataFrame(cursor.fetchall(),columns=['state','district','metric_amount'])
        col1,col2 = st.columns(2)
        with col1:
            st.write(df)
        with col2:
            st.title("Top 10 Districts based on states and amount of transaction")
            st._arrow_bar_chart(data=df,x="state",y="metric_amount")
    elif select=="Least 10 Districts based on states and amount of transaction":
        cursor.execute("SELECT DISTINCT state,district,max(metric_amount) as amount FROM map_transaction GROUP BY state,district ORDER BY amount ASC LIMIT 10");
        df = pd.DataFrame(cursor.fetchall(),columns=['state','district','metric_amount'])
        col1,col2 = st.columns(2)
        with col1:
            st.write(df)
        with col2:
            st.title("Least 10 Districts based on states and amount of transaction")
            st.bar_chart(data=df,x="state",y="metric_amount")
    elif select=="Least 10 registered-users based on Districts and states":
        cursor.execute("SELECT DISTINCT state,district_name,max(registered_users) as r_users FROM top_user GROUP BY state,district_name ORDER BY r_users ASC LIMIT 10");
        df = pd.DataFrame(cursor.fetchall(),columns=['state','district_name','registered_users'])
        col1,col2 = st.columns(2)
        with col1:
            st.write(df)
        with col2:
            st.title("Least 10 registered-users based on Districts and states")
            st._arrow_bar_chart(data=df,x="state",y="registered_users")
    elif select=="Top 10 transactions_type based on states and transaction_amount":
        cursor.execute("SELECT DISTINCT state,payment_type,max(payment_amount) as amount FROM aggregated_transaction GROUP BY state,payment_type ORDER BY amount DESC LIMIT 10");
        df = pd.DataFrame(cursor.fetchall(),columns=['state','payment_type','payment_amount'])
        col1,col2 = st.columns(2)
        with col1:
            st.write(df)
        with col2:
            st.title("Top 10 transactions_type based on states and transaction_amount")
            st._arrow_bar_chart(data=df,x="state",y="payment_amount")


if SELECT == "Home":
    col1, col2 = st.columns(2)
    col1.image(Image.open(r'C:\Users\Admin\PycharmProjects\pythonProject\phone\phonepe.jpg'), width=200)
    with col1:
        subheader_text="PhonePe is an Indian digital payments and financial technology company"
        st.write("#",subheader_text)
        st.download_button("DOWNLOAD THE APP NOW", "https://www.phonepe.com/app-download/")
    with col2:
        Year = st.sidebar.slider("**Year**", min_value=2018, max_value=2022)
        Quarter = st.sidebar.slider("Quarter", min_value=1, max_value=4)
        Type = st.sidebar.selectbox("**Type**", ("Transactions", "Users"))

        if Type == "Transactions":
            col3, col4 = st.columns(2)

            # Overall State Data - TRANSACTIONS AMOUNT - INDIA MAP
            with col3:
                st.markdown("## :violet[Overall State Data - Transactions Amount]")
                cursor.execute(f"select state, sum(metric_count) as Total_Transactions, sum(metric_amount) as Total from map_transaction where year = {Year} and quarter = {Quarter} group by state order by Total")
                df1 = pd.DataFrame(cursor.fetchall(), columns=['state', 'Total_Transactions', 'Total'])
                df2 = pd.read_csv(r'C:\Users\Admin\PycharmProjects\pythonProject\phone\Statenames.csv')
                df1.state = pd.Series(df2['state'].values)

                fig = px.choropleth(df1,
                                    geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                                    featureidkey='properties.ST_NM',
                                    locations='state',
                                    color='Total',
                                    color_continuous_scale='sunset')

                fig.update_geos(fitbounds="locations", visible=False)
                st.plotly_chart(fig, use_container_width=True)

            # Overall State Data - TRANSACTIONS COUNT - INDIA MAP
            with col4:
                st.markdown("## :violet[Overall State Data - Transactions Count]")
                cursor.execute(f"select state, sum(metric_count) as Total_Transactions, sum(metric_amount) as Total from map_trans where year = {Year} and quarter = {Quarter} group by state order by Total")
                df1 = pd.DataFrame(cursor.fetchall(), columns=['state', 'Total_Transactions', 'Total'])
                df2 = pd.read_csv(r'C:\Users\Admin\PycharmProjects\pythonProject\phone\Statenames.csv')
                df1.Total_Transactions = df1.Total_Transactions.astype(int)
                df1.state = pd.Series(df2['state'].values)

                fig = px.choropleth(df1,
                                    geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                                    featureidkey='properties.ST_NM',
                                    locations='state',
                                    color='Total_Transactions',
                                    color_continuous_scale='sunset')

                fig.update_geos(fitbounds="locations", visible=False)
                st.plotly_chart(fig, use_container_width=True)



        elif Type == "Users":
            col1, col2, col3, col4 = st.columns([2, 2, 2, 2], gap="small")

            with col1:
                st.markdown("### :violet[Brands]")
                if Year == 2022 and Quarter in [2, 3, 4]:
                    st.markdown("#### Sorry No Data to Display for 2022 Qtr 2,3,4")
                else:
                    cursor.execute(f"select user_brand, sum(count_users) as Total_Count, avg(percentage_user)*100 as Avg_Percentage from aggregated_user where year = {Year} and quarter = {Quarter} group by user_brand order by Total_Count desc limit 10")
                    df = pd.DataFrame(cursor.fetchall(), columns=['user_brand', 'count_users', 'Avg_Percentage'])
                    fig = px.bar(df,
                                 title='Top 10',
                                 x="count_users",
                                 y="user_brand",
                                 orientation='v',
                                 color='Avg_Percentage',
                                 color_continuous_scale=px.colors.sequential.Agsunset)
                    st.plotly_chart(fig, use_container_width=True)

            with col2:
                st.markdown("### :violet[District]")
                cursor.execute(f"select district, sum(registeredusers) as Total_Users  from map_user where year = {Year} and quarter = {Quarter} group by district order by Total_Users desc limit 10")
                df = pd.DataFrame(cursor.fetchall(), columns=['district', 'Total_Users'])
                df.Total_Users = df.Total_Users.astype(float)
                fig = px.bar(df,
                             title='Top 10',
                             x="Total_Users",
                             y="district",
                             orientation='v',
                             color='Total_Users',
                             color_continuous_scale=px.colors.sequential.Agsunset)
                st.plotly_chart(fig, use_container_width=True)

            with col3:
                st.markdown("### :violet[Pincode]")
                cursor.execute(f"select pincode_name, sum(registered_users) as Total_Users from top_user where year = {Year} and quarter = {Quarter} group by pincode_name order by Total_Users desc limit 10")
                df = pd.DataFrame(cursor.fetchall(), columns=['pincode_name', 'Total_Users'])
                fig = px.pie(df,
                             values='Total_Users',
                             names='pincode_name',
                             title='Top 10',
                             color_discrete_sequence=px.colors.sequential.Agsunset,
                             hover_data=['Total_Users'])
                fig.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(fig, use_container_width=True)

            with col4:
                st.markdown("### :violet[State]")
                cursor.execute(f"select state, sum(registeredusers) as Total_Users from map_user where year = {Year} and quarter = {Quarter} group by state order by Total_Users desc limit 10")
                df = pd.DataFrame(cursor.fetchall(), columns=['state', 'Total_Users'])
                fig = px.pie(df, values='Total_Users',
                             names='state',
                             title='Top 10',
                             color_discrete_sequence=px.colors.sequential.Agsunset,
                             hover_data=['Total_Users'],
                             labels={'Total_Users': 'Total_Users'})

                fig.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(fig, use_container_width=True)

if SELECT == "About":
    col1,col2 = st.columns(2)
    with col1:
        video_file = open(r"C:\Users\Admin\PycharmProjects\pythonProject\phone\p.mp4", "rb")
        video_bytes = video_file.read()
        st.video(video_bytes)

    with col2:
        st.image(Image.open(r"C:\Users\Admin\PycharmProjects\pythonProject\phone\phonepe.jpg"),width = 600)
        st.write("---")
        st.markdown("""
            <p style="text-align: justify;">
                <strong>The Indian digital payments story has truly captured the world's imagination.</strong>
                From the largest towns to the remotest villages, there is a payments revolution being driven by the penetration of mobile phones, mobile internet and state-of-the-art payments infrastructure built as Public Goods championed by the central bank and the government.
                Founded in December 2015, PhonePe has been a strong beneficiary of the API driven digitisation of payments in India. When we started, we were constantly looking for granular and definitive data sources on digital payments in India.
                PhonePe Pulse is our way of giving back to the digital payments ecosystem.
            </p>
        """, unsafe_allow_html=True)
        st.write("---")
    col1 = st.columns(1)
    with col1[0]:
        st.title("THE BEAT OF PHONEPE")
        st.write("---")
        st.subheader("Phonepe became a leading digital payments company")
        st.image(Image.open(r"C:\Users\Admin\PycharmProjects\pythonProject\phone\PhonePe-Features-1.jpeg"),width = 750)
        with open(r"C:\Users\Admin\PycharmProjects\pythonProject\phone\annualreport.pdf","rb") as f:
            data = f.read()
        st.download_button("DOWNLOAD REPORT",data,file_name="annual_report.pdf")


if SELECT == "Contact":
    name = "anu"
    mail = (f'{"Mail :"}  {"anupamasindgi@gmail.com"}')
    description = "An Aspiring DATA-SCIENTIST..!"
    social_media = {
        "TWITTER": "https://www.linkedin.com/in/anupama-sindgi-234b71114/",
    }
    col1, col2, col3 = st.columns(3)
    col2.image(Image.open(r"C:\Users\Admin\PycharmProjects\pythonProject\phone\phonepe.jpg"), width=350)
    with col3:
        st.title(name)
        st.write(description)
        st.write("---")
        st.subheader(mail)
    st.write("#")
    cols = st.columns(len(social_media))
    for index, (platform, link) in enumerate(social_media.items()):
        cols[index].write(f"[{platform}]({link})")

if SELECT =="Search":
    Topic = ["","Transaction-Type","District","Brand","Top-Transactions","Registered-users"]
    choice_topic = st.selectbox("Search by",Topic)

#creating functions for query search in sqlite to get the data
    def type_(type):
        cursor.execute(f"SELECT DISTINCT state,quarter,year,payment_type,payment_amount FROM aggregated_transaction WHERE payment_type = '{type}' ORDER BY state,quarter,year");
        df = pd.DataFrame(cursor.fetchall(), columns=['state','quarter', 'year', 'payment_type', 'payment_amount'])
        return df
    def type_year(year,type):
        cursor.execute(f"SELECT DISTINCT state,year,quarter,payment_type,payment_amount FROM aggregated_transaction WHERE year = '{year}' AND payment_type = '{type}' ORDER BY state,quarter,year");
        df = pd.DataFrame(cursor.fetchall(), columns=['state', 'year',"quarter", 'payment_type', 'payment_amount'])
        return df
    def type_state(state,year,type):
        cursor.execute(f"SELECT DISTINCT state,year,quarter,payment_type,payment_amount FROM aggregated_transaction WHERE state = '{state}' AND payment_type = '{type}' And year = '{year}' ORDER BY state,quarter,year");
        df = pd.DataFrame(cursor.fetchall(), columns=['state', 'year',"quarter", 'payment_type', 'payment_amount'])
        return df

    def district_choice_state(_state):
        cursor.execute(f"SELECT DISTINCT state,year,quarter,district,metric_amount FROM map_transaction WHERE state = '{_state}' ORDER BY state,year,quarter,district");
        df = pd.DataFrame(cursor.fetchall(), columns=['state', 'year',"quarter", 'district', 'metric_amount'])
        return df
    def dist_year_state(year,_state):
        cursor.execute(f"SELECT DISTINCT state,year,quarter,district,metric_amount FROM map_transaction WHERE year = '{year}' AND state = '{_state}' ORDER BY state,year,quarter,district");
        df = pd.DataFrame(cursor.fetchall(), columns=['state', 'year',"quarter", 'district', 'metric_amount'])
        return df
    def district_year_state(_dist,year,_state):
        cursor.execute(f"SELECT DISTINCT state,year,quarter,district,metric_amount FROM map_transaction WHERE district = '{_dist}' AND state = '{_state}' AND year = '{year}' ORDER BY state,year,quarter,district");
        df = pd.DataFrame(cursor.fetchall(), columns=['state', 'year', "quarter", 'district', 'metric_amount'])
        return df
    def brand_(brand_type):
        cursor.execute(f"SELECT state,year,quarter,user_brand,percentage_user FROM aggregated_user WHERE user_brand='{brand_type}' ORDER BY state,year,quarter,user_brand,percentage_user DESC");
        df = pd.DataFrame(cursor.fetchall(), columns=['state', 'year',"quarter", 'user_brand', 'percentage_user'])
        return df
    def brand_year(brand_type,year):
        cursor.execute(f"SELECT state,year,quarter,user_brand,percentage_user FROM aggregated_user WHERE year = '{year}' AND user_brand='{brand_type}' ORDER BY state,year,quarter,user_brand,percentage_user DESC");
        df = pd.DataFrame(cursor.fetchall(), columns=['state', 'year',"quarter", 'user_brand', 'percentage_user'])
        return df
    def brand_state(state,brand_type,year):
        cursor.execute(f"SELECT state,year,quarter,user_brand,percentage_user FROM aggregated_user WHERE state = '{state}' AND user_brand='{brand_type}' AND year = '{year}' ORDER BY state,year,quarter,user_brand,percentage_user DESC");
        df = pd.DataFrame(cursor.fetchall(), columns=['state', 'year',"quarter", 'user_brand', 'percentage_user'])
        return df
    def transaction_state(_state):
        cursor.execute(f"SELECT state,year,quarter,entity_name,c_ount,a_mount FROM top_transaction WHERE state = '{_state}' GROUP BY state,year,quarter")
        df = pd.DataFrame(cursor.fetchall(), columns=['state', 'year',"quarter", 'entity_name', 'c_ount', 'a_mount'])
        return df
    def transaction_year(_state,_year):
        cursor.execute(f"SELECT state,year,quarter,entity_name,c_ount,a_mount FROM top_transaction FROM top_transaction WHERE year = '{_year}' AND state = '{_state}' GROUP BY state,year,quarter")
        df = pd.DataFrame(cursor.fetchall(), columns=['state', 'year',"quarter", 'entity_name', 'c_ount', 'a_mount'])
        return df
    def transaction_quarter(_state,_year,_quarter):
        cursor.execute(f"SELECT state,year,quarter,entity_name,c_ount,a_mount FROM top_transaction FROM top_transaction WHERE year = '{_year}' AND quarter = '{_quarter}' AND state = '{_state}' GROUP BY state,year,quarter")
        df = pd.DataFrame(cursor.fetchall(), columns=['state', 'year',"quarter", 'entity_name', 'c_ount', 'a_mount'])
        return df
    def registered_user_state(_state):
        cursor.execute(f"SELECT state,year,quarter,district,registeredusers FROM map_user WHERE state = '{_state}' ORDER BY state,year,quarter,district")
        df = pd.DataFrame(cursor.fetchall(), columns=['state', 'year',"quarter", 'district', 'registeredusers'])
        return df
    def registered_user_year(_state,_year):
        cursor.execute(f"SELECT state,year,quarter,district,registeredusers FROM map_user WHERE year = '{_year}' AND state = '{_state}' ORDER BY state,year,quarter,district")
        df = pd.DataFrame(cursor.fetchall(), columns=['state', 'year',"quarter", 'district', 'registeredusers'])
        return df
    def registered_user_district(_state,_year,_dist):
        cursor.execute(f"SELECT state,year,quarter,districtrRegisteredusers FROM map_user WHERE year = '{_year}' AND state = '{_state}' AND district = '{_dist}' ORDER BY state,year,quarter,district")
        df = pd.DataFrame(cursor.fetchall(), columns=['state', 'year',"quarter", 'district', 'registeredusers'])
        return df


    if choice_topic=="Transaction-Type":
        col1,col2,col3 = st.columns(3)
        with col1:
            st.subheader("-- 5 TYPES OF TRANSACTION --")
            transaction_type = st.selectbox("search by", ["Choose an option", "Peer-to-peer payments",
                                                          "Merchant payments", "Financial Services",
                                                          "Recharge & bill payments", "Others"], 0)
        with col2:
            st.subheader("-- 5 YEARS --")
            choice_year = st.selectbox("Year", ["", "2018", "2019", "2020", "2021", "2022"], 0)
        with col3:
            st.subheader("-- 36 STATES --")
            menu_state = ["", 'uttar-pradesh', 'jharkhand', 'puducherry', 'rajasthan', 'odisha', 'nagaland',
                          'chandigarh', 'dadra-&-nagar-haveli-&-daman-&-diu', 'assam', 'haryana', 'jammu-&-kashmir',
                          'tamil-nadu', 'himachal-pradesh', 'ladakh', 'bihar', 'maharashtra', 'uttarakhand',
                          'karnataka', 'lakshadweep', 'andhra-pradesh', 'sikkim', 'madhya-pradesh', 'mizoram',
                          'kerala', 'manipur', 'arunachal-pradesh', 'andaman-&-nicobar-islands', 'delhi', 'tripura',
                          'chhattisgarh', 'meghalaya', 'goa', 'west-bengal', 'telangana', 'gujarat', 'punjab']
            choice_state = st.selectbox("state", menu_state, 0)

        if transaction_type:
            col1,col2,col3, = st.columns(3)
            with col1:
                st.subheader(f'{transaction_type}')
                st.write(type_(transaction_type))
        if transaction_type and choice_year:
            with col2:
                st.subheader(f' in {choice_year}')
                st.write(type_year(choice_year,transaction_type))
        if transaction_type and choice_state and choice_year:
            with col3:
                st.subheader(f' in {choice_state}')
                st.write(type_state(choice_state,choice_year,transaction_type))

    if choice_topic=="District":
        col1,col2,col3 = st.columns(3)
        with col1:
            st.subheader("-- 36 STATES --")
            menu_state = ["", 'uttar-pradesh', 'jharkhand', 'puducherry', 'rajasthan', 'odisha', 'nagaland',
                          'chandigarh', 'dadra-&-nagar-haveli-&-daman-&-diu', 'assam', 'haryana', 'jammu-&-kashmir',
                          'tamil-nadu', 'himachal-pradesh', 'ladakh', 'bihar', 'maharashtra', 'uttarakhand',
                          'karnataka', 'lakshadweep', 'andhra-pradesh', 'sikkim', 'madhya-pradesh', 'mizoram',
                          'kerala', 'manipur', 'arunachal-pradesh', 'andaman-&-nicobar-islands', 'delhi', 'tripura',
                          'chhattisgarh', 'meghalaya', 'goa', 'west-bengal', 'telangana', 'gujarat', 'punjab']
            choice_state = st.selectbox("state", menu_state, 0)
        with col2:
            st.subheader("-- 5 YEARS --")
            choice_year = st.selectbox("Year", ["", "2018", "2019", "2020", "2021", "2022"], 0)
        with col3:
            st.subheader("-- SELECT DISTRICTS --")
            district = st.selectbox("search by", df_map_transaction["district"].unique().tolist())
        if choice_state:
            col1,col2,col3 = st.columns(3)
            with col1:
                st.subheader(f'{choice_state}')
                st.write(district_choice_state(choice_state))
        if choice_year and choice_state:
            with col2:
                st.subheader(f'in {choice_year} ')
                st.write(dist_year_state(choice_year,choice_state))
        if district and choice_state and choice_year:
            with col3:
                st.subheader(f'in {district} ')
                st.write(district_year_state(district,choice_year,choice_state))

    if choice_topic=="Brand":
        col1,col2,col3 = st.columns(3)
        with col1:
            st.subheader("-- TYPES OF BRANDS --")
            mobiles = ["",'Xiaomi', 'Vivo', 'Samsung', 'Oppo', 'Realme', 'Apple', 'Huawei', 'Motorola', 'Tecno', 'Infinix',
                       'Lenovo', 'Lava', 'OnePlus', 'Micromax', 'Asus', 'Gionee', 'HMD Global', 'COOLPAD', 'Lyf',
                       'Others']
            brand_type = st.selectbox("search by",mobiles, 0)
        with col2:
            st.subheader("-- 5 YEARS --")
            choice_year = st.selectbox("Year", ["", "2018", "2019", "2020", "2021", "2022"], 0)
        with col3:
            st.subheader("-- 36 STATES --")
            menu_state = ["", 'uttar-pradesh', 'jharkhand', 'puducherry', 'rajasthan', 'odisha', 'nagaland',
                          'chandigarh', 'dadra-&-nagar-haveli-&-daman-&-diu', 'assam', 'haryana', 'jammu-&-kashmir',
                          'tamil-nadu', 'himachal-pradesh', 'ladakh', 'bihar', 'maharashtra', 'uttarakhand',
                          'karnataka', 'lakshadweep', 'andhra-pradesh', 'sikkim', 'madhya-pradesh', 'mizoram',
                          'kerala', 'manipur', 'arunachal-pradesh', 'andaman-&-nicobar-islands', 'delhi', 'tripura',
                          'chhattisgarh', 'meghalaya', 'goa', 'west-bengal', 'telangana', 'gujarat', 'punjab']
            choice_state = st.selectbox("State", menu_state, 0)

        if brand_type:
            col1,col2,col3, = st.columns(3)
            with col1:
                st.subheader(f'{brand_type}')
                st.write(brand_(brand_type))
        if brand_type and choice_year:
            with col2:
                st.subheader(f' in {choice_year}')
                st.write(brand_year(brand_type,choice_year))
        if brand_type and choice_state and choice_year:
            with col3:
                st.subheader(f' in {choice_state}')
                st.write(brand_state(choice_state,brand_type,choice_year))

    if choice_topic=="Top-Transactions":
        col1,col2,col3 = st.columns(3)
        with col1:
            st.subheader("-- 36 STATES --")
            menu_state = ["", 'uttar-pradesh', 'jharkhand', 'puducherry', 'rajasthan', 'odisha', 'nagaland',
                          'chandigarh', 'dadra-&-nagar-haveli-&-daman-&-diu', 'assam', 'haryana', 'jammu-&-kashmir',
                          'tamil-nadu', 'himachal-pradesh', 'ladakh', 'bihar', 'maharashtra', 'uttarakhand',
                          'karnataka', 'lakshadweep', 'andhra-pradesh', 'sikkim', 'madhya-pradesh', 'mizoram',
                          'kerala', 'manipur', 'arunachal-pradesh', 'andaman-&-nicobar-islands', 'delhi', 'tripura',
                          'chhattisgarh', 'meghalaya', 'goa', 'west-bengal', 'telangana', 'gujarat', 'punjab']
            choice_state = st.selectbox("State", menu_state, 0)
        with col2:
            st.subheader("-- 5 YEARS --")
            choice_year = st.selectbox("Year", ["", "2018", "2019", "2020", "2021", "2022"], 0)
        with col3:
            st.subheader("--4 quarters --")
            menu_quarter = ["", "1", "2", "3", "4"]
            choice_quarter = st.selectbox("quarter", menu_quarter, 0)

        if choice_state:
            with col1:
                st.subheader(f'{choice_state}')
                st.write(transaction_state(choice_state))
        if choice_state and choice_year:
            with col2:
                st.subheader(f'{choice_year}')
                st.write(transaction_year(choice_state,choice_year))
        if choice_state and choice_quarter:
            with col3:
                st.subheader(f'{choice_quarter}')
                st.write(transaction_quarter(choice_state,choice_year,choice_quarter))

    if choice_topic=="Registered-users":
        col1,col2,col3 = st.columns(3)
        with col1:
            st.subheader("-- 36 STATES --")
            menu_state = ["", 'uttar-pradesh', 'jharkhand', 'puducherry', 'rajasthan', 'odisha', 'nagaland',
                          'chandigarh', 'dadra-&-nagar-haveli-&-daman-&-diu', 'assam', 'haryana', 'jammu-&-kashmir',
                          'tamil-nadu', 'himachal-pradesh', 'ladakh', 'bihar', 'maharashtra', 'uttarakhand',
                          'karnataka', 'lakshadweep', 'andhra-pradesh', 'sikkim', 'madhya-pradesh', 'mizoram',
                          'kerala', 'manipur', 'arunachal-pradesh', 'andaman-&-nicobar-islands', 'delhi', 'tripura',
                          'chhattisgarh', 'meghalaya', 'goa', 'west-bengal', 'telangana', 'gujarat', 'punjab']
            choice_state = st.selectbox("State", menu_state, 0)
        with col2:
            st.subheader("-- 5 YEARS --")
            choice_year = st.selectbox("Year", ["", "2018", "2019", "2020", "2021", "2022"], 0)
        with col3:
            st.subheader("-- SELECT DISTRICTS --")
            district = st.selectbox("search by", df_map_transaction["district"].unique().tolist())

        if choice_state:
            with col1:
                st.subheader(f'{choice_state}')
                st.write(registered_user_state(choice_state))
        if choice_state and choice_year:
            with col2:
                st.subheader(f'{choice_year}')
                st.write(registered_user_year(choice_state,choice_year))
        if choice_state and choice_year and district:
            with col3:
                st.subheader(f'{district}')
                st.write(registered_user_district(choice_state,choice_year,district))



