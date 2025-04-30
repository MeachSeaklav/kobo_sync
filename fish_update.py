import streamlit as st
import requests
import pandas as pd
import sqlalchemy
from sqlalchemy import text
from datetime import datetime, timedelta
import streamlit as st
import time
# ========================================================
# Database connection
con = sqlalchemy.create_engine('mariadb+pymysql://root:Mymariadb123@104.248.155.82:3306/FishStat_ETL')

# Kobo API URL and Token
KOBO_URL = 'https://eu.kobotoolbox.org/api/v2/assets/aLQEf7RcyAYyquCRdDCr4J/data/?format=json'
KOBO_TOKEN = 'access_token 00ed4c22cb2cdc2bcd4ae1539c20aaa80c21b20d'


def parse_list(value: str):
    if isinstance(value, str):
        return value.split(' ')
    return value

# def clean_dataframe(df):
#     """ Clean invalid numbers and quotes before inserting """
#     for col in df.columns:
#         df[col] = df[col].apply(lambda x: x if pd.isnull(x) or str(x).isdigit() or str(x) == 'null' else 'null')
#     return df


def insert_or_update_db(df, table_name):
    """ Insert new or update existing rows based on id and uuid """
    df = df.where(pd.notnull(df), 'null')
    df.replace(['', 'NaN', 'nan', 'NULL', 'Null'], 'null', inplace=True)

    columns = ', '.join(df.columns)

    with con.begin() as connection:
        for row in df.itertuples(index=False, name=None):
            # Find id and uuid indexes
            try:
                id_index = list(df.columns).index('id')    # not _id after your rename
                uuid_index = list(df.columns).index('uuid')
            except ValueError:
                continue  # id or uuid missing, skip this row

            id_value = row[id_index]
            uuid_value = row[uuid_index]

            # Check if ID exists
            check_query = f"SELECT uuid FROM {table_name} WHERE id = '{id_value}'"
            result = connection.execute(text(check_query)).fetchone()

            # Prepare row values
            values = []
            for val in row:
                if val == 'null' or val is None:
                    values.append('null')
                else:
                    safe_val = str(val).replace("'", "''")
                    values.append(f"'{safe_val}'")
            values_str = ', '.join(values)

            if result:
                db_uuid = result[0]
                if db_uuid != uuid_value:
                    # UUID different, update (delete old, insert new)
                    delete_query = f"DELETE FROM {table_name} WHERE id = '{id_value}'"
                    connection.execute(text(delete_query))
                    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values_str})"
                    connection.execute(text(insert_query))
            else:
                # New record
                insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values_str})"
                connection.execute(text(insert_query))




# ========================================================
# Kobo Fetch Functions

def get_fishcatch_from_kobo():
    r = requests.get(KOBO_URL, headers={'Authorization': KOBO_TOKEN})
    main_df = pd.json_normalize(r.json()['results']).drop(columns=['nat_fishcatch', 'aqu_fishcatch', 'processing', 'patrol'])
    main_df.columns = [i.replace('_', '').replace('/', '_').replace('.', '_').lower() for i in main_df.columns.tolist()]
    main_df['inspectorate'] = main_df['inspectorate'].astype(int)
    main_df['province'] = main_df['province'].astype(int)
    main_df.attachments = main_df.attachments.astype(str).str.replace('[', '').replace(']', '')
    main_df.geolocation = main_df.geolocation.apply(lambda x: str(x).replace('[', '').replace(']', ''))
    main_df.tags = main_df.tags.apply(lambda x: str(x).replace('[', '').replace(']', ''))
    main_df.notes = main_df.notes.apply(lambda x: str(x).replace('[', '').replace(']', ''))
    main_df.submissiontime = pd.to_datetime(main_df.submissiontime).dt.strftime('%Y-%m-%d %H:%M:%S.%f')
    insert_or_update_db(main_df, 'KOBO_FAI_FISHCATCHING_MAIN')


def _add_id(id_value, uuid_value, value):
    if isinstance(value, dict):
        value['id'] = id_value
        value['uuid'] = uuid_value    # <- fix typo here (was uuide before)
    return value

def get_natural_fishcatch_from_kobo():
    r = requests.get(KOBO_URL, headers={'Authorization': KOBO_TOKEN})
    df = pd.json_normalize(r.json()['results'])
    df = df[['_id', "_uuid", "nat_fishcatch"]].explode('nat_fishcatch').dropna(subset=['nat_fishcatch'])
    df['nat_fishcatch_w_id'] = df.apply(lambda x: _add_id(x['_id'], x['_uuid'], x['nat_fishcatch']), axis=1)
    df = pd.json_normalize(df['nat_fishcatch_w_id'])
    df.columns = [i.replace('_', '').replace('/', '_').replace('.', '_').lower() for i in df.columns.tolist()]
    df['natfishcatch_date'] = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
    insert_or_update_db(df, 'KOBO_FAI_NAT_FISHCATCHING_MAIN')

def get_aqu_fishcatch_from_kobo():
    r = requests.get(KOBO_URL, headers={'Authorization': KOBO_TOKEN})
    df = pd.json_normalize(r.json()['results'])
    df = df[['_id', '_uuid', 'aqu_fishcatch']].explode('aqu_fishcatch').dropna(subset=['aqu_fishcatch'])
    df['aqu_fishcatch_w_id'] = df.apply(lambda x: _add_id(x['_id'], x['_uuid'], x['aqu_fishcatch']), axis=1)
    df = pd.json_normalize(df['aqu_fishcatch_w_id'])
    df.columns = [i.replace('_', '').replace('/', '_').replace('.', '_').lower() for i in df.columns.tolist()]
    df['aqu_fishcatch_date'] = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
    insert_or_update_db(df, 'KOBO_FIA_AQU_FISHCATCH_FISHCATCHING')

def get_processing_fishcatch_from_kobo():
    r = requests.get(KOBO_URL, headers={'Authorization': KOBO_TOKEN})
    df = pd.json_normalize(r.json()['results'])
    df = df[['_id', '_uuid', 'processing']].explode('processing').dropna(subset=['processing'])
    df['processing_w_id'] = df.apply(lambda x: _add_id(x['_id'], x['_uuid'], x['processing']), axis=1)
    df = pd.json_normalize(df['processing_w_id'])
    df.columns = [i.replace('_', '').replace('/', '_').replace('.', '_').lower() for i in df.columns.tolist()]
    df['processing_date'] = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
    insert_or_update_db(df, 'KOBO_PROCESSING_FIA_FISHCATCH')

def get_petrol_fishcatch_from_kobo():
    r = requests.get(KOBO_URL, headers={'Authorization': KOBO_TOKEN})
    data = r.json()
    df = pd.json_normalize(data['results'])
    patrol_df = df[['_id', '_uuid', 'patrol']].explode('patrol')
    patrol_df.dropna(subset=['patrol'], inplace=True)
    patrol_df['patrol_w_id'] = patrol_df.apply(lambda row: _add_id(row['_id'], row['_uuid'], row['patrol']), axis=1)
    patrol_df = pd.json_normalize(patrol_df['patrol_w_id'])
    patrol_df.columns = [c.replace('_', '').replace('/', '_').replace('.', '_').lower() for c in patrol_df.columns.tolist()]
    if 'patrol_enforcement' in patrol_df.columns:
        patrol_df['patrol_enforcement'] = patrol_df['patrol_enforcement'].apply(lambda x: parse_list(x) if isinstance(x, str) else x)
    patrol_df['patrol_date'] = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
    patrol_df = patrol_df.where(pd.notnull(patrol_df), 'null')
    patrol_df.replace('', 'null', inplace=True)
    for col in patrol_df.columns:
        patrol_df[col] = patrol_df[col].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)
    insert_or_update_db(patrol_df, 'KOBO_PATROL_FIA_FISHCATCH')






# Title with icon and custom style
st.markdown("""
    <div style='text-align: left; padding: 10px 0;'>
        <h1 style='font-family: "Segoe UI", "Times New Roman", serif; font-size: 38px; font-weight: bold; color: white;'>
            📤 Kobo Data Push Dashboard
        </h1>
        <p style='font-family: "Segoe UI", "Times New Roman", serif; font-size: 18px; color: #ccc; margin-top: 0px;'>
            🔄 Click a button below to push the latest Kobo data into the correct database table.
        </p>
    </div>
""", unsafe_allow_html=True)

# Spacer for layout clarity
st.markdown("<br>", unsafe_allow_html=True)

# Example button style block (optional if using native st.button)
button_style = """
    <style>
        div.stButton > button {
            background-color: #1f77b4;
            color: white;
            padding: 0.6em 1.2em;
            border-radius: 8px;
            border: 1px solid #444;
            font-size: 16px;
            
            transition: background-color 0.3s ease;
        }
        div.stButton > button:hover {
            background-color: #105177;
        }
    </style>
"""
st.markdown(button_style, unsafe_allow_html=True)



# def show_progress(task_func, label=""):
#     progress_bar = st.progress(0)
#     status_text = st.empty()
    
#     try:
#         # Simulate step-by-step progress (you can customize this to real steps)
#         for i in range(10):  # 10 steps = 10%, 20%, ..., 100%
#             time.sleep(0.2)  # Simulate processing time
#             progress_bar.progress((i + 1) * 10)
#             status_text.text(f"{label} Progress: {(i + 1) * 10}%")

#         # Execute the actual data push function
#         task_func()

#         # Final feedback
#         st.success(f"✅ {label} pushed successfully!")

#     except Exception as e:
#         st.error(f"❌ Failed to push {label}: {e}")

#     finally:
#         progress_bar.empty()
#         status_text.empty()


def show_progress(task_func, steps=5, sleep_time=0.5):
    progress_bar = st.progress(0)
    try:
        for i in range(steps):
            # Simulate task progress
            time.sleep(sleep_time)
            progress_bar.progress((i + 1) * (100 // steps))
        task_func()
        st.success("✅ Data pushed successfully!")
    except Exception as e:
        st.error(f"❌ Failed: {e}")
    finally:
        progress_bar.empty()

if st.button("🐳 Push Main Catch Data"):
    show_progress(get_fishcatch_from_kobo)

if st.button("🐢 Push Natural Fishcatch Data"):
    show_progress(get_natural_fishcatch_from_kobo)

if st.button("🐠 Push Aquaculture Fishcatch Data"):
    show_progress(get_aqu_fishcatch_from_kobo)

if st.button("⚙️ Push Processing Fishcatch Data"):
    show_progress(get_processing_fishcatch_from_kobo)

if st.button("🛠️ Push Patrol Data"):
    show_progress(get_petrol_fishcatch_from_kobo)

# ========================================================
# Streamlit page layout

# st.title("📦 Kobo Data Push Dashboard")
# st.write("Each button will push fresh Kobo data into the correct table.")

# if st.button("🚀 Push Main Catch Data"):
#     try:
#         get_fishcatch_from_kobo()
#         st.success("✅ Main Catch Data pushed successfully!")
#     except Exception as e:
#         st.error(f"❌ Failed: {e}")

# if st.button("🚀 Push Natural Fishcatch Data"):
#     try:
#         get_natural_fishcatch_from_kobo()
#         st.success("✅ Natural Fishcatch Data pushed successfully!")
#     except Exception as e:
#         st.error(f"❌ Failed: {e}")

# if st.button("🚀 Push Aquaculture Fishcatch Data"):
#     try:
#         get_aqu_fishcatch_from_kobo()
#         st.success("✅ Aquaculture Fishcatch Data pushed successfully!")
#     except Exception as e:
#         st.error(f"❌ Failed: {e}")

# if st.button("🚀 Push Processing Fishcatch Data"):
#     try:
#         get_processing_fishcatch_from_kobo()
#         st.success("✅ Processing Fishcatch Data pushed successfully!")
#     except Exception as e:
#         st.error(f"❌ Failed: {e}")

# if st.button("🚀 Push Patrol Data"):
#     try:
#         get_petrol_fishcatch_from_kobo()
#         st.success("✅ Patrol Data pushed successfully!")
#     except Exception as e:
#         st.error(f"❌ Failed: {e}")





# import streamlit as st
# import requests
# import pandas as pd
# import sqlalchemy
# from sqlalchemy import text
# from datetime import datetime, timedelta
# import time

# # ========================================================
# # Database connection
# con = sqlalchemy.create_engine('mariadb+pymysql://root:Mymariadb123@104.248.155.82:3306/FishStat_ETL')

# # Kobo API URL and Token
# KOBO_URL = 'https://eu.kobotoolbox.org/api/v2/assets/aLQEf7RcyAYyquCRdDCr4J/data/?format=json'
# KOBO_TOKEN = 'access_token 00ed4c22cb2cdc2bcd4ae1539c20aaa80c21b20d'


# def parse_list(value: str):
#     if isinstance(value, str):
#         return value.split(' ')
#     return value


# def insert_or_update_db(df, table_name):
#     """Insert new or update existing rows based on id and uuid"""
#     df = df.where(pd.notnull(df), 'null')
#     df.replace(['', 'NaN', 'nan', 'NULL', 'Null'], 'null', inplace=True)
#     columns = ', '.join(df.columns)

#     with con.begin() as connection:
#         for row in df.itertuples(index=False, name=None):
#             try:
#                 id_index = list(df.columns).index('id')
#                 uuid_index = list(df.columns).index('uuid')
#             except ValueError:
#                 continue  # skip if missing

#             id_value = row[id_index]
#             uuid_value = row[uuid_index]

#             check_query = f"SELECT uuid FROM {table_name} WHERE id = '{id_value}'"
#             result = connection.execute(text(check_query)).fetchone()

#             values = []
#             for val in row:
#                 if val == 'null' or val is None:
#                     values.append('null')
#                 else:
#                     safe_val = str(val).replace("'", "''")
#                     values.append(f"'{safe_val}'")
#             values_str = ', '.join(values)

#             if result:
#                 db_uuid = result[0]
#                 if db_uuid != uuid_value:
#                     connection.execute(text(f"DELETE FROM {table_name} WHERE id = '{id_value}'"))
#                     connection.execute(text(f"INSERT INTO {table_name} ({columns}) VALUES ({values_str})"))
#             else:
#                 connection.execute(text(f"INSERT INTO {table_name} ({columns}) VALUES ({values_str})"))


# def _add_id(id_value, uuid_value, value):
#     if isinstance(value, dict):
#         value['id'] = id_value
#         value['uuid'] = uuid_value
#     return value


# def get_fishcatch_from_kobo(progress_callback=None):
#     r = requests.get(KOBO_URL, headers={'Authorization': KOBO_TOKEN})
#     main_df = pd.json_normalize(r.json()['results']).drop(columns=['nat_fishcatch', 'aqu_fishcatch', 'processing', 'patrol'])
#     main_df.columns = [i.replace('_', '').replace('/', '_').replace('.', '_').lower() for i in main_df.columns.tolist()]
#     main_df['inspectorate'] = main_df['inspectorate'].astype(int)
#     main_df['province'] = main_df['province'].astype(int)
#     main_df.attachments = main_df.attachments.astype(str).str.replace('[', '').replace(']', '')
#     main_df.geolocation = main_df.geolocation.apply(lambda x: str(x).replace('[', '').replace(']', ''))
#     main_df.tags = main_df.tags.apply(lambda x: str(x).replace('[', '').replace(']', ''))
#     main_df.notes = main_df.notes.apply(lambda x: str(x).replace('[', '').replace(']', ''))
#     main_df.submissiontime = pd.to_datetime(main_df.submissiontime).dt.strftime('%Y-%m-%d %H:%M:%S.%f')
#     insert_or_update_db(main_df, 'KOBO_FAI_FISHCATCHING_MAIN')
#     if progress_callback:
#         progress_callback(1.0)


# def get_natural_fishcatch_from_kobo(progress_callback=None):
#     r = requests.get(KOBO_URL, headers={'Authorization': KOBO_TOKEN})
#     df = pd.json_normalize(r.json()['results'])
#     df = df[['_id', '_uuid', 'nat_fishcatch']].explode('nat_fishcatch').dropna(subset=['nat_fishcatch'])
#     df['nat_fishcatch_w_id'] = df.apply(lambda x: _add_id(x['_id'], x['_uuid'], x['nat_fishcatch']), axis=1)
#     df = pd.json_normalize(df['nat_fishcatch_w_id'])
#     df.columns = [i.replace('_', '').replace('/', '_').replace('.', '_').lower() for i in df.columns.tolist()]
#     df['natfishcatch_date'] = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
#     insert_or_update_db(df, 'KOBO_FAI_NAT_FISHCATCHING_MAIN')
#     if progress_callback:
#         progress_callback(1.0)


# def get_aqu_fishcatch_from_kobo(progress_callback=None):
#     r = requests.get(KOBO_URL, headers={'Authorization': KOBO_TOKEN})
#     df = pd.json_normalize(r.json()['results'])
#     df = df[['_id', '_uuid', 'aqu_fishcatch']].explode('aqu_fishcatch').dropna(subset=['aqu_fishcatch'])
#     df['aqu_fishcatch_w_id'] = df.apply(lambda x: _add_id(x['_id'], x['_uuid'], x['aqu_fishcatch']), axis=1)
#     df = pd.json_normalize(df['aqu_fishcatch_w_id'])
#     df.columns = [i.replace('_', '').replace('/', '_').replace('.', '_').lower() for i in df.columns.tolist()]
#     df['aqu_fishcatch_date'] = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
#     insert_or_update_db(df, 'KOBO_FIA_AQU_FISHCATCH_FISHCATCHING')
#     if progress_callback:
#         progress_callback(1.0)


# def get_processing_fishcatch_from_kobo(progress_callback=None):
#     r = requests.get(KOBO_URL, headers={'Authorization': KOBO_TOKEN})
#     df = pd.json_normalize(r.json()['results'])
#     df = df[['_id', '_uuid', 'processing']].explode('processing').dropna(subset=['processing'])
#     df['processing_w_id'] = df.apply(lambda x: _add_id(x['_id'], x['_uuid'], x['processing']), axis=1)
#     df = pd.json_normalize(df['processing_w_id'])
#     df.columns = [i.replace('_', '').replace('/', '_').replace('.', '_').lower() for i in df.columns.tolist()]
#     df['processing_date'] = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
#     insert_or_update_db(df, 'KOBO_PROCESSING_FIA_FISHCATCH')
#     if progress_callback:
#         progress_callback(1.0)


# def get_petrol_fishcatch_from_kobo(progress_callback=None):
#     r = requests.get(KOBO_URL, headers={'Authorization': KOBO_TOKEN})
#     data = r.json()
#     df = pd.json_normalize(data['results'])
#     patrol_df = df[['_id', '_uuid', 'patrol']].explode('patrol').dropna(subset=['patrol'])
#     patrol_df['patrol_w_id'] = patrol_df.apply(lambda row: _add_id(row['_id'], row['_uuid'], row['patrol']), axis=1)
#     patrol_df = pd.json_normalize(patrol_df['patrol_w_id'])
#     patrol_df.columns = [c.replace('_', '').replace('/', '_').replace('.', '_').lower() for c in patrol_df.columns.tolist()]
#     if 'patrol_enforcement' in patrol_df.columns:
#         patrol_df['patrol_enforcement'] = patrol_df['patrol_enforcement'].apply(lambda x: parse_list(x) if isinstance(x, str) else x)
#     patrol_df['patrol_date'] = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
#     patrol_df = patrol_df.where(pd.notnull(patrol_df), 'null')
#     patrol_df.replace('', 'null', inplace=True)
#     for col in patrol_df.columns:
#         patrol_df[col] = patrol_df[col].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)
#     insert_or_update_db(patrol_df, 'KOBO_PATROL_FIA_FISHCATCH')
#     if progress_callback:
#         progress_callback(1.0)


# # ========================================================
# # Streamlit Interface
# st.title("📦 Kobo Data Push Dashboard")
# st.write("Each button will push fresh Kobo data into the correct table.")

# def show_progress(task_func, label=""):
#     progress_bar = st.progress(0)
#     status_text = st.empty()

#     def update_progress(fraction_done):
#         percent = int(fraction_done * 100)
#         progress_bar.progress(percent)
#         status_text.text(f"{label} Progress: {percent}%")

#     try:
#         task_func(progress_callback=update_progress)
#         st.success(f"✅ {label} pushed successfully!")
#     except Exception as e:
#         st.error(f"❌ Failed to push {label}: {e}")
#     finally:
#         status_text.empty()

# if st.button("🚀 Push Main Catch Data"):
#     show_progress(get_fishcatch_from_kobo, label="Main Catch Data")

# if st.button("🚀 Push Natural Fishcatch Data"):
#     show_progress(get_natural_fishcatch_from_kobo, label="Natural Fishcatch Data")

# if st.button("🚀 Push Aquaculture Fishcatch Data"):
#     show_progress(get_aqu_fishcatch_from_kobo, label="Aquaculture Fishcatch Data")

# if st.button("🚀 Push Processing Fishcatch Data"):
#     show_progress(get_processing_fishcatch_from_kobo, label="Processing Fishcatch Data")

# if st.button("🚀 Push Patrol Data"):
#     show_progress(get_petrol_fishcatch_from_kobo, label="Patrol Data")
