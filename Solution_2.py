import psycopg2
import psycopg2.extras
import csv
import pandas as pd
import numpy as np
import os
from io import StringIO



# load csv files to postgresql table
def load_data_from_csv_to_postgres():
    cur = connection.cursor()
    # Truncate tables before loading the data
    cur.execute("""
                Truncate table clidentside_events; Truncate table serverside_events; TRUNCATE TABLE Output_CDS_DATA; 
                DROP INDEX if exists idx_ss_data;DROP INDEX if exists idx_cds_data;
                """)

    with open(file_path + r'\question2_clidentside_events.csv', 'r') as f:
        next(f)
        cur.copy_from(f, 'clidentside_events', sep=',')

    with open(file_path + r'\question2_serverside_events.csv', 'r') as b:
        next(b)
        cur.copy_from(b, 'serverside_events', sep=',')
    cur.close()
    connection.commit()


# Function to put the aggregated data in the new table using postgres windows function
def using_postgres_sql():
    cur = connection.cursor()
    cur.execute("""
            INSERT INTO Output_CDS_DATA 
            SELECT COALESCE(SSE.id,CDE.ID) AS ID,
            CASE WHEN MIN(SS_timestamp) <MIN(CDS_timestamp) THEN MIN(SS_timestamp) ELSE 
            CASE WHEN MIN(CDS_Timestamp) < MIN(SS_timestamp) THEN MIN(CDS_Timestamp) ELSE 
            CASE WHEN MIN(CDS_Timestamp) IS NULL THEN MIN(SS_Timestamp) ELSE MIN(CDS_Timestamp) END END END AS first_timestamp,
            COUNT(CASE WHEN EVENT_TYPE='click' THEN EVENT_TYPE END) AS total_number_of_clicks,
            subscription,subscription_timestamp,unsubscription,unsubscription_timestamp
                FROM clidentside_events CDE
                FULL OUTER JOIN serverside_events SSE ON SSE.ID= CDE.ID
                GROUP BY COALESCE(SSE.id,CDE.ID), subscription, subscription_timestamp, unsubscription, unsubscription_timestamp;
                CREATE INDEX idx_cds_data on clidentside_events(id,cds_timestamp);
				CREATE INDEX idx_ss_data on serverside_events(id,ss_timestamp);
                """
                )
    cur.close()
    connection.commit()


# Function to put the aggregated data in the new table using Pandas function
def using_pandas():
    with connection.cursor() as cursor:
        cursor.execute("SELECT * FROM clidentside_events")
        clidentside_events_data = cursor.fetchall()

    with connection.cursor() as cursors:
        cursors.execute("SELECT * FROM serverside_events")
        serverside_events_data = cursors.fetchall()

    df_client_data_events = pd.DataFrame(clidentside_events_data, columns=['cds_id', 'cds_timestamp', 'event_type'])

    df_client_data_events_1 = df_client_data_events[['cds_id', 'cds_timestamp']].rename(
        columns={"cds_id": "cds_id_1", "cds_timestamp": "cds_timestamp"})

    df_client_data_events_2 = df_client_data_events[df_client_data_events['event_type'] == 'click'].groupby(
        ['cds_id', 'event_type']) \
                                  .agg(
        Total_number_of_clicks=('event_type', lambda x: (x == 'click').astype(int).sum())).reset_index() \
                                  .loc[:, ['cds_id', 'Total_number_of_clicks']]

    df_client_data_events_mg = pd.merge(df_client_data_events_1, df_client_data_events_2, how='left',
                                        left_on='cds_id_1', right_on='cds_id')

    df_server_side_events = pd.DataFrame(serverside_events_data,
                                         columns=['sse_id', 'ss_timestamp', 'subscription', 'subscription_timestamp',
                                                  'unsubscription', 'unsubscription_timestamp'])

    cols_events = ['cds_id_1', 'Total_number_of_clicks', 'cds_timestamp']

    df_merged = pd.merge(df_client_data_events_mg[cols_events],
                         df_server_side_events, how='outer',
                         left_on='cds_id_1',
                         right_on='sse_id')

    df_merged['id'] = df_merged['cds_id_1'].combine_first(df_merged['sse_id'])

    df_merged.drop(df_merged.columns[[0, 3]], axis=1, inplace=True)

    cols = ['id', 'Total_number_of_clicks', 'subscription', 'subscription_timestamp', 'unsubscription',
            'unsubscription_timestamp', 'min_cds_timestamp', 'min_sse_timestamp']

    df_output_grouped_data = df_merged.groupby(
        ['id', 'subscription', 'Total_number_of_clicks', 'subscription_timestamp', 'unsubscription',
         'unsubscription_timestamp']) \
                                 .agg(min_cds_timestamp=('cds_timestamp', 'min'),
                                      min_sse_timestamp=('ss_timestamp', 'min')).reset_index() \
                                 .loc[:, cols]

    # first timestamp of the users,
    df_output_grouped_data['first_timestamp'] = df_output_grouped_data[['min_cds_timestamp', 'min_sse_timestamp']].min(
        axis=1)

    cols = ['id', 'first_timestamp', 'Total_number_of_clicks', 'subscription', 'subscription_timestamp',
            'unsubscription', 'unsubscription_timestamp']

    df_sld_grp = df_output_grouped_data.reindex(cols, axis=1)

    df_sld_grp['Total_number_of_clicks'] = df_sld_grp['Total_number_of_clicks'].astype(int)

    buffer = StringIO()

    df_sld_grp.to_csv(buffer, index=False, header=False)

    buffer.seek(0)

    with connection.cursor() as cursor_1:
        cursor_1.copy_from(buffer, 'output_cds_data', sep=',', null='')
    connection.commit()


if __name__ == '__main__':

    # csv file path
    file_path = r'C:\Users\laksh\Documents\data-engineer-talent-test\data-engineer-talent-test-master\question2_data'

    # connect to Postgres DB
    connection = psycopg2.connect(
        host="localhost",
        database="demo",
        user="postgres",
        password="password3"
    )

    # Call function to load csv files to postgresql table
    load_data_from_csv_to_postgres()

    # Function to put the aggregated data in the new table using postgres windows function
    # using_pandas()

    # Function to put the aggregated data in the new table using Pandas function
    using_postgres_sql()

    connection.close()
