import os
from typing import List
import sqlite3
import pandas as pd


def get_event_data_and_join_dates_in_string(cursor: sqlite3.Cursor, table_name: str, filter_on_date: bool,
                                            tables_duplicate_records: List[str] = None) -> pd.DataFrame:
    common_cols = ['batch_id', 'to_email']
    event_type = f'{table_name.split("_")[0]}_date'
    where_clause = f"WHERE {event_type} >= strftime('{os.environ['PREV_EXECUTION_TIME']}')"
    cursor.execute(f"""SELECT batch_id, 
                           to_email, 
                           {event_type} 
                       FROM {table_name} 
                       {where_clause if filter_on_date else ''}""")
    event_data = pd.DataFrame(cursor.fetchall(), columns=common_cols + [event_type])
    if not tables_duplicate_records or table_name in tables_duplicate_records:
        event_data = (event_data
                      .groupby(common_cols)
                      .apply(lambda df: ', '.join(df[event_type].tolist()))
                      .reset_index())
        event_data.columns = common_cols + [event_type]
    return event_data


def create_new_tables(conn: sqlite3.Connection) -> None:
    cursor = conn.cursor()
    cursor.execute("""CREATE TABLE IF NOT EXISTS event_summary_corrected ( 
                        batch_id int, to_email text, sent_date text, bounce_date text, open_date text, click_date text)""")
    cursor.execute("""CREATE TABLE IF NOT EXISTS updated_data (batch_id int, to_email text, event_dates text)""")
    print('created event_summary_corrected and batch_ids_updating tables to be used in Part A and Part B, respectively')


def diagnose_duplicate_records_issue(conn: sqlite3.Connection) -> List[str]:
    cursor = conn.cursor()
    tables_to_check = ['send_event', 'bounce_event', 'open_event', 'click_event']
    tables_with_duplicate_records = []
    for table in tables_to_check:
        cursor.execute(
            f"""
             SELECT batch_id, to_email, COUNT(*) AS number_events
             FROM {table} A
             GROUP BY batch_id, to_email
             HAVING COUNT(*) > 1
            """)
        first_problem_row = cursor.fetchone()
        if first_problem_row:
            tables_with_duplicate_records.append(table)
            print(
                f'{table} is CAUSE of duplicate issue. There is aleast one occurence of batch_id/to_email combo in table'
                ' with multiple rows and thus left joining on this table with others will cause multiple rows to appear'
                ' for those occurences.')
        else:
            print(f'{table} is NOT cause of duplicate issue.')
    return tables_with_duplicate_records


def fix_duplicate_records_issue(conn: sqlite3.Connection, tables_duplicate_records: List[str]) -> None:
    cursor = conn.cursor()
    common_cols = ['batch_id', 'to_email']
    output_tables_cols = common_cols + ['sent_date']
    tables_left_join = ['bounce_event', 'open_event', 'click_event']
    send_date_dup = 'send_event' in tables_duplicate_records
    cursor.execute(f"""SELECT batch_id, 
                              to_email, 
                              {'max(sent_date)' if send_date_dup else 'sent_date'} 
                       FROM send_event
                       {'GROUP BY batch_id, to_email' if send_date_dup else ''}
                    """)
    event_summary_data = pd.DataFrame(cursor.fetchall(), columns=output_tables_cols)
    for table in tables_left_join:
        event_data = get_event_data_and_join_dates_in_string(cursor, table, False, tables_duplicate_records)
        event_type = f'{table.split("_")[0]}_date'
        output_tables_cols.append(event_type)
        event_summary_data = event_summary_data.merge(event_data, on=common_cols, how='left')

    cursor.executemany("""INSERT INTO event_summary_corrected values (?, ?, ?, ?, ?, ?)""",
                       [tuple(row) for row in event_summary_data.to_numpy()])
    print('inserted into newly created table event_summary_corrected what data would like with my fix')


def update_click_open_events(conn: sqlite3.Connection) -> None:
    cursor = conn.cursor()
    for table in ['click_event', 'open_event']:
        event_type = f'{table.split("_")[0]}_date'
        event_data = get_event_data_and_join_dates_in_string(cursor, table, True)
        cursor.execute('DELETE FROM updated_data')  # sqlite does not have truncate statement
        cursor.executemany('INSERT INTO updated_data values (?, ?, ?)',
                           [tuple(row) for row in event_data.to_numpy()])
        cursor.execute(f"""
            SELECT A.batch_id, A.to_email, {event_type} || CASE WHEN event_dates IS NOT NULL THEN (", " || event_dates) ELSE "" END
            FROM event_summary A
            LEFT JOIN updated_data B ON A.batch_id = B.batch_id AND A.to_email = B.to_email
            """)
        (pd.DataFrame(cursor.fetchall(), columns=['batch_id', 'to_email', event_type])
         .to_csv(f'{event_type}_update.csv', index=False))
        conn.execute(f"""
                    UPDATE event_summary
                    SET {event_type} = (SELECT {event_type} || CASE WHEN event_dates IS NOT NULL THEN (", " || event_dates) ELSE "" END
                                        FROM event_summary A
                                        LEFT JOIN updated_data B ON A.batch_id = B.batch_id AND A.to_email = B.to_email)
                    """)
    print('updated click_event and open_event columns in event_summary with events that occured after'
          f' {os.environ["PREV_EXECUTION_TIME"]}')


def add_unsubscribe_to_event_summary(conn: sqlite3.Connection):
    cursor = conn.cursor()
    cursor.execute('ALTER TABLE event_summary ADD COLUMN unsub_date TEXT')
    query = """
             SELECT batch_id, to_email, sent_date, bounce_date, open_date, click_date,
                    CASE WHEN A.unsub_date IS NOT NULL THEN A.unsub_date
			              ELSE CASE WHEN B.unsub_date_as_date - A.sent_date_as_date <= 2 
                                         AND B.unsub_date_as_date - A.sent_date_as_date >= 0 
                                    THEN B.unsub_date ELSE NULL END END
                    FROM (SELECT *, julianday(strftime(sent_date)) AS sent_date_as_date FROM event_summary) A
					LEFT JOIN (SELECT *, julianday(strftime(unsub_date)) AS unsub_date_as_date FROM unsub_event) B 
                    ON  A.to_email = B.email
            """
    cursor.execute(query)
    (pd.DataFrame(cursor.fetchall(),
                  columns=['batch_id', 'to_email', 'sent_date', 'bounce_date', 'open_date', 'click_date', 'unsub_date'])
     .to_csv('event_summary_with_unsub_date.csv', index=False)
     )
    cursor.execute(f"""
                    REPLACE INTO event_summary 
                    {query}""")
    print('added unsub_event column to event_summary and outputted csv of how column would be populated')


##############################

def fix_duplicate_records_issue_max_date(conn: sqlite3.Connection, tables_duplicate_records: List[str]) -> None:
    cursor = conn.cursor()
    common_cols = ['batch_id', 'to_email']
    output_tables_cols = common_cols + ['sent_date']
    tables_left_join = ['bounce_event', 'open_event', 'click_event']
    send_date_dup = 'send_event' in tables_duplicate_records
    cursor.execute(f"""SELECT batch_id, 
                              to_email, 
                              {'max(sent_date)' if send_date_dup else 'sent_date'} 
                       FROM send_event
                       {'GROUP BY batch_id, to_email' if send_date_dup else ''}
                    """)
    event_summary_data = pd.DataFrame(cursor.fetchall(), columns=output_tables_cols)
    for table in tables_left_join:
        dup = table in tables_duplicate_records
        event_type = f'{table.split("_")[0]}_date'
        output_tables_cols.append(event_type)
        cursor.execute(f"""SELECT batch_id, 
                                  to_email, 
                                  {f'max({event_type}) AS {event_type}' if dup else event_type}  
                           FROM {table}
                           {'GROUP BY batch_id, to_email' if dup else ''}
                        """)
        event_data = pd.DataFrame(cursor.fetchall(), columns=common_cols + [event_type])
        event_summary_data = event_summary_data.merge(event_data, on=common_cols, how='left')

    cursor.executemany("""INSERT INTO event_summary_corrected values (?, ?, ?, ?, ?, ?)""",
                       [tuple(row) for row in event_summary_data.to_numpy()])


def update_click_open_events_max_date(conn: sqlite3.Connection) -> None:
    cursor = conn.cursor()
    for table in ['click_event', 'open_event']:
        event_type = f'{table.split("_")[0]}_date'
        cursor.execute(f"""
          UPDATE event_summary
          SET click_date = (SELECT CASE WHEN strftime(A.{event_type}) > strftime(B.{event_type}) OR B.{event_type} IS NULL
                                       THEN A.{event_type} ELSE B.{event_type} END
                            FROM (SELECT batch_id, to_email, MAX({event_type}) AS {event_type}
                                  FROM {table}
                                  GROUP BY batch_id, to_email) A
        			        JOIN event_summary B ON A.batch_id = B.batch_id AND A.to_email = B.to_email)
         """)
