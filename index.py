import sqlite3
from utility import diagnose_duplicate_records_issue, fix_duplicate_records_issue, create_new_tables, \
    update_click_open_events, add_unsubscribe_to_event_summary

if __name__ == '__main__':
    conn = sqlite3.connect('sqlite.db')
    create_new_tables(conn)
    # Part A
    print('------- PART A -------------')
    tables_duplicate_records = diagnose_duplicate_records_issue(conn)
    fix_duplicate_records_issue(conn, tables_duplicate_records)
    print('------- PART B -------------')
    update_click_open_events(conn)
    print('------- EXTRA FEATURE ------')
    add_unsubscribe_to_event_summary(conn)
    conn.close()
