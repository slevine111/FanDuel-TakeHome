# FanDuel Takehome

### Setup, Environmental Variables, and Running Code

To run code, first run ```pip install -r requirements.txt``` to install packages (only pandas in this case)

See sample.env for environmental variables that need to be set. Purpose of PREV_EXECUTION_TIME will be explained below.

Run ```python index.py``` to run code that accomplishes tasks

### Explanation of Code Logic/Task Implementations

The sequence of my code 

#### Part A

The functions diagnose_duplicate_records_issue and fix_duplicate_records_issue implement my logic for Part A.

##### Diagnosis

The diagnose_duplicate_records_issue diagnoses the cause of the issue by getting the number of rows per batch_id and to_email for each event table.
I did this to diagnose b/c I thought the duplication was likely caused by send_event left joining on another event table that had 
a batch_id/to_emil combo with more than one row, thus causing the returned table to have more than one row for that batch_id/to_email combo.
Running this function will confirm that to be the cause, as it will print out that the click_event table has aleast one batch_id/to_email combo with multiple rows.
 
##### Fix

The fix_duplicate_records_issue function populates the empty event_summary_corrected table from left joining send_event on the other event tables and
applying my fix. The event_summary_corrected table has the same schema as event_summary. My fix is to make each of the [event]_date columns (besides sent_event) a comma separated
string of the [event]_date values in the event table for all rows with each batch_id/to_email. For example, if in table click_event, batch_id A and to_email B has two rows with click_dates C and D,
the row in event_summary for batch_id A and to_email B would have click_date value 'C, D'.  
This fix implementated in Postgresql would provide the opportunity to further optimize the data types. Instead of all the [event]_date columns staying as text/varchar in event_summary_corrected,
I would switch the data type for all those columns (besides sent_date) to array data type and modify my code to match that.

#### Part B

The update_click_open_events function implements my logic for Part B. This function adds any new click_event and open_event values from the individual event tables
to the comma separated string of those columns in event_summary. 

##### PREVIUS_EXECUTION_TIME environment variable

The PREVIOUS_EXECUTION_TIME environment variable would be a way of not performing this update over all rows of the event table each time it is run. Instead, it would only [event]_date values that are greater than 
the PREVIOUS_EXECUTION_TIME. The PREVIOUS_EXECUTION_TIME variable would likely be normally dynamically set if this process was running on schedule. Additionally, an end time env variable could be added to only put in events in less than the end time.
This would also allow to backfill over a time range. One issue backfilling would cause is if the time range covered any time previously processed, you could add times to event_summary already in the table. The value I have for PREV_EXECUTION_TIME in sample.env will
demonstrate this as all the data is after the date and the data is already in event_summary, i.e., already has been processed, so running the function will cause duplicates. This issue would need to be addressed. It will likely be easier to be addressed with the columns
being array data type in Postgresql.

##### SQLite Limitations

This part required working a bit around sqlite limitations. Since sqlite does not allow JOINS in UPDATES and all rows have to be returned in the subquery, I had to do a LEFT JOIN from event_summary to the event table. As the event_summary table grows larger,
this LEFT JOIN could get very costly in terms of time. In Postgresql, I would be able to replace this with an UPDATE that had a direct JOIN and thus substantially improve performance. Additionally, my UPDATEs with both the click_event and open_event tables would produce wrong results
even though the SELECT subquery statement for both would produce expected results. Hence, I've also attached a CSV for each with the results of the SELECT statement for both tables

#### Extra Feature

The add_unsubscribe_to_event_summary function adds an unsub_event column to event_summary and then populates it. Two things to note are

-  The same date may show up for the same user for multiple batch_ids. This is because an user may have unsubscribed within two days of mutiple emails being sent. In that case, I decided to treat any of the emails as being the possible cause instead of using the email 
   closest to the unsubscribe date or some other tiebreaker.
- One potential issue this code would need to be modified to address is if an user unsubscribes multiple times. Right now, the UPDATE statement would randomly pick any unsubscribe event to compare against each email sent, instead of picking the unsubscribe closest to the email
that is after the email.

I also wrote what the updated event_summary table would look like to a CSV due to again having trouble with my sqlite REPLACE statement (click_date and open_date columns are wrong b/c of above UPDATE issues)

#### Alternate Strategy

There are two additional functions in utility.py, fix_duplicate_records_issue_max_date and update_click_open_events_max_date.
These functions could also be used for the fix part of Part A and Part B. These functions wud use the strategy of making all the
[event]_date columns in event_summary the max date of that event. However, I believe my main strategy is more in line with this assignment
and would do a better job of providing a full summary of all the events, as this max date strategy would only provide a very focused and narrow
summary.