# Celonis Data Model Minimizer

This python script works to minimize the event log present in the data model. Due to the nature of the event log, it will grow continuously on a live connection. This means that it can constantly require more data storage. If the event log is not properly maintained, it can cause issues with available storage.

One of the key features of the event log is the activity name. This is the largest factor of rows in the event log, with the case key and timestamp typically not taking more that 8 bytes each. The activity name can often take upwards of 25 bytes for each row, which can add up when you have been running for an extended period of time.

Our script adds a relational database format to the activity table. Instead of using the full text for each activity name, we create a code for each activity and only use a single character to store the activity name. This can decrease the space used by each row of the activity table by upwards of 50%.

In tests on active data models, we have been able to minimize the event log to the tune of 15% on average. If expanded to other fields, the savings could be north of 50%. 
