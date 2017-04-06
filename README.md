# event-ingestion-client
Event Ingestion Client used to push events to TSI

Compile and Build Requirements:
Java 1.8, Maven 3

How to run the program:
The program accepts four required parameters
1) url  (For production it is "https://api.truesight.bmc.com/v1/events")
2) email
3) apiKey
4) filePath (should be an Excel file i.e .xlsx)

If using an IDE like Eclipse you need to run EventIngestion class with above parameters as arguments
(or)
java -cp event-ingestion-client.jar com.bmc.event.EventIngestion $url $email $apiKey $filepath

As this is a multithreaded program we can't guarantee the order of execution of the rows in the Excel sheet.
So if there are multiple rows in Excel with same fingerprintFields value then we can't guarantee that the row with highest index is inserted at last
If the Excel data has a "createdAt" column then this is not a concern, as the EventService will take care that the summary Event will be the latest createdAt entry otherwise
EventService will take the received time as the createdAt time

Mandatory Fields for Event:
1) title
2) fingerprintFields (By default source.ref, source.type, eventClass will be part of fingerprintFields)
3) source.ref
4) source.type

predefined fields for Event:
1) title
2) fingerprintFields                     : This should be listed as comma separated values in fingerprintFields column in Excel. For eg: title,entry_id
3) source.ref, source.type, source.name  : This should be listed as comma separated values in source column in Excel. For eg: BMCData,BMCData,BMCData
4) sender.ref, sender.type, sender.name  : This should be listed as comma separated values in sender column in Excel. For eg: BMCData,BMCData,BMCData
5) severity
6) status
7) tags                                  : This should be listed as comma separated values in tags column in Excel. For eg: app_id:BMCData, ...
8) message
9) createdAt
10) eventClass

Any column name in the Excel other than the predefined fields for Event will be part of the properties in payload
* app_id is one predefined field for properties

ErrorHandling and Logging:
* If there is an error either parsing or processing a specific row, we do log the error indicating the row number and continue to the next.
