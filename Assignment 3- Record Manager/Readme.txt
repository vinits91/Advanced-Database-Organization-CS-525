The code execution for Record Manager starts with building the code with Make file which is responsible for creating the test_assign3_1 Binary file and linking test_assign3_1.c file with all corresponding *.h and *.c files. Make file inter-links each of the files in the directory with each other.

Extra Credit Implementation :- Tombstone functionality to efficiently manage the deletion of record fo better Space utilization.
3 extra test cases are implemented in test_assign3_1 file

Procedure to Execute the record manager:-

1)Copy all the files to one folder
2)In Unix Terminal navigate to folder where files are stored.
3)Execute "make" command
4)Execute "./test_assign3_1" command
5)Execute "make expr" command
6)execute "./expr" command to run

Structure of Record Manager

buffer_mgr.h --> It has definition for various structures and functions to be used by buffer_mgr.c. 

buffer_mgr.c --> It is the main file which contains the Buffer manager function used to initialize buffer as per the number of PageFrames. It is used to copy file in pages from disk to PageFrame. It checks for fixedcount of Pages and Dirty bits to write the edited page back to disk. It contain FIFO and LRU agorithm to evict pages based on algorithm strategy if page frames are full.

buffer_mgr_stat.c --> This file contains the BufferPool statistic functions.

buffer_mgr_stat.h --> his file contains the BufferPool statistic functions definitions.

dberror.c --> It contains printerror and errormessage function. For the specific error it outputs the error code and specific error message

dberror.h --> It defines page_size constant as 4096 and definition for RC(return codes) for various File and Block functions in storage_mgr.
 
dt.h --> It contains constant variables for True and False. 
 
storage_mgr.h--> This is the file responsible for containing various file and block functions which are called from test_assign1_1.c It contains read and write function from file on disk to buffer and vise-versa. It contains creating, opening and closing file functions as well. It implements dberror.h and test_helpers.h file.
 
test_helper.h -> The file contains validation tasks for each function that are called within test_assign1. It is responsible for printing the RC code and RC message passed based on function's success or failure depnding on the values being passed to it.

test_expr.c -> This file tests the serialize and descerialize value of tuples, compares value and boolean opeartors as well as test complex expressions. 
1 extra test case is implemented in test_expr file.

tables.h -> This file defines the basic structure for schemas, tables, records, records ids and values. It contains functions to serialize data structures as string.

re_serializer.c -> This file conatins functions to serialize tuples of table for various table fucntions like scan, update, and delete. It seralizes schema as per different datatypes. It contains functions for seralizing records and attributes of table. Serializes data value as per different datatype.

record_mgr.c -> It is the main file that conatins the all the functions for record manager. We can create, update, delete and insert in tables. We can search records within a table using scan functions. Creating table schema as well as initiating and shutting down record manager.

Following are record manager functions:-

1. updateRecordInfo(Record *record, int pageNumber,int slotNumber) : This function updates the record based on record id passed and  the tombstone infomation of a record which is deleted.

2. processTombStone(recordTableInfo *recordTable) : This function is used to find the head of tombstone node for a table.

3. *stringFromTableMaker(recordTableInfo *recordTable,VarString *varString): This function converts the table information to string for tombstones records using varstring and append function.

4. *translateTableToString(recordTableInfo *recordTable) : This functions converts the table information to string using varstring and append function.

5. processTombData(recordTableInfo *recordTable,char **s1,char **s2) : This function processes the tombstone data using record page number and slot number.

6. TranslateStringToRecordTable(char *recordTableInfoString) : This function converts the string to record table information to create table in Page.

7. dSchema(char *stringSchema,char temp,int count) : This function is used to convert the value from table into string output.

8. tableInformationToFileData(char *name,char *temp,recordTableInfo *recordTable) :- This function is used to convert table information to file data.

9. createTable (char *name, Schema *schema) : This function is used to create a table in page file with different datatypes and with custom defined schema size.

10. initBufferManagerForRecord(char *name,struct BM_BufferPool *bManager,struct BM_PageHandle *pageHandle) : This function is uesd to initialize the record manager by caling the initBufferpool function and PinPage function.

11. shutdownRecordManager () : This function is used to shutdown record manager.

12. openTable (RM_TableData *rel, char *name) : This function is used to open the table in page file to scan, update, insert, and delete from table.

13. closeTable (RM_TableData *rel) : This function closses the table in page file and frees up the record manager memory.

14. deleteTable (char *name) : This fucntion deletes the table from page file.

15. getNumTuples (RM_TableData *rel) : This function is used to get the number of tuples from a relation.

16. insertRecord (RM_TableData *rel, Record *record) : This function is used to add a record to a table inpage file.

17. deleteRecord (RM_TableData *rel, RID id) : This function is use to search a record in table and then delete a record from relation.

18. updateRecord (RM_TableData *rel, Record *record) : This function is used to search a record in table and then update the record of a relation.

19 getRecord (RM_TableData *rel, RID id, Record *record) : This function is called by updaterecord and deleterecord to find the tuple in the relation.

20. startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond) : This function is used to find the matching records fro the relation as per the condition passed as expression passed by client.

21. next (RM_ScanHandle *scan, Record *record) : This funciton helps in scanning the relaion to find the matching records which fulfills the condition.

22. closeScan (RM_ScanHandle *scan) : This function is used to cloase the scanning of relaiton.

23. getRecordSize (Schema *schema) : This function is used to get the record size of relation.

24. *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys) : This function is used to create table schema with different attibutes, datatypes, and keys.

25. freeSchema (Schema *schema) : This funciton is used to free the relation schema.

26. createRecord (Record **record, Schema *schema) : This function is used to create a new record in a relation.

27. freeRecord (Record *record) : This funciton free the data associated with record and then it frees the record.

28. getAttr (Record *record, Schema *schema, int attrNum, Value **value) : This function is used to get the attribute value from relation.

29. setAttr (Record *record, Schema *schema, int attrNum, Value *value) : This function is used to gset the attribute values of relation.

30. checkTombStone(Record *record,RM_TableData *rel,RID id,int *pageNumber,int *slotNumber, int *tcount) : This function is used to implement the tombstone functionality in relation.

Following are custom record manager structures:-

1. recordTableInfo:- this structure is used to store the data related to table structure. Like numberoftuples, schemesize, recordstart, recordend, slotsize etc. 

2. Variabstring :- This structure is used to point data in buffer along with its size.

3. searchrecord :- this structure store data related to record of a table in terms of slot number and page number which will be used for scanning and searching records.

4. tombStoneNode :- stores deleted records information like its RID and pointer in a linked list form.
 
expr.c -> This file conatins functions to evaluate an expression that client required during scan, update, and delete. It evaluates expression against records already contained in table.

definitions.h -> THis file contains the attribute offset function along with varstring, string, size, and append functions which are called in record_mgr.c

test_assign3_1.c -> This is the file that conatin the main function which is used to call all test function on record_mgr.c. 

