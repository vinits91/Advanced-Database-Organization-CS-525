#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<math.h>
#include<unistd.h>
#include"storage_mgr.h"
#include"buffer_mgr.h"
#include"record_mgr.h"
#include"tables.h"
#include"definition.h"

void updateRecordInfo(Record *record, int pageNumber,int slotNumber);
	//tombstone Node used for efficient management of memory in case of
	//delete functionality by creating LL of deleted record
	typedef struct tombStoneNode {
		struct tombStoneNode *next;
		RID id;
	}tombStoneNode;


//record manager related info.
typedef struct recordTableInfo{
	//need a buffer manager to store record
	BM_BufferPool *bufferManager;
	int maximumPageSlot;//Max. slots in a single page
    int numberOfTuples;//number of tuples in record
    int schemaSize;//size of schema
    int tombStoneLength;//tombStone length
    tombStoneNode *tombStoneHead;//start node of tombstone LL
    int recordStart;//record start
    int recordEnd;//record end pointer
    int slotSize;//actual slot size
    int tombInfoListSize;//Size of tombstone List
}recordTableInfo;

typedef struct VariabString {
    char *buf;
    int size;
    int bufsize;
} VarString;


//used during scanning of record
typedef struct searchRecord {
    int numberOfPagesPerRelation;//Total number of pages per relation
    int numberOfSlotsPerRelation;//slots per relation
    Expr *condition;//expression to be used while sacn
    int currentRecordSlotNo;//current slot number under scan
    int currentRecordPageNo;//current page number of record under scan
}searchRecord;

	//returns record manager Information data
	recordTableInfo *getRecordManagerTableInfo(RM_TableData *rel){
		 recordTableInfo *recordTable = (recordTableInfo *) (rel->mgmtData);
		 return recordTable;
	}
	
	//alloctes memory to tombNode
	tombStoneNode *getTombStoneNode(){
		tombStoneNode *tomb=(tombStoneNode *)calloc(sizeof(tombStoneNode),sizeof(tombStoneNode));
		tomb->next = NULL;
		return tomb;
	}


	//processes Tombstone LL to be stored as string
	int processTombStone(recordTableInfo *recordTable){
		tombStoneNode *tombStoneStart;
		tombStoneStart = recordTable->tombStoneHead;
		int tombStoneCount = 0,i=0;
		for(i=0;tombStoneStart != NULL;i++){
			tombStoneStart = tombStoneStart->next;
			tombStoneCount++;
		}
		return tombStoneCount;
	}
	
	
	char *stringFromTableMaker(recordTableInfo *recordTable,VarString *varString){
			tombStoneNode *tombStoneStart = recordTable->tombStoneHead;
			char *outputString;
			int i;
			for(i=0;tombStoneStart != NULL;i++){
				APPEND(varString,"%i:%i%s ",tombStoneStart->id.page, tombStoneStart->id.slot, (tombStoneStart->next != NULL) ? ", ": "");
				tombStoneStart = tombStoneStart->next;
			}
			APPEND_STRING(varString, ">\n");				\
			GET_STRING(outputString, varString);
			return outputString;
		}
		
		//translates the table information to string representation
	char *translateTableToString(recordTableInfo *recordTable){
		char *outputString;
		VarString *varString;
		MAKE_VARSTRING(varString);
		APPEND(varString, "SchemaLength <%i> FirstRecordPage <%i> LastRecordPage <%i> NumTuples <%i> SlotSize <%i> MaxSlots <%i> ", recordTable->schemaSize, recordTable->recordStart, recordTable->recordEnd, recordTable->numberOfTuples, recordTable->slotSize, recordTable->maximumPageSlot);
		int tombStoneCount =processTombStone(recordTable);
		APPEND(varString, "tNodeLen <%i> <", tombStoneCount);
		outputString=stringFromTableMaker(recordTable,varString);
		return outputString;

	}
	
	//alloctes memory to recordTable
	recordTableInfo *initRecordManagerTableInfo(){
		   recordTableInfo *recordTable = (recordTableInfo*) malloc(sizeof(recordTableInfo));
		   return recordTable;
	}

	//Extracts the content based on representation
	void stringTokenize(char **s1){
		 *s1 = strtok (NULL,"<");
		 *s1 = strtok (NULL,">");
	}
	
	long int strtExtraction(char **s1,char **s2){
		long int extractedData=strtol((*s1), &(*s2), 10);
		return extractedData;
	}
	
	//processes tombStone data
	void processDataForTomb(recordTableInfo **recordTable,int pageNumber,int slotNumber){
			tombStoneNode *tnode;
			if ((*recordTable)->tombStoneHead != NULL){
			tnode->next = getTombStoneNode();
            tnode->next->id.page = pageNumber;
            tnode->next->id.slot = slotNumber;
            tnode = tnode->next;
        }
        else{
				(*recordTable)->tombStoneHead = getTombStoneNode();
				(*recordTable)->tombStoneHead->id.page = pageNumber;
				(*recordTable)->tombStoneHead->id.slot = slotNumber;
				tnode = (*recordTable)->tombStoneHead;
			}
		}
	
	//processes tombstone data
	recordTableInfo *processTombData(recordTableInfo *recordTable,char **s1,char **s2){
		int k, pageNumber, slotNumber;
		recordTable->tombStoneHead = NULL;
		while(k<recordTable->tombStoneLength){
			*s1 = strtok (NULL,":");
			pageNumber = strtExtraction(&(*s1),&(*s2));
			if(k != (recordTable->tombStoneLength - 1)){
				*s1 = strtok (NULL,","); 
			}
			else{
				*s1 = strtok (NULL,">");
			}
			slotNumber = strtExtraction(&(*s1),&(*s2));
			processDataForTomb(&recordTable,pageNumber,slotNumber);
			k++;
		}
		return recordTable;
	}
	
	
	//translates the string to record table info.
	recordTableInfo *TranslateStringToRecordTable(char *recordTableInfoString){
		char recordTableData[strlen(recordTableInfoString)];
		strcpy(recordTableData, recordTableInfoString);	
		recordTableInfo *recordTable = initRecordManagerTableInfo();
		char *s1, *s2;
		s1 = strtok (recordTableData,"<");
		s1 = strtok (NULL,">");
		recordTable->schemaSize = strtExtraction(&s1,&s2);
		stringTokenize(&s1);
		recordTable->recordStart = strtExtraction(&s1,&s2);
		stringTokenize(&s1);
		recordTable->recordEnd =  strtExtraction(&s1,&s2);
		stringTokenize(&s1);
		recordTable->numberOfTuples =  strtExtraction(&s1,&s2);
		stringTokenize(&s1);
		recordTable->slotSize =  strtExtraction(&s1,&s2);
		stringTokenize(&s1);
		recordTable->maximumPageSlot =  strtExtraction(&s1,&s2);
		stringTokenize(&s1);
		recordTable->tombStoneLength = strtExtraction(&s1,&s2);
		recordTable->tombInfoListSize = strtExtraction(&s1,&s2);
		s1 = strtok (NULL,"<");
		recordTable=processTombData(recordTable,&s1,&s2);
		return recordTable;
	}

	//alloctes emory to schema
	Schema *getSchema(){
		Schema *schema = (Schema *) malloc(sizeof(Schema));
		return schema;
	}
	
	void manupulateSchema(char **s1,char *dataSchema){
		*s1 = strtok (dataSchema,"<");
		*s1 = strtok (NULL,">");
	}
	
	int getNumberOfAttributes(char **s1,char **s2){
		int numberOfAttributes = strtol(*s1, &(*s2), 10);
		return numberOfAttributes;
	}
	
	//allocates memory to schema info
	void populateSchema(Schema *schema,int numberOfAttributes){
		schema->typeLength=(int *)malloc(sizeof(int)*numberOfAttributes);
		schema->attrNames=(char **)malloc(sizeof(char*)*numberOfAttributes);
		schema->numAttr= numberOfAttributes;
		schema->dataTypes=(DataType *)malloc(sizeof(DataType)*schema->numAttr);
	}
	
	void schemaByattributeName(Schema *schema,char **s1,int i){
		schema->attrNames[i]=(char *)calloc(strlen(*s1), sizeof(char));
        strcpy(schema->attrNames[(i)], *s1);
        if(i == schema->numAttr-1){
			 *s1 = strtok (NULL,") ");  
        }
        else{
           *s1 = strtok (NULL,", "); 
        }
	}

	char *initializeStringRef(char *s1){
		char *str=(char *)calloc(strlen(s1), sizeof(char));
		return str;
	}
	
	bool populateDataTypes(Schema *schema,char **s1,int i){
		bool flag=true;
		if (strcmp(*s1, "FLOAT") == 0){
			schema->typeLength[i] = 0;
            schema->dataTypes[i] = DT_FLOAT;
            flag=false;
        } else if (strcmp(*s1, "BOOL") == 0){
            schema->typeLength[i] = 0;
            schema->dataTypes[i] = DT_BOOL;
            flag=false;
        }else if (strcmp(*s1, "INT") == 0){
			schema->typeLength[i] = 0;
            schema->dataTypes[i] = DT_INT;
            flag=false;
        }
        return flag;
	}
	char *extract(char **s1){
		*s1 = strtok (NULL,")");
        char *main = strtok (*s1,", ");
        return main;
	}
	
	char extractSquare(char **s1,char **s3){
		*s1 = strtok (*s3,"[");
        *s1 = strtok (NULL,"]");
	}
	
	//deserizaling the schema 
	Schema *dSchema(char *stringSchema,char temp,int count){
		int index=0, index1, sizeOfKey = 0;
		char *s1, *s2,*s3;
		char dataSchema[strlen(stringSchema)];
		strcpy(dataSchema, stringSchema);
		manupulateSchema(&s1,dataSchema);
		Schema *schema = getSchema();
		int numberOfAttributes=getNumberOfAttributes(&s1,&s2);
		char* Attributes[numberOfAttributes];
		populateSchema(schema,numberOfAttributes);
		char* stringRef[numberOfAttributes];
		s1 = strtok (NULL,"(");
		bool flag;
		while(index<schema->numAttr){
			s1 = strtok (NULL,": ");
			schemaByattributeName(schema,&s1,index);
			stringRef[index] = initializeStringRef(s1);
			if(flag=(populateDataTypes(schema,&s1,index))){
				strcpy(stringRef[index], s1);
				index++;
			}else{
				index++;
			}
			
		}
    flag = false;
    if((s1 = strtok (NULL,"(")) != NULL){
		flag = true;
        char *main=extract(&s1);
        for(index=0;main != NULL;index++){
            Attributes[sizeOfKey] = initializeStringRef(main);
            strcpy(Attributes[sizeOfKey], main);
            main = strtok (NULL,", ");
            sizeOfKey+=1;
        }
    }
	index=0;
    while(index<numberOfAttributes){
			if(strlen(stringRef[index]) > 0){
				schema->dataTypes[index] = DT_STRING;
				s3=initializeStringRef((stringRef[index]));
				memcpy(s3, stringRef[index], strlen(stringRef[index]));
				extractSquare(&s1,&s3);
				schema->typeLength[index] = getNumberOfAttributes(&s1,&s2);
				index++;
			}else{
				index++;
			}
    }
    if(flag){
        schema->keyAttrs=(int *)malloc(sizeof(int)*sizeOfKey);
        schema->keySize = sizeOfKey;
        index=0;
        while(index<sizeOfKey){
			index1=0;
            while(index1<schema->numAttr){
                if(strcmp(Attributes[index], schema->attrNames[index1]) == 0){
                    schema->keyAttrs[index] = index1;
                    index1++;
                }else{
					index1++;
				}
            }
            index++;
        }
         return schema;
    }else{
		 return schema;
	}
}

	//called to get page Hnadle pointer of buffer manager
	BM_PageHandle *getBufferPageHandle(){
		BM_PageHandle *pageHandle = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
	}
	

	//called to calculate file size based on schema to be stored
	int computefileSize(int schemaSize){
		int fileSize=(int)(ceil((float)schemaSize/PAGE_SIZE));
		return fileSize;
		
	}
	
	//called to calculate Max. slots for records per page
	int calculateMaximumSlotsPerPage(int slotSize){
		int maximumSlots=(int)(floor((double)(PAGE_SIZE/slotSize)));	
		return maximumSlots;
	}

	//it populates the record manager necessory information to be used by various functions
	recordTableInfo *populateRecordTableInfo(int schemaSize,int fileSize,int maximumSlots,int slotSize){
		recordTableInfo *recordTable=(recordTableInfo *) malloc(sizeof(recordTableInfo));
		recordTable->maximumPageSlot=maximumSlots;
		recordTable->numberOfTuples=0;
		recordTable->schemaSize=schemaSize;
		recordTable->slotSize=slotSize;
		recordTable->recordStart=fileSize+1;
		recordTable->recordEnd=fileSize+1;
		recordTable->tombStoneHead = NULL;
		recordTable->tombInfoListSize=0;
		return recordTable;
	}

	//return number of tuples of record stored in page
	int getNumTuples (RM_TableData *rel){
    return ((recordTableInfo *)rel->mgmtData)->numberOfTuples;
	}
	

	RC freeRecord (Record *record){
	//freeing space for record's data and then record
    free(record->data);
    free(record);
    return RC_OK;
	}
	
	//opening the file by storage manager
	RC openingFile(char *name,SM_FileHandle fHandle){
		RC flag;
		if((flag=openPageFile(name, &fHandle)) != RC_OK){
			return flag;
		}else{
			return flag;
			}
	}
	
	//writing to the file
	RC writeToFile(SM_FileHandle fHandle,recordTableInfo *recordTable){
		char *recordTableString = translateTableToString(recordTable);
		RC flag;
		
		if ((flag=writeBlock(0, &fHandle, recordTableString)) != RC_OK){
			free(recordTableString);
			return flag;
		}else{
			free(recordTableString);
			return flag;
		}
	}
	
	//closing the file
	RC closeFile(SM_FileHandle fHandle){
		RC flag;
		if ((flag=closePageFile(&fHandle)) != RC_OK){
        return flag;
		}
		return flag;
	}
	
	//checks table is exist or not
	bool checkTableToExist(char *name){
		if(access(name, 0) == -1) {
        return false;
		}else{
			return true;
		}
	}
	
	
	//called to write table to file
	RC tableInformationToFileData(char *name,char *temp,recordTableInfo *recordTable){
    SM_FileHandle fHandle;
    RC flag;
    
    //checks if table is exist or not
		if(!checkTableToExist(name)) {
			RC_message="Table does't exist";
			return TABLE_DOES_NOT_EXIST;
		}
		
		//opens the page file
		flag=openPageFile(name,&fHandle);
		if(flag==RC_OK){
			//writing to page file the created table
			char *recordTableString = translateTableToString(recordTable);
				flag=writeBlock(0,&fHandle,recordTableString);
				if(flag==RC_OK){
					//closing the page file
					flag=closePageFile(&fHandle);
					if(flag==RC_OK){
						return RC_OK;
					}else{
						return flag;
					}
					
				}else{
					return flag;	
				}
		}else{
				return flag;
			}
	}
	
	
	void pageOperation(recordTableInfo *recordTable,int pageNumber,int slotNumber,RM_TableData *rel, Record *record){
		//get the buffer manager page handle pointer to be used to write
		//data to the page file
		BM_PageHandle *pageHandle=getBufferPageHandle();
		//before writing record to pageFile serialize the record to be written
		char *rString = serializeRecord(record, rel->schema);
		//writing record to page
		pinPage(recordTable->bufferManager, pageHandle, pageNumber);
		//copy the  serialize data of record to the page data's last availble location
		memcpy(pageHandle->data + (slotNumber*recordTable->slotSize), rString, strlen(rString));
		//mark page as dirty before writing it back to disk
		markDirty(recordTable->bufferManager, pageHandle);
		//unpinning the page will decreases its fixed count
		unpinPage(recordTable->bufferManager, pageHandle);
		//this action will write the page data to page file on disk as its 
		//marked as dirty
		forcePage(recordTable->bufferManager, pageHandle);
		//making tombstone to false which is an indication that record slot is full and its not deleted
		record->id.tombS = false;
		//increasing number of tuples to keep the trak of records inserted
		recordTable->numberOfTuples+=1;
		char *temp;
		tableInformationToFileData(rel->name,temp ,recordTable);
		//freeing memory of pageHandle and serialized record
		free(pageHandle);
		free(rString);
	}
	
	void pageHandleOperations(recordTableInfo *recordTable,int pageNumber,int slotNumber,RM_TableData *rel,Record *record,int temp){
		int i=0;
		while(temp<10){
			i++;
			temp++;
		}
		
		pageOperation(recordTable,pageNumber,slotNumber,rel,record);
	}
	
	
	//writing schema to page file page 1  and also writing table information to page 0 
	RC writeSchemaToTable(recordTableInfo *recordTable,SM_FileHandle fHandle,Schema *schema){
		RC flag;
		int i;
		char *TableInfoString = translateTableToString(recordTable);

		if ((flag=writeBlock(0, &fHandle, TableInfoString)) == RC_OK){
			
			char *schemaToString = serializeSchema(schema);	
			if ((flag=writeBlock(1, &fHandle, schemaToString)) == RC_OK){
				if ((flag=closePageFile(&fHandle)) == RC_OK){
					i=1;
				}else{
					return flag;
				}
				
			}else{
			return flag;
			
			}
		}else{
			return flag;
		}
		
	}
	
	
	
	
	
	
	
		
//creates table for a tuples to write
RC createTable (char *name, Schema *schema){
	
	if(createPageFile(name)==RC_OK){
		RC flag;
		int k=0,schemaSize=0,slotSize=15,dtLength=0,fileSize=0,maximumSlots=0;	
		//calculating the shema size
		
		while(k<schema->numAttr){
        schemaSize=schemaSize+ strlen(schema->attrNames[k]);
        k++;
		}
		
		schemaSize+=sizeof(int)+sizeof(int)+sizeof(int)*(schema->numAttr)+sizeof(int)*(schema->numAttr)+sizeof(int)*(schema->keySize);
		//compute size of file required for schema
		fileSize=computefileSize(schemaSize);
		for(k=0; k<schema->numAttr; k++){
			if(schema->dataTypes[k]==DT_STRING){
				dtLength=schema->typeLength[k];
			}else if(schema->dataTypes[k]==DT_BOOL){
				dtLength=5;
			}else if(schema->dataTypes[k]==DT_INT){
				dtLength=5;
			}else if(schema->dataTypes[k]==DT_FLOAT){
				dtLength=10;
			}
			slotSize=slotSize+(dtLength + strlen(schema->attrNames[k]) + 2);
		}	
		
		
		//calculating maximum number of slots per page
		maximumSlots=calculateMaximumSlotsPerPage(slotSize);
		
		 SM_FileHandle fHandle;
		 //opening the page file
		 flag=openPageFile(name, &fHandle);
		 if(flag!=RC_OK){
			 return flag;
		 }else{
			 //creating a room to store schema so that fileSize+1;
			 ensureCapacity((fileSize + 1), &fHandle);
		
			//initilaze record manager information
			recordTableInfo *recordTable=populateRecordTableInfo(schemaSize,fileSize,maximumSlots,slotSize);
			
			//call to function which writes schema and table information to file.
			writeSchemaToTable(recordTable,fHandle,schema);
			 

		}
		return RC_OK;
		
	}
}

	//called to get pointer to buffer manager
	BM_BufferPool *getBufferManager(){
		BM_BufferPool *bManager = (BM_BufferPool *)malloc(sizeof(BM_BufferPool));
	}
	
	
	
	//called to initialize the buffer manager to be used to store schema and related information
	recordTableInfo *initBufferManagerForRecord(char *name,struct BM_BufferPool *bManager,struct BM_PageHandle *pageHandle){
			
			//initliazing buffer manager with 3 page frames and FIFO as replacement Strategy
			initBufferPool(bManager, name, 3, RS_FIFO, NULL);
			//pinning the 0th page of buffer mananger
			pinPage(bManager, pageHandle, 0);
			//record table translation from string as table
			recordTableInfo *recordTable = TranslateStringToRecordTable(pageHandle->data);
			if(recordTable->schemaSize < PAGE_SIZE){
				//pinning the 1st page if schemaLength is less than PAGE_SIZE
				pinPage(bManager, pageHandle, 1);
			}
			return recordTable;
	}
	
		extern RC initRecordManager (void *mgmtData)
		{
			return RC_OK;
		}
	//opening a Table for storing of records. It requires Buffer manager to be initilazed for stroring records
	//block wise
	RC openTable (RM_TableData *rel, char *name){
			//checks if table is already there or not
			if(access(name,0)!=-1){		
			//get pointer to buffer Manager
			BM_BufferPool *bManager = getBufferManager();
			//get pointer to pageHandle of buffer manager
			BM_PageHandle *pageHandle = getBufferPageHandle();
			//get the record table initlialized by which will pin pages 
			//via buffer manager to store deserialized schema for records
			recordTableInfo *recordTable = initBufferManagerForRecord(name,bManager,pageHandle);
			//deserializing schema for store it in file
			char temp;
			int count=recordTable->numberOfTuples;
			rel->schema = dSchema(pageHandle->data,temp,count);
			//setting the record manager related information 
			//to be used by record manager for performing different functions
			rel->name = name;
			recordTable->bufferManager = bManager;
			rel->mgmtData = recordTable;
			//freeing space used by pageHandle
			free(pageHandle);
			return RC_OK;
			
		}else{
			//if table doesn't exist return an error
			RC_message="table to be deleted doesn't exist";
			return TABLE_DOES_NOT_EXIST;
		}
	}

	RC closeTable (RM_TableData *rel){
		//shutting down the buffer pool to release meory and all 
		//pined pages and dirty pages will be written back to disk
		shutdownBufferPool(((recordTableInfo *)rel->mgmtData)->bufferManager);
		//releasing memory for all schema fields
		freeMemoryOfRecordManager(rel);
		return RC_OK;
	}
	
	//free memory related to schema
	RC freeMemoryOfRecordManager(RM_TableData *rel){
		free(rel->schema->dataTypes);
		free(rel->schema->keyAttrs);
		free(rel->schema->attrNames);
		free(rel->schema->typeLength);
		free(rel->mgmtData);
		free(rel->schema);
		return RC_OK;
	}
	
	RC deleteTable (char *name){
    if(access(name, 0) != -1) {
		if(remove(name) == 0){
			return RC_OK;
		}else{
			RC_message="table to be deleted doesn't exist";
			return TABLE_DOES_NOT_EXIST;
		}	
	}
	else{
			RC_message="table to be deleted doesn't exist";
			return TABLE_DOES_NOT_EXIST;
		}
    
	}
	

	RC shutdownRecordManager (){
    return RC_OK;
	}
	
	
	//it calculates record size by iterating throgh schema
	int getRecordSize (Schema *schema){
		int recordSize=0,counter=0;
		
		//based on number of attribute per schema calculate 
		//size of each attribute based on its data type
		while(counter<schema->numAttr){
		//if attribute is string
		if(schema->dataTypes[counter]==DT_STRING){
				recordSize+=schema->typeLength[counter];
		//if attribute is Boolean
			}else if(schema->dataTypes[counter]==DT_BOOL){
				recordSize+=sizeof(bool);
			//if attribute is INT
			}else if(schema->dataTypes[counter]==DT_INT){
				recordSize+=sizeof(int);
				//if attribute is FLOAT
			}else if(schema->dataTypes[counter]==DT_FLOAT){
				recordSize+=sizeof(float);
			}	
		counter++;
		}
		return recordSize;
	}
	
	
	//creating schema based on parameters passed
	Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys){
		Schema *schema = (Schema *) malloc(sizeof(Schema));
		schema->attrNames = attrNames;
		schema->numAttr = numAttr;
		schema->keySize = keySize;
		schema->dataTypes = dataTypes;
		schema->attrNames = attrNames;
		schema->typeLength = typeLength;
		schema->keyAttrs = keys;
		return schema;
	}
	
	//releasing the memory allocated for schema
	RC freeSchema (Schema *schema){
		free(schema);
		return RC_OK;
	}
	
	int calculateSlotNumber(int pageNumber,recordTableInfo *recordTable){
		//calculate slot number
		int slotNumber = recordTable->numberOfTuples - ((pageNumber - recordTable->recordStart)*recordTable->maximumPageSlot) ;	
		return slotNumber;
	}
	
	int setSlotNumber(recordTableInfo *recordTable){
			int slotNumber,i=0;
			while(i<1){
				slotNumber = recordTable->tombStoneHead->id.slot;
				i++;
			}
	}
	
	int setPageNumber(recordTableInfo *recordTable){
		int pageNumber;
		pageNumber = recordTable->tombStoneHead->id.page;
		//pointing to next tombstone record
        recordTable->tombStoneHead = recordTable->tombStoneHead->next;
		return pageNumber;
	}
	
	
	//called at the time of insertion of record
	RC insertRecord (RM_TableData *rel, Record *record){
		int pageNumber, slotNumber,temp=0;
		//get the record manager related info.
		recordTableInfo *recordTable=getRecordManagerTableInfo(rel);
		
		if (recordTable->tombStoneHead == NULL){
		//if no tombstone record found
		//then insert at the end of last recod at page
		pageNumber = recordTable->recordEnd;
		slotNumber=calculateSlotNumber(pageNumber,recordTable);
      
		//if slot number is equal to max allowed slot on page
		//then increase page number to next and make slot number to 0
		 if (slotNumber != recordTable->maximumPageSlot){
           
			}else{
				slotNumber = 0;
				pageNumber+=1;
			}
			
        //saving pointer to last inserted record
        recordTable->recordEnd = pageNumber;
		}//as we have to created the linked list of
		//deleted records and marked each record as tombstone
		//so before inserting we have to check whether any space with 
		//tombstone, if yes then insert new record to that place
		//else write record to the end of last record
		else{
			//if any recod with tombstone then insert new record at that place
			temp=1;
			slotNumber = setSlotNumber(recordTable);
			pageNumber = setPageNumber(recordTable);
		}
		
		updateRecordInfo(record,pageNumber,slotNumber);
		pageHandleOperations(recordTable,pageNumber,slotNumber,rel,record,temp);
		return RC_OK;
	}
	
	void updateRecordInfo(Record *record, int pageNumber,int slotNumber){
		//populating record ID and slot to
		//record where new record inserted
		
		record->id.page = pageNumber;
		record->id.slot = slotNumber;
	}
	
	
	
	//called to create record and allocates memory to it.
	RC createRecord (Record **record, Schema *schema){
	*record = (Record *)  malloc(sizeof(Record));
    (*record)->data = (char *)malloc((getRecordSize(schema)));
    return RC_OK;
	}
	
	//calculates the position of each of the attribute in schema
	int setPositionalDifference(Schema *schema, int attrNum){
		int positionalDifference;
		attrOffset(schema, attrNum, &positionalDifference);
		return positionalDifference;
	}
	
	void setStringAttribute(char *attribute,char *sValue,int length,Value **value){
		strncpy(sValue, attribute, length);
		sValue[length] = '\0';
		(*value)->v.stringV = sValue;
	}
	
	//get the attribute 
	RC getAttr (Record *record, Schema *schema, int attrNum, Value **value){
		int length;
		int positionalDifference;
		//allocating memory space to value
		 *value = (Value *)  malloc(sizeof(Value));
		 (*value)->dt = schema->dataTypes[attrNum];
		 char *attribute, *sValue;
		//getting attribute value position for each attributes
		positionalDifference=setPositionalDifference(schema,attrNum);
		attribute = (record->data) + positionalDifference;
		if(schema->dataTypes[attrNum]==DT_STRING){
			length = schema->typeLength[attrNum];
			sValue = (char *) malloc(length + 1);
		}
		if(schema->dataTypes[attrNum]==DT_INT){//if attribute is Int
			memcpy(&((*value)->v.intV),attribute, sizeof(int));
		}else if(schema->dataTypes[attrNum]==DT_BOOL){//if attribute is Boolean
			memcpy(&((*value)->v.boolV),attribute, sizeof(bool));
		}else if(schema->dataTypes[attrNum]==DT_FLOAT){//if attribute is Float
			 memcpy(&((*value)->v.floatV),attribute, sizeof(float));
		}else if(schema->dataTypes[attrNum]==DT_STRING){//if attribute is String
			setStringAttribute(attribute,sValue,length,value);
		}
			return RC_OK;
	}
	
	//setting the attribute based on data types
	RC setAttr (Record *record, Schema *schema, int attrNum, Value *value){
		int length;
		int positionalDifference;
		char *attribute, *sValue;
		//getting attribute value position for each attributes
		positionalDifference=setPositionalDifference(schema,attrNum);
		attribute = (record->data) + positionalDifference;
		if(schema->dataTypes[attrNum]==DT_STRING){
			length = schema->typeLength[attrNum];
			sValue = (char *) malloc(length);
			sValue = value->v.stringV;
		}
		if(schema->dataTypes[attrNum]==DT_INT){//if attribute is Int
			 memcpy(attribute,&(value->v.intV), sizeof(int));
		}else if(schema->dataTypes[attrNum]==DT_BOOL){//if attribute is Boolean
			 memcpy(attribute,&((value->v.boolV)), sizeof(bool));
		}else if(schema->dataTypes[attrNum]==DT_FLOAT){//if attribute is Float
			memcpy(attribute,&((value->v.floatV)), sizeof(float));
		}else if(schema->dataTypes[attrNum]==DT_STRING){//if attribute is String
            memcpy(attribute,(sValue), length);
		}
			return RC_OK;
	}
	
	
	//alloctes memory to searchRecord for scanning
	searchRecord *initSearchRecord(){
		searchRecord *recordSearch = (searchRecord *) malloc(sizeof(searchRecord));
		return recordSearch;
		}
	
		//alloctes memory to searchRecord data
		void initialize_Scanner(RM_TableData *rel,searchRecord *recordSearch,Expr *cond){
			recordSearch->numberOfSlotsPerRelation = ((recordTableInfo *)rel->mgmtData)->slotSize;
			recordSearch->numberOfPagesPerRelation = ((recordTableInfo *)rel->mgmtData)->recordEnd;
			recordSearch->currentRecordPageNo = ((recordTableInfo *)rel->mgmtData)->recordStart;
			recordSearch->currentRecordSlotNo = 0;
			recordSearch->condition = cond;
		}
	
	
	//seraches record in page
	void findRecordInScheme(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond) {
		//initializing RM_ScanHandle data structure
		scan->rel = rel;
		//initializing recordSearch to store information about record to 
		 //searched and to evaluate a condition
		searchRecord *recordSearch = initSearchRecord();
		initialize_Scanner(rel,recordSearch,cond);
		// recordSearch to scan->mgmtData
		scan->mgmtData = (void *) recordSearch;
	}
	
	//start scan 
	RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond){
	findRecordInScheme(rel, scan, cond);
    return RC_OK;
	}

	//returns the scan pointer
	searchRecord *getSearchRecordPointer(RM_ScanHandle *scan){
		searchRecord *recordSearch=scan->mgmtData;
		return recordSearch; 
	}
	
	//assigns value to record slot and page
	void populateScanData(Record *record,searchRecord *recordSearch){
		record->id.slot = recordSearch->currentRecordSlotNo;
		record->id.page = recordSearch->currentRecordPageNo;
	}
	

	void updateRecordSearch(searchRecord *recordSearch){
		(recordSearch->currentRecordPageNo)+=1;
		recordSearch->currentRecordSlotNo = 0;
	}
	
	void Evaluate(RM_ScanHandle *scan, Record *record,Value **value,searchRecord *recordSearch){
		evalExpr(record, scan->rel->schema, recordSearch->condition, &(*value));
	}
	
	RC next (RM_ScanHandle *scan, Record *record){
		RC flag;
		Value *value;
		searchRecord *recordSearch=getSearchRecordPointer(scan);
		populateScanData(record,recordSearch);
		
		//retrieve record with respect to page number and record id
		flag = getRecord(scan->rel, record->id, record);
		if(flag != RC_RM_NO_MORE_TUPLES){
				//if record is deleted (means marked as tombstone)
				if(record->id.tombS){
					if (recordSearch->currentRecordSlotNo == recordSearch->numberOfSlotsPerRelation - 1){
						updateRecordSearch(recordSearch);
						scan->mgmtData = recordSearch;
						return next(scan, record);
					}
					else{
						//if not increase the sacn pointer to next record
						(recordSearch->currentRecordSlotNo)+=1;
						scan->mgmtData = recordSearch;
						return next(scan, record);
					}
				
			}
			else{
				
				Evaluate(scan,record,&value,recordSearch);
				
				if (recordSearch->currentRecordSlotNo != recordSearch->numberOfSlotsPerRelation - 1){		
					(recordSearch->currentRecordSlotNo)+=1;
					scan->mgmtData = recordSearch;	
				}
				else{
					updateRecordSearch(recordSearch);
					scan->mgmtData = recordSearch;
				}

				if(value->v.boolV!=1){
					return next(scan, record);
				}
				else{
					return RC_OK;
				}
			}
		}else{
			RC_message="No More Tuples left in relation";
			return RC_RM_NO_MORE_TUPLES;
		}

		
	}
	
	RC closeScan (RM_ScanHandle *scan){
		return RC_OK;
	}
	
	//as record is deleted add tombstone mark to that slot
	void updateTombStone(tombStoneNode *tomb,RID id,RM_TableData *rel,recordTableInfo *recordTable){
			char *temp;
			tomb->id.page = id.page;
			tomb->id.slot = id.slot;
			tomb->id.tombS = TRUE;
			(recordTable->numberOfTuples)-=1;
			tableInformationToFileData(rel->name, temp,recordTable);
	}
	
	
	//delete the record based on record id
	RC deleteRecord (RM_TableData *rel, RID id){
		recordTableInfo *recordTable = getRecordManagerTableInfo(rel);
		tombStoneNode *tomb = recordTable->tombStoneHead;
		int i=0;
		if(recordTable->numberOfTuples<0){
			RC_message="Number of tuples are less than 0";
			return RC_WRITE_FAILED;  
		}
		if(recordTable->numberOfTuples>0){
			if(recordTable->tombStoneHead != NULL){
				for (i=0;tomb->next != NULL;i++){
					tomb = tomb->next;
				}
				tomb->next = getTombStoneNode();
				tomb = tomb->next;
				updateTombStone(tomb,id,rel,recordTable);
			}
			else{
				recordTable->tombStoneHead = getTombStoneNode();
				recordTable->tombStoneHead->next = NULL;
				tomb = recordTable->tombStoneHead;
				updateTombStone(tomb,id,rel,recordTable);
			}
					
		}

		return RC_OK;
	}
	
	void updatePageAndSlot(int *pageNumber,int *slotNumber,Record *record){
		*pageNumber = record->id.page;
		*slotNumber = record->id.slot;
	}
	
	void updateOperations(RM_TableData *rel,recordTableInfo *recordTable,BM_PageHandle *pageHandle){
		markDirty(recordTable->bufferManager, pageHandle);
		unpinPage(recordTable->bufferManager, pageHandle);
		forcePage(recordTable->bufferManager, pageHandle);
		char *temp;
		tableInformationToFileData(rel->name, temp,recordTable);
	}
	
	
	//update the record data
	RC updateRecord (RM_TableData *rel, Record *record){
		BM_PageHandle *pageHandle = getBufferPageHandle();
		recordTableInfo *recordTable = getRecordManagerTableInfo(rel);
		int pageNumber, slotNumber;
		updatePageAndSlot(&pageNumber,&slotNumber,record);
		char *stringRecord = serializeRecord(record, rel->schema);
		pinPage(recordTable->bufferManager, pageHandle, pageNumber);
		memcpy((slotNumber*(recordTable->slotSize))+pageHandle->data, stringRecord, strlen(stringRecord));
		updateOperations(rel,recordTable,pageHandle);
		return RC_OK;

	}
	
	Record *getRecordInitilaized(){
		Record *record = (Record *) malloc(sizeof(Record));
		return record;
	}
	char *getRecordDataInitialized(recordTableInfo *recordTable){
		char *data = (char *)malloc(sizeof(char) * recordTable->slotSize);
		return data;
	}
	
	char *getToken(char *recData){
		char *t;
		t = strtok(recData,"-");
		t = strtok (NULL,"]");
		t = strtok (NULL,"(");
		return t;
	}
	
	void setAttributeValues(Record *record, Schema *schema, int loop,Value *value,char *s2){
		setAttr (record, schema, loop, value);
	}
	
	void dBasedOnDataType(char *s1, Record *record, Schema *schema, int loop){
		Value *value;
		char *s2;
		 if(schema->dataTypes[loop]==DT_INT)
            {
                int intVal = strtol(s1, &s2, 10);
                MAKE_VALUE(value, DT_INT, intVal);
                setAttributeValues(record, schema, loop, value,s2);
            }
            else if(schema->dataTypes[loop]==DT_STRING)
            {
                MAKE_STRING_VALUE(value, s1);
				setAttributeValues(record, schema, loop, value,s2);
            }
               else if(schema->dataTypes[loop]==DT_FLOAT)
            {
                float floatVal = strtof(s1, NULL);
                MAKE_VALUE(value, DT_FLOAT, floatVal);
                setAttributeValues(record, schema, loop, value,s2);
            }
            else if(schema->dataTypes[loop]==DT_BOOL)  
            {
                 bool boolVal;
                 if(s1[0] == 't'){
					boolVal=TRUE; 
				}else{
					boolVal=FALSE;
					}
                MAKE_VALUE(value, DT_BOOL, boolVal);
                setAttributeValues(record, schema, loop, value,s2);
            }
		freeVal(value);
	}
	
	//deSerializing of Record
	Record *dRecord(RM_TableData *rel,char *stringRecord){
		char recData[strlen(stringRecord)];
		strcpy(recData, stringRecord);
		Schema *schema = rel->schema;
		int loop=0,temp=0;
		recordTableInfo *recordTable = getRecordManagerTableInfo(rel);
		Record *record = getRecordInitilaized();
		record->data = getRecordDataInitialized(recordTable);
		char *s1;
		s1=getToken(recData);
		free(stringRecord);
		while(loop<schema->numAttr){
			s1 = strtok (NULL,":");
			if(loop != (schema->numAttr - 1)){
				 s1 = strtok (NULL,",");
			}
			else{
				s1 = strtok (NULL,")");
			}
			dBasedOnDataType(s1,record,schema,loop);
			loop+=1;
			temp=temp+1;
		}
		return record;
	}
	
	
	void updateIDBased(RID id,int *pageNumber,int *slotNumber){
		*pageNumber = id.page;
		*slotNumber = id.slot;
	}
	
	//updates the tombstone details
	bool checkTombStone(Record *record,RM_TableData *rel,RID id,int *pageNumber,int *slotNumber, int *tcount){
		recordTableInfo *recordTable=getRecordManagerTableInfo(rel);
		tombStoneNode *tombNode = (recordTable)->tombStoneHead;
		
		bool tFlag=false;
		int i=0;
		while(i<(recordTable)->tombStoneLength && tombNode!=NULL){
			
			if (tombNode->id.page == *pageNumber && tombNode->id.slot == *slotNumber){	
				tFlag = true;
				record->id.tombS = true;
				//break;
			}
			
			tombNode = tombNode->next;
			(*tcount)+=1;
			i++;
		}
		return tFlag;
	}
	
	
	void updateRecordInformation(Record *record, int *pageNumber,int *slotNumber){
		//populating record ID and slot to
		//record where new record inserted
		
		record->id.page = *pageNumber;
		record->id.slot = *slotNumber;
	}
	
	void getRecordPageOperation(RM_TableData *rel,Record *record,recordTableInfo *recordTable,BM_PageHandle *pageHandle,int pageNumber,int slotNumber){
			char *stringRecord = (char *) malloc(sizeof(char) * recordTable->slotSize);
			int i=0;
			while(i<1){
			
			pinPage(recordTable->bufferManager, pageHandle, pageNumber);
			memcpy(stringRecord, pageHandle->data + ((slotNumber)*recordTable->slotSize), sizeof(char)*recordTable->slotSize);
			unpinPage(recordTable->bufferManager, pageHandle);	
			Record *tempR = dRecord(rel,stringRecord);
			record->data = tempR->data;
			free(tempR);
			i++;
		}
		
		free(pageHandle);
	}
	
	//get the record based on record ID
	RC getRecord (RM_TableData *rel, RID id, Record *record){
		
		BM_PageHandle *pageHandle=getBufferPageHandle();
		recordTableInfo *recordTable=getRecordManagerTableInfo(rel);
		int pageNumber, slotNumber;
		updateIDBased(id,&pageNumber,&slotNumber);
		updateRecordInformation(record,&pageNumber,&slotNumber);
		
		record->id.tombS = false;
		int tcount=0;
		
		bool tFlag=checkTombStone(record,rel,id,&pageNumber,&slotNumber,&tcount);
		
		if(pageHandle!=NULL){
				if (!tFlag){
					int tupleNumber = (pageNumber - recordTable->recordStart)*(recordTable->maximumPageSlot) + slotNumber + 1 - tcount;
					if (tupleNumber<=recordTable->numberOfTuples){
						getRecordPageOperation(rel,record,recordTable,pageHandle,pageNumber,slotNumber);
					}else{
							free(pageHandle);
							RC_message="No more tuples left in relation.";
							return RC_RM_NO_MORE_TUPLES;
						}
				}
		}else{
			free(pageHandle);
		}
		return RC_OK;
	}
