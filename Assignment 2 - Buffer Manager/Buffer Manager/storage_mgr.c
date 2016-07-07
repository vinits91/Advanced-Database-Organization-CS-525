/* Stoarge Manager responsible for reading and writing of memory to/from the file.
 * All its functions being called from test_assign1_1.c file to where test cases are written.
 * */


#include<stdio.h>
#include<stdlib.h>
//Inclusion of custom header files.
#include"storage_mgr.h" 
#include"dberror.h"

 FILE *file;//Global file variable used by all functions
int blockTobeRead;// Global varaible to store page number, to be used by all Read functions

/*It creates a new file with filename passed to it, and also initialize its contents
with '\0' such that file size becomes equal to PAGE_SIZE=4096*/
	RC createPageFile(char *fileName){
			file=fopen(fileName,"w");
			char *pointer=(char *)calloc(PAGE_SIZE,sizeof(char));//reserving the space of PAGE_SIZE
			fwrite(pointer,PAGE_SIZE,sizeof(char),file); //writing to the file
			fclose(file);
			free(pointer);
			return RC_OK;
			
		}
	

/*
 * This functions opens an existing file and if file doesn't exist it returns RC_FILE_NOT_FOUND error
 * function is also responsible for populating important file related data into file Handle.
 * */
	RC openPageFile (char *fileName, SM_FileHandle *fHandle){
		int fileSize,filePageCount;
		FILE *file;		
		//if file cannot be opened or its not exist
		if(!(file=fopen(fileName,"r"))){
			RC_message="Desired file doesn't exist";
			return RC_FILE_NOT_FOUND;
		}else{
			//opening a file in read mode and populating the fHandle structure with important information.
			file=fopen(fileName,"r");
			fHandle->mgmtInfo=file;
			fHandle->fileName=fileName;
			fHandle->curPagePos=0;
			fseek(file,0,SEEK_END);//seeking throgh end of file.
			fileSize=ftell(file);
			//counting number of pages in file.
			filePageCount=fileSize/PAGE_SIZE;
			fHandle->totalNumPages=filePageCount;
			return RC_OK;
		}		
	}
	
	/*
	 * Function removes file from Disk.
	 * */
	RC destroyPageFile (char *fileName){
		remove(fileName);
		return RC_OK;
	}
	
	//upon closing, Releasing all memory resources
	RC closePageFile (SM_FileHandle *fHandle){
		fHandle=NULL;
		if(file!=NULL){
			fclose(file);
			file=NULL;
		}	
		free(file);
		
		return RC_OK;	
	}

	/*
	 * Function reads a Block of Data from file indicated  by pageNum and write it to the memory pointed by memPage
	 * */
	RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
		if(fHandle!=NULL){
			int success;
			//checking the desired page to be read is within limit of totalNumPages also checks numPage should be more than 0
			if(((pageNum)<=(fHandle->totalNumPages)) && (pageNum)>=0){
					success=fseek(fHandle->mgmtInfo,(((pageNum+1)*PAGE_SIZE)),SEEK_SET);//seek to desired position to be read
					fread(memPage,sizeof(char),PAGE_SIZE,fHandle->mgmtInfo);// Reading from file and writing data to memory
					fHandle->curPagePos=pageNum;//setting the current page position file pointer			
					return RC_OK;
			}
			else{
					RC_message="Page to be read doesn't exist in file";
					return RC_READ_NON_EXISTING_PAGE;
				}
		}else{
			RC_message="desired file related data is not initialized";
			return RC_FILE_HANDLE_NOT_INIT;
		}	
	}
	
	/*function returns current file pointer position*/
	int getBlockPos (SM_FileHandle *fHandle){
		if(fHandle!=NULL){
			return (fHandle->curPagePos);
		}
	}
	
	/*Reads first block of file to memory*/
	RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
		//setting PageNum to first block and calling readBlock function.
		blockTobeRead=0;
		return readBlock (blockTobeRead,fHandle, memPage);
	}
	/*Reads block of data to memory, previous to the block pointed by current page pointer of file*/
	RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
		blockTobeRead=getBlockPos(fHandle)-1;
		return readBlock (blockTobeRead,fHandle, memPage);
	}
	
	/*Reads block of data to memory pointed by current page pointer of file*/
	RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
		blockTobeRead=getBlockPos(fHandle);
		return readBlock (blockTobeRead,fHandle, memPage);
	}
	
	//Reads the next block of data to memory, which a block next to the block pointed by current page file pointer
	RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
		blockTobeRead=getBlockPos(fHandle)+1;
		return readBlock (blockTobeRead,fHandle, memPage);
	}
	
	//reads the last block of file data to memory
	RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
		blockTobeRead=fHandle->totalNumPages-1;
		return readBlock (blockTobeRead,fHandle, memPage);
	}
	
	//writes block of data from Memory to file to the page idicated by pageNum
	RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
	if(fHandle!=NULL){
			if(((pageNum)<=(fHandle->totalNumPages))&&(pageNum)>=0){
					fseek(fHandle->mgmtInfo,(((pageNum+1)*PAGE_SIZE)),SEEK_SET);//seek to position to be written
					fwrite(memPage,sizeof(char),PAGE_SIZE,fHandle->mgmtInfo);//writing to file from memory
					fHandle->curPagePos=pageNum;//updating current page file pointer
					return RC_OK;			
			}
	}else{
			RC_message="desired file related data is not initialized";
			return RC_FILE_HANDLE_NOT_INIT;
		 }
	}
	
	//Writing to file block pointed by current file page pointer from memory
	writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
		int blockTobeWritten=getBlockPos(fHandle);
		return writeBlock(blockTobeWritten,fHandle,memPage);	
	}
	
	/*
	 * Appending the file with an empty block page at the end of file
	 * */
	RC appendEmptyBlock (SM_FileHandle *fHandle){
		int totalBytes;
		FILE *file;
		if(fHandle!=NULL){
			file=fopen(fHandle->fileName,"a");//opening a file in append mode
			fHandle->totalNumPages+=1;//updating total number of pages in file
			fseek(file,(((fHandle->totalNumPages+1)*PAGE_SIZE)),SEEK_END);//seek to end of file for append operation
			char *pointer=(char *)calloc(PAGE_SIZE,sizeof(char));
			fwrite(pointer,PAGE_SIZE,sizeof(char),file);//writing an empty block to file
			//fHandle->curPagePos=fHandle->totalNumPages;//updating current block page pointer of file
			free(pointer);//freeing the occupied memory
			fclose(file);//closing of file
			return RC_OK;
		}else{
			RC_message="desired file related data is not initialized";
			return RC_FILE_HANDLE_NOT_INIT;
		}
	}
		
	
	//Checks whether file has numberOfPages or not, if not then it creates new pages in a file upto numberOfPages and sets totalNumPages to numberOfPages
	RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle){
		int diff,i;
		if(fHandle!=NULL){
			if((fHandle->totalNumPages)<numberOfPages){
				diff=numberOfPages-(fHandle->totalNumPages);
				for(i=0;i<diff;i++){
					appendEmptyBlock(fHandle);
				}
				return RC_OK;
			}else{
				return RC_OK;
			}	
		}else{
			RC_message="desired file related data is not initialized";
			return RC_FILE_HANDLE_NOT_INIT;
		}
	}
