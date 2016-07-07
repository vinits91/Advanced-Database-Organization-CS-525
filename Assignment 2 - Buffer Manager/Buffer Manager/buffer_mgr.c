#include <stdio.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "dberror.h"
#include <stdlib.h>
#include <string.h>

#define LRUCOUNTERFRAME 100
#define PAGEMAXSIZE 7000
#define FRAMEMAXSIZE 100


//stores important information about buffer manager which is to be attached to mgmtdata of buffer manager
typedef struct bufferPoolInfo{
		int pagesToPageFrame[PAGEMAXSIZE];//mapping of pageNumbers to page frames
		int pageFramesToPage[FRAMEMAXSIZE];//mapping of page frames to pages
		bool pageFrameDirtyBit[FRAMEMAXSIZE];// maintains dirty flags for each page frames
		int pageFrameFixedCount[FRAMEMAXSIZE];//maintains fixed count of each page frame
		int numReadIO;// stores total number of Read IO
		int numWriteIO;//stores total number of Write IO
		int totalFrames;//maintains total number of filled frames count
		int maxFrames;//stores number of page frames inside buffer pool
		int totalCount;//maintains total number of pages in frames of buffer pool
		SM_FileHandle filePointer;//stores file address of file
		struct pageFrame *head; //head of page frames linked list
		struct pageFrame *tail;//tail of page frames linked list
		int lruCounter4PageFrame[LRUCOUNTERFRAME];// LRU counter information
		struct pageFrame *lastNode;//maintains last node address of page frames list
	}bufferPoolInfo;
	
	//keeps the iformation regarding each node in linked list of page frames
	typedef struct pageFrame{
		bool dirtyBit; //dirty bit for page  true=dirty false= Not dirty
		int fixedCount;//fixed count to mark usage of page by client
		int pinUnpin; //pinning and unpinnig of page
		int pageNumber;//page number stored in buffer page frame
		int pageFrameNo;//frame number in page frames linked list
		int filled; // whether frame is filled or not
		char *data;//stores content of page.
		struct pageFrame *next;//pointer to next node in page frames linked list
		struct pageFrame *previous;//pointer to previous node in page frames linked list
	}pageFrame;


	//called when new page frame is created during initialization of buffer, each information is initialized with default value.
	pageFrame *getNewNode(){
		pageFrame *linkNode = calloc(PAGE_SIZE,sizeof(SM_PageHandle));
		linkNode->dirtyBit=false;
		linkNode->pinUnpin=0;
		linkNode->pageNumber=NO_PAGE;
		linkNode->pageFrameNo=0;
		linkNode->fixedCount=0;
		linkNode->filled=0;
		linkNode->next=NULL;
		linkNode->previous=NULL;
		linkNode->data=(char *)calloc(PAGE_SIZE,sizeof(SM_PageHandle));;
		return linkNode;
		
	}

	//storing the important information about buffer manager during initialization of buffer. 
	bufferPoolInfo *initBufferPoolInfo(const int numPages,SM_FileHandle fileHandle){
		bufferPoolInfo *bufferPool=calloc(PAGE_SIZE,sizeof(SM_PageHandle));
		bufferPool->numReadIO=0;
		bufferPool->numWriteIO=0; 
		bufferPool->totalFrames=0;
		bufferPool->totalCount=0;
		bufferPool->maxFrames=numPages;//setting to number of frames maintained by Buffer Mananger
		bufferPool->filePointer=fileHandle;//file used by buffer manager
		
		//allocating memory to each array of buffer information.
		memset(bufferPool->lruCounter4PageFrame,NO_PAGE,LRUCOUNTERFRAME*sizeof(int));
		memset(bufferPool->pagesToPageFrame,NO_PAGE,PAGEMAXSIZE*sizeof(int));
		memset(bufferPool->pageFramesToPage,NO_PAGE,FRAMEMAXSIZE*sizeof(int));
		memset(bufferPool->pageFrameDirtyBit,NO_PAGE,FRAMEMAXSIZE*sizeof(bool));
		memset(bufferPool->pageFrameFixedCount,NO_PAGE,FRAMEMAXSIZE*sizeof(int));
		return bufferPool;
	}


	//called at the time of buffer initialization
	RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, 
		  const int numPages, ReplacementStrategy strategy, 
		  void *stratData){
		
		SM_FileHandle fileHandle;
		int i=1;
		//opening a file to be used by BM.
		if (openPageFile ((char *)pageFileName, &fileHandle) == RC_OK){
		bufferPoolInfo *bufferPool=initBufferPoolInfo(numPages,fileHandle);
		
		//setting buffer manager fields.
		bm->numPages=numPages;
		bm->pageFile=(char *)pageFileName;
		bm->strategy=strategy;//stores strategy to be used by BM when the page is to be replaced in frames.
		bufferPool->head=bufferPool->tail=getNewNode();
		bufferPool->head->pageFrameNo=0;
		
		//creating page frame node linked list with number of page frames = numPages
		while(i<numPages){
			bufferPool->tail->next = getNewNode();
			bufferPool->tail->next->previous = bufferPool->tail;
			bufferPool->tail = bufferPool->tail->next;
			bufferPool->tail->pageFrameNo = i;
			i++;
		}
		bufferPool->lastNode=bufferPool->tail;
		bm->mgmtData=bufferPool;//attaching bufferpool info to data of BM to be used by various functions of BM.
		return RC_OK;
	}else{
		RC_message="File to be opened doesn't exist";
		return RC_FILE_NOT_FOUND;
		}
		
	}
	
	
	
	//returns BM info.
	bufferPoolInfo *getMgmtInfo(BM_BufferPool *const bm){
		if(bm!=NULL){
		bufferPoolInfo *mgmtInfo=(bufferPoolInfo*)bm->mgmtData;
		return mgmtInfo;
		}
	}
	

	//returns fixed count value at each page frames.
	int *getFixCounts (BM_BufferPool *const bm){
		if(bm!=NULL){
			bufferPoolInfo *mgmtInfo=getMgmtInfo(bm);
			pageFrame *temp=mgmtInfo->head;//starting from head
			while(temp!=NULL){//traverse till there are no frames.
				//stores fixed count value at each page frame to an aaray
				(mgmtInfo->pageFrameFixedCount)[temp->pageFrameNo]=temp->fixedCount; 
				temp=temp->next;
			}
			free(temp);
			return mgmtInfo->pageFrameFixedCount;
		}
	}
	
	//returns total number of read operation done by BM from disk
	int getNumReadIO (BM_BufferPool *const bm){
		if(bm!=NULL){
			bufferPoolInfo *mgmtInfo=getMgmtInfo(bm);
			return mgmtInfo->numReadIO;
		}
	}

	
	//returns an array of dirty flags at each page frame of BM
	bool *getDirtyFlags (BM_BufferPool *const bm){
		if(bm!=NULL){
		bufferPoolInfo *mgmtInfo=getMgmtInfo(bm);
		pageFrame *temp=mgmtInfo->head;
			while(temp!=NULL){
				//stores dirty bit value at each page in page frame.
				(mgmtInfo->pageFrameDirtyBit)[temp->pageFrameNo]=temp->dirtyBit;
				temp=temp->next;
			}
		free(temp);
		return mgmtInfo->pageFrameDirtyBit;
		}
	}
	
	//returns total number of write operation performed by BM to disk.
	int getNumWriteIO (BM_BufferPool *const bm){
		if(bm!=NULL){
		bufferPoolInfo *mgmtInfo=getMgmtInfo(bm);
		return mgmtInfo->numWriteIO;
		}
	}
	
	//markes page specified in page->pageNum as dirty
	RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page){
		if(bm!=NULL){
			bufferPoolInfo *mgmtInfo=getMgmtInfo(bm);
			pageFrame *temp=mgmtInfo->head;
			while(temp!=NULL){
				if(temp->pageNumber==page->pageNum){//searches page to be marked dirty in page frames.
					temp->dirtyBit=true;//marking page as dirty.
				}
				temp=temp->next;
			}
			free(temp);
			
		}else{
			RC_message="Buffer is not initialized ";
			return RC_BUFFER_NOT_INITIALIZED;
		}
	}
	
	//called when client no longer requires the page
	//unpins the page specified in page->pageNum only if the page has fixed count greater than 1.
	RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page){
		if(bm!=NULL){
			bufferPoolInfo *mgmtInfo=getMgmtInfo(bm);
			pageFrame *temp=mgmtInfo->head;
			while(temp!=NULL){
				if(temp->pageNumber==page->pageNum && temp->fixedCount>0){
					temp->fixedCount-=1;//decreasing fixed count value of page.
				}
				temp=temp->next;
			}
			free(temp);
			return RC_OK;
		}else{
			RC_message="Buffer is not initialized ";
			return RC_BUFFER_NOT_INITIALIZED;
		}
	}
	
	

	//called when client want to read any page into memory, it decides on strategy which will be
	//used at the time of page replacement.
	RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum){
	if(bm!=NULL){
		
			if(bm->strategy==RS_FIFO){
				 fifo(bm,page,pageNum);//calls FIFO.
				
			}else if(bm->strategy==RS_LRU){
				lru(bm,page,pageNum);//calls LRU
			}
				return RC_OK;
			}else{
				RC_message="Buffer is not initialized ";
				return RC_BUFFER_NOT_INITIALIZED;
			}
	}
		
	//calls during the shutting down of buffer
	//this action releases all the consumed memory and also writes back pages which are dirty to disk.
	RC shutdownBufferPool(BM_BufferPool *const bm){
		if(bm!=NULL){
			forceFlushPool(bm);//flushes the page frames and writes dirty pages return to disk
			bufferPoolInfo *mgmtInfo=getMgmtInfo(bm);
			pageFrame *temp=mgmtInfo->head;
			int i;
			for(i=0;temp!=NULL;i++){
				
				//assignes each page frames to head and releases head node.
				free(mgmtInfo->head->data);
				free(mgmtInfo->head);
				temp=temp->next;
				mgmtInfo->head=temp;
			}
		//makes head and tail node of linked listpage frames to NULL
		mgmtInfo->head=NULL;
		free(temp);
		mgmtInfo->tail=NULL;
		
		return RC_OK;
		}else{
			RC_message="Buffer is not initialized ";
			return RC_BUFFER_NOT_INITIALIZED;
		}
	}

	
	//flushesall pages in page frames and if any page is dirty then write it back to disk.
	RC forceFlushPool(BM_BufferPool *const bm){
		if(bm!=NULL){
			SM_FileHandle fileHandle;
			bufferPoolInfo *mgmtInfo=getMgmtInfo(bm);
			pageFrame *temp=mgmtInfo->head;
			if (openPageFile ((char *)(bm->pageFile), &fileHandle) == RC_OK){
			while(temp != NULL){
			if(temp->dirtyBit){//if dirty flag is true
			//write the page back to disk file
            if(writeBlock(temp->pageNumber, &fileHandle, temp->data) == RC_OK){
				temp->dirtyBit = false;
				(mgmtInfo->numWriteIO)+=1;//updtaes write count of BM by 1.
			}else{
			  return RC_WRITE_FAILED;
			}
			}
			temp = temp->next;
			}
			//free(temp);
			//closePageFile(&fileHandle);
			return RC_OK;
		}else{
			RC_message="file to be opened doesn't exist";
			return RC_FILE_NOT_FOUND;
		}
		}else{
			RC_message="Buffer is not initialized ";
			return RC_BUFFER_NOT_INITIALIZED;
		}
		
	}
	
	RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page){
		if(bm!=NULL){
			SM_FileHandle fileHandle;
			bufferPoolInfo *mgmtInfo=getMgmtInfo(bm);
			pageFrame *temp=mgmtInfo->head;
		if (openPageFile ((char *)(bm->pageFile), &fileHandle) == RC_OK){		
			while(temp!=NULL){
				if(temp->pageNumber==page->pageNum && temp->dirtyBit){
					if(writeBlock(temp->pageNumber,&(mgmtInfo->filePointer),temp->data)==RC_OK){
						temp->dirtyBit=false;
						mgmtInfo->numWriteIO+=1;
					}
				}
				temp=temp->next;
			}
			free(temp);
			return RC_OK;
		}else{
			RC_message="file to be opened doesn't exist";
			return RC_FILE_NOT_FOUND;
		}
		}else{
			RC_message="Buffer is not initialized ";
			return RC_BUFFER_NOT_INITIALIZED;
		}
	}
	
	//returns an array page numbers of page stored in page frames
	PageNumber *getFrameContents (BM_BufferPool *const bm){
		bufferPoolInfo *mgmtInfo=getMgmtInfo(bm);
		return mgmtInfo->pageFramesToPage;
	}
	
	
	//finds and returns node's page with pageNumber with pageNum
	pageFrame *locateNode(BM_BufferPool *const bm,BM_PageHandle *const page,const PageNumber pageNum ){	 
	if(bm!=NULL){
			bufferPoolInfo *mgmtInfo = getMgmtInfo(bm);
			pageFrame *exist=mgmtInfo->head;
			   while(exist!=NULL){
				   if(exist->pageNumber==pageNum){//compares frame page's page number with pageNum to be search
						return exist;
					 }
					exist=exist->next;
				}
				return NULL;
		}else{
			RC_message="Buffer is not initialized ";
			return NULL;
		}
	}//
	
	//finds if page to be read by client is already there in memory or not
	pageFrame *locateNodeinMemory(BM_PageHandle *const page,const PageNumber pageNum,BM_BufferPool *const bm){
		if(bm!=NULL){
		bufferPoolInfo *mgmtInfo=getMgmtInfo(bm);
		pageFrame *temp=mgmtInfo->head;
		pageFrame *exist;
		 if((mgmtInfo->pagesToPageFrame)[pageNum] != NO_PAGE){
			if(((exist = locateNode(bm,page, pageNum)) != NULL)){
			//if page to be read is already in memory then increase the fixed count of page
			//and set the page handle information
			exist->fixedCount+=1;
			page->pageNum = pageNum;
			page->data = exist->data; 
			return exist;
		}else{
			return NULL;
		}
		}else{
			return NULL;
		}
	}else{
		RC_message="Buffer is not initialized ";
		return NULL;
	}
	
	}
	
	//called at the time of page replacement
	void modifyPageHead(BM_BufferPool *const bm,pageFrame *node){
		
		if(bm!=NULL){
					bufferPoolInfo *mgmtInfo=getMgmtInfo(bm);
					pageFrame *h=mgmtInfo->head;
					
					//if page to be replaced is at head of page frame linked list then no replacement
					if(node==mgmtInfo->head){
						return;
					}
					
					
					else{
						//adjust page frames if page to be replaced is last node in the page frames linked list
						if(node==mgmtInfo->lastNode){
							pageFrame *t = mgmtInfo->lastNode->previous;
							mgmtInfo->lastNode = t;
							t->next = NULL;
							h->previous = node;
							node->next = h;
							mgmtInfo->head=node;
							node->previous = NULL;
							mgmtInfo->head->previous=NULL;
							mgmtInfo->head=node;
							mgmtInfo->head->previous=NULL;
							return;
						}else{
							//adjust page frames if page to be replaced, if its in-between node
							node->previous->next = node->next;
							node->next->previous = node->previous;
							h->previous = node;
							node->next = h;
							mgmtInfo->head=node;
							node->previous = NULL;
							mgmtInfo->head->previous=NULL;
							mgmtInfo->head=node;
							mgmtInfo->head->previous=NULL;
							return;
						}
							
				}
			
		}else{
			return;
		}
	}//
	
	
	
	//called at the time of replacement of pages in page frame
	RC updatePage(BM_BufferPool *const bm,BM_PageHandle *const page,pageFrame *node,const PageNumber pageNum){
		bufferPoolInfo *mgmtInfo = getMgmtInfo(bm);
		RC flag;
		SM_FileHandle fileHandle;
		if(bm!=NULL){
			if ((flag = openPageFile ((char *)(bm->pageFile), &fileHandle)) == RC_OK){
			//if page to be replaced is dirty then write it back to disk before replacement 
			if(node->dirtyBit){
				ensureCapacity(pageNum, &fileHandle);
				if((flag = writeBlock(node->pageNumber,&fileHandle, node->data)) == RC_OK){
					(mgmtInfo->numWriteIO)+=1;
			  }else{
				  return flag;
				}		
			}
			//ensuing capacity of file before reading the page from file
			ensureCapacity(pageNum, &fileHandle);
			//reading the pageNum from file to data field.
			if((flag = readBlock(pageNum, &fileHandle, node->data)) == RC_OK){
				(mgmtInfo->pagesToPageFrame)[node->pageNumber] = NO_PAGE;
				node->pageNumber = pageNum;
				(mgmtInfo->pagesToPageFrame)[node->pageNumber] = node->pageFrameNo;
				(mgmtInfo->pageFramesToPage)[node->pageFrameNo] = node->pageNumber;
				node->dirtyBit = false;
				node->fixedCount = 1;
				page->pageNum = pageNum;
				page->data = node->data;
				mgmtInfo->numReadIO+=1;
			}else{
					return flag;
				}
			return RC_OK;
			   }else{
				   return flag;
			  }
		 }else{
			 RC_message="Buffer is not initialized ";
			 return RC_BUFFER_NOT_INITIALIZED;
			}
	}
	
	//calls modify page head
	void update(BM_BufferPool *const bm,pageFrame *node,const PageNumber pageNum){
		int i=0,j=0;
		for(i=0;i<bm->numPages;i++){
			j++;
		}
		modifyPageHead(bm,node);
	}
	
	//calls updatepage function
	RC updatePageFrame(BM_BufferPool *const bm,BM_PageHandle *const page,pageFrame *node,const PageNumber pageNum){
		int i=0,j=0;
		RC flag;
		for(i=0;i<bm->numPages;i++){
			j++;
		}
		flag=updatePage(bm, page, node, pageNum);
		return flag;
	}
		
	//This page replacement algorithm will replace the page from page frame which has read into meory first
	RC fifo (BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum){
	if(bm!=NULL){
				bufferPoolInfo *mgmtInfo=getMgmtInfo(bm);
				RC flag;
				pageFrame *existStatus;
				int j;
				
				//if page to be read is already in memory then return pointer to that frame
				if((existStatus=(locateNodeinMemory(page,pageNum,bm)))!=NULL){
					return RC_OK;
				}
				
				//check if there are any empty frames in memory, if yes then read page to that frame from disk file.
				if((mgmtInfo->totalFrames) < mgmtInfo->maxFrames){
					existStatus = mgmtInfo->head;
			   
					for(j=0;j<mgmtInfo->totalFrames;++j){
						existStatus = existStatus->next;
					}
					//empty frame is found so read page to page frame and increase the total page count in page frames.
					(mgmtInfo->totalFrames)+=1;
					update(bm, existStatus,pageNum);
					
					//reading page to page frame
					flag = updatePageFrame(bm, page, existStatus, pageNum);
						if(flag!=RC_OK){
								return flag;
						}
					return RC_OK;	
				}//
				else{
					//if page frame is not empty then search for a page which has come first with fixed count=0
					existStatus = mgmtInfo->lastNode;
					for(j=0;(existStatus != NULL && existStatus != NULL && existStatus->fixedCount != 0);j++){
						existStatus = existStatus->previous;
					}
					update(bm, existStatus,pageNum);
					//read the page from disk file to the frame whose page is being replaced.
					flag = updatePageFrame(bm, page, existStatus, pageNum);
						if(flag!=RC_OK){
								return flag;
						}
					return RC_OK;	
				}
							
							
			
	}else{
			RC_message="Buffer is not initialized ";
			return RC_BUFFER_NOT_INITIALIZED;
	}
}	
			

//this page replacement strategy will find and replace the page which is least recently used by client
RC lru (BM_BufferPool *const bm, BM_PageHandle *const page,const PageNumber pageNum)
{
	if(bm!=NULL){
			RC flag;
			pageFrame *existStatus;
			bufferPoolInfo *mgmtInfo=getMgmtInfo(bm);
			int j;
			//if page to be read is already in memory then return the pointer to page to client
			if((existStatus = locateNodeinMemory(page,pageNum,bm)) != NULL){
				update(bm, existStatus,pageNum);
				return RC_OK;
			}
			
			//check if there are any empty frames in memory, if yes then read page to that frame from disk file.
			if((mgmtInfo->totalFrames) < mgmtInfo->maxFrames){
					existStatus = mgmtInfo->head;
						for(j=0;j<mgmtInfo->totalFrames;++j){
							existStatus = existStatus->next;
						}
		//empty frame is found so read page to page frame and increase the total page count in page frames.
					mgmtInfo->totalFrames+=1;
					
				//reading page to page frame
					if((flag = updatePageFrame(bm, page, existStatus, pageNum)) == RC_OK){
							update(bm, existStatus,pageNum);
					
					}else{
							return flag;
					}
					return RC_OK;
			}
			else{
				//if no page frame is empty then serach for a page which is least recently used in past with fixed count 0.
				existStatus = mgmtInfo->lastNode;      
				while(existStatus != NULL && existStatus->fixedCount != 0){
					existStatus = existStatus->previous;
				}
				
				//if frame is found with page fixed count 0 then read the page to that page frame.
				//if no frames are found with fixed count 0 then return error
				if (existStatus != NULL){
					if((flag = updatePageFrame(bm, page, existStatus, pageNum)) == RC_OK){
						update(bm, existStatus,pageNum);
					}else{
						return flag;
					}
					return RC_OK;
				}else{
					RC_message="No Frame is availble for page to load ";
					   return RC_NO_MORE_EMPTY_FRAME;
				}
			}
    
	}else{
		RC_message="Buffer is not initialized ";
		return RC_BUFFER_NOT_INITIALIZED;
	}
}

	

	
	
