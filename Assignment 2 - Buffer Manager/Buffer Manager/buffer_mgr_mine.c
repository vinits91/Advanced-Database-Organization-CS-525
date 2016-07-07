#include <stdio.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "dberror.h"
#include <stdlib.h>
#include <string.h>

#define LRUCOUNTERFRAME 100
#define PAGEMAXSIZE 7000
#define FRAMEMAXSIZE 100

typedef struct bufferPoolInfo{
	
		int pagesToPageFrame[PAGEMAXSIZE];
		int pageFramesToPage[FRAMEMAXSIZE];
		bool pageFrameDirtyBit[PAGEMAXSIZE];
		int pageFrameFixedCount[PAGEMAXSIZE];
		int numReadIO;
		int numWriteIO;
		int totalFrames;
		int maxFrames;
		SM_FileHandle fileHandle;
		struct pageFrame *head;
		struct pageFrame *tail;
		int lruCounter4PageFrame[LRUCOUNTERFRAME];
		struct pageFrame *lastNode;
	}bufferPoolInfo;
	
	typedef struct pageFrame{
		bool dirtyBit;
		int fixedCount;
		int pinUnpin; //false- unpinned, true- pinned
		int pageNumber;
		int pageFrameNo;
		int filled;
		char *data;
		struct pageFrame *next;
		struct pageFrame *previous;
	}pageFrame;


	
	pageFrame *getNewNode(){
		pageFrame *linkNode = malloc(sizeof(pageFrame));
		linkNode->dirtyBit=false;
		linkNode->pinUnpin=0;
		linkNode->pageNumber=NO_PAGE;
		linkNode->pageFrameNo=0;
		linkNode->fixedCount=0;
		linkNode->filled=0;
		linkNode->next=NULL;
		linkNode->previous=NULL;
		linkNode->data=(char *)calloc(PAGE_SIZE,sizeof(SM_FileHandle));;
		return linkNode;
		
	}


	RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, 
		  const int numPages, ReplacementStrategy strategy, 
		  void *stratData){
		SM_FileHandle file_Handle;
		int i;
		if (openPageFile ((char *)pageFileName, &file_Handle) != RC_OK){
        return RC_FILE_NOT_FOUND;
		}
	
		
		bufferPoolInfo *bufferPool=malloc(sizeof(bufferPoolInfo));
		bufferPool->numReadIO=0;
		bufferPool->numWriteIO=0; 
		bm->numPages=numPages;
		bm->pageFile=pageFileName;
		bm->strategy=strategy;
		
		memset(bufferPool->lruCounter4PageFrame,NO_PAGE,LRUCOUNTERFRAME*sizeof(int));
		memset(bufferPool->pagesToPageFrame,NO_PAGE,PAGEMAXSIZE*sizeof(int));
		memset(bufferPool->pageFramesToPage,NO_PAGE,FRAMEMAXSIZE*sizeof(int));
		memset(bufferPool->pageFrameDirtyBit,NO_PAGE,PAGEMAXSIZE*sizeof(bool));
		memset(bufferPool->pageFrameFixedCount,NO_PAGE,PAGEMAXSIZE*sizeof(int));
		
		bufferPool->head=bufferPool->tail=getNewNode();
		bufferPool->head->pageFrameNo=0;
		bufferPool->maxFrames=numPages;
		bufferPool->totalFrames=1;
		for(i=0;i<numPages-1;i++){
			printf("\ncreating node print");
			bufferPool->tail->next=getNewNode();
			bufferPool->tail->next->previous=bufferPool->tail;
			bufferPool->tail->next->pageFrameNo=i+1;
			bufferPool->tail=bufferPool->tail->next;
			bufferPool->totalFrames++;
		}
		bufferPool->lastNode=bufferPool->tail;
		printf("\nNumber of frames created %d\n",bufferPool->totalFrames);
		bufferPool->fileHandle=file_Handle;
		bm->mgmtData=bufferPool;
		return RC_OK;
	}
	
	bufferPoolInfo *getMgmtInfo(BM_BufferPool *const bm){
		if(bm!=NULL){
		bufferPoolInfo *mgmtInfo=(bufferPoolInfo*)bm->mgmtData;
		return mgmtInfo;
		}
	}
	

	int *getFixCounts (BM_BufferPool *const bm){
		if(bm!=NULL){
			bufferPoolInfo *mgmtInfo=getMgmtInfo(bm);
			pageFrame *temp=mgmtInfo->head;
			while(temp!=NULL){
				(mgmtInfo->pageFrameFixedCount)[temp->pageFrameNo]=temp->fixedCount;
				temp=temp->next;
			}
			return mgmtInfo->pageFrameFixedCount;
		}
	}
	
	int getNumReadIO (BM_BufferPool *const bm){
		if(bm!=NULL){
			bufferPoolInfo *mgmtInfo=getMgmtInfo(bm);
			return mgmtInfo->numReadIO;
		}
	}

	int getNumWriteIO (BM_BufferPool *const bm){
		if(bm!=NULL){
		bufferPoolInfo *mgmtInfo=getMgmtInfo(bm);
		return mgmtInfo->numWriteIO;
		}
	}
	
	bool *getDirtyFlags (BM_BufferPool *const bm){
		if(bm!=NULL){
		bufferPoolInfo *mgmtInfo=getMgmtInfo(bm);
		pageFrame *temp=mgmtInfo->head;
			while(temp!=NULL){
				(mgmtInfo->pageFrameDirtyBit)[temp->pageFrameNo]=temp->dirtyBit;
				temp=temp->next;
			}
		return mgmtInfo->pageFrameDirtyBit;
		}
		
		
	}
	
	RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page){
		if(bm!=NULL){
			bufferPoolInfo *mgmtInfo=getMgmtInfo(bm);
			pageFrame *temp=mgmtInfo->head;
			while(temp!=NULL){
					temp->dirtyBit=true;
				
				temp=temp->next;
			}
			
		}else{
			return RC_BUFFER_NOT_INITIALIZED;
		}
	}
	
	RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page){
		if(bm!=NULL){
			bufferPoolInfo *mgmtInfo=getMgmtInfo(bm);
			pageFrame *temp=mgmtInfo->head;
			while(temp!=NULL){
				if(temp->pageNumber==page->pageNum && temp->fixedCount>0 && temp->pinUnpin==1){
					temp->pinUnpin=0;
					temp->fixedCount--;
					printf("\nunpinning page %d\n",temp->pageNumber);
					break;
				}
				temp=temp->next;
			}
			return RC_OK;
		}else{
			return RC_BUFFER_NOT_INITIALIZED;
		}
	}
	
	

	
	RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum){
		
			if(pageNum<0){
				return RC_READ_NON_EXISTING_PAGE;
			}
		//printf("Strategyy----------------------            %d",bm->strategy);
		switch (bm->strategy)
		{
        case RS_FIFO:
            fifo(bm,page,pageNum);
            break;
        case RS_LRU:
            LRU_Algo(bm,page,pageNum);
            break;
		}
			
		}
		
	
	RC shutdownBufferPool(BM_BufferPool *const bm){
		forceFlushPool(bm);
		if(bm!=NULL){
			bufferPoolInfo *mgmtInfo=getMgmtInfo(bm);
			pageFrame *temp=mgmtInfo->head;
			while(temp!=NULL){
				temp=temp->next;
				free(mgmtInfo->head->data);
				free(mgmtInfo->head);
				mgmtInfo->head=temp;
			}
			mgmtInfo->head=mgmtInfo->tail=NULL;
			free(mgmtInfo);

		return RC_OK;
		}else{
			return RC_BUFFER_NOT_INITIALIZED;
		}
	}

	
	RC forceFlushPool(BM_BufferPool *const bm){
		if(bm!=NULL){
			bufferPoolInfo *mgmtInfo=getMgmtInfo(bm);
			pageFrame *temp=mgmtInfo->head;
			
			while(temp!=NULL){
				if(temp->dirtyBit){
					if(writeBlock(temp->pageNumber,&(mgmtInfo->fileHandle),temp->data)==RC_OK){
						printf("Writing to the file\n");
						temp->dirtyBit=false;
						temp->filled=0;
						printf("\nwriting operation Numbers %d\n",temp->pageNumber);
						mgmtInfo->numWriteIO+=1;
						mgmtInfo->pageFrameDirtyBit[(temp->pageNumber)-1]=false;
					}
				}
				temp=temp->next;
			}
			free(temp);
			//closePageFile(&(mgmtInfo->fileHandle));
			return RC_OK;
		}else{
			return RC_BUFFER_NOT_INITIALIZED;
		}
		
	}
	
	RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page){
		if(bm!=NULL){
			bufferPoolInfo *mgmtInfo=getMgmtInfo(bm);
			pageFrame *temp=mgmtInfo->head;
			while(temp!=NULL){
				if(temp->pageNumber==page->pageNum && temp->dirtyBit){
					ensureCapacity((temp->pageNumber)+1,&(mgmtInfo->fileHandle));
					if(writeBlock(temp->pageNumber,&(mgmtInfo->fileHandle),page->data)==RC_OK){
						temp->dirtyBit=false;
						mgmtInfo->numWriteIO+=1;
						mgmtInfo->pageFrameDirtyBit[(temp->pageNumber)-1]=false;
					}
				}
				temp=temp->next;
			}
			return RC_OK;
		}else{
			return RC_BUFFER_NOT_INITIALIZED;
		}
	}
	
	PageNumber *getFrameContents (BM_BufferPool *const bm){
		bufferPoolInfo *mgmtInfo=getMgmtInfo(bm);
		return mgmtInfo->pageFramesToPage;
	}
	
	pageFrame *locateNode(BM_BufferPool *const bm,const PageNumber pageNum ){
		bufferPoolInfo *mgmtInfo=getMgmtInfo(bm);
		pageFrame *temp=mgmtInfo->head;
		pageFrame *t=mgmtInfo->head;
		
		while(t!=NULL){
			printf("\n page number in frames %d\n",t->pageNumber);
			t=t->next;
		}
		printf("\npage to be found   %d\n",pageNum);
		

	
		while(temp!=NULL){
			if(temp->pageNumber==pageNum){
				printf("\npage not in head %d ->",temp->pageNumber);
				return temp;
			}
		temp=temp->next;
		}
	printf("Returning NULL");
	return NULL;
	
}
	pageFrame *locateNodeinMemory(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum){
		bufferPoolInfo *mgmtInfo=getMgmtInfo(bm);
		pageFrame *temp=mgmtInfo->head;
		pageFrame *exist;
			if(((exist = locateNode(bm, pageNum)) != NULL)){
			return exist;
			}
		
		return NULL;
	}
	
	
	pageFrame *findEmptyFrame(BM_BufferPool *const bm){
		bufferPoolInfo *info=getMgmtInfo(bm);
		pageFrame *temp=info->head;
		while(temp!=NULL){
			if(temp->pageNumber==NO_PAGE){
				printf("\n find empty print 3 times \n");
				return temp;
			}
			temp=temp->next;
		}
		return NULL;
		
	}
	
			
	void updatePageHead(BM_BufferPool *const bm,BM_PageHandle *const page,pageFrame *node,const PageNumber pageNum){
			bufferPoolInfo *mgmtInfo=getMgmtInfo(bm);
			mgmtInfo->head->pageNumber=pageNum;
			mgmtInfo->head->fixedCount+=1;
			mgmtInfo->head->filled=1;
			mgmtInfo->head->pinUnpin=1;
			page->pageNum=pageNum;
			printf("\ncreading page %d\n",pageNum);
			ensureCapacity(pageNum,&(mgmtInfo->fileHandle));
			printf("\npageeeeeeeeeee dataaaaaaaaaaa %s\n",page->data);		
			readBlock(pageNum,&(mgmtInfo->fileHandle),page->data);
			printf("\npageeeeeeeeeee dataaaaaaaaaaa %s\n",node->data);
			(mgmtInfo->pagesToPageFrame)[mgmtInfo->head->pageNumber] = node->pageFrameNo;
			(mgmtInfo->pageFramesToPage)[mgmtInfo->head->pageFrameNo] = mgmtInfo->head->pageNumber;
	}
	
	pageFrame *getEmptyFrame(BM_BufferPool *const bm){
		bufferPoolInfo *mgmtInfo=getMgmtInfo(bm);
		pageFrame *temp=mgmtInfo->head;
		while(temp!=NULL){
					if(temp->pageNumber==NO_PAGE){
						return temp;
					}
					temp=temp->next;
				}
		return NULL;
		
	}
	
	pageFrame *getPageWithFixedCountZero(BM_BufferPool *const bm){
		bufferPoolInfo *mgmtInfo=getMgmtInfo(bm);
		pageFrame *test=mgmtInfo->head;
		while(test!=NULL){
				printf("\nFixed Count is %d\n",test->fixedCount);
				if(test->fixedCount==0){
					printf("\nyea it is\n");
					return test;
				}
				test=test->next;
			}
		
		return NULL;
	}
	
	void updatePage(BM_BufferPool *const bm,BM_PageHandle *const page,pageFrame *node,const PageNumber pageNum){
		printf("\nwohooooooooooooooooooo\n");
		bufferPoolInfo *mgmtInfo=getMgmtInfo(bm);
				node->fixedCount+=1;
				node->pageNumber=pageNum;
				node->filled=1;
				page->pageNum=pageNum;
				node->pinUnpin=1;
				ensureCapacity(pageNum,&(mgmtInfo->fileHandle));
				readBlock(pageNum,&(mgmtInfo->fileHandle),page->data);
				(mgmtInfo->pagesToPageFrame)[node->pageNumber] = node->pageFrameNo;
				(mgmtInfo->pageFramesToPage)[node->pageFrameNo] = node->pageNumber;
	}
	
	
	RC fifo (BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum){
		bufferPoolInfo *mgmtInfo=getMgmtInfo(bm);
		pageFrame *temp;
		pageFrame *newNode;
		pageFrame *test=mgmtInfo->head;
		printf("head page number %d",mgmtInfo->head->pageNumber);
		pageFrame *existStatus,*emptyFrame,*lastNode=mgmtInfo->head,*tes;
		pageFrame *temp1=NULL;
		if((existStatus=(locateNodeinMemory(bm,page,pageNum)))!=NULL){
			printf("printiingggg");
			page->pageNum=pageNum;
			existStatus->pinUnpin=1;
			return RC_OK;
		}
	
		if(((emptyFrame=findEmptyFrame(bm))!=NULL)){
			if(mgmtInfo->head->pageNumber==NO_PAGE){
				printf("\ndisplayed once\n");
				updatePageHead(bm,page,emptyFrame,pageNum);
			}else{
				temp=getEmptyFrame(bm);
				if(temp!=NULL){
					updatePage(bm,page,emptyFrame,pageNum);
				}	
			}
			
		}else{
			printf("\nNO empty frame found\n");
			test=getPageWithFixedCountZero(bm);
			
			if(test!=NULL){
				printf("test data ------------------ %d",test->pageNumber);
				if(test==mgmtInfo->head){
				printf("coming 1\n");
				if(mgmtInfo->head->dirtyBit){
					printf("\ngaaaaaaaaaaaaaaaayaaaaaaaaaaa\n");
					ensureCapacity(mgmtInfo->head->pageNumber,&(mgmtInfo->fileHandle));
					writeBlock(mgmtInfo->head->pageNumber,&(mgmtInfo->fileHandle),mgmtInfo->head->data);
				}
				mgmtInfo->head=mgmtInfo->head->next;
				printf("coming 2\n");
				mgmtInfo->lastNode->next=test;
				printf("coming 3\n");
				test->previous=lastNode;
				printf("coming 4\n");
				test->next=NULL;
				mgmtInfo->lastNode=test;	
				updatePage(bm,page,test,pageNum);
				
				}else{
					if(test->dirtyBit){
					printf("\ngaaaaaaaaaaaaaaaayaaaaaaaaaaa\n");
					ensureCapacity(test->pageNumber,&(mgmtInfo->fileHandle));
					writeBlock(mgmtInfo->head->pageNumber,&(mgmtInfo->fileHandle),mgmtInfo->head->data);
				}
					test->previous->next=test->next;
					test->next->previous=test->previous;
					mgmtInfo->lastNode->next=test;
					mgmtInfo->lastNode->next->previous=lastNode;
					test->next=NULL;
					mgmtInfo->lastNode=test;		
					updatePage(bm,page,emptyFrame,pageNum);
				}
				
				
				
				
			}
		}
			tes=mgmtInfo->head;
			while(tes!=NULL){
				printf("\nafter adding page %d\n",tes->pageNumber);
				tes=tes->next;
			}
			
		return RC_OK;	
	}	
			

			
		
		
		/*else{
			newNode=getNewNode();
			while(temp!=NULL){
			if(temp->fixedCount==0){
				newNode->pageFrameNo=temp->pageFrameNo;
				newNode->pageNumber=pageNum;
				newNode->filled=true;
				mgmtInfo->numReadIO+=1;
				page->pageNum=(pageNum-1);
				(mgmtInfo->pagesToPageFrame)[newNode->pageNumber] = newNode->pageFrameNo;
				(mgmtInfo->pageFramesToPage)[newNode->pageFrameNo] = newNode->pageNumber;
				readBlock(pageNum,&(mgmtInfo->fileHandle),newNode->data);
				
				
				newNode->fixedCount+=1;
				temp1->next=newNode;
				newNode->next=temp->next;
				temp->next->previous=newNode;
				newNode->previous=temp1;
				temp->next=NULL;
				temp->previous=NULL;
				printf("\n new node \n");
				return RC_OK;
			}
			temp1=temp;
			temp=temp->next;
		}
		//free(emptyFrame);
		//free(temp1);
		//free(temp);
		}*/



	pageFrame *locateNodeLRU(BM_BufferPool *const bm, PageNumber pageNum) {
		bufferPoolInfo *bpi = getMgmtInfo(bm);
		pageFrame *pF = bpi->head;
		while(pF != NULL) {
				if(pF->pageNumber == pageNum) {
					return pF;
				}		
				pF = pF->next;
			}
			return NULL;
	}
	
	
	RC AddNewPage(BM_BufferPool *const bm, BM_PageHandle *const page, 
				int fn, int const pageNum) {
		bufferPoolInfo *bpi5 = getMgmtInfo(bm);
		pageFrame *pF5 = bpi5->head;
		int i;
		for(i=0;i<(bpi5->maxFrames);i++){
			if(pF5->pageFrameNo==fn){
				readBlock(pageNum,&(bpi5->fileHandle),pF5->data);
				bpi5->numReadIO+=1;
				break;
			}
			pF5 = pF5->next;
		}
		return RC_OK;
	}
	
	RC DetermineIfFrameIsEmptyOrNot(BM_BufferPool *const bm, BM_PageHandle *const page, 
		const PageNumber pageNum) {
		bufferPoolInfo *bpi4 = getMgmtInfo(bm);
		pageFrame *pF4= bpi4->head;
		int FLAG = 0,i;
		int FrameNumber = -1;
		for(i=0;(i<bpi4->maxFrames); i++) {
			if(pF4->pageNumber == NO_PAGE) {
				FLAG =1;
				FrameNumber = (int)pF4->pageFrameNo;
				pF4->pageNumber =pageNum;
				break;
			}
			pF4 = pF4->next;
		}
		if(FLAG == 1) {
			
			AddNewPage(bm, page, FrameNumber,pageNum);
			printf("printtt");
			IncreaseLRU_Counter(bm, pageNum);	
		}
		
		else {
			printf("\n\n\nits time to evict\n\n");
			FrameNumber = EvictPageWithLRU(bm);
			printf("Frame number --- %d\n",FrameNumber);
			AddNewPage(bm,page,FrameNumber, pageNum);
			//IncreaseLRU_Counter(bm, pageNum);
		}
		return RC_OK;
	}
	int EvictPageWithLRU(BM_BufferPool *const bm, int fn) {
		int FrameNumber, i;
		bufferPoolInfo *bpi6 = getMgmtInfo(bm);
		pageFrame *pF6 = bpi6->head;
		for(i=0;((i<bpi6->maxFrames)) && (pF6->pageNumber!=NO_PAGE); i++) {
			if (bpi6->lruCounter4PageFrame[i] < bpi6->lruCounter4PageFrame[i+1]) {
					printf("Hellooooooo");
					FrameNumber = i;
					break;
				}	
		}
		printf("Evict page :--- %d",FrameNumber);
		return FrameNumber;
	}
	
	
	RC IncreaseLRU_Counter(BM_BufferPool *const bm, BM_PageHandle *const page,
	    const PageNumber pageNum) {
		int i,j;
		bufferPoolInfo *bpi3 = getMgmtInfo(bm);
		pageFrame *pF3 = bpi3->head;
		int max=0;
		
		for(i=0; (i<bpi3->maxFrames && (pF3->pageNumber!=NO_PAGE)); i++) {
			while(pF3!=NULL){
			
			if(max<bpi3->lruCounter4PageFrame[i]){
				printf("Maxxxxxxxxxxxxxxxxx");
				max=bpi3->lruCounter4PageFrame[i];
			}
		if(pF3->pageNumber==pageNum){
		break;
		
		}
		pF3=pF3->next;
	}
			/*if(pF3->pageNumber == pageNum) {
				for(j=0; (j<bpi3->maxFrames)&&(pF3->pageNumber!=NO_PAGE); j++) {
					if(bpi3->lruCounter4PageFrame[j] < bpi3->lruCounter4PageFrame[j+1]) {
						printf("Heelllllllllllllllllllllllllllllllllllll");
						bpi3->lruCounter4PageFrame[i] = bpi3->lruCounter4PageFrame[j]+1;
						pF3->fixedCount+=1;
					}
				// break;
				}
			}*/
		
		}
		
		max=max+1;
		bpi3->lruCounter4PageFrame[pF3->pageFrameNo]=max;
		pF3->fixedCount+=1;
		return RC_OK;
	}
	
	
	pageFrame *pagesExistsInPageFrame(BM_BufferPool *const bm, BM_PageHandle *const page, 
	    const PageNumber pageNum) {
			
		bufferPoolInfo *bpi = getMgmtInfo(bm);
		pageFrame *nodefind = bpi->head;
		
		if((nodefind = locateNodeLRU(bm, pageNum)) == NULL) {
			return NULL;
		}
		
		page->pageNum = (pageNum-1);
		//page->data = nodefind->data;
		
		//nodefind->lruRf4Node = 1;
		nodefind->fixedCount +=1;
		nodefind->pinUnpin=1;
		IncreaseLRU_Counter(bm, page, pageNum);
		
		return nodefind;
	}

	RC LRU_Algo(BM_BufferPool *const bm, BM_PageHandle *const page, 
	    const PageNumber pageNum) {
		
		bufferPoolInfo *bpi = getMgmtInfo(bm);
		pageFrame *nodefind = bpi->head;

			if((nodefind = pagesExistsInPageFrame(bm, page, pageNum)) != NULL) {
				printf("calllinggg 111");
				IncreaseLRU_Counter(bm, page, pageNum);
			}
			else {
				printf("calllinggg 222");
				DetermineIfFrameIsEmptyOrNot(bm, page, pageNum);
				
			}				
				return RC_OK;	
	}
	
	

	
	
