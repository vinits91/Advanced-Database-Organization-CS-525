Assignment 1- Storage Manager 

Shruti Naik (Leader)-  A20352355
Sumit Rana- A20350795
Vinit Shah- A20350453

Storage Manager is responsible for reading of Fixed Size memory page block to be written in file, and writing of fixed size page block read from file to be written in memory, apart from that it also manages file related information in FileHandle for better processing, also it supports various operations such as create/open/close/read/write/append.

To execute program follow following steps:-

1)Copy all the files to one folder
2)In Unix Terminal navigate to folder where files are stored.
3)Execute "make" command
4)Execute "./test_assign1_1" command


Breif descriptions about implementation:-

Make file:-

	File creates an executable file of test_assign1_1.c, which will be used to execute the storage manager. It creates .exe file of it by compiling and linking all files which are required to execute the storage manager. Plus it contains code for cleaning of all .exe and .gch.h files to remove previously existed file.

test_assign1_1.c

	This file is responsible for testing of storage manager, it runs various test cases to carry out testing on different operation of storage manager. File makes use of header files like storage_mgr.h and dberror.h, also it uses test_helper.h which assist it in testing operations.
It executes following testcases:-

1) testCreateOpenClose():- It tests the functionality related to Creating, Opening and Closing of file.
when it opens the file, it writes all the file related data such as fileName,totalNumPages,curPagePos to file handle, which will be used by all functions of storage manager.
When file is created for 1st time, it stores '\0' to 4096 bytes which makes up one page size.
when file is closed it releases all consumed resources such as memory and closing of file.
If any errors or file/page is not exist to read or write, it will generate an error which are initialized in dberror.h

2) testSinglePageContent():-
	It test the functionality related to reading of first page block from file and write to memory and Writing of page block to file whose data read from memory.After writing to file it again reads the page block from file, to check whether data was really written by previous writing operation. After all operations it also destroy the created page file.

3) testReadAppendBlocks():-
	This function tests operation related to file reading and writing it to memory which is pointed by page handle.
It reads the current page from file indicated by curPagePos and write it memory which is indicated by page handle.
It also appends an empty page block of size 4096bytes to file.
  

 





