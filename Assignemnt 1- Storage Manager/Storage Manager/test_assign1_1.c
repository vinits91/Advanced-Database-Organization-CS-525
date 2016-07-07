#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "storage_mgr.h"
#include "dberror.h"
#include "test_helper.h"

// test name
char *testName;

/* test output files */
#define TESTPF "test_pagefile.bin"

/* prototypes for test functions */
static void testCreateOpenClose(void);
static void testSinglePageContent(void);
static void testReadWriteAppendBlocks(void);

/* main function running all tests */
int
main (void)
{
  testName = "";
  
  //initStorageManager();

  testCreateOpenClose();
  testSinglePageContent();
  testReadWriteAppendBlocks();



  return 0;
}


/* check a return code. If it is not RC_OK then output a message, error description, and exit */
/* Try to create, open, and close a page file */
void
testCreateOpenClose(void)
{
  SM_FileHandle fh;

  testName = "test create open and close methods";

  TEST_CHECK(createPageFile (TESTPF));
  
  TEST_CHECK(openPageFile (TESTPF, &fh));
  ASSERT_TRUE(strcmp(fh.fileName, TESTPF) == 0, "filename correct");
  ASSERT_TRUE((fh.totalNumPages == 1), "expect 1 page in new file");
  ASSERT_TRUE((fh.curPagePos == 0), "freshly opened file's page position should be 0");

  TEST_CHECK(closePageFile (&fh));
  TEST_CHECK(destroyPageFile (TESTPF));

  // after destruction trying to open the file should cause an error
  ASSERT_TRUE((openPageFile(TESTPF, &fh) != RC_OK), "opening non-existing file should return an error.");

  TEST_DONE();
}

/* Try to create, open, and close a page file */
void
testSinglePageContent(void)
{
  SM_FileHandle fh;
  SM_PageHandle ph;
  int i;

  testName = "test single page content";

  ph = (SM_PageHandle) malloc(PAGE_SIZE);

  // create a new page file
  TEST_CHECK(createPageFile (TESTPF));
  TEST_CHECK(openPageFile (TESTPF, &fh));
  printf("created and opened file\n");
  
  // read first page into handle
  TEST_CHECK(readFirstBlock (&fh, ph));
  // the page should be empty (zero bytes)
  
 
  
  for (i=0; i < PAGE_SIZE; i++)
   ASSERT_TRUE((ph[i] == 0), "expected zero byte in first page of freshly initialized page");
  printf("first block was empty\n");
    
  // change ph to be a string and write that one to disk
  for (i=0; i < PAGE_SIZE; i++)
    ph[i] = (i % 10) + '0';
  TEST_CHECK(writeBlock (0, &fh, ph));
  printf("writing first block\n");

  // read back the page containing the string and check that it is correct
  TEST_CHECK(readFirstBlock (&fh, ph));
  for (i=0; i < PAGE_SIZE; i++)
    ASSERT_TRUE((ph[i] == (i % 10) + '0'), "character in page read from disk is the one we expected.");
  printf("reading first block\n");

  // destroy new page file
  TEST_CHECK(destroyPageFile (TESTPF));  
  
  TEST_DONE();
}

//Function includes all test cases to check for reading and writing and appending of block's on different conditions.
void testReadWriteAppendBlocks(void){

  SM_FileHandle fh;
  SM_PageHandle ph;
  int i;

  testName = "test read, Write, Append block functionality.";

  ph = (SM_PageHandle) malloc(PAGE_SIZE);
  
  // create a new page file
  TEST_CHECK(createPageFile (TESTPF));
  TEST_CHECK(openPageFile (TESTPF, &fh));
  printf("Create and Open file operation Finished.");
  
  // change ph to be a string and write that one to disk
  for (i=0; i < PAGE_SIZE; i++)
    ph[i] = (i % 10) + '0';
  TEST_CHECK(writeCurrentBlock (&fh, ph));
  printf("\n writing current block Finished \n");

  // read back the page containing the string and check that it is correct
  TEST_CHECK(readCurrentBlock (&fh, ph));
  for (i=0; i < PAGE_SIZE; i++){
  ASSERT_TRUE((ph[i] == (i % 10) + '0'), "character in page read from disk is the one we expected.");
  }
  printf("reading current block finished\n");
  
  
  
  //Appending to file
  ASSERT_TRUE((appendEmptyBlock (&fh)==RC_OK),"File append operation successful");
  printf("file append operation finished.\n");
  
  //Reading last block
  ASSERT_TRUE((readLastBlock (&fh,ph)==RC_OK),"Reading Last Block operation successful");
  printf("read last block operation finished.\n");
 
  //Read Previous Block
  ASSERT_TRUE((readPreviousBlock (&fh,ph)==RC_OK),"Reading previous Block operation successful");
  printf("read previous block operation finished.\n");
  
  printf("Again reading previous block\n");
  ASSERT_TRUE((readPreviousBlock(&fh,ph) != RC_OK), "Reading a previous block should return an error");
  
   //reading next block
  ASSERT_TRUE((readNextBlock (&fh,ph)==RC_OK),"Reading next block in file is done without error");
  printf("Reading Next block operation finished.\n");
  
  printf("Again reading Next block\n");
  ASSERT_TRUE((readNextBlock(&fh,ph) != RC_OK), "Reading a Next block should return an error");
  
  //ensuring capacity of file whether it has 5 pages  or not, if not then it will create pages in file upto 5
  ASSERT_TRUE((ensureCapacity (5, &fh) == RC_OK), "Ensuring the capacity of file, File has desired number of pages");
  
  // destroy new page file
  ASSERT_TRUE((destroyPageFile (TESTPF)==RC_OK),"File will get deleted");  
  
  TEST_DONE();
    
}


