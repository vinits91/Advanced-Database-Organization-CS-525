
#creates an EXE file of test_assign1_1, which is linked with other headers and C files, so 
#make command will then compile and link all the required files which are used by test_assign1_1.exe
#further more it also cleans all pre-existing files.


all:	test_assign2_1.exe
test_assign2_1.exe: test_assign2_1.c
	gcc dberror.c storage_mgr.c test_assign2_1.c -o test_assign2_1 buffer_mgr.c dt.h buffer_mgr_stat.c
clean:
	rm	test_assign2_1.exe storage_mgr.h.gch test_assign2_1.o dberror.h.gch test_helper.h.gch buffer_mgr.h.gch
