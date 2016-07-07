
#creates an EXE file of test_assign3_1, which is linked with other headers and C files, so 
#make command will then compile and link all the required files which are used by test_assign1_1.exe
#further more it also cleans all pre-existing files.


all:	test_assign3_1.exe
test_assign3_1.exe: test_assign3_1.c
	gcc dberror.c storage_mgr.c test_assign3_1.c -o test_assign3_1 buffer_mgr.c dt.h buffer_mgr_stat.c record_mgr.c tables.h expr.c expr.h test_helper.h rm_serializer.c -lm
expr: test_expr.o rm_serializer.o record_mgr.o buffer_mgr.o expr.o  storage_mgr.o buffer_mgr_stat.o  dberror.o
	gcc -o expr record_mgr.o test_expr.o expr.o buffer_mgr_stat.o rm_serializer.o buffer_mgr.o  storage_mgr.o dberror.o -I. -lm
clean:
	rm	test_assign3_1.exe expr.exe storage_mgr.h.gch test_assign3_1.o dberror.h.gch test_helper.h.gch buffer_mgr.h.gch,record_mgr.h.gch
