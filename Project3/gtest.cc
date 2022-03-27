#include "gtest.h"
#include "BigQ.h"
#include <pthread.h>
#include "DBFile.h"
#include "limits.h"
#include <fstream>
#include <chrono>
#include <thread>
#include <gtest/gtest.h>
int gtest1 ();
int gtest2 ();
int gtest3 ();


 

//Test that it can sort on `colnames`
int gtest1 (int table, int runlength, char* colname) {
    relation *rel_ptr[] = {n, r, c, p, ps, s, o, li};
    rel = rel_ptr[table];
    OrderMaker om;

    rel->get_sort_order(om, colname);

    DBFile::sortutil startup = {runlength, &om};
	DBFile dbfile;
    dbfile.Create (rel->path(), sorted, &startup);
    dbfile.Close();
    dbfile.Open(rel->path());
	char tbl_path[100];
	sprintf (tbl_path, "%s%s.tbl", tpch_dir, rel->name());
    dbfile.Load (*(rel->schema ()), tbl_path);
    dbfile.Close();

    return 1;

}

//Test that the sort order is correct
int gtest2(char* colname) {
    ComparisonEngine ceng;
    OrderMaker om;
    rel->get_sort_order(om, colname);
	DBFile dbfile;
	dbfile.Open (rel->path());
	dbfile.MoveFirst ();
	Record temp;
    Record old;
    int err = 0;
    // dbfile.GetNext(old);
    int i = 0;
    Record rec[2];
	Record *last = NULL, *prev = NULL;
	while (dbfile.GetNext (rec[i%2])) {
		prev = last;
		last = &rec[i%2];

		if (NULL != prev && NULL != last) {
			if (ceng.Compare (prev, last, &om) == 1) {
				err++;
			}
        }
		i++; 
	}

	// while (dbfile.GetNext (temp)) {
	// 	if (ceng.Compare(&old, &temp, &om)==1) {
    //         return 0;
    //     }
        

	// 	old.Consume(&temp);
	// }
	
	dbfile.Close ();
    return err;
}
//Tests the CNF from output.log in project 1
int gtest3(relation *rel, char* input_string) {
    CNF cnf; 
	Record literal;
	rel->get_cnf (cnf, literal, input_string);

    	DBFile dbfile;
	dbfile.Open (rel->path());
	dbfile.MoveFirst ();

	Record temp;

	int counter = 0;
	while (dbfile.GetNext (temp, cnf, literal) == 1) {
		counter += 1;
	}
	dbfile.Close ();
	return counter;
}
                      // 0  1  2  3  4   5  6   7
//relation *rel_ptr[] = {n, r, c, p, ps, s, o, li};
// TEST(SortStrings, P_Name) {
//     char* col = "(p_name)";
//     EXPECT_EQ(1, gtest1(3,4, col));
//     EXPECT_EQ(0, gtest2(col));
// }
// TEST(SortStrings, P_Mfgr) {
    
//     char* col = "(p_mfgr)";
//     EXPECT_EQ(1, gtest1(3,4, col));
//     EXPECT_EQ(0, gtest2(col));

// }
// TEST(SortStrings, P_Brand) {
//     char* col = "(p_brand)";
//     EXPECT_EQ(1, gtest1(3,7, col));
//     EXPECT_EQ(0, gtest2(col));
// }
// TEST(SortStrings, P_Type) {
//     char* col = "(p_type)";
//     EXPECT_EQ(1, gtest1(3,4, col));
//     EXPECT_EQ(0, gtest2(col));
// }
// TEST(SortStrings, P_Container) {
//     char* col = "(p_container)";
//     EXPECT_EQ(1, gtest1(3,4, col));
//     EXPECT_EQ(0, gtest2(col));
// }

TEST(SortOnOne, Region_RegionKey){
    char* col = "(r_name)";
    EXPECT_EQ(1, gtest1(1,2, col));
    EXPECT_EQ(0, gtest2(col));
}
TEST(SortOnOne, Nation_RegionKey){
    // rel=rel_ptr[0];
    char* col = "(n_regionkey)";
    EXPECT_EQ(1, gtest1(0,2, col));
    EXPECT_EQ(0, gtest2(col));
}
TEST(SortOnOne, Customer_AcctBal){
    // rel=rel_ptr[2];
    char* col = "(c_acctbal)";
    EXPECT_EQ(1, gtest1(2,8, col));
    EXPECT_EQ(0, gtest2(col));
}


TEST(SortOnTwo, Part_Price_AND_Size){
    // rel=rel_ptr[3];
    char* col = "(p_retailprice) AND (p_size)";
    EXPECT_EQ(1, gtest1(3,9, col));
    EXPECT_EQ(0, gtest2(col));
}

TEST(SortOnTwo, Order_OPriority_AND_Status){
    // rel=rel_ptr[6];
    char* col = "(o_orderpriority) AND (o_orderstatus)";
    EXPECT_EQ(1, gtest1(6,7, col));
    EXPECT_EQ(0, gtest2(col));
}

TEST(SortOnTwo, Supplier_Nation_AND_phone){
    // rel=rel_ptr[5];
    char* col = "(s_nationkey) AND (s_phone)";
    EXPECT_EQ(1, gtest1(5,15, col));
    EXPECT_EQ(0, gtest2(col));
}

TEST(SortOnThree, PartSupp_){
    // rel=rel_ptr[4];
    char* col = "(ps_partkey) AND (ps_availqty) AND (ps_supplycost)";
    EXPECT_EQ(1, gtest1(4,11, col));
    EXPECT_EQ(0, gtest2(col));
}
TEST(ScanAndFilter, LoadsAllDatabase) {
    EXPECT_EQ(5, gtest3(r, ((char*) "(r_regionkey>-1)")));
    EXPECT_EQ(25, gtest3(n, ((char*) "(n_regionkey>-1)")));
    EXPECT_EQ(10000, gtest3(s, ((char*) "(s_suppkey>-1)")));
    EXPECT_EQ(800000, gtest3(ps, ((char*) "(ps_suppkey>-1)")));
    EXPECT_EQ(200000, gtest3(p, ((char*) "(p_partkey>-1)")));

}

TEST(ScanAndFilter, TestQueriesResultInCorrectSize) {
	
    EXPECT_EQ(1, gtest3(r, ((char*) "(r_name = 'EUROPE')")));
	EXPECT_EQ(2, gtest3(r, ((char*) "(r_name < 'MIDDLE EAST') AND (r_regionkey > 1)")));
	EXPECT_EQ(3, gtest3(n, ((char*) "(n_regionkey = 3) AND (n_nationkey > 10) AND (n_name > 'JAPAN')")));
	EXPECT_EQ(9, gtest3(s, ((char*) "(s_suppkey < 10)")));
	EXPECT_EQ(19, gtest3(s, ((char*)"(s_nationkey = 18) AND (s_acctbal > 1000.0) AND (s_suppkey < 400)")));
	EXPECT_EQ(10, gtest3(c, ((char*)"(c_nationkey = 23) AND (c_mktsegment = 'FURNITURE') AND (c_acctbal > 7023.99) AND (c_acctbal < 7110.83)")));
	EXPECT_EQ(8, gtest3(p, ((char*)"(p_brand = 'Brand#13') AND (p_retailprice > 500.0) AND (p_retailprice < 930.0) AND (p_size > 28) AND (p_size < 1000000)")));
	EXPECT_EQ(14, gtest3(ps, ((char*) "(ps_supplycost > 999.98)")));
	EXPECT_EQ(14, gtest3(ps, ((char*) "(ps_availqty < 10) AND (ps_supplycost > 100.0) AND (ps_suppkey < 300)")));
	EXPECT_EQ(10, gtest3(o, ((char*) "(o_orderpriority = '1-URGENT') AND (o_orderstatus = 'O') AND (o_shippriority = 0) AND (o_totalprice > 1015.68) AND (o_totalprice < 1051.89)")));
	
}
 
int main (int argc, char *argv[]) {

	setup ();
    cout << "There are three tests:\n" 
    "  1. It can create the sorted database with the given CNF\n"
    "  2. The database is actually sorted\n"
    "  3. Query with CNF returns expected number of records\n"
    "Each test suite tests (1) and (2). The final two test suite tests 3, where:\n"
    "  - One tests the correct number of records in the full database\n" 
    "  - One tests the correct number of records from queries in Project 1: output.log\n\n"<< endl;
	relation *rel_ptr[] = {n, r, c, p, ps, s, o, li};
    testing::InitGoogleTest();
  	int ret_val = RUN_ALL_TESTS();

	cleanup ();
	return ret_val;
}
