#include "gtest.h"
#include "BigQ.h"
#include "RelOp.h"
#include <pthread.h>
#include <gtest/gtest.h>



Attribute IA = {"int", Int};
Attribute SA = {"string", String};
Attribute DA = {"double", Double};

int clear_pipe (Pipe &in_pipe, Schema *schema, bool print) {
	Record rec;
	int cnt = 0;
	while (in_pipe.Remove (&rec)) {
		if (print) {
			rec.Print (schema);
		}
		cnt++;
	}
	return cnt;
}

int clear_pipe (Pipe &in_pipe, Schema *schema, Function &func, bool print) {
	Record rec;
	int cnt = 0;
	double sum = 0;
	while (in_pipe.Remove (&rec)) {
		if (print) {
			rec.Print (schema);
		}
		int ival = 0; double dval = 0;
		func.Apply (rec, ival, dval);
		sum += (ival + dval);
		cnt++;
	}
	cout << " Sum: " << sum << endl;
	return cnt;
}
int pipesz = 100; // buffer sz allowed for each pipe
int buffsz = 100; // pages of memory allowed for operations


// SelectFile SF_ps, SF_p, SF_s, SF_o, SF_li, SF_c;
DBFile dbf_ps, dbf_p, dbf_s, dbf_o, dbf_li, dbf_c;
Pipe _ps (pipesz), _p (pipesz), _s (pipesz), _o (pipesz), _li (pipesz), _c (pipesz);
CNF cnf_ps, cnf_p, cnf_s, cnf_o, cnf_li, cnf_c;
Record lit_ps, lit_p, lit_s, lit_o, lit_li, lit_c;
Function func_ps, func_p, func_s, func_o, func_li, func_c;

DBFile currentDB;
Pipe  currentPipe(pipesz);
CNF currentCnf;
Record currentRec;
Function currentFunc;

int pAtts = 9;
int psAtts = 5;
int liAtts = 16;
int oAtts = 9;
int sAtts = 7;
int cAtts = 8;
int nAtts = 4;
int rAtts = 3;
relation *rel_ptr[] = {n, r, c, p, ps, s, o, li};

void init_SF_pt (relation rel, char*pred, DBFile &db) {
    db.Open(rel.path());
    get_cnf(pred, rel.schema (),currentCnf, currentRec);

}

void init_SF_ps (char *pred_str, int numpgs) {
	dbf_ps.Open (ps->path());
	get_cnf (pred_str, ps->schema (), cnf_ps, lit_ps);
	//SF_ps.Use_n_Pages (numpgs);
}

void init_SF_p (char *pred_str, int numpgs) {
	dbf_p.Open (p->path());
	get_cnf (pred_str, p->schema (), cnf_p, lit_p);
	//SF_p.Use_n_Pages (numpgs);
}

void init_SF_s (char *pred_str, int numpgs) {
	dbf_s.Open (s->path());
	get_cnf (pred_str, s->schema (), cnf_s, lit_s);
	//SF_s.Use_n_Pages (numpgs);
}

void init_SF_o (char *pred_str, int numpgs) {
	dbf_o.Open (o->path());
	get_cnf (pred_str, o->schema (), cnf_o, lit_o);
	//SF_o.Use_n_Pages (numpgs);
}

void init_SF_li (char *pred_str, int numpgs) {
	dbf_li.Open (li->path());
	get_cnf (pred_str, li->schema (), cnf_li, lit_li);
	//SF_li.Use_n_Pages (numpgs);
}

void init_SF_c (char *pred_str, int numpgs) {
	dbf_c.Open (c->path());
	get_cnf (pred_str, c->schema (), cnf_c, lit_c);
	//SF_c.Use_n_Pages (numpgs);
}


int gtest1 ();
int gtest2 ();
int gtest3 ();
// [0   1   2   3   4   5]
// //  ps  p   s   o   li  c
// void init_SF (int table, char* pred) {
//         switch (table){
//             case (0): {
//                 init_SF_ps(pred, 100);
//                 // currentDB = &dbf_ps;
//                 // currentPipe = &_ps;
//                 // currentCnf = &cnf_ps;
//                 // currentRec = &lit_ps;
//                 // currentFunc = &func_ps;
//                 break;
//             }
//             case (1): {
//                 init_SF_p(pred, 100);
//                 // currentDB = &dbf_p;
//                 // currentPipe = &_p;
//                 // currentCnf = &cnf_p;
//                 // currentRec = &lit_p;
//                 // currentFunc = &func_p;
//                 break;
//             }
//             case (2): {
//                 init_SF_s(pred,100);
//                 // currentDB = &dbf_s;
//                 // currentPipe = &_s;
//                 // currentCnf = &cnf_s;
//                 // currentRec = &lit_s;
//                 // currentFunc = &func_s;
//                 break;
//             }
//             case (3): {
//                 init_SF_o(pred,100);
//                 // currentDB = &dbf_o;
//                 // currentPipe = &_o;
//                 // currentCnf = &cnf_o;
//                 // currentRec = &lit_o;
//                 // currentFunc = &func_o;
//                 break;
//             }
//             case (4): {
//                 init_SF_li(pred,100);
//                 currentDB = &dbf_li;
//                 currentPipe = &_li;
//                 currentCnf = &cnf_li;
//                 currentRec = &lit_li;
//                 currentFunc = &func_li;
//                 break;
//             }
//             case (5): {
//                 init_SF_c(pred,100);
//                 // currentDB = &dbf_c;
//                 // currentPipe = &_c;
//                 // currentCnf = &cnf_c;
//                 // currentRec = &lit_c;
//                 // currentFunc = &func_c;
//                 break;
//             }
//             default:
//                 break;
//         }
            

// }

//Duplicate Removal
int gtest1 (relation table, char* pred) {
    init_SF_pt(table, pred, currentDB);
	SelectFile SF_test1( currentDB, currentPipe, currentCnf, currentRec, "1");
	//SF_ps.Run (dbf_ps, _ps, cnf_ps, lit_ps);
    Pipe out(pipesz);
	DuplicateRemoval D( currentPipe, out, *table.schema(), 10);
    
    int cnt = clear_pipe (out, table.schema(), true);
	D.WaitUntilDone();

    
	
    currentDB.Close();

    return cnt;

}

//Test that the sum is correct
int gtest2 (relation table, char* pred, char* str_sum) {
    init_SF_pt(table, pred, currentDB);
	SelectFile SF_test1( currentDB, currentPipe, currentCnf, currentRec, "1");
	
    Pipe _out (1);
	Function func;
	get_cnf (str_sum, table.schema (), func);
	//T.Use_n_Pages (1);
    Sum T(currentPipe, _out, func, 1);
    T.WaitUntilDone ();
	Schema out_sch ("out_sch", 1, &DA);
    Record temp;
	_out.Remove(&temp);
    double cnt = std::stod(temp.getValue(Double, 0));
    currentDB.Close();
    return cnt;

}
//Tests GroupBy
int gtest3 (relation table, char* pred, char* str_sum, int attNum) {
    init_SF_pt(table, pred, currentDB);
	SelectFile SF_test1( currentDB, currentPipe, currentCnf, currentRec, "1");
	OrderMaker grp_order;
	grp_order.AddAttr(Int, attNum);
    Pipe _out (100);
	Function func;
	get_cnf (str_sum, table.schema (), func);
	//T.Use_n_Pages (1);
    Sum T(currentPipe, _out, func, 1);
    T.WaitUntilDone ();
	Schema out_sch ("out_sch", 1, &DA);
	GroupBy G(currentPipe, _out, grp_order, func, 10);

    G.WaitUntilDone();
	int cnt = clear_pipe (_out, &out_sch, true);



    currentDB.Close();
    return cnt;

}

TEST(SUM, PartSupplier) {
    char* pred = "(ps_suppkey=ps_suppkey)";
    char* str_sum = "(ps_supplycost)";
    
    EXPECT_EQ(400420638.0, gtest2(*ps, pred, str_sum));
}

TEST(DUPLICATEREMOVAL, Supplier) {
    char * pred = "(s_suppkey = s_suppkey)";
    EXPECT_EQ(10000, gtest1(*s, pred));
}

// TEST(SortOnOne, Region_RegionKey){
//     char* col = "(r_name)";
//     EXPECT_EQ(1, gtest1(1,2, col));
//     EXPECT_EQ(0, gtest2(col));
// }
// TEST(SortOnOne, Nation_RegionKey){
//     // rel=rel_ptr[0];
//     char* col = "(n_regionkey)";
//     EXPECT_EQ(1, gtest1(0,2, col));
//     EXPECT_EQ(0, gtest2(col));
// }
// TEST(SortOnOne, Customer_AcctBal){
//     // rel=rel_ptr[2];
//     char* col = "(c_acctbal)";
//     EXPECT_EQ(1, gtest1(2,8, col));
//     EXPECT_EQ(0, gtest2(col));
// }


// TEST(SortOnTwo, Part_Price_AND_Size){
//     // rel=rel_ptr[3];
//     char* col = "(p_retailprice) AND (p_size)";
//     EXPECT_EQ(1, gtest1(3,9, col));
//     EXPECT_EQ(0, gtest2(col));
// }

// TEST(SortOnTwo, Order_OPriority_AND_Status){
//     // rel=rel_ptr[6];
//     char* col = "(o_orderpriority) AND (o_orderstatus)";
//     EXPECT_EQ(1, gtest1(6,7, col));
//     EXPECT_EQ(0, gtest2(col));
// }

// TEST(SortOnTwo, Supplier_Nation_AND_phone){
//     // rel=rel_ptr[5];
//     char* col = "(s_nationkey) AND (s_phone)";
//     EXPECT_EQ(1, gtest1(5,15, col));
//     EXPECT_EQ(0, gtest2(col));
// }

// TEST(SortOnThree, PartSupp_){
//     // rel=rel_ptr[4];
//     char* col = "(ps_partkey) AND (ps_availqty) AND (ps_supplycost)";
//     EXPECT_EQ(1, gtest1(4,11, col));
//     EXPECT_EQ(0, gtest2(col));
// }
// TEST(ScanAndFilter, LoadsAllDatabase) {
//     EXPECT_EQ(5, gtest3(r, ((char*) "(r_regionkey>-1)")));
//     EXPECT_EQ(25, gtest3(n, ((char*) "(n_regionkey>-1)")));
//     EXPECT_EQ(10000, gtest3(s, ((char*) "(s_suppkey>-1)")));
//     EXPECT_EQ(800000, gtest3(ps, ((char*) "(ps_suppkey>-1)")));
//     EXPECT_EQ(200000, gtest3(p, ((char*) "(p_partkey>-1)")));

// }

// TEST(ScanAndFilter, TestQueriesResultInCorrectSize) {
	
//     EXPECT_EQ(1, gtest3(r, ((char*) "(r_name = 'EUROPE')")));
// 	EXPECT_EQ(2, gtest3(r, ((char*) "(r_name < 'MIDDLE EAST') AND (r_regionkey > 1)")));
// 	EXPECT_EQ(3, gtest3(n, ((char*) "(n_regionkey = 3) AND (n_nationkey > 10) AND (n_name > 'JAPAN')")));
// 	EXPECT_EQ(9, gtest3(s, ((char*) "(s_suppkey < 10)")));
// 	EXPECT_EQ(19, gtest3(s, ((char*)"(s_nationkey = 18) AND (s_acctbal > 1000.0) AND (s_suppkey < 400)")));
// 	EXPECT_EQ(10, gtest3(c, ((char*)"(c_nationkey = 23) AND (c_mktsegment = 'FURNITURE') AND (c_acctbal > 7023.99) AND (c_acctbal < 7110.83)")));
// 	EXPECT_EQ(8, gtest3(p, ((char*)"(p_brand = 'Brand#13') AND (p_retailprice > 500.0) AND (p_retailprice < 930.0) AND (p_size > 28) AND (p_size < 1000000)")));
// 	EXPECT_EQ(14, gtest3(ps, ((char*) "(ps_supplycost > 999.98)")));
// 	EXPECT_EQ(14, gtest3(ps, ((char*) "(ps_availqty < 10) AND (ps_supplycost > 100.0) AND (ps_suppkey < 300)")));
// 	EXPECT_EQ(10, gtest3(o, ((char*) "(o_orderpriority = '1-URGENT') AND (o_orderstatus = 'O') AND (o_shippriority = 0) AND (o_totalprice > 1015.68) AND (o_totalprice < 1051.89)")));
	
// }
 
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
