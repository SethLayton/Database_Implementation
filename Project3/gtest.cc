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
		// if (print) {
		// 	rec.Print (schema);
		// }
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


//Duplicate Removal
int gtest1 (relation table, char* pred) {
    init_SF_pt(table, pred, currentDB);
	SelectFile SF_test1( currentDB, currentPipe, currentCnf, currentRec, true);
	//SF_ps.Run (dbf_ps, _ps, cnf_ps, lit_ps);
    Pipe out(pipesz);

	DuplicateRemoval D( currentPipe, out, *table.schema(), 10);
    SF_test1.WaitUntilDone();
	D.WaitUntilDone();

    int cnt = clear_pipe (out, table.schema(), false);

	
    currentDB.Close();

    return cnt;

}

//Test that the sum is correct
int gtest2 (relation table, char* pred, char* str_sum) {
    init_SF_pt(table, pred, currentDB);
	SelectFile SF_test1( currentDB, currentPipe, currentCnf, currentRec, true);
	
    Pipe _out (1);
	Function func;
	get_cnf (str_sum, table.schema (), func);
	//T.Use_n_Pages (1);
    Sum T(currentPipe, _out, func, 1);
    SF_test1.WaitUntilDone();
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
    Pipe _in(100);
	SelectFile SF_test1 (currentDB, _in, currentCnf, currentRec, true);
	OrderMaker grp_order;
	grp_order.AddAttr(Int, attNum);
    Pipe _out (100);
	Function func;
	get_cnf (str_sum, table.schema (), func);
	//T.Use_n_Pages (1);
    Attribute s_nationkey = {"s_nationkey", Int};
    Attribute sum = {"sum", Double};
	Attribute groupatt[] = {sum, s_nationkey};
   
	Schema out_sch ("out_sch", 2, groupatt);
	GroupBy G(currentPipe, _out, grp_order, func, 10);
    SF_test1.WaitUntilDone();
    G.WaitUntilDone();

	int cnt = clear_pipe (_out, &out_sch, true);



    currentDB.Close();
    return cnt;

}

// TEST(SUM, PartSupplier) {
//     char* pred = "(ps_suppkey=ps_suppkey)";
//     char* str_sum = "(ps_supplycost)";
    
//     EXPECT_EQ(400420638.0, gtest2(*ps, pred, str_sum));
// }

// TEST(DUPLICATEREMOVAL, Supplier) {
//     char * pred = "(s_suppkey < 10)";
//     EXPECT_EQ(9, gtest1(*s, pred));
// }
// TEST(DUPLICATEREMOVAL, PartSupplier) {
//     char * pred = "(ps_suppkey =10)";
//     EXPECT_EQ(80, gtest1(*ps, pred));
// }

// TEST(GROUPBY, Supplier) {
//     char * pred = "(s_suppkey < 10)";
//     char * str_sum = "(s_acctbal)";
//     EXPECT_EQ(25 ,gtest3(*s, pred, str_sum, 3));

// }


int q1 () {


	char *pred_ps = "(c_custkey < 100)";
	init_SF_c (pred_ps, 100);
	SelectFile SF_c(dbf_c, _c, cnf_c, lit_c, 1);

	SF_c.WaitUntilDone ();

	int cnt = clear_pipe (_c, c->schema (), true);
	// cout << "\n\n query1 returned " << cnt << " records \n";
	dbf_c.Close ();
    return cnt;
}

int q2 () {

	char *pred_p = "(p_retailprice > 931.00) AND (p_retailprice < 931.31)";
	init_SF_p (pred_p, 100);
	
	Pipe _out (pipesz);
	int keepMe[] = {0,1,7};
	int numAttsIn = pAtts;
	int numAttsOut = 3;
	//P_p.Use_n_Pages (buffsz);
	SelectFile SF_p (dbf_p, _p, cnf_p, lit_p, false);
	
	//SF_p.Run (dbf_p, _p, cnf_p, lit_p);
	Project P_p(_p, _out, keepMe, numAttsIn, numAttsOut, buffsz);
	//P_p.Run (_p, _out, keepMe, numAttsIn, numAttsOut);

	SF_p.WaitUntilDone ();
	P_p.WaitUntilDone ();

	Attribute att3[] = {IA, SA, DA};
	Schema out_sch ("out_sch", numAttsOut, att3);
	int cnt = clear_pipe (_out, &out_sch, true);

	// cout << "\n\n query2 returned " << cnt << " records \n";

	dbf_p.Close ();
    
    return cnt;
}
int q3 () {

	char *pred_s = "(s_suppkey = s_suppkey)";
	init_SF_s (pred_s, 100);

	//Sum T;
	// _s (input pipe)
	Pipe _out (1);
	Function func;
	char *str_sum = "(s_acctbal + (s_acctbal * 1.05))";
	get_cnf (str_sum, s->schema (), func);
	func.Print ();
	//T.Use_n_Pages (1);
	SelectFile SF_s (dbf_s, _s, cnf_s, lit_s, 2);
	//SF_s.Run (dbf_s, _s, cnf_s, lit_s);
	Sum T(_s, _out, func, 1);
	//T.Run (_s, _out, func);

	SF_s.WaitUntilDone ();
	T.WaitUntilDone ();

	Schema out_sch ("out_sch", 1, &DA);
	int cnt = clear_pipe (_out, &out_sch, true);

	// cout << "\n\n query3 returned " << cnt << " records \n";

	dbf_s.Close ();
    return cnt;
}
int q4 () {

	char *pred_s = "(s_suppkey = s_suppkey)";
	init_SF_s (pred_s, 100);
	SelectFile SF_s(dbf_s, _s, cnf_s, lit_s, 3);
	//SF_s.Run (dbf_s, _s, cnf_s, lit_s); // 10k recs qualified

	char *pred_ps = "(ps_suppkey = ps_suppkey)";
	init_SF_ps (pred_ps, 100);

	// Join J;
	// left _s
	// right _ps
	Pipe _s_ps (pipesz);
	CNF cnf_p_ps;
	Record lit_p_ps;
	get_cnf ("(s_suppkey = ps_suppkey)", s->schema(), ps->schema(), cnf_p_ps, lit_p_ps);

	int outAtts = sAtts + psAtts;
	Attribute s_nationkey = {"s_nationkey", Int};
	Attribute ps_supplycost = {"ps_supplycost", Double};
	Attribute joinatt[] = {IA,SA,SA,s_nationkey,SA,DA,SA,IA,IA,IA,ps_supplycost,SA};
	Schema join_sch ("join_sch", outAtts, joinatt);

	
	// GroupBy G;
	// _s (input pipe)
	Pipe _out (100);
	Function func;
	char *str_sum = "(ps_supplycost)";
	get_cnf (str_sum, &join_sch, func);
	func.Print ();
	OrderMaker grp_order;
	grp_order.AddAttr(Int, 3);
	//G.Use_n_Pages (1);

	SelectFile SF_ps(dbf_ps, _ps, cnf_ps, lit_ps,7);
	//SF_ps.Run (dbf_ps, _ps, cnf_ps, lit_ps); // 161 recs qualified
	Join J(_s, _ps, _s_ps, cnf_p_ps, lit_p_ps);
	//J.Run (_s, _ps, _s_ps, cnf_p_ps, lit_p_ps);
	GroupBy G(_s_ps, _out, grp_order, func, 10);
	//G.Run (_s_ps, _out, grp_order, func);

	SF_ps.WaitUntilDone ();
	J.WaitUntilDone ();
	G.WaitUntilDone ();
	Attribute sum = {"sum", Double};
	Attribute groupatt[] = {sum, s_nationkey};
	Schema sum_sch ("sum_sch", 2, groupatt);
	int cnt = clear_pipe (_out, &sum_sch, true);
	// cout << " query6 returned sum for " << cnt << " groups (expected 25 groups)\n"; 
    return cnt;
}

TEST(SELECT, Customer) {
    EXPECT_EQ(99,q1());
}
 
 TEST(PROJECT, Part) {
    EXPECT_EQ(22,q2());
}

TEST(SUM, Part) {
     EXPECT_EQ(1,q3());
}
TEST(JOIN, PartSupp_Supp) {
     EXPECT_EQ(25,q4());
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
