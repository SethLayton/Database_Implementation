#include "gtestpartone.h"
#include "BigQ.h"
#include <pthread.h>
#include <cstring>
#include <chrono>
#include <thread>
#include <bits/stdc++.h>
#include <algorithm>
#include <filesystem>
#include <iostream>
#include <sys/stat.h>
#include <unistd.h>
#include <gtest/gtest.h>

struct consumer_args {
	testutil *t;
	int success_val;
};

void *producer (void *arg) {

	Pipe *myPipe = (Pipe *) arg;

	Record temp;

	DBFile dbfile;
	dbfile.Open (rel->path ()); 
	dbfile.MoveFirst ();

	while (dbfile.GetNext (temp) == 1) {
		myPipe->Insert (&temp);
	}

	dbfile.Close ();
	myPipe->ShutDown ();

	pthread_exit(NULL);
}

void *consumer (void *arg) {
	testutil *t = (testutil *) arg;
	ComparisonEngine ceng;

	DBFile dbfile; 
	char outfile[100];

	if (t->write) {
		strcpy(outfile, rel->path());
		strcat(outfile,".bigq");
		dbfile.Create (outfile, heap, NULL);
	}

	int err = 0;
	int i = 0;

	Record rec[2];
	Record *last = NULL, *prev = NULL;
	while (t->pipe->Remove (&rec[i%2])) {
		prev = last;
		last = &rec[i%2];

		if (NULL != prev && NULL != last) {
			if (ceng.Compare (prev, last, t->order) == 1) {
				err++;
			}
			if (t->write) {
				dbfile.Add (*prev);
			}
		}
		if (t->print) {
			last->Print (rel->schema ());
		}
		i++; 
	}
 
	if (t->write) {
		if (last) {
			dbfile.Add (*last);
		}
		dbfile.Close ();
	}
	if (err) {
		t->success = 0;
	} else {
		t->success=1;
	}
	
	pthread_exit(NULL);

}

int initAllDBFiles () {

		relation *rel_ptr[] = {n, r, c, p, ps, o, li, s};
		struct stat buffer;   
		for (int i = 0; i < 8; i++){
			DBFile dbfile;
			rel = rel_ptr[i];
			const std::string& name = rel->path ();
			if (stat (name.c_str(), &buffer) == 0) { 
				cout << " DBFile already exists. Loading. " << rel->path () << endl;
			}
			else {				
				cout << " DBFile will be created at " << rel->path () << endl;
				dbfile.Create (rel->path(), heap, NULL);				
			}

			char tbl_path[100]; // construct path of the tpch flat text file
			sprintf (tbl_path, "%s%s.tbl", tpch_dir, rel->name()); 
			cout << " tpch file will be loaded from " << tbl_path << endl;

			dbfile.Load (*(rel->schema ()), tbl_path);
			dbfile.Close ();
			
		}
		return 0;
}




int test1 (int option, int runlen,int table, char* colname) {
	relation *rel_ptr[] = {n, r, c, p, ps, o, li,s}; 

	rel = rel_ptr[table];

	// sort order for records
	OrderMaker sortorder (rel->schema()); 
	rel->get_sort_order (sortorder, colname);
	int buffsz = 100; // pipe cache size
	Pipe input (buffsz);
	Pipe output (buffsz);

	// thread to dump data i nto the input pipe (for BigQ's consumption)
	pthread_t thread1;
	pthread_create (&thread1, NULL, producer, (void *)&input);
	// thread to read sorted data from output pipe (dumped by BigQ)
	pthread_t thread2;
	
	testutil tutil = {&output, &sortorder, false, false, 0 };
	if (option == 2) {
		tutil.print = true;
	}
	else if (option == 3) {
		tutil.write = true;
	}
	pthread_create (&thread2, NULL, consumer, (void *)&tutil);

	BigQ bq (input, output, sortorder, runlen);

	pthread_join (thread1, NULL);
	pthread_join (thread2, NULL);
	return tutil.success;
}
// NATION TESTS
TEST(NATION, NationKey) {
	char* col = "(n_nationkey)";
	EXPECT_EQ(1, test1(1, 1, 0, col));
	EXPECT_EQ(1, test1(1, 2, 0, col));
	EXPECT_EQ(1, test1(1, 3, 0, col));
	EXPECT_EQ(1, test1(1, 4, 0, col));

}

TEST(NATION, RegionKey) {
	char* col = "(n_regionkey)";
	EXPECT_EQ(1, test1(1, 1, 0, col));
	EXPECT_EQ(1, test1(1, 2, 0, col));
	EXPECT_EQ(1, test1(1, 3, 0, col));
	EXPECT_EQ(1, test1(1, 4, 0, col));	
}

TEST(NATION, Name) {
	char* col = "(n_name)";
	EXPECT_EQ(1, test1(1, 1, 0, col));
	EXPECT_EQ(1, test1(1, 2, 0, col));
	EXPECT_EQ(1, test1(1, 3, 0, col));
	EXPECT_EQ(1, test1(1, 4, 0, col));	
}

TEST(NATION, Comment) {
	char* col = "(n_comment)";
	EXPECT_EQ(1, test1(1, 1, 0, col));
	EXPECT_EQ(1, test1(1, 2, 0, col));
	EXPECT_EQ(1, test1(1, 3, 0, col));
	EXPECT_EQ(1, test1(1, 4, 0, col));	
}
//REGION TESTS
TEST(REGION, RegionKey) {
	char* col = "(r_regionkey)";
	EXPECT_EQ(1, test1(1, 1, 1, col));
	EXPECT_EQ(1, test1(1, 2, 1, col));
	EXPECT_EQ(1, test1(1, 3, 1, col));
	EXPECT_EQ(1, test1(1, 4, 1, col));
}

TEST(REGION, Name) {
	char* col = "(r_name)";
	EXPECT_EQ(1, test1(1, 1, 1, col));
	EXPECT_EQ(1, test1(1, 2, 1, col));
	EXPECT_EQ(1, test1(1, 3, 1, col));
	EXPECT_EQ(1, test1(1, 4, 1, col));
}
TEST(REGION, Comment) {
	char* col = "(r_comment)";
	EXPECT_EQ(1, test1(1, 1, 1, col));
	EXPECT_EQ(1, test1(1, 2, 1, col));
	EXPECT_EQ(1, test1(1, 3, 1, col));
	EXPECT_EQ(1, test1(1, 4, 1, col));
}

TEST(PART, PartKey) {
	char* col = "(p_partkey)";
	EXPECT_EQ(1, test1(1, 1, 3, col));
	EXPECT_EQ(1, test1(1, 2, 3, col));
	EXPECT_EQ(1, test1(1, 3, 3, col));
	EXPECT_EQ(1, test1(1, 4, 3, col));
}

TEST(PART, Price) {
	char* col = "(p_retailprice)";
	EXPECT_EQ(1, test1(1, 1, 3, col));
	EXPECT_EQ(1, test1(1, 2, 3, col));
	EXPECT_EQ(1, test1(1, 3, 3, col));
	EXPECT_EQ(1, test1(1, 4, 3, col));
}

TEST(PART, Brand) {
	char* col = "(p_brand)";
	EXPECT_EQ(1, test1(1, 1, 3, col));
	EXPECT_EQ(1, test1(1, 2, 3, col));
	EXPECT_EQ(1, test1(1, 3, 3, col));
	EXPECT_EQ(1, test1(1, 4, 3, col));
}

TEST(CUSTOMER, CustomerKey) {
	char* col = "(c_custkey)";
	EXPECT_EQ(1, test1(1, 1, 2, col));
	EXPECT_EQ(1, test1(1, 2, 2, col));
	EXPECT_EQ(1, test1(1, 3, 2, col));
	EXPECT_EQ(1, test1(1, 4, 2, col));
}

TEST(CUSTOMER, Phone) {
	char* col = "(c_phone)";
	EXPECT_EQ(1, test1(1, 1, 2, col));
	EXPECT_EQ(1, test1(1, 2, 2, col));
	EXPECT_EQ(1, test1(1, 3, 2, col));
	EXPECT_EQ(1, test1(1, 4, 2, col));
}

TEST(CUSTOMER, AccountBalance) {
	char* col = "(c_acctbal)";
	EXPECT_EQ(1, test1(1, 1, 2, col));
	EXPECT_EQ(1, test1(1, 2, 2, col));
	EXPECT_EQ(1, test1(1, 3, 2, col));
	EXPECT_EQ(1, test1(1, 4, 2, col));
}

TEST(PARTSUPPLIER, PartKey) {
	char* col = "(ps_partkey)";
	EXPECT_EQ(1, test1(1, 1, 4, col));
	EXPECT_EQ(1, test1(1, 4, 4, col));
	
}

TEST(PARTSUPPLIER, SupplierKey) {
	char* col = "(ps_suppkey)";
	EXPECT_EQ(1, test1(1, 1, 4, col));
	EXPECT_EQ(1, test1(1, 4, 4, col));
}

TEST(PARTSUPPLIER, AvailableQty) {
	char* col = "(ps_availqty)";
	EXPECT_EQ(1, test1(1, 1, 4, col));
	EXPECT_EQ(1, test1(1, 4, 4, col));
}

TEST(SUPPLIER, SupplierKey) {
	char* col = "(s_suppkey)";
	EXPECT_EQ(1, test1(1, 1, 7, col));
	EXPECT_EQ(1, test1(1, 4, 7, col));
}

TEST(SUPPLIER, Address) {
	char* col = "(s_address)";
	EXPECT_EQ(1, test1(1, 1, 7, col));
	EXPECT_EQ(1, test1(1, 4, 7, col));
}

TEST(SUPPLIER, AccountBalance) {
	char* col = "(s_acctbal)";
	EXPECT_EQ(1, test1(1, 1, 7, col));
	EXPECT_EQ(1, test1(1, 4, 7, col));
}

TEST(ORDER, OrderKey) {
	char* col = "(o_orderkey)";
	EXPECT_EQ(1, test1(1, 1, 5, col));
	EXPECT_EQ(1, test1(1, 4, 5, col));
}

TEST(ORDER, OrderStatus) {
	char* col = "(o_orderstatus)";
	EXPECT_EQ(1, test1(1, 1, 5, col));
	EXPECT_EQ(1, test1(1, 4, 5, col));
}

TEST(ORDER, TotalPrice) {
	char* col = "(o_totalprice)";
	EXPECT_EQ(1, test1(1, 1, 5, col));
	EXPECT_EQ(1, test1(1, 4, 5, col));
}

TEST(LINEITEM, LineNumber) {
	char* col = "(l_linenumber)";
	EXPECT_EQ(1, test1(1, 3, 6, col));
}

TEST(LINEITEM, Discount) {
	char* col = "(l_discount)";
	EXPECT_EQ(1, test1(1, 1, 6, col));
}

TEST(LINEITEM, ShipMode) {
	char* col = "(l_shipmode)";
	EXPECT_EQ(1, test1(1, 4, 6, col));
}

// The tests in this file will go through the three of the columns
// of the database and check that they do return a sorted order
// on the column. Then it will try them at with sorting windows 
// of size 1, 2, 3, and 4.
int main () {
	setup ();
	initAllDBFiles();
	testing::InitGoogleTest();
  	int ret_val = RUN_ALL_TESTS();

	cleanup ();
	return ret_val;
}

