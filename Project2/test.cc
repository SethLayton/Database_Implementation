#include "test.h"
#include "BigQ.h"
#include <pthread.h>
#include "DBFile.h"
#include "limits.h"
#include <fstream>
#include <chrono>
#include <thread>
void test1 ();
void test2 ();
void test3 ();

int add_data (FILE *src, int numrecs, int &res) {
	
	DBFile dbfile;
	dbfile.Open(rel->path ());
	Record temp;
	
	int proc = 0;
	int xx = 20000; 
	while (proc < numrecs) {
		res = temp.SuckNextRecord (rel->schema (), src);

		if (res != 0) {
			dbfile.Add(temp);
			if (proc == xx) cerr << "\t ";
			if (proc % xx == 0) cerr << ".";
			temp.SetNull();
			proc++;
		}		
		else {
			break;
		}
	}
	dbfile.Close();
	return proc;
}
 
 
// create a dbfile interactively
void test1 () {

	int runlen = 0;
	while (runlen < 1) {
		cout << "\t\n specify runlength:\n\t ";
		cin >> runlen;
	}

	OrderMaker om;
	rel->get_sort_order (om);  
	
	DBFile::sortutil startup = {runlen, &om};

	DBFile dbfile;
	cout << "\n output to dbfile : " << rel->path () << endl;
	dbfile.Create (rel->path(), sorted, &startup);
	dbfile.Close ();
	char tbl_path[100];
	sprintf (tbl_path, "%s%s.tbl", tpch_dir, rel->name()); 
	
	cout << " input from file : " << tbl_path << endl;


	FILE *tblfile = fopen (tbl_path, "r");

	srand48 (time (NULL));

	int proc = 1, res = 1, tot = 0;
	while (proc && res) {		
		int x = 0;
		while (x < 1 || x > 3) {
			cout << "\n select option for : " << rel->path () << endl;
			cout << " \t 1. add a few (1 to 1k recs)\n";
			cout << " \t 2. add a lot (1k to 1e+06 recs) \n";
			cout << " \t 3. run some query \n \t";
			cin >> x;
		}
		if (x == 1 || x == 2) {

			
			int rand = lrand48()%(int)pow(1e3,x)+(x-1)*1000;
			// int rand = 70000;
			cout << "random number of recs: " << rand << endl;
			proc = add_data (tblfile, rand, res);
			tot += proc;
			cout << proc << endl;
			if (proc) 
				cout << "\n\t added " << proc << " recs..so far " << tot << endl;
		}
		else {
			test3 ();
		}
	}
	cout << "\n create finished.. " << tot << " recs inserted\n";
	fclose (tblfile);
}

// sequential scan of a DBfile 
void test2 () {

	cout << " scan : " << rel->path() << "\n";
	DBFile dbfile;
	dbfile.Open (rel->path());
	dbfile.MoveFirst ();
	Record temp;

	int cnt = 0;
	while (dbfile.GetNext (temp) && ++cnt) {
		cout << "\t";
		temp.Print (rel->schema());
		cout << endl;
		if (cnt % 10000) {
			// cerr << ".";
		}
	}
	cout << "\n scanned " << cnt << " recs \n";
	dbfile.Close ();
}

void test3 () {

	cout << " Filter with CNF for : " << rel->name() << "\n";
	CNF cnf; 
	Record literal;
	rel->get_cnf (cnf, literal);

	DBFile dbfile;
	dbfile.Open (rel->path());
	dbfile.MoveFirst ();
	Record temp;

	int cnt = 0;
	while (dbfile.GetNext (temp, cnf, literal) && ++cnt) {
		cout << "\t";
		temp.Print (rel->schema());
		cout << endl;
		if (cnt % 10000 == 0) {
			// cerr << ".";
		}
	}
	cout << "\n query over " << rel->path () << " returned " << cnt << " recs\n";
	dbfile.Close ();

}
 
int main (int argc, char *argv[]) {

	setup ();

	relation *rel_ptr[] = {n, r, c, p, ps, s, o, li};
	void (*test_ptr[]) () = {&test1, &test2, &test3};  
	void (*test) ();

	int tindx = 0;
	while (tindx < 1 || tindx > 3) {
		cout << " select test option: \n";
		cout << " \t 1. create sorted dbfile\n";
		cout << " \t 2. scan a dbfile\n";
		cout << " \t 3. run some query \n \t ";
		cin >> tindx;
	}

	int findx = 0;
	while (findx < 1 || findx > 8) {
		cout << "\n select table: \n";
		cout << "\t 1. nation \n";
		cout << "\t 2. region \n";
		cout << "\t 3. customer \n";
		cout << "\t 4. part \n";
		cout << "\t 5. partsupp \n";
		cout << "\t 6. supplier \n";
		cout << "\t 7. orders \n";
		cout << "\t 8. lineitem \n \t ";
		cin >> findx;
	}
	rel = rel_ptr [findx - 1];

	test = test_ptr [tindx-1];
	test ();

	cleanup ();
	cout << "\n\n";
}