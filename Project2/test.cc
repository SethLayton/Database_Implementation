#include "test.h"
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

void *producer (void *arg) {

	Pipe *myPipe = (Pipe *) arg;

	Record temp;
	int counter = 0;

	DBFile dbfile;
	dbfile.Open (rel->path ()); 
	cout << " producer: opened DBFile " << rel->path () << endl;
	dbfile.MoveFirst ();

	while (dbfile.GetNext (temp) == 1) {
		counter += 1;
		if (counter%100000 == 0) {
			 cerr << " producer: " << counter << endl;	
		}
		myPipe->Insert (&temp);
	}

	dbfile.Close ();
	myPipe->ShutDown ();

	cout << " producer: inserted " << counter << " recs into the pipe\n";
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

	cout << " consumer: removed " << i << " recs from the pipe\n";
 
	if (t->write) {
		if (last) {
			dbfile.Add (*last);
		}
		cerr << " consumer: recs removed written out as heap DBFile at " << outfile << endl;
		dbfile.Close ();
	}
	cerr << " consumer: " << (i - err) << " recs out of " << i << " recs in sorted order \n";
	if (err) {
		cerr << " consumer: " <<  err << " recs failed sorted order test \n" << endl;
	}
	pthread_exit(NULL);
}

int gtest1 () {  

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

void test1 (int option, int runlen) {

	// sort order for records
	OrderMaker sortorder (rel->schema()); 
	//char string10[] = "(r_regionkey > 0)";
	// rel->get_sort_order (sortorder, string10);
	rel->get_sort_order (sortorder);
	
	int buffsz = 100; // pipe cache size
	Pipe input (buffsz);
	Pipe output (buffsz);

	// thread to dump data i nto the input pipe (for BigQ's consumption)
	pthread_t thread1;
	pthread_create (&thread1, NULL, producer, (void *)&input);

	// thread to read sorted data from output pipe (dumped by BigQ)
	pthread_t thread2;
	testutil tutil = {&output, &sortorder, false, false};
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
}

int main (int argc, char *argv[]) {

	setup ();
	relation *rel_ptr[] = {n, r, c, p, ps, o, li}; 

	cout << "loading all tables -> dbfiles" << endl;
	gtest1();

	int tindx = 0;
	while (tindx < 1 || tindx > 3) {
		cout << " select test option: \n";
		cout << " \t 1. sort \n";
		cout << " \t 2. sort + display \n";
		cout << " \t 3. sort + write \n\t ";
		cin >> tindx;
	}

	int findx = 0;
	while (findx < 1 || findx > 7) {
		cout << "\n select dbfile to use: \n";
		cout << "\t 1. nation \n";
		cout << "\t 2. region \n";
		cout << "\t 3. customer \n";
		cout << "\t 4. part \n";
		cout << "\t 5. partsupp \n";
		cout << "\t 6. orders \n";
		cout << "\t 7. lineitem \n \t ";
		cin >> findx;
	}
	rel = rel_ptr [findx - 1];

	int runlen;
	cout << "\t\n specify runlength:\n\t ";
	cin >> runlen;
	
	test1 (tindx, runlen);

	cleanup ();
	
}
