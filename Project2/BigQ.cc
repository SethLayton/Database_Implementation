#include "BigQ.h"
#include <bits/stdc++.h>
#include "ComparisonEngine.h"
#include "Defs.h"
#include <algorithm>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void segfault_sigaction(int signal, siginfo_t *si, void *arg)
{
    printf("Caught segfault at address %p\n", si->si_addr);
    exit(0);
}

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
	
	struct sigaction sa;

	memset(&sa, 0, sizeof(struct sigaction));
	sigemptyset(&sa.sa_mask);
	sa.sa_sigaction = segfault_sigaction;
	sa.sa_flags   = SA_SIGINFO;

	sigaction(SIGSEGV, &sa, NULL);


	//create our file that stores all the runs
	myFile.Open(0, "f_path");
	
	//create thread and initialize starting values
	//pass the thread the bigqutil structure for all the information it needs
	pthread_t threads;
	bigqutil bqutil = {&in, &out, &sortorder, runlen, this};
	//actually create the thread
	pthread_create(&threads, NULL, ts, (void *) &bqutil);    
	//clean up the thread
	pthread_join(threads, NULL);
	out.ShutDown();	
	//pthread_exit (NULL);


}

BigQ::~BigQ () {
}

//thread starter
void * ts(void *arg)
{
	bigqutil *b = (bigqutil *) arg;
  	return reinterpret_cast<BigQ*>(b->context)->DoWork(arg);
}

void *BigQ::DoWork(void *arg) {
	try
	{	
		bigqutil *b = (bigqutil *) arg;		
		Record myRec;
		std::vector<Record> records;
		file_length = myFile.GetLength();
		long curSizeInBytes = 0;
		//calculate how long a run can be in bytes
		long maxRunBytes = b->runlength * PAGE_SIZE;
		OrderMaker* tempOrder = b->order;
		Compare comparator(tempOrder);
		int count = 0;
		//read in a record from the input pipe
		while (b->inpipe->Remove (&myRec)) {
			count++;
			//get the size of that record in bytes
			char *bytes = myRec.GetBits();
			int recBytes = ((int *)bytes)[0];
			//update the total size of the vector
			curSizeInBytes = curSizeInBytes + recBytes;
			//the vector is now full with enough records to fill up a run
			if (curSizeInBytes > maxRunBytes) {
				cout << "Total in run: ----------------------------------------------------------------------------------" << count << endl;
				//sort the vector				
				std::sort(records.begin(),records.end(),comparator);
				//write the records from the vector to pages, and the pages to the file
				int othercount = 0;
				for (auto & currRec : records) {
					if (count < 15000  && (othercount < 10 || othercount > records.size() - 10)) {
						Schema ms("catalog", "lineitem");	
						currRec.Print(&ms);	
						othercount++;	
					}	
					//write record to page (returns 0 if page is full)
					if (myPage.Append(&currRec) == 0) {
						//if that page is full, write it to the file
						myFile.AddPage(&myPage, file_length);
						file_length++;
						//empty the page out
						myPage.EmptyItOut();
						pageCount++;
						//add the record to the new empty page
						myPage.Append(&currRec);				
					} 
				}	

				//this is to handle residuals for any non full pages
				//at the tail end of reading from the vector
				if (myPage.GetNumRecs() > 0){
					myFile.AddPage(&myPage, file_length);
					file_length++;
					myPage.EmptyItOut();
					pageCount++;
				}			
				
				//clear the vector and reset the current bytes for the next run
				curSizeInBytes = recBytes;
				records.clear();
			}	
			// //add the record to the vector for future sorting			
			records.push_back(myRec);
			myRec.SetNull();
		}
		if (records.size() > 0){			
			std::sort(records.begin(),records.end(),comparator);
			for (int i = 0; i < records.size(); i++) {		
				//write record to page (returns 0 if page is full)
				if (myPage.Append(&records[i]) == 0) {
					//if that page is full, write it to the file
					myFile.AddPage(&myPage, file_length);
					file_length++;
					//empty the page out
					myPage.EmptyItOut();
					pageCount++;
					//add the record to the new empty page
					myPage.Append(&records[i]);				
				}
			}
			if (myPage.GetNumRecs() > 0){
				myFile.AddPage(&myPage, file_length);
				file_length++;
				myPage.EmptyItOut();
				pageCount++;
			}	
			records.clear();	
		}

		cout << "total removed from pipe " << count << endl;

		//now we need to sort all the runs
		//This is "Phase 2" of the TPMMS algo
		FinalSort(b);

		cout << "Shutting down the output pipe" << endl;
		//shutdown the out pipe
		b->outpipe->ShutDown();	
		cout << "ending thread" << endl;
		pthread_exit(NULL);
		
	}
	catch(const std::exception& e)
	{
		std::cerr << e.what() << '\n';
		pthread_exit(NULL);
	}
	
	
}

void BigQ::FinalSort(bigqutil *b) {
	b->order->Print();
	cout << "final sort called" << endl; 
	//get the total number of runs in the file
	//this is based on the length of the file and specified run length
	off_t totalPages = myFile.GetLength() - 1;
	int totalRuns = int(ceil(double(totalPages) / double(b->runlength)));
	cout << "total pages: " << totalPages << endl;
	cout << "total runs: " << totalRuns << endl;
	cout << "run length: " << b->runlength << endl; 
	//init the queue
	std::priority_queue<Record,std::vector<Record>,Compare> queue(b->order);  
	//set the number of pages in each run
	int offset = b->runlength;
	//create vector to hold a page from each run
	Page myPagez[totalRuns];
	Record PQ[totalRuns];
	int ci[totalRuns] = { 0 };
	ComparisonEngine c;		
	
	int failedPages = 0;
	int count = 0;
	int cc = 0;
	Record tempRecord;
	for (int j = 0; j < totalRuns; j++) {
		int t = offset * j;

		if (t < totalPages) {
			myFile.GetPage(&myPagez[count],t);
			int res = myPagez[count].GetFirst(&tempRecord);
			PQ[j].Consume(&tempRecord);
			count++;			
		}
	}
	tempRecord.SetNull();
	int true_min = 0;
	while (failedPages < totalPages) {
		int index_min = true_min;
		// cout << "\tindex_min: " << index_min << endl;
		// cout << "\ttotal runs: " << totalRuns << endl;

		for (int i=index_min + 1; i<totalRuns; i++) {
			// cout << "i: " << i << endl;
			// cout << " ci[i]: " << ci[i] << endl;
			if (ci[i] < offset) {
					
				if (c.Compare(&PQ[i], &PQ[index_min], b->order) < 0) {
					index_min = i;
				}	
			}			
		}
		Schema ms("catalog", "lineitem");
		// if (cc == 1214 || cc == 1215 || cc == 1216 || cc == 1217) {
		// 	PQ[index_min].Print(&ms);
		// }
		b->outpipe->Insert(&PQ[index_min]);
		
		cc++;
		// cout << cc << " total pages: " << totalPages << " failed pages: " << failedPages <<  endl;
		int res = myPagez[index_min].GetFirst(&tempRecord);
		// cout << "\t res: " << res << endl;
		// cout << "\t index_min: " << index_min << endl;
		// cout << "\t ci[index_min]: " << ci[index_min] << endl;
		if (res == 0) {
			failedPages++;
			if (failedPages != totalPages) {						
				
				ci[index_min]++;
				if (ci[index_min] < offset) {
					int newPage = (index_min * offset) + ci[index_min];	
					myFile.GetPage(&myPagez[index_min],newPage);
					tempRecord.SetNull();
					myPagez[index_min].GetFirst(&tempRecord);
					PQ[index_min].Consume(&tempRecord);	
				}							
								
				for (int k = 0; k < totalRuns; k++) {
					// cout << k << endl;
					if (ci[k] < offset) {
						true_min=k;
						// cout << "true min: " << true_min  << endl;
						break;
						
					}
				}
			}			
		}
		else {
			PQ[index_min].Consume(&tempRecord);
		}
		
	}
	
	cout << "finished putting into pipe" << endl;
	cout << "testing again" << endl;
	return;
}


