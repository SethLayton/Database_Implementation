#include "BigQ.h"
#include <bits/stdc++.h>
#include "ComparisonEngine.h"
#include "Defs.h"
#include <algorithm>

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
	//create our file that stores all the runs
	myFile.Open(0, "f_path");
	//set sort order for use in the comparison function later on
	
	//create thread and initialize starting values
	//pass the thread the bigqutil structure for all the information it needs
	pthread_t threads;
	bigqutil bqutil = {&in, &out, &sortorder, runlen, this};
	//actually create the thread
	pthread_create(&threads, NULL, ts, (void *) &bqutil);    
	//clean up the thread
	pthread_join(threads, NULL);
	pthread_exit (NULL);

    // finally shut down the outpipe
	out.ShutDown ();
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
			//get the size of that record in bytes	
			char *bytes = myRec.GetBits();
			int recBytes = ((int *)bytes)[0];
			//update the total size of the vector
			curSizeInBytes = curSizeInBytes + recBytes;
			//the vector is now full with enough records to fill up a run
			if (curSizeInBytes > maxRunBytes) {
				//sort the vector
				
				std::sort(records.begin(),records.end(),comparator);

				//write the records from the vector to pages, and the pages to the file
				for (auto & currRec : records) {				
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
			count ++;
			

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
		//quick check to make sure nothing is going awry with the number of pages
		if (pageCount > b->runlength){
			cerr << "Too many pages for the runlength. Pages: " << pageCount << " runlength: " << b->runlength << endl; 
		}

		//now we need to sort all the runs
		//This is "Phase 2" of the TPMMS algo
		FinalSort(b);

		//close the thread
		
	}
	catch(const std::exception& e)
	{
		std::cerr << e.what() << '\n';
	}
	pthread_exit(NULL);
}

void BigQ::FinalSort(bigqutil *b)
{
	cout << "final sort called" << endl;
	//get the total number of runs in the file
	//this is based on the length of the file and specified run length
	off_t totalPages = myFile.GetLength() - 1;
	long totalRuns = ceil(totalPages / b->runlength);
	//init the queue
	std::priority_queue<Record,std::vector<Record>,Compare> queue;
	//set the number of pages in each run
	int offset = b->runlength;
	//create vector to hold a page from each run 
	std::vector<Page> myPages;
	
	cout << "totalRuns = " << totalRuns << " offset = " << offset << endl;
	
	//We loop through a page of each run and read that
	//into our page vector and then we those are exhausted
	//we move onto the next page from each run
	for (int i = 0; i < offset; i=i++){	

		//grab a page from each of the runs
		//want to make sure we arent trying to access a page
		//that doesnt exist
		Page tempPage;			
		for (int j = 0; j < totalRuns; j++) {
			int t = (i * offset) + j;
			cout << "t = " << t << endl;
			if (t <= totalPages) {
				myFile.GetPage(&tempPage,t);
				myPages.push_back(tempPage);
			}
		}

		//Now loop through the n=totalRuns pages
		//and continuously grab the first record
		//from each page and insert that into the queue
		//continue doing this until all pages have no records
		int cont = 0;
		while (cont < totalRuns) {
			for (int k = 0; k < totalRuns; k++) {
				Record tempRecord;
				int res = myPages[k].GetFirst(&tempRecord);
				if (res == 0) {
					cont++;
				}
				else {
					queue.push(tempRecord);
				}					
			}		
		}
		//now we move back to the top of the loop
		//and grab the next page in each run and do this again
	}

	//Start inserting the sorted elements from the queue into the pipe
	while (!queue.empty()) {
		Record temp = queue.top();
		queue.pop();
		b->outpipe->Insert(&temp);
	}
	//shutdown the out pipe
	b->outpipe->ShutDown();	
}


