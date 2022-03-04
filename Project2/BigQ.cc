#include "BigQ.h"
#include <bits/stdc++.h>
#include "ComparisonEngine.h"
#include "Defs.h"
#include <algorithm>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

BigQ :: BigQ (Pipe &inp, Pipe &outp, OrderMaker &sortorder, int runlen) :in(inp), out(outp) {	

	so = sortorder;
	runlength = runlen;
	threads = pthread_t();
	//create thread and initialize starting values
	//bigqutil bqutil = {&inp, &outp, &sortorder, runlen, this}; //pass the thread the bigqutil structure for all the information it needs
	pthread_create(&threads, NULL, ts, this); //actually create the thread
}

BigQ::~BigQ () { 
}
void BigQ::pthreadwait () {
	pthread_join(threads, NULL);
}
//thread starter
void * ts(void *arg)
{
  	return reinterpret_cast<BigQ*>(arg)->DoWork();
}
 
void *BigQ::DoWork() {
	myFile.Open(0, "f_path"); //create our file that stores all the runs
	Record myRec; //create a record to store read in records from the input pipe
	std::vector<Record> records; //create a vector to store above records
	file_length = myFile.GetLength(); //get the current length of the file
	int curSizeInBytes = 0; //init a length variable for total bytes of a run read
	int rlen = runlength; 		
	int maxRunBytes = rlen * PAGE_SIZE; //calculate how long a run can be in bytes
	Compare comparator(&so); //create the comparator used in the vector sort
	//read in a record from the input pipe
	while (in.Remove (&myRec)) {
		char *bytes = myRec.GetBits(); //get the size of that record in bytes
		int recBytes = ((int *)bytes)[0]; //convert to an int		
		curSizeInBytes = curSizeInBytes + recBytes; //update the total size of the vector
		//the vector is now full with enough records to fill up a run
		if (curSizeInBytes > maxRunBytes) {								
			std::sort(records.begin(),records.end(),comparator); //sort the vector
			//write the records from the vector to pages, and the pages to the file
			for (auto & currRec : records) {
				//write record to page (returns 0 if page is full)
				if (myPage.Append(&currRec) == 0) {	
					myFile.AddPage(&myPage, file_length); //if that page is full, write it to the file
					file_length++; //update the new length of the file						
					myPage.EmptyItOut(); //empty the page out
					pageCount++; //update the total pages done						
					myPage.Append(&currRec); //add the record to the new empty page	 			
				} 
			}
			
			//clear the vector and reset the current bytes for the next run
			curSizeInBytes = recBytes;
			records.clear();
			//this block handles residuals
			//write these to the vector for next run processing
			//only if the number of pages is greater than run length
			if (myPage.GetNumRecs() > 0) {
				Record temp; //init a new record
				//read from the page
				while (myPage.GetFirst(&temp) != 0) {
					records.push_back(temp); //put record on the vector
					char *bytes = temp.GetBits();
					int recBytes = ((int *)bytes)[0];
					curSizeInBytes = curSizeInBytes + recBytes; //update bytes
					temp.SetNull();
				}
			}
			//if we get here that means there was a non full page at the end
			//we need to add this page to the file
			else {
				if (myPage.GetNumRecs() > 0){
					myFile.AddPage(&myPage, file_length); //add page to the file
					file_length++; //update file length
					myPage.EmptyItOut(); //clear the page
					pageCount++; //incrememnt total number of pages
				}			
			}
		}						
		records.push_back(myRec); // //add the record to the vector for future sorting	
		myRec.SetNull(); //deallocate the record to clear residuals
		
	}
	//this block is only hit if we read all the information from the input
	//pipe and it wasnt enough to fill an entire page
	if (records.size() > 0){					
		std::sort(records.begin(),records.end(),comparator); //sort our vector		
		//write each record to the page
		int totalbytes = 0;
		for (int i = 0; i < records.size(); i++) {	
			Record temp;
			temp.Consume(&records[i]);
			//write record to page (returns 0 if page is full)
			char *bytes = temp.GetBits(); //get the size of that record in bytes
			int recBytes = ((int *)bytes)[0]; //convert to an int		
			totalbytes = totalbytes + recBytes; //update the total size of the vector			
			if (myPage.Append(&temp) == 0) {				
				myFile.AddPage(&myPage, file_length); //if that page is full, write it to the file
				file_length++; //increment file length					
				myPage.EmptyItOut();//empty the page out
				pageCount++; //update the total number of pages processed					
				myPage.Append(&temp);	//add the record to the new empty page
				totalbytes = recBytes;			
			}
		}
		//if we looped through the above and didnt get enough records to fill a page
		//write this last un-filled page out to the file
		if (myPage.GetNumRecs() > 0){
			myFile.AddPage(&myPage, file_length);
			file_length++;
			myPage.EmptyItOut();
			pageCount++;
		}	
		records.clear(); //clears out the vector of records	
	}

	//now we need to sort all the runs
	//This is "Phase 2" of the TPMMS algo
	FinalSort();
	//shutdown the out pipe
	out.ShutDown();	
	pthread_exit(NULL);	
	
}

void BigQ::FinalSort() {
	//get the total number of runs in the file
	//this is based on the length of the file and specified run length
	off_t totalPages = myFile.GetLength() - 1;
	int rlen = runlength; 
	int totalRuns = int(ceil(double(totalPages) / double(rlen)));	
	int offset = rlen;	//set the number of pages in each run
	Page myPagez[totalRuns]; //initialize an array to hold the pages of the runs	
	Record PQ[totalRuns]; //initialize our custome priority queue array
	int pagecount[totalRuns] = { 0 }; //initialize our array for the number of completed pages in a run
	ComparisonEngine c;	//initialize the comp engine to do the compares for the PQ		
	int failedPages = 0; //count how many completed pages there are in total	
	Record tempRecord; //init a record to store records used temporarily
	int run_index = 0; //counts which run we are currently initializing
	//Loop through one time and grab the first page from each run
	//Also grab the first record from each of these first runs and put into the PQ
	for (int j = 0; j < totalRuns; j++) {
		int t = offset * j;
		if (t < totalPages) {
			myFile.GetPage(&myPagez[run_index],t); //read in the first page in the run
			int res = myPagez[run_index].GetFirst(&tempRecord); //read in the first record of that page
			PQ[j].Consume(&tempRecord); //insert into PQ array
			run_index++; //move on to next run			
		}
	}
	tempRecord.SetNull(); //deallocate the temp record Record

	int true_min_index = 0; //Stores the lowest run that has no yet been depleted of pages (between 0 and runlength -1)
	//While there are pages to read continuously loop
	while (failedPages < totalPages) {
		int index_min = true_min_index; //update the index of the minimum value based on the run

		//determine the miniumum element in the PQ list
		for (int i=index_min + 1; i<totalRuns; i++) {
			if (pagecount[i] < offset) {					
				if (c.Compare(&PQ[i], &PQ[index_min], &so) < 0) {
					index_min = i; //update the minimum elements location (used later)
				}	
			}			
		}
		out.Insert(&PQ[index_min]); //write out the minimum element to the output pipe

		int res = myPagez[index_min].GetFirst(&tempRecord); //from the vacated run, read in the next record from the current page
		//if there are no records in the current page
		if (res == 0) {
			failedPages++; //increment the total number of failed pages (while loop condition)
			//if there are still pages left to read
			if (failedPages != totalPages) {	
				pagecount[index_min]++; //increment this runs total number of completed pages
				//if this run has not completed all of its pages
				if (pagecount[index_min] < offset) {
					int newPage_index = (index_min * offset) + pagecount[index_min]; //set the index of the next page in this run	
					if (newPage_index < totalPages) {
						myFile.GetPage(&myPagez[index_min],newPage_index); //read in the next page
						tempRecord.SetNull(); //deallocate the temprecord incase there is leftovers
						myPagez[index_min].GetFirst(&tempRecord); //read in the first record from the new page
						PQ[index_min].Consume(&tempRecord);	//back fill the empty spot in the PQ
					}					
				}	

				//Calculates the lowest run that has not been completely exhausted of pages
				for (int k = 0; k < totalRuns; k++) {
					if (pagecount[k] < offset) {
						true_min_index = k;
						break;						
					}
				}
			}			
		}
		//there are records to read from the current page still
		else {			
			PQ[index_min].Consume(&tempRecord); //insert the record in our custom priority queue
		}		
	}
}


