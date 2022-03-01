#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFileSorted.h"
#include "Defs.h"
#include <iostream>
#include <fstream>
#include <cstring>


DBFileSorted::DBFileSorted(int runlength, OrderMaker om) {

    runlen = runlength;
    so = om;
}

int DBFileSorted::Create (const char *f_path, fType f_type, void *startup) {

    try
    {
        
        //create the new file based on the passed in name
        myFile.Open(0, f_path);
        //set the current page index to 0
        MoveFirst();        
        //return success
        return 1;
    }
    catch(const std::exception& e)
    {
        //there was some error, return fail
        return 0;
    }      
}

int DBFileSorted::Open (const char *f_path) {

    try
    {        
        //open the file
        myFile.Open(1, f_path);
        //emty our our current page
        myPage.EmptyItOut();
        //move the current page pointer to 0
        MoveFirst();
        
        //return success
        return 1;
    }
    catch(const std::exception& e)
    {
        //there was some error, return fail
        return 0;
    }    
}

void DBFileSorted::Load (Schema &f_schema, const char *loadpath) {

    //bool to track if the page has content
    //used to make sure we don't try and double write
    //a page to the file and end up with an empty page at the end
    bool has_contents = false;
    //get the number of pages in the file
    off_t page_counter = myFile.GetLength();
    //empty out our current page
    myPage.EmptyItOut();
    //create a temporary record object to handle each read record
    Record temp;
    //open up the file we want to read records from
    FILE *tableFile = fopen (loadpath, "r");
    //do the actual record reading until the end of file
    while (temp.SuckNextRecord (&f_schema, tableFile) == 1) 
	{
        //we've successfully grabbed a record
        //add it to the current page
        if (myPage.Append(&temp) == 0)
		{
            //our current page is full
            //write this page out to the file
			myFile.AddPage(&myPage, page_counter);
            //empty out our current page
			myPage.EmptyItOut();
            //increment the total number of pages
			page_counter++;
            //add the record to the newly created page
            myPage.Append(&temp);
            //reset the page content check
            has_contents = false;
		}
        //set the tracker to show there is content in the current page
        has_contents = true;
    }
    //add our page to the file
    //the above addpage call only gets hit if the page is full
    //this will add the page to the file at the end if there are still records
    if (has_contents){
        myFile.AddPage(&myPage, page_counter);
    }
}



void DBFileSorted::MoveFirst () {

    //an index to the current page that we are reading from the overall file
    //just reseting this index to 0
    curr_page = 0;
}

int DBFileSorted::Close () {

    try
    {
        //empty our current page
        myPage.EmptyItOut();
        //close the file
        myFile.Close();
        //return success
        return 1;
    }
    catch(const std::exception& e)
    {
        //there was some error, return fail
        return 0;
    }
}

void DBFileSorted::Add (Record &rec) {

    //get the number of pages in the file
    off_t file_length = myFile.GetLength();

    //there could be a non full page in the file
    //load the last page from the file
    if (!is_write && file_length > 1) {        
        myFile.GetPage(&myPage, file_length - 1);
    }
    
    is_write = true;
    //write record to page (returns 0 if page is full)   
    if (myPage.Append(&rec) == 0) {
        //if that page is full, add it to the file
        myFile.AddPage(&myPage, file_length);
        //empty the page out
        myPage.EmptyItOut();
        //add the record to the new empty page
        myPage.Append(&rec);
    } 
    
}

int DBFileSorted::GetNext (Record &fetchme) {

    off_t file_length = myFile.GetLength();
    //checking to see if we are swaping from a write to a read
    //if we were writing we need to write the page out to the file
    //and clear the page out, before we starting reading
    //this might lead to a nonfull page at the end of the file
    if (is_write) {
        myFile.AddPage(&myPage, file_length);
        is_write = false;
        myPage.EmptyItOut();
    }
    //get the updated number of pages
    file_length = myFile.GetLength();

    if (!is_read) {
        //set this to true. This helps monitor if this is a first read or not
        //if this is a first read we need to read the current page from the file        
        is_read = true;
        //get page associated with the current location of our pointer "curr_page"
        myFile.GetPage(&myPage, curr_page);
    }
    
    //read the first item from our current page
    if (myPage.GetFirst(&fetchme) == 0) {
        //inside this loop means that we reached the end of the current page
        //we need to check the next page now
        curr_page++;
        //make sure that we arent going to check a page that is outside the actual number of pages in the file
        if (curr_page < (file_length - 1)) {
            //if the page exists, grab that page
            myFile.GetPage(&myPage, curr_page);
            //get the first item from our new current page
            if (myPage.GetFirst(&fetchme) == 0) {
                //we're at the end of all the records
                return 0; //all pages in the file are exhausted
            }
        }
        else {
            //we're at the end of all the records
            return 0;
        }    
    } 
    return 1;
}

int DBFileSorted::GetNext (Record &fetchme, CNF &cnf, Record &literal) {

    //create our comparison engine object
    ComparisonEngine comp;

    //call the above GetNext function continually until a match is found
    //since the test.cc function calls this CNF version of GetNext continually
    //we only need to return one matching record each time as
    //test.cc will keep calling until all the records are exhausted
    while (GetNext(fetchme) == 1) {
        //if we get a record back, make a comparison check
        if (comp.Compare (&fetchme, &literal, &cnf)) {
            //return success if there was a match            
            return 1;
        }
    }
    //no matching records were found
    return 0;
}
