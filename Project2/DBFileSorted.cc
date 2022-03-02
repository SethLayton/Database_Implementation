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

int DBFileSorted::Close () {

    try
    {
        if (is_write) {
            MergeInternal();
        }
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

void DBFileSorted::MoveFirst () {

    if (is_write) {
        MergeInternal();
    }
    //an index to the current page that we are reading from the overall file
    //just reseting this index to 0
    curr_page = 0;
}

void DBFileSorted::Load (Schema &f_schema, const char *loadpath) {

    is_write = true; //set current state to writing
    if(bigQ == NULL) { //if bigQ isnt set up
        bigQ = new BigQ(*input, *output, so, runlen);
    }
    Record temp;    
    FILE *tableFile = fopen (loadpath, "r"); //open up the file we want to read records from
    //do the actual record reading until the end of file
    while (temp.SuckNextRecord (&f_schema, tableFile) == 1) 
	{
        //we've successfully grabbed a record
        //add it to the BigQ pipe        
        input->Insert(&temp);
    }
}

void DBFileSorted::Add (Record &rec) {

    is_write = true; //set current state to writing
    if(bigQ == NULL) { //if bigQ isnt set up
        bigQ = new BigQ(*input, *output, so, runlen);
    }      
    input->Insert(&rec); //write the record to the pipe   
}

int DBFileSorted::GetNext (Record &fetchme) {

    if (is_write) {
        MergeInternal();
    }
    off_t file_length = myFile.GetLength();
    
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

int DBFileSorted::GetNext (Record &fetchme, CNF &applyMe, Record &literal) {

    if (is_write) {
        MergeInternal();
    }
    
    int numAtts = so.GetNumAtts();
    int *whichAtts = so.GetWhichAtts();

    for (int i = 0; i < numAtts; i++) { 
		cout << whichAtts[i] << endl; 
	}

    Schema ms("catalog", "nation");
    literal.Print(&ms);
    return 1;
}

void DBFileSorted::MergeInternal() {

    is_write = false; //set the current state to reading
    input->ShutDown(); //shut down the pipe
    Record piperec; //create a record to hold records from the pipe
    Record filerec; //create a record to hold records from the file   
    ComparisonEngine ce; //init comp engine
    File newMyFile; //create a new file used to store our merged records
    Page newMyPage; //create a new temp page used to store our merged records
    int page_counter = 0; //page counter for newMyFile
    bool contReadFile = true; //exit condition for inner while loop
    while (input->Remove (&piperec)) { //Coninuously read from the pipe     
        while (contReadFile) { //read from the file as long as the file value is less than the pipe value
            Record temp;
            if (GetNext(filerec) != 0) { //read the first value from the file               
                if (ce.Compare(&piperec, &filerec, &so) == 1) { //compare the file record with the pipe record
                    //filerec is smallest
                    temp = filerec;
                    contReadFile = true; //continue reading from the file
                }
                else {
                    //piperec is the smallest
                    temp = piperec;
                    contReadFile = false; //stop reading from the file and get the next pipe record
                }
            }
            else {
                //no data left in the file, continue reading from the pipe exclusively
                temp = piperec;
                contReadFile = false;
            }
            if (newMyPage.Append(&temp) == 0) //append the current smallest (between pipe and file) to our new temp page
            {
                //our current page is full
                //write this page out to the file
                newMyFile.AddPage(&newMyPage, page_counter);
                //empty out our current page
                newMyPage.EmptyItOut();
                //increment the total number of pages
                page_counter++;
                //add the record to the newly created page
                newMyPage.Append(&temp);
            }
            temp.SetNull(); //clear out the temp record just in case
        }        
    }
    if (newMyPage.GetNumRecs() > 0) { //if our new temp page still has some records in it
        newMyFile.AddPage(&newMyPage,page_counter); //add that page to the end of the file
        newMyPage.EmptyItOut(); //clear out the page
    }
    myFile = newMyFile; //set our myFile to our newly created merged File
    newMyFile.~File(); //destroy our temp File
    myPage = newMyPage;
    piperec.SetNull();
    filerec.SetNull();
}