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
#include <bits/stdc++.h>

DBFileSorted::DBFileSorted(int &runlength, OrderMaker &om) : so(om), runlen(runlength) {
    // cout << "DBFileSorted::DBFileSorted -- RunLength: " << runlen << endl;

}
 

int DBFileSorted::Create (const char *f_path, fType f_type, void *startup) {

    try
    {        
        //create the new file based on the passed in name
        myFile.Open(0, f_path);
        //set the current page index to 0
        MoveFirst();
        contQuery = false; //reset this for the GetNext function        
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
    //cout << "DBFileSorted::OPEN" << endl;
    //so.Print();
    try
    {        
        //open the file
        myFile.Open(1, f_path);
        f_name = f_path;
        //emty our our current page
        myPage.EmptyItOut();
        //move the current page pointer to 0
        MoveFirst();
        if (myFile.GetLength() > 0) {
            myFile.GetPage(&myPage, curr_page);
        }

        contQuery = false; //reset this for the GetNext function
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
            cout << "DBFileSorted::Close - MERGE INTERNAL" << endl;
            MergeInternal();
        }
        is_read = true;
        //empty our current page

        myPage.EmptyItOut();
        //close the file
        cout << "DBFileSorted::Close - CLOSE FILE" << endl;
        myFile.Close();
        f_name = "";
        contQuery = false; //reset this for the GetNext function
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
    //cout << "DBFileSorted::MoveFirst" << endl;
    //so.Print();
    //cout << is_write << endl;
    if (is_write) {
        MergeInternal();
    }
    is_read = true;
    //an index to the current page that we are reading from the overall file
    //just reseting this index to 0
    curr_page = 0;
    contQuery = false; //reset this for the GetNext function
}

void DBFileSorted::Load (Schema &f_schema, const char *loadpath) {
    //cout << "DBFileSorted::Load" << endl;
    //so.Print();
    is_write = true; //set current state to writing 
    if (is_read) {   //check if reading and we need to set up our pipe
        is_read = false; //unset reading
        if(!init) { //if bigQ isnt set up
            input = new Pipe(pipeBufferSize, "Input");
            output = new Pipe(pipeBufferSize, "Output");            
            bigQ = new BigQ(*input, *output, so, runlen);
            pthread_t pt = bigQ->getpt();
            init = true;
        }        
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
  
    contQuery = false; //reset this for the GetNext function
}

void DBFileSorted::Add (Record &rec) {
    //so.Print();
    // exit(1);
    is_write = true; //set current state to writing
    
    if (is_read) {   //check if reading and we need to set up our pipe
        is_read = false; //unset reading
        if(!init) { //if bigQ isnt set up
            cout << "DBFileSorted::Add - Creating BigQ" << endl;
            input = new Pipe(pipeBufferSize, "Input");
            output = new Pipe(pipeBufferSize, "Output"); 
            so.Print();  
            cout << "DBFileSorted::Add - runlen: " << runlen << endl;
            bigQ = new BigQ(*input, *output, so, runlen);
            pthread_t pt = bigQ->getpt();
            init = true;
        }        
    }
    
    input->Insert(&rec); //write the record to the pipe  
}

int DBFileSorted::GetNext (Record &fetchme) {

    if (is_write) {
        MergeInternal();
    }
    is_read = true;
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
    //commented out for testing
    // if (is_write) {
    //     MergeInternal();
    // }
    
    bool recordFound = false;
    ComparisonEngine comp;
    Record temp;

    //contQuery is here to check for back to back calls to this function.
    //This means that we do not need to redo our Query build, or our search for first
    if (!contQuery) {
    
        int numAtts = so.GetNumAtts(); //get our DBFile sort order information
        int *whichAtts = so.GetWhichAtts(); //get our DBFile sort order information
        Type *whichTypes = so.GetWhichTypes(); //get our DBFile sort order information
        // so.Print();
        // applyMe.Print();
        // cout << "DBFileSorted::GetNext -- Starting for loop" << endl;
        for (int i = 0; i < numAtts; i++) { //loop through all of our DBFile sorted attributes
            // cout << "DBFileSorted::GetNext -- whichAtts: "<< whichAtts[i] << endl;
            // cout << "DBFileSorted::GetNext -- ApplyMeGetSub: "<< applyMe.GetSubExpressions(whichAtts[i]) << endl;
            if (applyMe.GetSubExpressions(whichAtts[i])) { //if this attribute is in the applyMe CNF
                query.AddAttr(whichTypes[i], whichAtts[i]); //Add this attribute to the query OrderMaker
            }
            else {
                break; //first attribute that is not in the CNF we need to stop
            }
        }
            
        //if the query OrderMaker has useful stuff in it
        if (query.GetNumAtts() > 0) {
            cout << "Query had useful stuff" << endl;
            query.Print();           
            // TODO: UPDATE THIS TO BE BINARY SEARCH
            while (GetNext(fetchme)) { //read from the current location until we find a match
                if (comp.Compare(&query,&fetchme,&literal) == 0) { //Only do if this returns 0 (equals to) per the project description 
                    
                    recordFound = true;
                    break; //just need to find the first record with this search
                }
            }
        }
        else { //query OrderMaker is is empty            
            MoveFirst(); //set to the first record
        }
        contQuery = true; 
    }
    
    if (contQuery) { //if we are running a back to back GetNext call or a record was found in the first call

        if (recordFound) { //this means we have a residual record that we already grabbed that we need to check before looping
            query.Print();
            if (comp.Compare(&query, &fetchme,&literal) == 0 || query.GetNumAtts() == 0) { //compare it with the query OrderMaker first
                if (comp.Compare(&fetchme,&literal,&applyMe) == 1) { //then compare it with the CNF
                    return 1;
                }
                else {
                    return 0; // 0
                }
            }
        }
        
        while (GetNext(fetchme)) { //grab the next record that satisfies the condition
            // query.Print();
            if (comp.Compare(&query, &fetchme,&literal) == 0 || query.GetNumAtts() == 0) { //compare it with the query OrderMaker first
                if (comp.Compare(&fetchme,&literal,&applyMe) == 1) { //then compare it with the CNF
                    return 1;
                }
                // else { // remove when done
                //     return 1;
                // }
            }
            else {
                return 0;// 0
            }
        }        
    }
    else {
        return 0;
    }
    
    return 0;
}

void DBFileSorted::MergeInternal() {

    is_read = true; //set the current state to reading
    is_write = false; //unset the current writing state
    input->ShutDown(); //shut down the pipe
    Record piperec; //create a record to hold records from the pipe
    Record filerec; //create a record to hold records from the file   
    ComparisonEngine ce; //init comp engine
    const char* tempFileName = "tempfileName_myFile";
    File newMyFile; //create a new file used to store our merged records
    newMyFile.Open(0,tempFileName);
    Page newMyPage; //create a new temp page used to store our merged records
    int page_counter = 0; //page counter for newMyFile
    bool contReadFile = true; //exit condition for inner while loop
    int count = 0;
    // so.Print();
    while (output->Remove (&piperec)) { //Coninuously read from the pipe
        //cout << "DBFileSorted::MergeInternal() - whileloop start" << endl; 
        count++;
        Record temp;    
        while (contReadFile) { //read from the file as long as the file value is less than the pipe value  
            int con = GetNext(filerec);
            if (con != 0) { //read the first value from the file  
                //cout << "DBFileSorted::MergeInternal() - contRead read first val" << endl;             
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
                piperec.SetNull();
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
        
        if (!piperec.isNull()){ 
            //cout << "DBFileSorted::MergeInternal() - piperec if " << endl;
            if (newMyPage.Append(&piperec) == 0) //append the current smallest from pipe to our new temp page
            {                
                //our current page is full
                //write this page out to the file
                // cout << "DBFileSorted::MergeInternal() - piperec AddPage" << endl;
                newMyFile.AddPage(&newMyPage, page_counter);
                //empty out our current page
                newMyPage.EmptyItOut();
                //increment the total number of pages
                page_counter++;
                //add the record to the newly created page
                newMyPage.Append(&piperec);
            }
            piperec.SetNull(); //clear out the temp record just in case  
        }
        
    }
    Record temp;
    while (GetNext(temp) != 0) {
        if (newMyPage.Append(&temp) == 0) //append the current smallest in file to our new temp page
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
    if (newMyPage.GetNumRecs() > 0) { //if our new temp page still has some records in it
        newMyFile.AddPage(&newMyPage,page_counter); //add that page to the end of the file
        newMyPage.EmptyItOut(); //clear out the page
    } 
    newMyFile.Close();
    newMyFile.Open(1, tempFileName);
    myFile.~File(); //clean our current file to prepare to be overwritten   
    myFile = newMyFile; //set our myFile to our newly created merged File
    newMyFile.~File(); //destroy our temp File
    // newMyPage.~Page();

    remove(f_name);    
    std::rename(tempFileName, f_name);
    remove(tempFileName);
}