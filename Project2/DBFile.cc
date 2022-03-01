#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "DBFileSorted.h"
#include "DBFileHeap.h"
#include "Defs.h"
#include <iostream>
#include <fstream>
#include <cstring>


DBFile::DBFile () {}

void DBFile::CreateSubClass(fType type) {
    if (type == sorted) {
        cout << "Creating DBFileSorted" << endl;
        myInteralClass = new DBFileSorted();
    }
    else if (type == heap) {
        cout << "Creating DBFileHeap" << endl;
        myInteralClass = new DBFileHeap();
    }
    else if (type == tree) {
        //For future use
    }
    else {
        cerr << "Unknown file type" << endl;
        throw(1);
    }
}

int DBFile::Create (const char *f_path, fType f_type, void *startup) {

    try
    {
        //set the type as passed in
        myType = f_type;
        std::string metafile = f_path;
        metafile.append(".meta");
        ofstream mf;
        mf.open(metafile, fstream::in | fstream::out | fstream::trunc); 
        //write out the type to the first line of the file
        mf << myType << endl;
        mf.close();
        
        //set up the virtual base class to be the right type of DBFile
        CreateSubClass(myType);
       
        return myInteralClass->Create(f_path,f_type,startup);
    }
    catch(const std::exception& e)
    {
        //there was some error, return fail
        return 0;
    }      
}

void DBFile::Load (Schema &f_schema, const char *loadpath) {
    myInteralClass->Load(f_schema, loadpath);
}

int DBFile::Open (const char *f_path) {

    try
    {
        //open the meta file
        int type;
        std::string metafile = f_path;
        metafile.append(".meta");
        ifstream mf;
        mf.open(metafile);
        if(!mf) return 1;

        //read in the type from the first line of the fiel
        mf >> type;
        myType = (fType) type; //convert that value to a type
        mf.close();
        //set up the virtual base class to be the right type of DBFile
        CreateSubClass(myType);
       
        return myInteralClass->Open(f_path);
    }
    catch(const std::exception& e)
    {
        //there was some error, return fail
        return 0;
    }    
}

void DBFile::MoveFirst () {

    myInteralClass->MoveFirst();
}

int DBFile::Close () {

    return myInteralClass->Close();
}

void DBFile::Add (Record &rec) {

    myInteralClass->Add(rec);    
}

int DBFile::GetNext (Record &fetchme) {

    return myInteralClass->GetNext(fetchme);
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {

    return myInteralClass->GetNext(fetchme, cnf, literal);
}
