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

void DBFile::CreateSubClass(fType type, int &runlength, OrderMaker &so) {
    if (type == sorted) {
        myInteralClass = new DBFileSorted(runlength, so);
    }
    else if (type == heap) {
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
        if (myType == sorted) {
            sortutil su = *((sortutil*)startup);
            OrderMaker om = *(OrderMaker *)su.order;
            mf << su.runlen << endl;
            mf << om << endl;
            CreateSubClass(myType, su.runlen, om);
        }
        else {
            int run = -1;
            OrderMaker om;
            CreateSubClass(myType, run, om);
        }
        mf.close();
        
        //set up the virtual base class to be the right type of DBFile
        

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
        int runlength = -1;
        OrderMaker so;
        std::string metafile = f_path;
        metafile.append(".meta");
        ifstream mf;
        mf.open(metafile);
        if(!mf) {
            cout << "No metadata file found for: " << metafile << endl;
            return 1;
        }
        //read in the type from the first line of the fiel
        mf >> type;
        myType = (fType) type; //convert that value to a type
        if (myType == sorted) {
            mf >> runlength;
            mf >> so;
        }
        mf.close();
        //set up the virtual base class to be the right type of DBFile
        CreateSubClass(myType, runlength, so);
       
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