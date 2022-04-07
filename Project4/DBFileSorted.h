#ifndef DBFILESORTED_H
#define DBFILESORTED_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "BigQ.h"


class DBFileSorted : public GenericDBFile {
private:
    int  runlen;
    OrderMaker so;
    
    bool is_write = false;
    bool is_read = false;
    int pipeBufferSize = 100;
    Pipe * input; 
    Pipe * output;
    const char* f_name;   
    bool init = false;
    BigQ * bigQ; 
    bool contQuery = false;
    OrderMaker query;

public:
    DBFileSorted (int &runlength, OrderMaker &so);  
    int Create (const char *fpath, fType file_type, void *startup) override;
	int Open (const char *fpath) override;
    void Load (Schema &myschema, const char *loadpath) override;
	int Close () override;
	void MoveFirst () override;
	void Add (Record &addme) override;
	int GetNext (Record &fetchme) override;
	int GetNext (Record &fetchme, CNF &cnf, Record &literal) override;
    void MergeInternal();

};

#endif