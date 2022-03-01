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
    int runlen;
    OrderMaker so;
    BigQ * bigQ;
    bool is_write;
    Pipe * input = new Pipe(100);
    Pipe * output = new Pipe (100);
    int pipeBufferSize = 100;

public:
	DBFileSorted (): GenericDBFile() {};
    DBFileSorted (int runlength, OrderMaker so);  
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