#ifndef DBFILEHEAP_H
#define DBFILEHEAP_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"


class DBFileHeap : public GenericDBFile {
public:
	DBFileHeap () {}; 
    int Create (const char *fpath, fType file_type, void *startup) override;
	int Open (const char *fpath) override;
    void Load (Schema &myschema, const char *loadpath) override;
	int Close () override;
	void MoveFirst () override;
	void Add (Record &addme) override;
	int GetNext (Record &fetchme) override;
	int GetNext (Record &fetchme, CNF &cnf, Record &literal) override;
	void cleanup() override {};

};

#endif