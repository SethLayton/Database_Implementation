#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

typedef enum {heap, sorted, tree} fType;



class GenericDBFile {
protected:
	TwoWayList <Record> *myRecs;
	Page myPage;
	File myFile;
	fType myType;
	bool is_write = false;
	bool is_read = false;
	int curr_page = 0;

public:
	GenericDBFile () {}; 
	virtual ~GenericDBFile() {};
	virtual int Create (const char *fpath, fType file_type, void *startup) = 0;
	virtual int Open (const char *fpath) = 0;
	virtual void Load (Schema &myschema, const char *loadpath) = 0;
	virtual int Close () = 0;
	virtual void MoveFirst () = 0;
	virtual void Add (Record &addme) = 0;
	virtual int GetNext (Record &fetchme) = 0;
	virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal) = 0;
	virtual void cleanup() = 0;
};

class DBFile {
private:

	fType myType;
	GenericDBFile *myInteralClass = NULL;

public:
	DBFile (); 
	int Create (const char *fpath, fType file_type, void *startup);
	int Open (const char *fpath);
	int Close ();
	void Load (Schema &myschema, const char *loadpath);
	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);
	void CreateSubClass(fType inputType, int &runlength, OrderMaker &so);
	void cleanup();

	typedef struct {
		int runlen;
		OrderMaker *order;
	}sortutil;

};

#endif
