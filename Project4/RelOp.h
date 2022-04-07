#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"
#include "Defs.h"

typedef struct {
	int _class;
	void *context;
}threadutil;

class RelationalOp {
	public:
	RelationalOp() {};
	// blocks the caller until the particular relational operator 
	// has run to completion
	virtual void WaitUntilDone () = 0;

	// tell us how much internal memory the operation can use
	virtual void Use_n_Pages (int n) = 0;
};

class SelectFile : public RelationalOp { 

	private:
		pthread_t thread = pthread_t();
		DBFile& dbfile;
		Pipe& out;
		CNF& op;
		Record& lit;
		bool first;
		int intfirst;
		threadutil tutil = {selectfile, this};
	public:
		SelectFile(DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
		void WaitUntilDone ();
		void Use_n_Pages (int n);
		void *DoWork();
};

class SelectPipe : public RelationalOp {
	private:
		pthread_t thread = pthread_t();
		Pipe& in;
		Pipe& out;
		CNF op;
		Record lit;
		threadutil tutil = {selectpipe, this};
	public:
		SelectPipe(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
		void WaitUntilDone ();
		void Use_n_Pages (int n);
		void *DoWork();
};

class Project : public RelationalOp { 
	private:
		pthread_t thread = pthread_t();
		Pipe& in;
		Pipe& out;
		int* indexLocations;
		int numInput;
		int numOutput;
		int runlength;
		threadutil tutil = {project, this};
 	public:
	 	Project(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput, int pages);
		void WaitUntilDone ();
		void Use_n_Pages (int n);
		void *DoWork();
};

class Join : public RelationalOp { 
	private:
		Pipe& inL;
		Pipe& inR;
		Pipe& out;
		CNF& op;
		Record& lit;
		pthread_t thread = pthread_t();
		int npage;
		threadutil tutil = {join, this};
	public:
		Join(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal, int);
		void WaitUntilDone ();
		void Use_n_Pages (int n);
		void *DoWork();
};

class DuplicateRemoval : public RelationalOp {
	private:
		pthread_t thread = pthread_t();
		Pipe& in;
		Pipe& out;
		Schema& schema;
		int runlength = 0;
		threadutil tutil = {duplicateremoval, this};
	public:
		DuplicateRemoval(Pipe &inPipe, Pipe &outPipe, Schema &mySchema, int pages);
		void WaitUntilDone ();
		void Use_n_Pages (int n);
		void *DoWork();
};

class Sum : public RelationalOp {
	private:
		pthread_t thread = pthread_t();
		Pipe& in;
		Pipe& out;
		Function& func;
		int runlength;
		threadutil tutil = {sum, this};
	public:
		Sum(Pipe &inPipe, Pipe &outPipe, Function &computeMe, int in_pages);
		void WaitUntilDone ();
		void Use_n_Pages (int n);
		void *DoWork();
};

class GroupBy : public RelationalOp {
	private:
		pthread_t thread = pthread_t();
		Pipe& in;
		Pipe& out;
		OrderMaker& groups;
		Function& func;
		int runlength = 0;
		threadutil tutil = {groupby, this};
	public:
		GroupBy(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe, int in_pages);
		void WaitUntilDone ();
		void Use_n_Pages (int n);
		void *DoWork();
};

class WriteOut : public RelationalOp {
	private:
		pthread_t thread = pthread_t();
		Pipe& in;
		FILE* file;
		Schema& schema;
		threadutil tutil = {writeout, this};
	public:
		WriteOut(Pipe &inPipe, FILE *outFile, Schema &mySchema);
		void WaitUntilDone ();
		void Use_n_Pages (int n);
		void *DoWork();
};

void* thread_starter(void* obj);
#endif
