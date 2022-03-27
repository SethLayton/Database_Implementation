#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"
#include "Defs.h"

class RelationalOp {
	public:
	// blocks the caller until the particular relational operator 
	// has run to completion
	virtual void WaitUntilDone () = 0;

	// tell us how much internal memory the operation can use
	virtual void Use_n_Pages (int n) = 0;
};

class SelectFile : public RelationalOp { 

	private:
		pthread_t thread = pthread_t();
		DBFile & dbfile;
		Pipe & out;
		CNF & op;
		Record & lit;
	// pthread_t thread;
	// Record *buffer;

	public:

		void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
		void WaitUntilDone ();
		void Use_n_Pages (int n);
		void *DoWork();

};

class SelectPipe : public RelationalOp {
	private:
		pthread_t thread = pthread_t();
		Pipe & in;
		Pipe & out;
		CNF & op;
		Record & lit;
	public:
		void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
		void WaitUntilDone () { }
		void Use_n_Pages (int n) { }
		void *DoWork();
};

class Project : public RelationalOp { 
	private:
		pthread_t thread = pthread_t();
	public:
		void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) { }
		void WaitUntilDone () { }
		void Use_n_Pages (int n) { }
		void *DoWork();
};

class Join : public RelationalOp { 
	private:
		pthread_t thread = pthread_t();
	public:
		void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) { }
		void WaitUntilDone () { }
		void Use_n_Pages (int n) { }
		void *DoWork();
};

class DuplicateRemoval : public RelationalOp {
	private:
		pthread_t thread = pthread_t();
	public:
		void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema) { }
		void WaitUntilDone () { }
		void Use_n_Pages (int n) { }
		void *DoWork();
};

class Sum : public RelationalOp {
	private:
		pthread_t thread = pthread_t();
	public:
		void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe) { }
		void WaitUntilDone () { }
		void Use_n_Pages (int n) { }
		void *DoWork();
};

class GroupBy : public RelationalOp {
	private:
		pthread_t thread = pthread_t();
	public:
		void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) { }
		void WaitUntilDone () { }
		void Use_n_Pages (int n) { }
		void *DoWork();
};

class WriteOut : public RelationalOp {
	private:
		pthread_t thread = pthread_t();
		Pipe & in;
		FILE* file;
		Schema & schema;
	public:
		void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) { }
		void WaitUntilDone () { }
		void Use_n_Pages (int n) { }
		void *DoWork();
};

typedef struct {
	int _class;
	void *context;
}threadutil;

void* thread_starter(void* obj);
#endif
