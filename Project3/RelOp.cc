#include "RelOp.h"

void* thread_starter(void* obj) {
	threadutil *t = (threadutil *) obj;

	switch (t-> _class)	{

		case selectsipe:
		{
			SelectPipe* threadSP = static_cast<SelectPipe*>(t->context);
			threadSP->DoWork();
			break;
		}
		case selectfile:
		{
			SelectFile* threadSF = static_cast<SelectFile*>(t->context);
			threadSF->DoWork();
			break;
		}
		case project:
		{
			Project* threadP = static_cast<Project*>(t->context);
			threadP->DoWork();
			break;
		}
		case join:
		{
			Join* threadJ = static_cast<Join*>(t->context);
			threadJ->DoWork();
			break;
		}
		case duplicateremoval:
		{
			DuplicateRemoval* threadDR = static_cast<DuplicateRemoval*>(t->context);
			threadDR->DoWork();
			break;
		}
		case sum:
		{
			Sum* threadS = static_cast<Sum*>(t->context);
			threadS->DoWork();
			break;
		}
		case groupby:
		{
			GroupBy* threadGB = static_cast<GroupBy*>(t->context);
			threadGB->DoWork();
			break;
		}
		case writeout:
		{
			WriteOut* threadWO = static_cast<WriteOut*>(t->context);
			threadWO->DoWork();
			break;
		}
		
		default:
			break;
	}

	return nullptr;
}

/* #region  SelectFile */
void SelectFile::Run(DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal)
{

	//initialize starting values
	dbfile = &inFile;
	out = &outPipe;
	op = selOp;
	lit = literal;
	thread = pthread_t();
	//create the thread util to pass to the starter
	threadutil tutil = {selectfile, this};
	//create thread
	pthread_create(&thread, NULL, thread_starter, (void *)&tutil);

}

void* SelectFile::DoWork() {

	Record temp;
	//scan all the records in the dbfile
	//only grabbing those where the CNF op
	//equates to true
	while (dbfile->GetNext(temp, op, lit)) {
		//insert the selected record into the pipe
		out->Insert(&temp);
		//for sanity clear out the temp record
		temp.SetNull();
	}
	//shutdown the output pipe
	out->ShutDown();
	//exit the thread
	pthread_exit(NULL);	
}

void SelectFile::WaitUntilDone()
{
	pthread_join (thread, NULL);
}

void SelectFile::Use_n_Pages(int runlen)
{
}
/* #endregion */

/* #region  SelectPipe */
void SelectPipe::Run(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {

	//initialize starting values
	in = &inPipe;
	out = &outPipe;
	op = selOp;
	lit = literal;
	thread = pthread_t();
	//create the thread util to pass to the starter
	threadutil tutil = {selectsipe, this};
	//create thread
	pthread_create(&thread, NULL, thread_starter, (void *)&tutil);

}

void* SelectPipe::DoWork() {

	ComparisonEngine ce;
	Record temp;
	//read everything from the given input pipe
	while (in->Remove(&temp)) {
		//compare the record to the given CNF and (I think)
		//if its not 0 then it is 'accepted' by the CNF and
		//we add it to the given output pipe
		if (ce.Compare(&temp,&lit,&op) != 0) {
			out->Insert(&temp);
		}
		//even though temp is 'cleared out' do this 
		//for sanity
		temp.SetNull();
	}
	//shutdown the thread
	pthread_exit(NULL);	
}

void SelectPipe::WaitUntilDone()
{
	pthread_join (thread, NULL);
}

void SelectPipe::Use_n_Pages(int runlen)
{
}
/* #endregion */

/* #region  Project */
void Project::Run(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput)
{

	thread = pthread_t();
	threadutil tutil = {project, this};
	//create thread and initialize starting values
	pthread_create(&thread, NULL, thread_starter, (void *)&tutil); //actually create the thread

}

void* Project::DoWork() {

	pthread_exit(NULL);	
}

void Project::WaitUntilDone()
{
	pthread_join (thread, NULL);
}

void Project::Use_n_Pages(int runlen)
{
}
/* #endregion */

/* #region  Join */
void Join::Run(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal)
{

	thread = pthread_t();
	threadutil tutil = {join, this};
	//create thread and initialize starting values
	pthread_create(&thread, NULL, thread_starter, (void *)&tutil); //actually create the thread

}

void* Join::DoWork() {

	pthread_exit(NULL);	
}

void Join::WaitUntilDone()
{
	pthread_join (thread, NULL);
}

void Join::Use_n_Pages(int runlen)
{
}
/* #endregion */

/* #region  DuplicateRemoval */
void DuplicateRemoval::Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema)
{

	thread = pthread_t();
	threadutil tutil = {duplicateremoval, this};
	//create thread and initialize starting values
	pthread_create(&thread, NULL, thread_starter, (void *)&tutil); //actually create the thread

}

void* DuplicateRemoval::DoWork() {

	pthread_exit(NULL);	
}

void DuplicateRemoval::WaitUntilDone()
{
	pthread_join (thread, NULL);
}

void DuplicateRemoval::Use_n_Pages(int runlen)
{
}
/* #endregion */

/* #region  Sum */
void Sum::Run(Pipe &inPipe, Pipe &outPipe, Function &computeMe)
{

	thread = pthread_t();
	threadutil tutil = {sum, this};
	//create thread and initialize starting values
	pthread_create(&thread, NULL, thread_starter, (void *)&tutil); //actually create the thread

}

void* Sum::DoWork() {

	pthread_exit(NULL);	
}

void Sum::WaitUntilDone()
{
	pthread_join (thread, NULL);
}

void Sum::Use_n_Pages(int runlen)
{
}
/* #endregion */

/* #region  GroupBy */
void GroupBy::Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe)
{

	thread = pthread_t();
	threadutil tutil = {groupby, this};
	//create thread and initialize starting values
	pthread_create(&thread, NULL, thread_starter, (void *)&tutil); //actually create the thread

}

void* GroupBy::DoWork() {

	pthread_exit(NULL);	
}

void GroupBy::WaitUntilDone()
{
	pthread_join (thread, NULL);
}

void GroupBy::Use_n_Pages(int runlen)
{
}
/* #endregion */

/* #region  WriteOut */
void WriteOut::Run(Pipe &inPipe, FILE *outFile, Schema &mySchema)
{

	//initialize starting values
	in = &inPipe;
	file = outFile;
	schema = &mySchema;
	thread = pthread_t();
	//create struct to pass to thread starter
	threadutil tutil = {writeout, this};
	//create thread
	pthread_create(&thread, NULL, thread_starter, (void *)&tutil);

}

void* WriteOut::DoWork() {
	
	Record temp;
	//read all the records from the input pipe
	while (in->Remove(&temp)) {
		//write the record out to the specified stream
		temp.Print(schema, file);
		//for sanity set the temp record to null
		temp.SetNull();
	}
	//exit thread
	pthread_exit(NULL);	
}

void WriteOut::WaitUntilDone()
{
	pthread_join (thread, NULL);
}

void WriteOut::Use_n_Pages(int runlen)
{
}
/* #endregion */




