#include "RelOp.h"

void* thread_starter(void* obj) {
	threadutil *t = (threadutil *) obj;

	switch (t-> _class)	{

		case selectsipe:
			SelectPipe* thread = static_cast<SelectPipe*>(t->context);
			thread->DoWork();
			break;
		case selectfile:
			SelectFile* thread = static_cast<SelectFile*>(t->context);
			thread->DoWork();
			break;
		case project:
			Project* thread = static_cast<Project*>(t->context);
			thread->DoWork();
			break;
		case join:
			Join* thread = static_cast<Join*>(t->context);
			thread->DoWork();
			break;
		case duplicateremoval:
			DuplicateRemoval* thread = static_cast<DuplicateRemoval*>(t->context);
			thread->DoWork();
			break;
		case sum:
			Sum* thread = static_cast<Sum*>(t->context);
			thread->DoWork();
			break;
		case groupby:
			GroupBy* thread = static_cast<GroupBy*>(t->context);
			thread->DoWork();
			break;
		case writeout:
			WriteOut* thread = static_cast<WriteOut*>(t->context);
			thread->DoWork();
			break;
		
		default:
			break;
	}

	return nullptr;
}

/* #region  SelectFile */
void SelectFile::Run(DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal)
{

	thread = pthread_t();
	threadutil tutil = {selectfile, this};
	//create thread and initialize starting values
	pthread_create(&thread, NULL, thread_starter, (void *)&tutil); //actually create the thread

}

void* SelectPipe::DoWork() {
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
void SelectPipe::Run(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal)
{

	thread = pthread_t();
	threadutil tutil = {selectsipe, this};
	//create thread and initialize starting values
	pthread_create(&thread, NULL, thread_starter, (void *)&tutil); //actually create the thread

}

void* SelectPipe::DoWork() {
	
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

void* SelectPipe::DoWork() {
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

void* SelectPipe::DoWork() {
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

void* SelectPipe::DoWork() {
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

void* SelectPipe::DoWork() {
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

void* SelectPipe::DoWork() {
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

	thread = pthread_t();
	threadutil tutil = {writeout, this};
	//create thread and initialize starting values
	pthread_create(&thread, NULL, thread_starter, (void *)&tutil); //actually create the thread

}

void* SelectPipe::DoWork() {
}

void WriteOut::WaitUntilDone()
{
	pthread_join (thread, NULL);
}

void WriteOut::Use_n_Pages(int runlen)
{
}
/* #endregion */




