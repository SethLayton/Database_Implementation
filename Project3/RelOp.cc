#include "RelOp.h"
#include <bits/stdc++.h>
#include <algorithm>
#include "BigQ.h"

typedef void * (*THREADFUNCPTR)(void *);

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
SelectFile::SelectFile(DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal, bool in_first): dbfile(inFile), out(outPipe), op(selOp), lit(literal) {
	dbfile.MoveFirst();
	first = in_first;
	Run();
}

void SelectFile::Run() {
	thread =  pthread_t();
	//create the thread util to pass to the starter
	if (first) {
		static threadutil tutil1 = { selectfile, this};
		pthread_create(&thread, NULL, thread_starter, &tutil1);
	} else {
		static threadutil tutil2 = {selectfile, this};
		pthread_create(&thread, NULL, thread_starter, &tutil2);
	}
}

void* SelectFile::DoWork() {
	Record temp;
	//scan all the records in the dbfile
	//only grabbing those where the CNF op
	//equates to true
	int counter = 0;
	while (dbfile.GetNext(temp, op, lit)) {
		//insert the selected record into the pipe
		out.Insert(&temp);
		//for sanity clear out the temp record
		temp.SetNull();
		counter++;
	}
	//shutdown the output pipe
	out.ShutDown();
	//exit the thread
	pthread_exit(NULL);	
}

void SelectFile::WaitUntilDone() {
	pthread_join (thread, NULL);
}

void SelectFile::Use_n_Pages(int runlen) {
}
/* #endregion */

/* #region  SelectPipe */
SelectPipe::SelectPipe(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal): in(inPipe), out(outPipe), op(selOp), lit(literal) {
	Run();
}

void SelectPipe::Run() {
	thread = pthread_t();
	//create the thread util to pass to the starter
	static threadutil tutil = {selectsipe, this};
	//create thread
	pthread_create(&thread, NULL, thread_starter, &tutil);

}

void* SelectPipe::DoWork() {

	ComparisonEngine ce;
	Record temp;
	//read everything from the given input pipe
	while (in.Remove(&temp)) {
		//compare the record to the given CNF and (I think)
		//if its not 0 then it is 'accepted' by the CNF and
		//we add it to the given output pipe
		if (ce.Compare(&temp,&lit,&op) != 0) {
			out.Insert(&temp);
		}
		//even though temp is 'cleared out' do this 
		//for sanity
		temp.SetNull();
	}
	//shutdown the output pipe
	out.ShutDown();
	//shutdown the thread
	pthread_exit(NULL);	
}

void SelectPipe::WaitUntilDone() {

	pthread_join (thread, NULL);
}

void SelectPipe::Use_n_Pages(int runlen) {
}
/* #endregion */

/* #region  Project */
Project::Project(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput, int in_pages): in(inPipe), out(outPipe) {
	indexLocations = keepMe;
	numInput = numAttsInput;
	numOutput = numAttsOutput;
	runlength = in_pages;
	Run();
}

void Project::Run() {
	thread = pthread_t();
	//create thread struct to pass to thread starter for initialization
	static threadutil tutil = {project, this};
	//create thread
	pthread_create(&thread, NULL, thread_starter, &tutil);

}

void* Project::DoWork() {

	Record temp;	
	while (in.Remove(&temp)) {
		//do the projection
		temp.Project(indexLocations, numOutput, numInput);
		//place the projected record into the output pipe
		out.Insert(&temp);
		//for sanity clear out the temp record
		temp.SetNull();
	}
	//shutdown the output pipe
	out.ShutDown();
	//exit the thread
	pthread_exit(NULL);	
}

void Project::WaitUntilDone() {
	pthread_join (thread, NULL);
}

void Project::Use_n_Pages(int runlen) {
}
/* #endregion */

/* #region  Join */
Join::Join(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal): inL(inPipeL), inR(inPipeR), out(outPipe), op(selOp), lit(literal) {//, sL(schemaL), sR(schemaR) {
	npage = 10;
	Run();
	
}

void Join::Run() {
	thread = pthread_t();
	static threadutil tutil = {join, this};
	//create thread and initialize starting values
	pthread_create(&thread, NULL, thread_starter, &tutil); //actually create the thread

}

void* Join::DoWork() {
	//--CREATE ORDERMAKER
	OrderMaker omR;
	OrderMaker omL;
	int sortFound = op.GetSortOrders(omL, omR);
	
	//--INSTANTIATE TWO BIGQs
	int pipeBufferSize = 200;
	//We need two output pipes for each BigQ
	Pipe* outputL;
	outputL = new Pipe(pipeBufferSize);

	Pipe* outputR;
	outputR =  new Pipe(pipeBufferSize);
	//Temp Pipe for stray records
	Pipe* outR_temp;
	outR_temp =  new Pipe(pipeBufferSize);
	Pipe* inR_temp;
	inR_temp = new Pipe(pipeBufferSize);

	Pipe* outL_temp;
	outL_temp =  new Pipe(pipeBufferSize);
	Pipe* inL_temp;
	inL_temp = new Pipe(pipeBufferSize);
	
	//Create the BigQs
	BigQ bigQR (inR, *outputR, omR, npage);
	BigQ bigQL (inL, *outputL, omL, npage+10);


	//--JOIN
	ComparisonEngine ce;
	Record tempR;
	Record prevR;
	Record tempL;
	Record merge;
	//Define Num attributes

	int lAtt =-1;
	int rAtt = -1;

	//TODO: Remove the attribute of the key we are joining on
	bool tempPopulated = false;
	int totAtt = -1;
	int *atts = new int[100];
	int startR = -1;

	if (sortFound == 0) {
		cout << "No sort order found" << endl;
		int counter = 0;
		
		while(outputL->Remove(&tempL)) {
			while (outR_temp->Remove(&tempR)) {
				if (totAtt == -1){
					
					lAtt = tempL.GetNumAtts();
					rAtt = tempR.GetNumAtts();
					totAtt = lAtt + rAtt;
					int* newArr = new int[totAtt];
					delete [] atts;
					atts = newArr;
					startR = lAtt;
					for (int i = 0; i < lAtt; i++) {
						atts[i] = i;
					}
					for (int i = 0; i < rAtt; i++) {
						atts[i+lAtt] = i;
					}
				}
				merge.MergeRecords(&tempL, &tempR,lAtt, rAtt, atts, totAtt, startR );
				out.Insert(&merge);
			}
		}

	}
	//Case 2: OM is meaningful
	//Joining right to left, thus outer loop is left
	else {
		cout << " - sort Order found" << endl;
		while (outputL->Remove(&tempL)) {
			Schema S("catalog", "supplier");
			if (tempPopulated) {
				tempR.Consume(&prevR);
				int result = ce.Compare(&tempL, &omL, &tempR, &omR);
				if (result ==0) {
					merge.MergeRecords(&tempL, &tempR,lAtt, rAtt, atts, totAtt, startR );
					out.Insert(&merge);
				}
				tempPopulated = false;
			}
			//After temp pipe is empty, go to right BigQ
			while (outputR->Remove(&tempR)){
				

				if (totAtt == -1){
					lAtt = tempL.GetNumAtts();
					rAtt = tempR.GetNumAtts();
					totAtt = lAtt + rAtt;
					startR = lAtt;
					for (int i = 0; i < lAtt; i++) {
						atts[i] = i;
					}
					for (int i = 0; i < rAtt; i++) {
						atts[i+lAtt] = i;
					}
				}
				int result = ce.Compare(&tempL, &omL, &tempR, &omR);
				if ( result > 0) {
					continue;
				} else if (result < 0) {
					// Possibly add moving this record to the temp Q
					tempPopulated = true;
					prevR.Consume(&tempR);
					break;
				}
				else {
					merge.MergeRecords(&tempL, &tempR,lAtt, rAtt, atts, totAtt, startR );
					out.Insert(&merge);
					
				}
				
			}
		}
	}
	out.ShutDown();
	pthread_exit(NULL);	
	
}

void Join::WaitUntilDone()
{
	pthread_join (thread, NULL);
}

void Join::Use_n_Pages(int runlen)
{
	npage = runlen;
}


/* #endregion */

/* #region  DuplicateRemoval */
DuplicateRemoval::DuplicateRemoval(Pipe &inPipe, Pipe &outPipe, Schema &mySchema, int in_pages): in(inPipe), out(outPipe), schema(mySchema) {
	runlength = in_pages;
	Run();
}

void DuplicateRemoval::Run() {
	thread = pthread_t();
	//create thread util to pass to thread starter
	static threadutil tutil = {duplicateremoval, this};
	//create thread
	pthread_create(&thread, NULL, thread_starter, &tutil); 

}

void* DuplicateRemoval::DoWork() {

	Record temp;
	//create output pipe for out BigQ run
	Pipe* output;
	int pipeBufferSize = 100;
	output = new Pipe(pipeBufferSize); 
	//create an ordermaker object for us to build
	OrderMaker tempOrder;
	int numAtts = schema.GetNumAtts();
	Attribute* atts = schema.GetAtts();
	//convert each attribute in the schema into
	//an attribute of the ordermaker
	for (int i = 0; i < numAtts; i++) {
		tempOrder.AddAttr(atts[i].myType, i);
	}
	//create and start the BigQ class to start
	//consuming records from the in Pipe
	//and placing them into the output Pipe  
	BigQ bigQ (in, *output, tempOrder, runlength);
	//create a comparison engine object
	ComparisonEngine ce;
	//create a record to store the previous record
	//used for comparison
	Record prev;
	bool init = false;
	while (output->Remove(&temp)) {
		if (!init) {
			prev.Copy(&temp);
			out.Insert(&temp);
			init = true;
		}
		else {
			if (ce.Compare(&prev, &temp, &tempOrder) != 0) {
				prev.Copy(&temp);
				out.Insert(&temp);
			}
		}
	}	
	//shutdown output pipe
	out.ShutDown();
	//exit thread
	pthread_exit(NULL);	
}

void DuplicateRemoval::WaitUntilDone() {
	pthread_join (thread, NULL);
}

void DuplicateRemoval::Use_n_Pages(int runlen) {
	runlength = runlen;
}
/* #endregion */

/* #region  Sum */
Sum::Sum(Pipe &inPipe, Pipe &outPipe, Function &computeMe, int in_pages): in(inPipe), out(outPipe), func(computeMe) {
	runlength = in_pages;
	Run();
}

void Sum::Run() {
	thread = pthread_t();
	//create thread helper struct
	static threadutil tutil = {sum, this};
	//create thread
	pthread_create(&thread, NULL, thread_starter, &tutil);

}

void* Sum::DoWork() {
	
	Record temp;
	//get in if the function is returning int or double
	int isInt = func.getReturnsInt();
	//set up aggregates
	int intResultsTotal = 0;
	double doubleResultsTotal = 0.0;
	//remove all records from the input pipe
	while (in.Remove(&temp)) {
		//set up intermediate results
		int intResults = 0;
		double doubleResults = 0.0;
		//apply the function to the given record
		func.Apply(temp, intResults, doubleResults);
		//update the aggregate values
		intResultsTotal += intResults;
		doubleResultsTotal += doubleResults;
		//for sanity set the temp record to null
		temp.SetNull();
	}
	//create the record object to hold the returned sum
	Record returnRecord;
	//create the attribute object to hold the type
	Attribute attr;
	//name the attribute
	const char* name = "Sum";
    attr.name = name;	
	//create the string to hold the value converted below
	std::string value = "";
	if (isInt) {
		//create the tuple that contains the aggregated int value
		//set the type
		attr.myType = Int;
		//convert the "Value" that gets strored in the record
		value = std::to_string(intResultsTotal) + "|";	
	}
	else {
		//create the tuple that contains the aggregated double value
		//set the type
		attr.myType = Double;
		//convert the "Value" that gets strored in the record
		value = std::to_string(doubleResultsTotal) + "|";				
	}
	
	//create the schema for this returning record
	Schema returnSchema ("out_sch", 1, &attr);
	//create the record
	returnRecord.ComposeRecord(&returnSchema, value.c_str());
	//put that record into the output pipe
	out.Insert(&returnRecord);
	//shut down output pipe
	out.ShutDown();
	//exit thread
	pthread_exit(NULL);	
}

void Sum::WaitUntilDone() {
	pthread_join (thread, NULL);
}

void Sum::Use_n_Pages(int runlen) {
}
/* #endregion */

/* #region  GroupBy */
GroupBy::GroupBy(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe, int in_pages): in(inPipe), out(outPipe), groups(groupAtts), func(computeMe) {
	runlength = in_pages;
	Run();
}

void GroupBy::Run() {

	thread = pthread_t();
	//initialize the thread starter util
	static threadutil tutil = {groupby, this};
	//create thread
	pthread_create(&thread, NULL, thread_starter, &tutil);

}

void* GroupBy::DoWork() {

	Record temp;
	//get in if the function is returning int or double
	int isInt = func.getReturnsInt();
	//set up aggregates
	int intResultsTotal = 0;
	double doubleResultsTotal = 0.0;
	//set up the output pipe for the BigQ class
	//to place the sorted records into
	Pipe* output;
	int pipeBufferSize = 100;
	output = new Pipe(pipeBufferSize); 
	//create and start the BigQ class to start
	//consuming records from the in Pipe
	//and placing them into the output Pipe  
	BigQ bigQ (in, *output, groups, runlength);
	//creat a record to store previous records
	Record prev;
	//create our comparison engine object
	ComparisonEngine ce;
	//bool for tracking the first run of
	//the comparison, since prev starts
	//out as nothing it'll always fail
	//the first if statement below
	bool init = false;
	int numGroups = groups.GetNumAtts() + 1;
	//read in the values from the BigQ output pipe
	while (output->Remove(&temp)) {	
		//set up intermediate results
		int intResults = 0;
		double doubleResults = 0.0;
		if (!init) {
			prev.Copy(&temp);
			init = true;
		}
		if (ce.Compare(&temp, &prev, &groups) == 0) {
			//this record is part of the grouping
			//apply the function to the given record
			func.Apply(temp, intResults, doubleResults);
			//update the aggregate values
			intResultsTotal += intResults;
			doubleResultsTotal += doubleResults;
			init = true;
		}
		else {
			//not part of the grouping need to write out the grouping
			//and start the aggregation of the next
			//create the record object to hold the returned sum
			Record returnRecord;
			//create the attribute object to hold the type
			Attribute attr[numGroups];
			//name the attribute
			const char* name = "Sum";
			attr[0].name = name;	
			//create the string to hold the value converted below
			std::string value = "";
			if (isInt) {
				//create the tuple that contains the aggregated int value
				//set the type
				attr[0].myType = Int;
				//convert the "Value" that gets strored in the record
				value = std::to_string(intResultsTotal) + "|";	
			}
			else {
				//create the tuple that contains the aggregated double value
				//set the type
				attr[0].myType = Double;
				//convert the "Value" that gets strored in the record
				value = std::to_string(doubleResultsTotal) + "|";				
			}
			
			int *atts = groups.GetWhichAtts();
			Type* attsTypes = groups.GetWhichTypes();
			for (int i = 1; i < numGroups; i++) {
				const char* attrname = "Attr";
				std::string attrnum = std::to_string(i);
				std::string attrnameFinal( string(attrname) + attrnum );
				attr[i].name = attrnameFinal.c_str();
				attr[i].myType = attsTypes[i-1];
				value += prev.getValue(attsTypes[i-1], atts[i-1]) + "|";
			}
			//create the schema for this returning record
			Schema returnSchema ("sum_sch", numGroups, attr);
			//create the record
			returnRecord.ComposeRecord(&returnSchema, value.c_str());
			//put that record into the output pipe
			out.Insert(&returnRecord);


			//now we need to start a new aggregate for the current
			//read in record
			func.Apply(temp, intResults, doubleResults);
			//update the aggregate values
			intResultsTotal = intResults;
			doubleResultsTotal = doubleResults;

		}
		//update the previous record
		//for comparison with the next
		prev.Copy(&temp);
	}


	Record returnRecord;
	//create the attribute object to hold the type
	Attribute attr[numGroups];
	//name the attribute
	const char* name = "Sum";
	attr[0].name = name;	
	//create the string to hold the value converted below
	std::string value = "";
	if (isInt) {
		//create the tuple that contains the aggregated int value
		//set the type
		attr[0].myType = Int;
		//convert the "Value" that gets strored in the record
		value = std::to_string(intResultsTotal) + "|";	
	}
	else {
		//create the tuple that contains the aggregated double value
		//set the type
		attr[0].myType = Double;
		//convert the "Value" that gets strored in the record
		value = std::to_string(doubleResultsTotal) + "|";				
	}
	
	int *atts = groups.GetWhichAtts();
	Type* attsTypes = groups.GetWhichTypes();
	for (int i = 1; i < numGroups; i++) {
		const char* attrname = "Attr";
		std::string attrnum = std::to_string(i);
		std::string attrnameFinal( string(attrname) + attrnum );
		attr[i].name = attrnameFinal.c_str();
		attr[i].myType = attsTypes[i-1];
		value += prev.getValue(attsTypes[i-1], atts[i-1]) + "|";
	}
	//create the schema for this returning record
	Schema returnSchema ("sum_sch", numGroups, attr);
	//create the record
	returnRecord.ComposeRecord(&returnSchema, value.c_str());
	//put that record into the output pipe
	out.Insert(&returnRecord);
	//shutdown output pipe
	out.ShutDown();
	//exit thread
	pthread_exit(NULL);	
}

void GroupBy::WaitUntilDone() {
	pthread_join (thread, NULL);
}

void GroupBy::Use_n_Pages(int runlen) {
	runlength = runlen;
}
/* #endregion */

/* #region  WriteOut */
WriteOut::WriteOut(Pipe &inPipe, FILE *outFile, Schema &mySchema): in(inPipe), file(outFile), schema(mySchema) {
	file = outFile;
	Run();
}

void WriteOut::Run() {
	
	//create struct to pass to thread starter
	thread = pthread_t();	
	static threadutil tutil = {writeout, this};
	//create thread
	pthread_create(&thread, NULL, thread_starter, &tutil);
	

}

void* WriteOut::DoWork() {
	
	Record temp;
	//read all the records from the input pipe
	while (in.Remove(&temp)) {
		//temp.Print(&schema);
		//write the record out to the specified stream
		temp.Print(&schema, file);
		//for sanity set the temp record to null
		temp.SetNull();
	}
	//exit thread
	pthread_exit(NULL);	
}

void WriteOut::WaitUntilDone() {
	pthread_join (thread, NULL);
}

void WriteOut::Use_n_Pages(int runlen) {
}
/* #endregion */




