#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include <string>


class Statistics {
private:

public:
	Statistics();
	Statistics(Statistics &copyMe);	 // Performs deep copy
	~Statistics();


	void AddRel(std::string relName, int numTuples);
	void AddAtt(std::string relName, std::string attName, int numDistincts);
	void CopyRel(std::string oldName, std::string newName);
	
	void Read(std::string fromWhere);
	void Write(std::string fromWhere);

	void  Apply(struct AndList *parseTree, std::string relNames[], int numToJoin);
	double Estimate(struct AndList *parseTree, std::string *relNames, int numToJoin);

};

#endif
