#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include <string>
#include <unordered_map>
#include <iostream>

using namespace std;

typedef struct {
	std::string name;
	int numDistincts;
}addatt;

typedef struct {
	std::string name;
	int numTuples;
	std::unordered_map<std::string, addatt> atts;
}addrel;

class Statistics {
private:
	std::unordered_map<std::string, addrel> rels;
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

	void printRels();

};

#endif
