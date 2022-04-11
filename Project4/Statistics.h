#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <iostream>
#include <vector>

using namespace std;

typedef struct {
	std::string name;
	int numDistincts;
}att;

typedef struct {
	std::string name;
	int numTuples;
	std::unordered_map<std::string, att> atts;
}rel;

class Statistics {
private:
	std::unordered_map<std::string, rel> rels;
	std::unordered_map<std::string, vector<rel>> subsets;
	std::unordered_map<std::string, std::string> att_to_rel;
	int maxSubsetSize = 0;
	void CheckTree(AndList* parseTree, std::string *relNames, int numToJoin); 
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
