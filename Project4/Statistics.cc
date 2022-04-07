#include "Statistics.h"

Statistics::Statistics()
{
}
Statistics::Statistics(Statistics &copyMe)
{
}
Statistics::~Statistics()
{
}

void Statistics::AddRel(std::string relName, int numTuples)
{
}
void Statistics::AddAtt(std::string relName, std::string attName, int numDistincts)
{
}
void Statistics::CopyRel(std::string ldName, std::string newName)
{
}
	
void Statistics::Read(std::string fromWhere)
{
}
void Statistics::Write(std::string fromWhere)
{
}

void  Statistics::Apply(struct AndList *parseTree, std::string relNames[], int numToJoin)
{
}
double Statistics::Estimate(struct AndList *parseTree, std::string *relNames, int numToJoin)
{
}

