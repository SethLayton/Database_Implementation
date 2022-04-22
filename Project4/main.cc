
#include <iostream>
#include "ParseTree.h"
#include "Statistics.h"
#include "string.h"
#include <array>
using namespace std;

extern "C" {
	struct YY_BUFFER_STATE *yy_scan_string(const char*);
	int yyparse(void);   // defined in y.tab.c
}

extern struct TableList *tables;
extern struct AndList *boolean; 
extern struct NameList *groupingAtts;
extern struct NameList *attsToSelect;
extern int distinctAtts; 
extern int distinctFunc; 

void writeStat(std::string, Statistics);
std::vector<std::string> msplit(string, string);
std::unordered_map<std::string, std::string> alias_to_rel;

void PrintOperand(struct Operand *pOperand) {
	if(pOperand!=NULL) {
		cout<<pOperand->value;
	}
	else
		return;
}

void PrintComparisonOp(struct ComparisonOp *pCom) {
	if(pCom!=NULL) {
		PrintOperand(pCom->left);
		// cout << "code: " << pCom->code << endl;
		switch(pCom->code) {
			case 5:
				cout<<" < "; break;
			case 6:
				cout<<" > "; break;
			case 7:
				cout<<" = ";
		}
		PrintOperand(pCom->right);

	}
	else {
		return;
	}
}

void PrintOrList(struct OrList *pOr) {
	if(pOr !=NULL) {
		struct ComparisonOp *pCom = pOr->left;
		PrintComparisonOp(pCom);

		if(pOr->rightOr)
		{
				cout<<" OR ";
				PrintOrList(pOr->rightOr);
		}
	}
	else {
		return;
	}
}

void PrintAndList(struct AndList *pAnd) {
	if(pAnd !=NULL) {
		struct OrList *pOr = pAnd->left;
		PrintOrList(pOr);
		if(pAnd->rightAnd) {
			cout<<" AND ";
			PrintAndList(pAnd->rightAnd);
		}
	}
	else {
		return;
	}
}

std::vector<AndList*> OptimizeQuery (std::vector<AndList*> joins,  Statistics* s) {

	std::vector<AndList*> ret;
	while (joins.size() > 1) {
		AndList temp = *joins[0]; 
		std::string lAttrel(temp.left->left->left->value); //grab name of the left attribute
		std::string rAttrel(temp.left->left->right->value); //grab name of the right attribute
		std::string lAtt = msplit(lAttrel, ".").at(1);
		std::string rAtt = msplit(rAttrel, ".").at(1);
		std::string lRel = s->GetRelFromAtt(lAtt);
		std::string rRel = s->GetRelFromAtt(rAtt);
		std::string relations[] = {lRel, rRel};
		temp.rightAnd = NULL;
		double lowest_cost = s->Estimate(&temp, relations, 2);	
		cout << "estimate: " << lowest_cost << endl;	
		int index = 0;
		for (int i = 1; i < joins.size(); i++) {
			AndList temp = *joins[i];
			lAttrel = temp.left->left->left->value; //grab name of the left attribute
			rAttrel = temp.left->left->right->value; //grab name of the right attribute
			lAtt = msplit(lAttrel, ".").at(1);
			rAtt = msplit(rAttrel, ".").at(1);
			lRel = s->GetRelFromAtt(lAtt);
			rRel = s->GetRelFromAtt(rAtt);
			std::string relations[] = {lRel, rRel};
			temp.rightAnd = NULL;
			double temp_lowest_cost = s->Estimate(&temp, relations, 2);
			cout << "new estimate: " << lowest_cost << endl;
			if (temp_lowest_cost < lowest_cost) {
				index = i;
			}

		}
		ret.push_back(joins[index]);
		joins.erase(joins.begin() + index);

	}

	return ret;
}

int main () {
	
	
	std::string fileName = "Statistics.txt";
	std::string cnf = "SELECT SUM (ps.ps_supplycost), s.s_suppkey\nFROM part AS p, supplier AS s, partsupp AS ps\nWHERE (p.p_partkey = ps.ps_partkey) AND (s.s_suppkey = ps.ps_suppkey) AND (s.s_acctbal > 2500.0)\nGROUP BY s.s_suppkey";
	yy_scan_string(cnf.c_str());
	yyparse();
	Statistics s;
	//writeStat(fileName, s);
	s.Read(fileName);

	cout << "distinct atts: " << distinctAtts << endl;
	cout << "distinct func: " << distinctFunc << endl;
	
	PrintAndList(boolean);
	cout << endl;
	while (tables != NULL) {
		cout << "table name: " << tables->tableName;
		cout << " alias: " << tables->aliasAs << endl;
		alias_to_rel[tables->tableName] = tables->aliasAs;
		tables = tables->next;
	}

	while (attsToSelect != NULL) {

		cout << "project name: " << attsToSelect->name << endl;

		attsToSelect = attsToSelect->next;
	}

	while (groupingAtts != NULL) {

		cout << "group name: " << groupingAtts->name << endl;

		groupingAtts = groupingAtts->next;
	}


	std::vector<AndList*> joins;
	std::vector<AndList*> selects;

	while (boolean != NULL) {

		struct OrList *Or = boolean->left;
		while (Or !=NULL) {
			struct ComparisonOp *Com = Or->left; //get the comparison operator

			if (Com->code == EQUALS){ //this an equality check

				if (Com->right->code == NAME) { //this means we're doing a join
					joins.push_back(boolean);
				}
				else {
					selects.push_back(boolean);
				}
			}
			else //this is > or <
			{
				selects.push_back(boolean);
			}

			Or = Or->rightOr;
		}

		boolean = boolean->rightAnd;
		
	}

	//OptimizeQuery(joins, &s);
	// for (auto x : joins) {
	// 	std::string lAttrel(x->left->left->left->value); //grab name of the left attribute
	// 	std::string rAttrel(x->left->left->right->value); //grab name of the right attribute
	// 	std::string lAtt = msplit(lAttrel, ".").at(1);
	// 	std::string rAtt = msplit(rAttrel, ".").at(1);
	// 	std::string lRel = s.GetRelFromAtt(lAtt);
	// 	std::string rRel = s.GetRelFromAtt(rAtt);		
	// 	cout << "join: (" << lRel << ") " << lAtt << " " << x->left->left->code << " (" << rRel << ") " << rAtt << endl;
	// }

	// for (auto x : selects) { 
	// 	std::string lAttrel(x->left->left->left->value); //grab name of the left attribute
	// 	std::string lAtt = msplit(lAttrel, ".").at(1);
	// 	std::string rAtt(x->left->left->right->value); //grab name of the right attribute
	// 	std::string lRel = s.GetRelFromAtt(lAtt);
	// 	cout << "select: (" << lRel << ") " << lAtt << " " << x->left->left->code << " " << rAtt << endl;
	// }

	
	

}



std::vector<std::string> msplit(string s, string del = " ")
{
	std::vector<std::string> ret;
    int start = 0;
    int end = s.find(del);
    while (end != -1) {
        ret.push_back(s.substr(start, end - start));
        start = end + del.size();
        end = s.find(del, start);
    }
   ret.push_back(s.substr(start, end - start));

   return ret;
}

void writeStat (std::string fileName, Statistics s) {

	std::string relName[] = {"supplier","partsupp", "lineitem", "orders","customer","nation", "part", "region"};
	
	s.AddRel(relName[0],10000);
	s.AddAtt(relName[0], "s_suppkey",10000);
	s.AddAtt(relName[0], "s_nationkey",25);
	

	s.AddRel(relName[1],800000);
	s.AddAtt(relName[1], "ps_suppkey", 10000);
	s.AddAtt(relName[1], "ps_partkey", 200000);	

	s.AddRel(relName[2],6001215);
	s.AddAtt(relName[2], "l_returnflag",3);
	s.AddAtt(relName[2], "l_discount",11);
	s.AddAtt(relName[2], "l_shipmode",7);
	s.AddAtt(relName[2], "l_orderkey",1500000);
	s.AddAtt(relName[2], "l_receiptdate",3);
	s.AddAtt(relName[2], "l_partkey",200000);
	s.AddAtt(relName[2], "l_shipinstruct",4);
	
	s.AddRel(relName[3],1500000);
	s.AddAtt(relName[3], "o_custkey",150000);	
	s.AddAtt(relName[3], "o_orderkey",1500000);
	s.AddAtt(relName[3], "o_orderdate",150000);

	s.AddRel(relName[4],150000);
	s.AddAtt(relName[4], "c_custkey",150000);
	s.AddAtt(relName[4], "c_nationkey",25);
	s.AddAtt(relName[4], "c_mktsegment",5);
	
	s.AddRel(relName[5],25);
	s.AddAtt(relName[5], "n_nationkey",25);
	s.AddAtt(relName[5], "n_regionkey",5);
	s.AddAtt(relName[5], "n_name",25);


	s.AddRel(relName[6], 200000);
	s.AddAtt(relName[6], "p_partkey",200000);
	s.AddAtt(relName[6], "p_size",50);
	s.AddAtt(relName[6], "p_name", 199996);
	s.AddAtt(relName[6], "p_container",40);
	

	s.AddRel(relName[7], 5);
	s.AddAtt(relName[7], "r_regionkey",5);
	s.AddAtt(relName[7], "r_name",5);

	s.Write(fileName);

}