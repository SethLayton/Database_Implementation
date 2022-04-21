
#include <iostream>
#include "ParseTree.h"
#include "Statistics.h"
#include "string.h"
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

void PrintOperand(struct Operand *pOperand) {
	if(pOperand!=NULL) {
		cout<<pOperand->value<<" ";
	}
	else
		return;
}

void PrintComparisonOp(struct ComparisonOp *pCom) {
	if(pCom!=NULL) {
		PrintOperand(pCom->left);
		cout << "code: " << pCom->code << endl;
		switch(pCom->code) {
			case 1:
				cout<<" < "; break;
			case 2:
				cout<<" > "; break;
			case 3:
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

int main () {
	
	std::string fileName = "Statistics.txt";
	std::string cnf = "SELECT SUM (n.n_regionkey)\nFROM nation AS n, region AS r\nWHERE (n.n_regionkey = r.r_regionkey) AND (n.n_name = 'UNITED STATES')\nGROUP BY n.n_regionkey";
	yy_scan_string(cnf.c_str());
	yyparse();
	Statistics s;
	s.Read(fileName);

	cout << "distinct atts: " << distinctAtts << endl;
	cout << "distinct func: " << distinctFunc << endl;
	
	PrintAndList(boolean);
	cout << endl;
	while (tables != NULL) {
		cout << "table name: " << tables->tableName;
		cout << " alias: " << tables->aliasAs << endl;
		
		tables = tables->next;
	}



	//std::string relName[] = {"supplier","partsupp", "lineitem", "orders","customer","nation", "part", "region"};
	
	// s.AddRel(relName[0],10000);
	// s.AddAtt(relName[0], "s_suppkey",10000);
	// s.AddAtt(relName[0], "s_nationkey",25);
	

	// s.AddRel(relName[1],800000);
	// s.AddAtt(relName[1], "ps_suppkey", 10000);
	// s.AddAtt(relName[1], "ps_partkey", 200000);	

	// s.AddRel(relName[2],6001215);
	// s.AddAtt(relName[2], "l_returnflag",3);
	// s.AddAtt(relName[2], "l_discount",11);
	// s.AddAtt(relName[2], "l_shipmode",7);
	// s.AddAtt(relName[2], "l_orderkey",1500000);
	// s.AddAtt(relName[2], "l_receiptdate",3);
	// s.AddAtt(relName[2], "l_partkey",200000);
	// s.AddAtt(relName[2], "l_shipinstruct",4);
	
	// s.AddRel(relName[3],1500000);
	// s.AddAtt(relName[3], "o_custkey",150000);	
	// s.AddAtt(relName[3], "o_orderkey",1500000);
	// s.AddAtt(relName[3], "o_orderdate",150000);

	// s.AddRel(relName[4],150000);
	// s.AddAtt(relName[4], "c_custkey",150000);
	// s.AddAtt(relName[4], "c_nationkey",25);
	// s.AddAtt(relName[4], "c_mktsegment",5);
	
	// s.AddRel(relName[5],25);
	// s.AddAtt(relName[5], "n_nationkey",25);
	// s.AddAtt(relName[5], "n_regionkey",5);
	// s.AddAtt(relName[5], "n_name",25);


	// s.AddRel(relName[6], 200000);
	// s.AddAtt(relName[6], "p_partkey",200000);
	// s.AddAtt(relName[6], "p_size",50);
	// s.AddAtt(relName[6], "p_name", 199996);
	// s.AddAtt(relName[6], "p_container",40);
	

	// s.AddRel(relName[7], 5);
	// s.AddAtt(relName[7], "r_regionkey",5);
	// s.AddAtt(relName[7], "r_name",5);

	// s.Write(fileName);

	

}