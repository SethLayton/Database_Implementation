// #include "y.tab.h"
#include <iostream>
#include <stdlib.h>
#include "Statistics.h"
#include "ParseTree.h"
#include <gtest/gtest.h>
#include <math.h>
extern "C" { 
	struct YY_BUFFER_STATE *yy_scan_string(const char*);
	int yyparse(void);
}
extern struct AndList *final;

using namespace std;


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

void checkResult(double correct, double result) {
	if(fabs(result-correct)>0.5) {
		cout<<"error in estimation\nExpected: "<< correct<< "\nEstimation: " << result << endl;;
	} else {
		cout << "Correct Estimation!" << endl;
	}

}

std::string fileName = "Statistics.txt";



TEST(TEST1, q7Test) {
	Statistics s;
	std::string relName[] = { "orders", "lineitem"};

	

	s.AddRel(relName[0],1500000);
	s.AddAtt(relName[0], "o_orderkey",1500000);
	
	
	s.AddRel(relName[1],6001215);
	s.AddAtt(relName[1], "l_orderkey",1500000);
	s.AddAtt(relName[1], "l_receiptdate",3);
	

	std::string cnf = "(l_receiptdate >'1995-02-01' ) AND (l_orderkey = o_orderkey)";

	yy_scan_string(cnf.c_str());
	yyparse();
	double result = s.Estimate(final, relName, 2);

	EXPECT_EQ(2000405, round(result));

}

TEST(TEST2, q8Test) {
	Statistics s;
	std::string relName[] = { "part",  "partsupp"};

	s.Read(fileName);
	
	s.AddRel(relName[0],200000);
	s.AddAtt(relName[0], "p_partkey",200000);
	s.AddAtt(relName[0], "p_size",50);

	s.AddRel(relName[1],800000);
	s.AddAtt(relName[1], "ps_partkey",200000);
	

	std::string cnf = "(p_partkey=ps_partkey) AND (p_size =3 OR p_size=6 OR p_size =19)";

	yy_scan_string(cnf.c_str());
	yyparse();
	
		
	double result = s.Estimate(final, relName,2);

	EXPECT_EQ(48000, result);

}

int main(int argc, char *argv[]) {
	testing::InitGoogleTest();
  	int ret_val = RUN_ALL_TESTS();
	return ret_val;

}
