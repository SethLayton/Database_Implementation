#include "TreeNode.h"
#include "Statistics.h"

void TreeNode::PrintTree() {

    if (left != nullptr)
        left->PrintTree();
    Print();
    if (right != nullptr)
        right->PrintTree();
}

SelectFileNode::SelectFileNode(Schema *in, int ourNode) {

    op_schema = in;
    ourId = ourNode;    
}

void SelectFileNode::Print() {

    cout << " ***********" << endl;
    cout << "SELECT FILE operation" << endl;
    cout << "Input Pipe " << leftPipeId << endl;
    cout << "Output Pipe " << outputPipeId << endl;
    cout << "Output Schema:" << endl;    
    op_schema->Print();
    
}

JoinNode::JoinNode(AndList *al, TreeNode *l, TreeNode *r) {

    andlist = al;     
    setLeft(l);
    setRight(r);
    op_schema = new Schema (left->op_schema, right->op_schema);
    Record temp;
    cnf.GrowFromParseTree(andlist, op_schema, temp);
}

void JoinNode::Print() {

    cout << " ***********" << endl;
    cout << "JOIN operation" << endl;
    cout << "Left Input Pipe " << leftPipeId << endl;
    cout << "Right Input Pipe " << rightPipeId << endl;
    cout << "Output Pipe " << outputPipeId << endl;
    cout << "Output Schema:" << endl;    
    op_schema->Print();
    cout << endl << "CNF: " << endl;    
    cnf.Print();

}

ProjectNode::ProjectNode(TreeNode &l, std::vector<std::string> attsToKeep) {
 
    setLeft(&l);
    Attribute att[attsToKeep.size()];
    for (int i = 0; i < attsToKeep.size(); i++) {
        att[i] = {(char*)attsToKeep[i].c_str(), left->op_schema->FindType((char*)attsToKeep[i].c_str())};
    }
	Schema out_sch ("out_sch", attsToKeep.size(), att);  
}

void ProjectNode::Print() {

    cout << " ***********" << endl;
    cout << "PROJECT operation" << endl;
    cout << "Input Pipe " << leftPipeId << endl;
    cout << "Output Pipe " << outputPipeId << endl;
    cout << "Output Schema:" << endl;    
    op_schema->Print();

}

SelectPipeNode::SelectPipeNode(AndList* al, TreeNode &l) {

    andlist = al; 
    setLeft(&l);
    op_schema = left->op_schema;    
    Record temp;
    cnf.GrowFromParseTree(andlist, left->op_schema, temp);
}

void SelectPipeNode::Print() {

    cout << " ***********" << endl;
    cout << "SELECT PIPE operation" << endl;
    cout << "Input Pipe " << leftPipeId << endl;
    cout << "Output Pipe " << outputPipeId << endl;
    cout << "Output Schema:" << endl;    
    op_schema->Print();
    cout << endl << "SELECTION CNF: " << endl;    
    cnf.Print();

}

DuplicateRemovalNode::DuplicateRemovalNode(TreeNode &l) {

    setLeft(&l);
    op_schema = left->op_schema;
}

void DuplicateRemovalNode::Print() {

    cout << " ***********" << endl;
    cout << "DUPLICATE REMOVAL operation" << endl;
    cout << "Input Pipe " << leftPipeId << endl;
    cout << "Output Pipe " << outputPipeId << endl;
    cout << "Output Schema:" << endl;    
    op_schema->Print();

}

SumNode::SumNode(TreeNode &l, std::string fun) {

    setLeft(&l);
    Attribute sum = {"sum", Double};
    Schema out_sch ("out_sch", 1, &sum);
    op_schema = &out_sch;
	get_cnf_function (fun, op_schema, func);
	
}

void SumNode::Print() {

    cout << " ***********" << endl;
    cout << "SUM operation" << endl;
    cout << "Input Pipe " << leftPipeId << endl;
    cout << "Output Pipe " << outputPipeId << endl;
    cout << "Output Schema:" << endl;    
    op_schema->Print();
    cout << endl << "SUM FUNCTION: " << endl;
    func.Print ();

}

GroupByNode::GroupByNode(TreeNode &l, std::string fun, std::vector<std::string> groupingAtts) {

    setLeft(&l);
    op_schema = left->op_schema;
	get_cnf_function (fun, op_schema, func);
    
    for (auto grp : groupingAtts) {

        groups.AddAttr(op_schema->FindType((char*)grp.c_str()), op_schema->Find((char*)grp.c_str()));
    }
	
}

void GroupByNode::Print() {

    cout << " ***********" << endl;
    cout << "GROUP BY operation" << endl;
    cout << "Input Pipe " << leftPipeId << endl;
    cout << "Output Pipe " << outputPipeId << endl;
    cout << "Output Schema:" << endl;    
    op_schema->Print();
    cout << endl << "GROUP BY ORDER MAKER: " << endl;
    groups.Print();
    cout << endl << "GROUP BY FUNCTION: " << endl;
    func.Print ();

}

void TreeNode::get_cnf_function (std::string input, Schema *left, Function &fn_pred) {
		init_lexical_parser_func ((char*)input.c_str());
  		if (yyfuncparse() != 0) {
			cout << " Error: can't parse your arithmetic expr. " << input << endl;
			exit (1);
		}
		fn_pred.GrowFromParseTree (finalfunc, *left); // constructs CNF predicate
		close_lexical_parser_func ();
}