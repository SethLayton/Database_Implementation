#include "TreeNode.h"
#include "Statistics.h"

void TreeNode::PrintTree() {
    if (left != nullptr)
        left->PrintTree();
    
    if (right != nullptr)
        right->PrintTree();
    Print();
    return;
}

SelectFileNode::SelectFileNode(Schema *in, int ourNode) {

    op_schema = in;
    ourId = ourNode;    
}

void SelectFileNode::Print() {

    cout << " ***********" << endl;
    cout << "SELECT FILE operation" << endl;
    cout << "Input Pipe " << leftPipeId << endl;
    cout << "Output Pipe " << ourId << endl;
    cout << "Output Schema:" << endl;    
    op_schema->Print();
    
}

JoinNode::JoinNode(AndList *al, TreeNode *l, TreeNode *r, std::string joinCNF, int nodeId) {

    andlist = al;     
    setLeft(l);
    setRight(r);
    op_schema = new Schema (left->op_schema, right->op_schema);
    Record temp;
    cnf.GrowFromParseTree(andlist, op_schema, temp);
    ourId = nodeId;
    leftPipeId = left->ourId;
    rightPipeId = right->ourId;
    join = joinCNF;
}

void JoinNode::Print() {

    cout << " ***********" << endl;
    cout << "JOIN operation" << endl;
    cout << "Left Input Pipe " << leftPipeId << endl;
    cout << "Right Input Pipe " << rightPipeId << endl;
    cout << "Output Pipe " << ourId << endl;
    cout << "Output Schema:" << endl;    
    op_schema->Print();
    cout << endl << "CNF: " << endl;   
    cout << join << endl; 
    cnf.Print();

}

ProjectNode::ProjectNode(TreeNode *l, NameList* attsToKp, int nodeId) {
 
    setLeft(l);
    std::vector<std::string> attsToKeep;
    while (attsToKp != NULL) {

        attsToKeep.push_back(attsToKp->name);
        attsToKp = attsToKp->next;
    }
    Attribute att[attsToKeep.size()];
    for (int i = 0; i < attsToKeep.size(); i++) {
        att[i] = {(char*)attsToKeep[i].c_str(), left->op_schema->FindType((char*)attsToKeep[i].c_str())};
    }
	op_schema = new Schema ("out_sch", attsToKeep.size(), att); 
    for (int i = 0; i < attsToKeep.size(); i++) {
        Type t = op_schema->FindType((char*)attsToKeep[i].c_str());
        int it = op_schema->Find((char*)attsToKeep[i].c_str());
        ord.AddAttr(t, it);
    }
    ourId = nodeId; 
    leftPipeId = left->ourId;
}

void ProjectNode::Print() {

    cout << " ***********" << endl;
    cout << "PROJECT operation" << endl;
    cout << "Input Pipe " << leftPipeId << endl;
    cout << "Output Pipe " << ourId << endl;
    cout << "Output Schema:" << endl; 
    ord.Print();   
    op_schema->Print();

}

SelectPipeNode::SelectPipeNode(AndList* al, TreeNode *l, std::string selectCNF, int nodeId) {

    andlist = al; 
    setLeft(l);
    op_schema = left->op_schema;    
    Record temp;
    cnf.GrowFromParseTree(andlist, left->op_schema, temp);
    ourId = nodeId;
    leftPipeId = left->ourId;
    select = selectCNF;
}

void SelectPipeNode::Print() {

    cout << " ***********" << endl;
    cout << "SELECT PIPE operation" << endl;
    cout << "Input Pipe " << leftPipeId << endl;
    cout << "Output Pipe " << ourId << endl;
    cout << "Output Schema:" << endl;    
    op_schema->Print();
    cout << endl << "SELECTION CNF: " << endl;
    cout << select << endl;   
    cnf.Print();

}

DuplicateRemovalNode::DuplicateRemovalNode(TreeNode *l, int nodeId) {

    setLeft(l);
    op_schema = left->op_schema;
    ourId = nodeId;
    leftPipeId = left->ourId;
}

void DuplicateRemovalNode::Print() {

    cout << " ***********" << endl;
    cout << "DUPLICATE REMOVAL operation" << endl;
    cout << "Input Pipe " << leftPipeId << endl;
    cout << "Output Pipe " << ourId << endl;
    cout << "Output Schema:" << endl;    
    op_schema->Print();

}

SumNode::SumNode(TreeNode *l, std::string fun, int nodeId) {

    setLeft(l);
    Attribute sum = {"sum", Double};
    Schema out_sch ("out_sch", 1, &sum);
    op_schema = &out_sch;
    sfun = fun;
    ourId = nodeId;
    leftPipeId = left->ourId;
	
}

void SumNode::Print() {

    cout << " ***********" << endl;
    cout << "SUM operation" << endl;
    cout << "Input Pipe " << leftPipeId << endl;
    cout << "Output Pipe " << ourId << endl;
    cout << "Output Schema:" << endl;    
    op_schema->Print();
    cout << endl << "SUM FUNCTION: " << endl;
    cout << sfun << endl;
    func.Print ();

}

GroupByNode::GroupByNode(TreeNode *l, NameList* groupingAtts, Statistics* s, FuncOperator* fun, int nodeId) {

    setLeft(l);
    op_schema = left->op_schema;
    sfun = "(";
	
    while (groupingAtts != NULL) {

        std::string lAttrel(groupingAtts->name); //grab name of the left attribute
		std::string lAtt = s->split(lAttrel, ".").at(1);
		std::string lRel = s->GetRelFromAtt(lAtt);
        Type t = op_schema->FindType((char*)lAtt.c_str());
        int i = op_schema->Find((char*)lAtt.c_str());
        groups.AddAttr(t, i);
        sfun += groupingAtts->next == NULL ? lAtt + ")" : lAtt + ",";
        groupingAtts = groupingAtts->next;
    }
    if (fun != NULL) 
        func.GrowFromParseTree(fun, *op_schema);
    ourId = nodeId;
	leftPipeId = left->ourId;
}

void GroupByNode::Print() {

    cout << " ***********" << endl;
    cout << "GROUP BY operation" << endl;
    cout << "Input Pipe " << leftPipeId << endl;
    cout << "Output Pipe " << ourId << endl;
    cout << "Output Schema:" << endl;    
    op_schema->Print();
    cout << endl << "GROUP BY ORDER MAKER: " << endl;
    groups.Print();
    cout << endl << "GROUP BY FUNCTION: " << endl;
    cout << sfun << endl;
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

TreeNode* TreeNode::GetOldestParent() {

    TreeNode *retParent = parent;
    if (retParent == nullptr)
        return nullptr;
    while (retParent->parent != nullptr) {
        retParent = retParent->parent;
    }

    return retParent;
}