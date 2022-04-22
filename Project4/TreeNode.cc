#include "TreeNode.h"


void TreeNode::PrintTree() {

    if (left != nullptr)
        left->PrintTree();
    Print();
    if (right != nullptr)
        right->PrintTree();
}

void SelectFileNode::Print() {

}

void JoinNode::Print() {

    cout << "***********" << endl;
    cout << "JOIN operation" << endl;
    cout << "Left Input Pipe " << leftPipeId << endl;
    cout << "Right Input Pipe " << rightPipeId << endl;
    cout << "Output Pipe " << outputPipeId << endl;
    cout << "Output Schema:" << endl;    
    op_schema->Print();
    cout << endl << "CNF: " << endl;
    CNF cnf;
    Record temp;
    cnf.GrowFromParseTree(andlist, left->op_schema, right->op_schema, temp);
    cnf.Print();

}

void ProjectNode::Print() {

}

void SelectPipeNode::Print() {

}

void DuplicateRemovalNode::Print() {

}

void SumNode::Print() {

}

void GroupByNode::Print() {

}