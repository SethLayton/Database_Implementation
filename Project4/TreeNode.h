#ifndef TREE_NODE_H
#define TREE_NODE_H

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"
#include "Comparison.h"

class TreeNode {

public:

    TreeNode *parent = nullptr, *left = nullptr, *right = nullptr;
    Schema *op_schema = nullptr;
    int leftPipeId = 0, rightPipeId = 0, outputPipeId = 0;

    void PrintTree ();
    virtual void Print() = 0;
    void setLeft (TreeNode *left_in) { left = left_in; left->setParent(this); }
    void setRight (TreeNode *right_in) { right = right_in; right->setParent(this); }
    void setParent (TreeNode *p) { parent = p; }
    
};

class SelectFileNode : public TreeNode {

    private: 
        // CNF& op;
        // Record& lit;
    public:
        SelectFileNode() {  };
        void Print() override;

};

class JoinNode : public TreeNode {

    private: 
        AndList *andlist;
		// Record& lit;
    public:
        JoinNode(AndList *al) { andlist = al; op_schema = new Schema(left->op_schema, right->op_schema); }
        void Print() override;

};

class ProjectNode : public TreeNode {

    private: 
        int* indexLocations;
		int numInput;
		int numOutput;
    public:
        ProjectNode(Schema* schema);
        void Print() override;

};

class SelectPipeNode : public TreeNode {

    private: 
        CNF op;
		Record lit;
    public:
        SelectPipeNode(Schema* schema);
        void Print() override;

};

class DuplicateRemovalNode : public TreeNode {

    private: 
        Schema& schema;
    public:
        DuplicateRemovalNode(Schema* schema);
        void Print() override;

};

class SumNode : public TreeNode {

    private: 
        Function& func;
    public:
        SumNode(Schema* schema);
        void Print() override;

};

class GroupByNode : public TreeNode {

    private: 
        OrderMaker& groups;
		Function& func;
    public:
        GroupByNode(Schema* schema);
        void Print() override;

};
#endif