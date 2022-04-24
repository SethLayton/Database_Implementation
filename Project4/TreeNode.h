#ifndef TREE_NODE_H
#define TREE_NODE_H

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"
#include "Comparison.h"
#include <unordered_map>
#include "Statistics.h"


extern "C" {
	int yyparse(void);   // defined in y.tab.c
	int yyfuncparse(void);   // defined in yyfunc.tab.c
	void init_lexical_parser (char *); // defined in lex.yy.c (from Lexer.l)
	void close_lexical_parser (); // defined in lex.yy.c
	void init_lexical_parser_func (char *); // defined in lex.yyfunc.c (from Lexerfunc.l)
	void close_lexical_parser_func (); // defined in lex.yyfunc.c
}
extern struct FuncOperator *finalfunc;

class TreeNode {    

public:

    TreeNode *parent = nullptr, *left = nullptr, *right = nullptr;
    Schema *op_schema;
    int leftPipeId = 0, rightPipeId = 0, outputPipeId = 0;
    int ourId = 0;

    TreeNode() {};
    virtual ~TreeNode() {};
    void PrintTree ();
    virtual void Print() = 0;
    void setLeft (TreeNode *left_in) { left = left_in; left->setParent(this); }
    void setRight (TreeNode *right_in) { right = right_in; right->setParent(this); }
    void setParent (TreeNode *p) { parent = p; outputPipeId = parent->ourId; }
    void get_cnf_function (std::string input, Schema *left, Function &fn_pred);
    
};

class SelectFileNode : public TreeNode {

    private: 
        // CNF& op;
        // Record& lit;
    public:
        SelectFileNode(Schema *, int);
        void Print() override;

};

class JoinNode : public TreeNode {

    private: 
        AndList *andlist;
		CNF cnf;
    public:
        JoinNode(AndList *al, TreeNode *l, TreeNode *r);
        void Print() override;

};

class ProjectNode : public TreeNode {

    private: 
        int* indexLocations;
		int numInput;
		int numOutput;
    public:
        ProjectNode(TreeNode &l, std::vector<std::string> attsToKeep);
        void Print() override;

};

class SelectPipeNode : public TreeNode {

    private: 
        CNF cnf;
		// Record lit;
        AndList *andlist;
    public:
        SelectPipeNode(AndList* al, TreeNode &l);
        void Print() override;

};

class DuplicateRemovalNode : public TreeNode {

    private: 
        //Schema& schema;
    public:
        DuplicateRemovalNode(TreeNode &l);
        void Print() override;

};

class SumNode : public TreeNode {

    private: 
        Function func;
    public:
        SumNode(TreeNode &l, std::string fun);
        void Print() override;

};

class GroupByNode : public TreeNode {

    private: 
        OrderMaker groups;
		Function func;
    public:
        GroupByNode(TreeNode &l, std::string fun, std::vector<std::string> groupingAtts);
        void Print() override;

};





#endif