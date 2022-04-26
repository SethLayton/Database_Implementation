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
    TreeNode* GetOldestParent();
    
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
        std::string join;
    public:
        JoinNode(AndList *al, TreeNode *l, TreeNode *r, std::string joinCNF, int);
        void Print() override;

};

class ProjectNode : public TreeNode {

    private: 
        int* indexLocations;
		int numInput;
		int numOutput;
        OrderMaker ord;
    public:
        ProjectNode(TreeNode *l, NameList* attsToKeep, int);
        void Print() override;

};

class SelectPipeNode : public TreeNode {

    private: 
        CNF cnf;
		std::string select;
        AndList *andlist;
    public:
        SelectPipeNode(AndList* al, TreeNode *l, std::string, int);
        void Print() override;

};

class DuplicateRemovalNode : public TreeNode {

    private: 
        //Schema& schema;
    public:
        DuplicateRemovalNode(TreeNode *l, int);
        void Print() override;

};

class SumNode : public TreeNode {

    private: 
        Function func;
        std::string sfun = "";
    public:
        SumNode(TreeNode *l, std::string fun, Statistics *, int);
        void Print() override;

};

class GroupByNode : public TreeNode {

    private: 
        OrderMaker groups;
		Function func;
        std::string sumfun = "";
        std::string groupfun = "";
    public:
        GroupByNode(TreeNode *l, NameList* groupingAtts, Statistics *, std::string fun, int);
        void Print() override;

};





#endif