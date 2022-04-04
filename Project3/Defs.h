#ifndef DEFS_H
#define DEFS_H


#define MAX_ANDS 20
#define MAX_ORS 20

#define PAGE_SIZE 131072


enum Target {Left, Right, Literal};
enum CompOperator {LessThan, GreaterThan, Equals};
enum Type {Int, Double, String};
enum ClassList {selectsipe, selectfile, selectfile2, project, join, duplicateremoval, sum, groupby, writeout};


unsigned int Random_Generate();


#endif

