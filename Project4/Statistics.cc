#include "Statistics.h"

Statistics::Statistics() {
}

Statistics::Statistics(Statistics &copyMe) {
    
    //loop through all the relations
    for (auto i : copyMe.rels) {
        //create a new relation structure for this relation
        addrel rel = {i.second.name, i.second.numTuples};
        //loop through all the relations attributes
        for (auto k : i.second.atts) {
            //create a new copy of that attribute structure
            addatt att = {k.second.name, k.second.numDistincts};
            //add this copied attribute to the new relation
            rel.atts[k.first] = att;
        }
        //update or add the relation in the hashmap
        rels[i.first] = rel;
    } 
}

Statistics::~Statistics() {
}

void Statistics::AddRel(std::string relName, int numTuples) {
    //create a relation structure to store in our relations hashmap
    addrel rel = {relName, numTuples};
    //update or add the structure in the hashmap
    //key is the relName
    rels[relName] = rel;
}

void Statistics::AddAtt(std::string relName, std::string attName, int numDistincts) {
    //create a attribute structure to store in the relation attributes hashmap
    addatt att = {attName, numDistincts};
    //grab the required relation from the hashmap based on the given relName
    addrel rel = rels.at(relName);
    //update or add the attribute structure to the hashmap
    rel.atts[attName] = att;
    //store the updates back in the relations hashmap
    rels.at(relName) = rel;    
}

void Statistics::CopyRel(std::string oldName, std::string newName) {
    //grab the old relation structure
    addrel oldRel = rels.at(oldName);
    //create the new relation structure
    addrel newRel = {newName, oldRel.numTuples};
    //loop through all the old relations attributes
    for (auto k : oldRel.atts) {
        //create a copy of that attribute
        addatt att = {k.second.name, k.second.numDistincts};
        //add this copied attribute to the new relation
        newRel.atts[k.first] = att;
    }
    //update or add the relation in the hashmap
    rels[newName] = newRel;
}
	
void Statistics::Read(std::string fromWhere) {
}

void Statistics::Write(std::string fromWhere) {
}

void  Statistics::Apply(struct AndList *parseTree, std::string relNames[], int numToJoin) {
}

double Statistics::Estimate(struct AndList *parseTree, std::string *relNames, int numToJoin) {
}

void Statistics::printRels() {
    cout << "All relations : \n";
    for (auto i : rels) {
        cout << "relName: " << i.second.name << " numTuples: " << i.second.numTuples << endl;
        for (auto k : i.second.atts) {
            cout << "\tattName: " << k.second.name << " numDistincts: " << k.second.numDistincts << endl;
        }
    } 
}
