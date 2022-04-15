#include "Statistics.h"

Statistics::Statistics() {
}

Statistics::Statistics(Statistics &copyMe) {
    
    //loop through all the relations
    for (auto i : copyMe.rels) {
        //create a new relation structure for this relation
        rel Rel = {i.second.name, i.second.numTuples};
        //loop through all the relations attributes
        for (auto k : i.second.atts) {
            //create a new copy of that attribute structure
            att att = {k.second.name, k.second.numDistincts};
            //add this copied attribute to the new relation
            Rel.atts[k.first] = att;
            //add a map from the attribute to the relation for future lookup
            att_to_rel[k.second.name] = Rel.name; 
        }
        //update or add the relation in the hashmap
        rels[i.first] = Rel;

        //Copying a relation means that we need to add
        //the newly copied rel as a singleton subset
        vector<rel> vRels;
        vRels.push_back(Rel);
        subsets[Rel.name] = vRels;
    } 
}

Statistics::~Statistics() {
}

void Statistics::AddRel(std::string relName, int numTuples) {
    //create a relation structure to store in our relations hashmap
    rel Rel = {relName, numTuples};
    //update or add the structure in the hashmap
    //key is the relName
    rels[relName] = Rel;

    //adding a new relation means that
    //it gets its own singleton
    //subset to start with
    vector<rel> vRels;
    vRels.push_back(Rel);
    subsets[relName] = vRels;
    maxSubsetSize = 1;
}

void Statistics::AddAtt(std::string relName, std::string attName, int numDistincts) {
    
    //grab the required relation from the hashmap based on the given relName
    rel Rel = rels.at(relName);
    //create a attribute structure to store in the relation attributes hashmap
    att Att;
    if (numDistincts == -1) {
        Att = {attName, Rel.numTuples};
    }
    else {
        Att = {attName, numDistincts};
    }
    //update or add the attribute structure to the hashmap
    Rel.atts[attName] = Att;
    //store the updates back in the relations hashmap
    rels.at(relName) = Rel;

    //add a map from the attribute to the relation for future lookup
    att_to_rel[attName] = Rel.name;    
}

void Statistics::CopyRel(std::string oldName, std::string newName) {
    //grab the old relation structure
    rel oldRel = rels.at(oldName);
    //create the new relation structure
    rel newRel = {newName, oldRel.numTuples};
    //loop through all the old relations attributes
    for (auto k : oldRel.atts) {
        //create a copy of that attribute
        att att = {k.second.name, k.second.numDistincts};
        //add this copied attribute to the new relation
        newRel.atts[k.first] = att;
    }
    //update or add the relation in the hashmap
    rels[newName] = newRel;

    //Copying a relation means that we need to add
    //the newly copied rel as a singleton subset
    vector<rel> vRels;
    vRels.push_back(newRel);
    subsets[newName] = vRels;
    
}
	
void Statistics::Read(std::string fromWhere) {
}

void Statistics::Write(std::string fromWhere) {
}

void Statistics::Apply(struct AndList *parseTree, std::string relNames[], int numToJoin) {

    //check to see if the parseTree is valid
    CheckTree(parseTree, relNames, numToJoin);
}

double Statistics::Estimate(struct AndList *parseTree, std::string *relNames, int numToJoin) {

    //check to see if the parseTree is valid
    //CheckTree(parseTree, relNames, numToJoin);
    unordered_map<std::string, double> and_ratio_map;
    double andMin = -1.0;
    //Loop through all the AND operations
    while (parseTree !=NULL) {
        struct OrList *Or = parseTree->left; //grab all the OR operations from this AND
        unordered_set<std::string> comp_relations;
        unordered_map<std::string, double> or_ratio_map;
        double orMax = 0.0;
        //loop through all the OR operations in this AND
        while (Or !=NULL) {
            
            struct ComparisonOp *Com = Or->left; //get the comparison operator
            std::string lAtt(Com->left->value); //grab name of the left attribute
			std::string rAtt(Com->right->value); //grab name of the right attribute            
            std::string lRel = att_to_rel.at(lAtt);
            rel lRelation = rels.at(lRel);
            att lAttribute = lRelation.atts.at(lAtt);
            bool newrelation = comp_relations.insert(lRel).second;
            double tempMax = 0.0;
            //switch on the type of the operator in this OR operation
            switch(Com->code) {
                case LESS_THAN: 
                    tempMax = 1.0 / 3.0;        
                    //tempMax = lRelation.numTuples / lAttribute.numDistincts;
                    break;
                case GREATER_THAN:
                    tempMax = 1.0 / 3.0;   
                    //tempMax = lRelation.numTuples / lAttribute.numDistincts;
                    break;
                case EQUALS:
                    //if we're comparing to another column and not a literal value
                    if (Com->right->code == NAME) {
                        std::string rRel = att_to_rel.at(rAtt);
                        rel rRelation = rels.at(rRel);
                        att rAttribute = rRelation.atts.at(rAtt);
                        double join_ratio = (rRelation.numTuples / rAttribute.numDistincts) * (lRelation.numTuples / lAttribute.numDistincts);
                        tempMax = join_ratio * (lAttribute.numDistincts > rAttribute.numDistincts ? rAttribute.numDistincts : lAttribute.numDistincts);
                    }
                    else { //now we're comparing with a literal
                        tempMax = 1.0 / lAttribute.numDistincts;                            
                        //tempMax = lRelation.numTuples / lAttribute.numDistincts;                        
                    }
                    break;
            }

            or_ratio_map[lRel] += tempMax;

            if (or_ratio_map.find(lRel) != or_ratio_map.end()) {
                if (or_ratio_map.at(lRel) < tempMax) {
                    or_ratio_map[lRel] = tempMax;
                }
            }
            else
            {
                or_ratio_map[lRel] = tempMax;
            }
            
            Or = Or->rightOr;
        }
        for (auto i : or_ratio_map) {
            cout << i.first <<  " ratio: " << i.second <<  endl;
            if (and_ratio_map.find(i.first) != and_ratio_map.end()) {
                cout << "and_map relation found" << endl;
                if (i.second < and_ratio_map.at(i.first)) {
                    cout << "updating and_map to smaller ratio" << endl;
                    and_ratio_map[i.first] = i.second;
                }
            }
            else
            {
                cout << "and_map relation not found" << endl;
                and_ratio_map[i.first] = i.second;
            }
            cout << "and_ratio value = " << and_ratio_map[i.first] << endl;
        }
        // if (orMax < andMin || andMin == -1.0) {
        //     andMin = orMax;
        // }
        parseTree = parseTree->rightAnd;
    }
    double result = 0.0;
    for (auto i : and_ratio_map) {
        result += i.second * rels.at(i.first).numTuples; 
    }
    return result;
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

void Statistics::CheckTree(AndList* parseTree, std::string* relNames, int numToJoin) {

    unordered_set<std::string> relations;
    unordered_set<std::string> tempRelations;
    for (int i = 0; i < numToJoin; i++) {
       relations.insert(relNames[i]);
    }

    while (parseTree !=NULL) {
        struct OrList *Or = parseTree->left; //grab all the OR operations from this AND

        //loop through all the OR operations in this AND
        while (Or !=NULL) {
            struct ComparisonOp *Com = Or->left; //get the comparison operator
            std::string lAtt(Com->left->value); //grab name of the left attribute
            std::string rAtt(Com->right->value); //grab name of the right attribute
            std::string lRel = "";
            std::string rRel = "";
            try
            {
                lRel = att_to_rel.at(lAtt);
            }
            catch(const std::exception& e)
            {
                cout << "Error in CheckTree. parseTree contains a left relation that does not exist in the given list of relations." << endl;
                exit(0);
            }                              
            
            //switch on the type of the operator in this OR operation
            if (Com->code == 3 && Com->right->code == NAME) {                
                //if we're comparing to another column and not a literal value 
                try
                {
                    rRel = att_to_rel.at(rAtt); 
                }
                catch(const std::exception& e)
                {
                    cout << "Error in CheckTree. parseTree contains a right relation that does not exist in the given list of relations." << endl;
                    exit(0);
                }   
            }
            bool setSuccessL = relations.insert(lRel).second;
            bool setSuccessR = rRel == "" ? false : relations.insert(rRel).second;
            if (setSuccessL || setSuccessR) {
                //we were able to insert into the set
                //this means this relation was not
                //in the passed in list of relation names
                cout << "Error in CheckTree. parseTree contains a relation that does not exist in the given list of relations." << endl;
                exit(0);
            }
            tempRelations.insert(lRel);
            if(rRel != "") {
                tempRelations.insert(rRel);
            }
            
            Or = Or->rightOr;
            
        }

        parseTree = parseTree->rightAnd;
    }

    //check to see if there are any 'partially used' subsets in our relation
    for (auto set : subsets) {
        bool currentSet = false; 
        bool found = false;
        for (rel set_rel : set.second) {     
            found = tempRelations.find(set_rel.name) != tempRelations.end();
            if (currentSet && !found) {
                //we have a partially used subset
                cout << "Error in CheckTree. Found a 'partially used' subset of relations. Mismatch with the given list of relations." << endl;
                exit(0);
            }
            if (found) {
                currentSet = true;
            }
            
        }
        //this is used to check the last element of the subset vector
        if (found && !currentSet) {
            //we have a partially used subset
            cout << "Error in CheckTree. Found a 'partially used' subset of relations. Mismatch with list of relations." << endl;
            exit(0);
        }
    }
}
