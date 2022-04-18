#include "Statistics.h"
#include <iostream>
#include <fstream>
#include <cmath>

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
        subsets_n[Rel.name] = 0;
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
    subsets_n[relName] = 0;
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
        std::string newattName = newName+"." + k.second.name;
        att att = {newattName, k.second.numDistincts};
        //add this copied attribute to the new relation
        newRel.atts[newattName] = att;
        att_to_rel[newattName] = newName;
    }
    //update or add the relation in the hashmap
    rels[newName] = newRel;

    //Copying a relation means that we need to add
    //the newly copied rel as a singleton subset
    vector<rel> vRels;
    vRels.push_back(newRel);
    subsets[newName] = vRels;
    subsets_n[newName] = 0;
    
}
	
void Statistics::Read(std::string fromWhere) {
    ifstream file(fromWhere);
    string line;
    std::string currentRel = "";
    while (getline (file, line)) {
        
        if (line.find("relName: ",0) != -1){
            int pos1 = line.find("relName: ")+9;
            int pos2 = line.find("!");
            std::string relName = line.substr( pos1, pos2-pos1 );
            std::string numtups = line.substr(line.find("!numTuples: ")+12);
            int nt = std::stoi(numtups);
            AddRel(relName, nt);
        } else if (line.find("attName: ", 0)!= -1)
        {
            int pos1 = line.find("attName: ")+9;
            int pos2 = line.find("!");
            std::string attName = line.substr( pos1, pos2-pos1 );
            std::string numdis = line.substr(line.find("!numDistincts: ")+15);
            int nd = std::stoi(numdis);
            AddAtt(attName, nd);
        }
        
    }
    file.close();
}
bool isNumber(const string& str)
{
    for (char const &c : str) {
        if (std::isdigit(c) == 0) return false;
    }
    return true;
}
void Statistics::Write(std::string fromWhere) {
    ofstream file(fromWhere);
     for (auto i : rels) {
        if (!isNumber(i.second.name)){
            file << "relName: " << i.second.name << "!numTuples: " << i.second.numTuples << endl;
            for (auto k : i.second.atts) {
                file << "\tattName: " << k.second.name << "!numDistincts: " << k.second.numDistincts << endl;
            }
        }
        
    } 
    file.close();
}

int Statistics::GetNumTuples(std::string name) {
    if (subsets_n[name] == 0 ){
        return rels.at(name).numTuples;
    } else {
        std::string intName = std::to_string(subsets_n[name]);
        return rels.at(intName).numTuples;
    }
    


}

void Statistics::Apply(struct AndList *parseTree, std::string relNames[], int numToJoin) {
     //check to see if the parseTree is valid
    CheckTree(parseTree, relNames, numToJoin);
    unordered_set<std::string> comp_relations;
    unordered_set<std::string> comp_attributes;
    double ratio = 1;
    bool isSameCols = true;
    bool join = false;
    //Loop through all the AND operations
    while (parseTree !=NULL) {
        struct OrList *Or = parseTree->left; //grab all the OR operations from this AND
        double orRatio = 0.0;
        vector<double> orRatioVector;
        //loop through all the OR operations in this AND
        while (Or !=NULL) {
            
            struct ComparisonOp *Com = Or->left; //get the comparison operator
            std::string lAtt(Com->left->value); //grab name of the left attribute
			std::string rAtt(Com->right->value); //grab name of the right attribute  
            std::string lRel = att_to_rel.at(lAtt);
            rel lRelation = rels.at(lRel);
            att lAttribute = lRelation.atts.at(lAtt);
            isSameCols = !comp_attributes.insert(lAtt).second;
            comp_relations.insert(lRel);
            double tempMax = 1.0;
            //switch on the type of the operator in this OR operation
            switch(Com->code) {
                case EQUALS:
                    //if we're comparing to another column and not a literal value
                    if (Com->right->code == NAME) {
                        std::string rRel = att_to_rel.at(rAtt);
                        rel rRelation = rels.at(rRel);
                        att rAttribute = rRelation.atts.at(rAtt);
                        int rNumTups = GetNumTuples(rRelation.name);
                        int lNumTups = GetNumTuples(lRelation.name);

                        double join_ratio = ((double)rNumTups / (double)lAttribute.numDistincts) * ((double)lNumTups / (double)lAttribute.numDistincts);
                        tempMax = join_ratio * (lAttribute.numDistincts > rAttribute.numDistincts ? (double)rAttribute.numDistincts : (double)lAttribute.numDistincts);
                        join = true;
                    }
                    else { //now we're comparing with a literal
                        tempMax = tempMax / lAttribute.numDistincts;                      
                    }
                    break;
                default:
                    tempMax = 1.0 / 3.0;
                    break;
            }
            orRatioVector.push_back (tempMax);
            Or = Or->rightOr;
        }
        if(isSameCols) {
			for (auto x : orRatioVector) {
                orRatio += x;
            }
		}	
        else {
            orRatio = 1.0;
            for (auto x : orRatioVector) {
                orRatio *= (1 - x);
            }
            orRatio  = 1 - orRatio;
		}
		ratio *= orRatio;
        parseTree = parseTree->rightAnd;
    }
    
    if (!join) {
        for (auto i : comp_relations) {
            // Look up subset to get numTuples
            //ratio *= rels.at(i).numTuples; 
            ratio *= GetNumTuples(i);
        }

    } else { // If there was a join, we need to create the new relationship
        numSubsets++;
        std::string intName = std::to_string(numSubsets);
        AddRel(intName, round(ratio));
        std::vector<rel> tempRel;
        
        for (int i = 0; i < numToJoin; i++) {
            //rels.at(relNames[i]);
            subsets_n[relNames[i]] = numSubsets;
            rel r = rels.at(relNames[i]);
            tempRel.push_back(r);
        }
        subsets[intName] = tempRel;
        
    }    


}

double Statistics::Estimate(struct AndList *parseTree, std::string *relNames, int numToJoin) {

    //check to see if the parseTree is valid
    CheckTree(parseTree, relNames, numToJoin);
    unordered_set<std::string> comp_relations;
    unordered_set<std::string> comp_attributes;
    double ratio = 1.0;
    bool isSameCols = true;
    bool join = false;
    //Loop through all the AND operations
    while (parseTree !=NULL) {
        struct OrList *Or = parseTree->left; //grab all the OR operations from this AND
        double orRatio = 0.0;
        vector<double> orRatioVector;
        //loop through all the OR operations in this AND
        while (Or !=NULL) {
            
            struct ComparisonOp *Com = Or->left; //get the comparison operator
            std::string lAtt(Com->left->value); //grab name of the left attribute
			std::string rAtt(Com->right->value); //grab name of the right attribute            
            std::string lRel = att_to_rel.at(lAtt);
            rel lRelation = rels.at(lRel);
            att lAttribute = lRelation.atts.at(lAtt);
            isSameCols = !comp_attributes.insert(lAtt).second;
            comp_relations.insert(lRel);
            double tempMax = 1.0;
            //switch on the type of the operator in this OR operation
            switch(Com->code) {
                case EQUALS:
                    //if we're comparing to another column and not a literal value
                    if (Com->right->code == NAME) {
                        std::string rRel = att_to_rel.at(rAtt);
                        rel rRelation = rels.at(rRel);
                        att rAttribute = rRelation.atts.at(rAtt);
                        int rNumTups = GetNumTuples(rRelation.name);
                        int lNumTups = GetNumTuples(lRelation.name);
                        double join_ratio = ((double)rNumTups / (double)lAttribute.numDistincts) * ((double)lNumTups / (double)lAttribute.numDistincts);
                 
                        tempMax = join_ratio * (lAttribute.numDistincts > rAttribute.numDistincts ? (double)rAttribute.numDistincts : (double)lAttribute.numDistincts);
                        join = true;
                    }
                    else { //now we're comparing with a literal
                        tempMax = tempMax / lAttribute.numDistincts;                      
                    }
                    break;
                default:
                    tempMax = 1.0 / 3.0;
                    break;
            }
            orRatioVector.push_back (tempMax);
            Or = Or->rightOr;
        }
        if(isSameCols) {
			for (auto x : orRatioVector) {
                orRatio += x;
            }
            
		}	
        else {
            orRatio = 1.0;
            for (auto x : orRatioVector) {
                orRatio *= (1 - x);
            }
            orRatio  = 1 - orRatio;
		}
    
		ratio *= orRatio;
        parseTree = parseTree->rightAnd;
    }
    
    if (!join) {
        for (auto i : comp_relations) {
            ratio *= rels.at(i).numTuples; 
        }
    }
    
    return ratio;
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
                cout << lAtt << endl;
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
            found = relations.find(set_rel.name) != relations.end();
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
            
            cout << "!!!!Error in CheckTree. Found a 'partially used' subset of relations. Mismatch with list of relations." << endl;
            exit(0);
        }
    }
}
