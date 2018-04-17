
#include <iostream>
#include "ParseTree.h"
#include <unordered_map>
#include <vector>
#include "Schema.h"
#include <string>
#include <string.h>
#include <sstream>
#include <set>
#include "QueryPlanNode.h"
#include <list>
#include "Statistics.h"

using namespace std;

extern "C" {
	int yyparse(void);   // defined in y.tab.c
}

extern struct FuncOperator *finalFunction;

extern struct TableList *tables;
extern struct AndList *boolean;

extern struct NameList *groupingAtts;
extern struct NameList *attsToSelect;
extern int distinctAtts;
extern int distinctFunc;

void printNameList(struct NameList * list){
	if(list != NULL){
		cout << list->name << " ";
		printNameList(list->next);
	}
}

void printTableList(struct TableList * list){
	if(list != NULL){
		cout << "name: " << list->tableName << " Alias: " << list->aliasAs << endl;
		printTableList(list->next);
	}
}

void PrintOperand(struct Operand *pOperand){
        if(pOperand!=NULL)
        {
                cout <<pOperand->value<<" ";
        }
        else
                return;
}

void PrintComparisonOp(struct ComparisonOp *pCom){
        if(pCom!=NULL)
        {
                PrintOperand(pCom->left);
                switch(pCom->code)
                {
                        case 5:
                                cout<<" < "; break;
                        case 6:
                                cout<<" > "; break;
                        case 7:
                                cout<<" = ";
                }
                PrintOperand(pCom->right);

        }
        else
        {
                return;
        }
}

void PrintOrList(struct OrList *pOr){
        if(pOr !=NULL)
        {
                struct ComparisonOp *pCom = pOr->left;
                PrintComparisonOp(pCom);

                if(pOr->rightOr)
                {
                        cout <<" OR ";
                        PrintOrList(pOr->rightOr);
                }
        }
        else
        {
                return;
        }
}

void PrintAndList(struct AndList *pAnd){
        if(pAnd !=NULL)
        {
                struct OrList *pOr = pAnd->left;
                PrintOrList(pOr);
                if(pAnd->rightAnd)
                {
                        cout <<" AND ";
                        PrintAndList(pAnd->rightAnd);
                }
        }
        else
        {
                return;
        }
}

void printOperand(struct FuncOperand *operand, int level){
	if(operand != NULL){
		cout << "[Operand] Level: " << level << " Code: " << operand->code << " Value: " << operand->value << endl;
	}
	else{
		cout << "[Operand] Level: " << level << "********NULL******\n\n";
	}
}

void printFuncOperator(struct FuncOperator * op, int level){
	if(op == NULL) {
		cout << "[Operator] Level: " << level-1 << "********NULL******\n\n";
		return;
	}

	cout << "[Operator] Level: " << level << "\n Operator Code: " << op->code << endl;

	cout << "[Operator] Level: " << level << " Left Operator:\n";
	printFuncOperator(op->leftOperator, level+1);

	cout << "[Operator] Level: " << level << " Left Operand:\n";

	printOperand(op->leftOperand, level);

	cout << "[Operator] Level: " << level << " Right Operator:\n";
	printFuncOperator(op->right, level+1);

}

void parseAndList(struct AndList * andList);

string getAttName(char** strPtr);

int main () {

	yyparse();
	printFuncOperator(finalFunction, 1);

	cout << "********   AND LIST ***********\n";
	PrintAndList(boolean);
	cout << "\n\n";

	cout << "********   Table LIST ***********\n";
	printTableList(tables);
	cout << "\n\n";

	cout << "********   Name LIST groupingAtts ***********\n";
	printNameList(groupingAtts);
	cout << "\n\n";

	cout << "********   Name LIST attsToSelect***********\n";
	printNameList(attsToSelect);
	cout << "\n\n";

	cout << "********  distinctAtts ***********\n";
	cout << distinctAtts << "\n\n";

	cout << "********   distinctFunc ***********\n";
	cout << distinctFunc << endl;

	parseAndList(boolean);
	
}

string getAttName(char** strPtr){
	int strLen = strlen(*strPtr);
	char tmp[strLen];
	strcpy(tmp, *strPtr);
	string alias(strtok(tmp, "."));
	*strPtr = (char*) malloc(strLen);
	strcpy(*strPtr, strtok(NULL, "."));
	return alias;
}

void parseAndList(struct AndList * andList){
	typedef struct OrList orList;

	unordered_map<char*, Schema*> tableToSchema;
	vector<orList*> joinOps;
	Schema* sch;
	orList *left;
	ComparisonOp *compOp;
	unordered_map<char*, vector<orList*>> cnf_map ;
	unordered_map<char*, char*> aliasToTable;

	// c1:n1 -> c_custkey = n_nationkey
	unordered_map<string, vector<orList*>> join_map ;

	while(tables != NULL){
		sch = new Schema("catalog",tables->tableName);
		tableToSchema[tables->aliasAs] = sch;
		tables = tables->next;
		aliasToTable[tables->aliasAs] = tables->tableName;
	}
	
	set<string> tblNameSet; 
	string relation1;
	string relation2;
	char* rel;
	stringstream joinKey;
	while(andList != NULL){

		left = andList->left;
		
		while(left != NULL){
			compOp = left->left;
			if(compOp->left->code == NAME && compOp->right->code == NAME){
				// Remove . from attribute names AND also get alias
				relation1 = getAttName(&(compOp->left->value));
				relation2 = getAttName(&(compOp->right->value));
				join_map[relation1 + ":" +relation2].push_back(andList->left);
			}
			else{
				char* attName;
				if(compOp->left->code == NAME){
					getAttName(&(compOp->left->value));
					attName = compOp->left->value;
				}
				else if(compOp->right->code == NAME){
					getAttName(&(compOp->right->value));
					attName = compOp->right->value;
				}

				for(pair<char*, Schema*> entry : tableToSchema){
					if(entry.second->Find(attName) != -1){
						tblNameSet.insert(entry.first);
					}
				}
			}
			left = left->rightOr;
		}

		if(tblNameSet.size() > 1){
			// joinOps.push_back(andList->left);
			joinKey.str("");
			for (set<string>::iterator it = tblNameSet.begin(); it != tblNameSet.end();)
			{
				joinKey << *it;
				++it;
				if(it != tblNameSet.end()) {
					joinKey << ":";	
				}
			}
			join_map[joinKey.str()].push_back(andList->left);
		}
		else if(tblNameSet.size() == 1){
			rel = (char*)(*tblNameSet.begin()).c_str();
			cnf_map[rel].push_back(andList->left);
		}
		andList = andList->rightAnd;
		tblNameSet.clear();
	}

	AndList* sfAndList;
	CNF *cnf;
	Record *literal;

	QueryPlanNode *dummy = new SelectFileNode();
	QueryPlanNode *currentNode = dummy;
	int pipeID = 1;
	unordered_map<char*, int> aliasToPipeID;

	for(pair<char*, vector<orList*>> entry : cnf_map){
		cout << "vector size: " << entry.second.size() << endl;
		int index = 0;

		sfAndList = new AndList();
		AndList* dummy = sfAndList;

		for(orList* l : entry.second){

			dummy->rightAnd = new AndList();
			dummy->rightAnd->left = l;
			dummy = dummy->rightAnd;
		}

		// sfAndList->rightAnd;

		PrintAndList(sfAndList->rightAnd);
		cout << endl;

		// Create CNF here
		cnf = new CNF();
		literal = new Record();
		cnf->GrowFromParseTree(sfAndList->rightAnd, tableToSchema[entry.first], *literal);
		cnf->Print();

		// Create Select File Node
		// SelectFile is going to expect an opened alias.bin file in current folder
		currentNode->next = new SelectFileNode(strcat(aliasToTable[entry.first], ".bin"), pipeID, tableToSchema[entry.first], cnf, literal);
		aliasToPipeID[entry.first] = pipeID++;
		currentNode = currentNode->next;

	}

	// Create And Lists for joins
	AndList *jAndList;
	AndList *dummyNode;
	list<AndList*> join_candidates;
	for(pair<string, vector<orList*>> entry: join_map) {
		jAndList = new AndList();
		dummyNode = jAndList;

		for(auto list: entry.second) {
			jAndList->rightAnd = new AndList();
			jAndList->rightAnd->left = list;
			jAndList = jAndList->rightAnd;
		}

		// AndList is present in dummyNode->nextAnd
		join_candidates.push_back(dummyNode->rightAnd);
	}

	// Assuming text file is already present
	Statistics s;
	s.Read("Statistics.txt");

	int bestEstimate = INT_MAX;
	list<AndList*>::iterator list_it;
	while(join_candidates.size() > 0) {
		list_it = join_candidates.begin();
		while(list_it != join_candidates.end()){
			// Need relnames and numtojoin for invoking estimate
			// Might need to store a map of AndList and relnames
		}
	}

}