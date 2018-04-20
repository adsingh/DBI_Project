
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
#include <map>
#include <istream>
#include <climits>

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

Schema* CombineSchema(Schema* schema1, Schema* schema2);

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
	unordered_map<char*, vector<orList*>> sf_cnf_map ;
	unordered_map<char*, char*> aliasToTable;
	unordered_map<char*, SelectFileNode*> aliasToSfNode;

	// c1:n1 -> c_custkey = n_nationkey
	unordered_map<string, vector<orList*>> join_map ;
	map<string, AndList*> join_cnf_map;

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
			sf_cnf_map[rel].push_back(andList->left);
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

	for(pair<char*, vector<orList*>> entry : sf_cnf_map){
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
		currentNode->next = new SelectFileNode(strcat(aliasToTable[entry.first], ".bin"), pipeID, tableToSchema[entry.first], cnf, literal);
		aliasToPipeID[entry.first] = pipeID++;
		currentNode = currentNode->next;

		// Adding entry in aliasToSFNode
		aliasToSfNode[entry.first] = (SelectFileNode*)currentNode;

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
				
		// Filling ordered map
		join_cnf_map[entry.first] = dummyNode->rightAnd;
	}

	// Assuming text file is already present
	Statistics s;
	s.Read("Statistics.txt");

	int bestEstimate = INT_MAX;
	int size = join_map.size();
	
	set<string> joined_rel;
	string bestEntry;
	AndList* tempAndList;
	for(int i = 0 ; i < size; i++) {
		char* rel_name[2+i];

		bestEstimate = INT_MAX;
		for(pair<string, vector<orList*>> entry: join_map) {
			int index = 0;
			stringstream ss(entry.first);
			string item1, item2;
			getline(ss, item1, ':');
			getline(ss, item2, ':');
			
			if(joined_rel.count(item1) != 0 || joined_rel.count(item2) != 0) {
				for(auto rel:joined_rel) {
					rel_name[index++] = (char*)rel.c_str();
				}
				if(joined_rel.count(item1) == 0) {
					rel_name[index++] = (char*)item1.c_str();
				} else {
					rel_name[index++] = (char*) item2.c_str();
				}
				
			} else {
				rel_name[index++] = (char*) item1.c_str();
				rel_name[index++] = (char*) item2.c_str();
			}

			int estimate = s.Estimate(join_cnf_map[entry.first], rel_name, index);
			if(estimate < bestEstimate) {
				bestEstimate = estimate;
				bestEntry = entry.first;
			}
		}

		// Remove the entry from the inner map
		// so that it is not used in future cases
		join_map.erase(bestEntry);

		// Delete and re insert into join_cnf_map. to maintain final order
		tempAndList = join_cnf_map[bestEntry];
		join_cnf_map.erase(bestEntry);
		join_cnf_map[bestEntry] = tempAndList;

		// Best estimates Found. Adding rel names to set
		stringstream ss(bestEntry);
		string item;
		while (getline(ss, item, ':')) {
			if(joined_rel.count(item) == 0) {
				joined_rel.insert(item);
			}
		}

	}

	// At this stage, join_cnf_map should have the correct order
	unordered_map<string, int> relToGroupNo;
	unordered_map<int, JoinNode*> groupToJoinNode;
	// string relation1;
	// string relation2;
	int groupRel1 = -1;
	int groupRel2 = -1;
	int groupNo = 0;
	JoinNode *join_node;
	Schema *schema1, *schema2, *outSchema;
	int inPipe1ID = -1;
	int inPipe2ID = -1;
	int outPipeID = -1;
	for(pair<string, AndList*> entry: join_cnf_map) {
		// Read relation names
		stringstream ss(entry.first);
		getline(ss, relation1, ':');
		getline(ss, relation2, ':'); 

		// Find groupNo if already present in map
		groupRel1 = -1;
		groupRel2 = -1;
		if(relToGroupNo.count(relation1) > 0) {
			groupRel1 = relToGroupNo[relation1];
		}
		if(relToGroupNo.count(relation2) > 0) {
			groupRel2 = relToGroupNo[relation2];
		}

		// Get appropriate inputs for join
		if(groupRel1 == -1) {
			// If was not joined previously, use its SelectFileNode
			inPipe1ID = aliasToSfNode[(char*)relation1.c_str()]->outPipeID;
			schema1 = aliasToSfNode[(char*)relation1.c_str()]->outSchema;

		} else { // Else use info from previous join
			inPipe1ID = groupToJoinNode[groupRel1]->outPipeID;
			schema1 = groupToJoinNode[groupRel1]->outSchema;
		}

		if(groupRel2 == -1) {
			inPipe2ID = aliasToSfNode[(char*)relation2.c_str()]->outPipeID;
			schema2 = aliasToSfNode[(char*)relation2.c_str()]->outSchema;
		} else{
			inPipe2ID = groupToJoinNode[groupRel2]->outPipeID;
			schema2 = groupToJoinNode[groupRel2]->outSchema;
		}

		// create cnf and literal using the 2 schemas
		cnf = new CNF();
		literal = new Record();
		cnf->GrowFromParseTree(entry.second, schema1, schema2, *literal);

		// Create Combined Schema
		outSchema = CombineSchema(schema1, schema2);

		// Create JoinNode
		join_node = new JoinNode(inPipe1ID, inPipe2ID, pipeID++, outSchema, cnf, literal);
		currentNode->next = join_node;
		currentNode = currentNode->next;

		// Update relToGroupNo and groupToJoinNode accordingly
		
		// If both relations are not previously joined before
		// add new entries in both maps
		if(groupRel1 == -1 && groupRel2 == -1) {
			relToGroupNo[relation1] = groupNo;
			relToGroupNo[relation2] = groupNo;			
			groupToJoinNode[groupNo] = join_node;
			groupNo++;
		} else if(groupRel1 == -1) { // relation2 was previously joined
			relToGroupNo[relation1] = relToGroupNo[relation2];
			groupToJoinNode[relToGroupNo[relation1]] = join_node;
		} else if(groupRel2 == -1) { // relation1 was previously joined
			relToGroupNo[relation2] = relToGroupNo[relation1];
			groupToJoinNode[relToGroupNo[relation2]] = join_node;
		} else { // both are previously joined 
			// Update the smaller group
			// Delete the larger group
			int minGroup = min(relToGroupNo[relation1], relToGroupNo[relation2]);
			int maxGroup = max(relToGroupNo[relation1], relToGroupNo[relation2]);

			for(pair<string, int> entry : relToGroupNo) {
				if(entry.second == maxGroup) {
					relToGroupNo[entry.first] = minGroup;
				}
			}

			groupToJoinNode[minGroup] = join_node;
			groupToJoinNode.erase(maxGroup);
		}

	}

    //Group by  ------- Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe
    if(groupingAtts != NULL){

        // Create AND List
        AndList *dummy = new AndList();
        AndList *grpByAndList = dummy;

        while(groupingAtts != NULL){

            char* name = groupingAtts->name;
            Operand *left = new Operand();
            left->code = NAME;
            left->value = name;
            ComparisonOp *grpByCompOp = new ComparisonOp();
            grpByCompOp->code = EQUALS;
            grpByCompOp->left = left;
            grpByCompOp->right = left;
            OrList *grpByOrList = new OrList();
            grpByOrList->left = grpByCompOp;
            grpByOrList->rightOr = NULL;
            grpByAndList->rightAnd = new AndList();
            grpByAndList->rightAnd->left = grpByOrList;
            grpByAndList = grpByAndList->rightAnd;
        }

        // Final AndList
        grpByAndList = dummy->rightAnd;

        // Create CNF
        cnf = new CNF();
        literal = new Record();
        cnf->GrowFromParseTree(grpByAndList, currentNode->outSchema, *literal);

        // Create OrderMaker
        OrderMaker *grp_order, *dummyOrderMaker;
        grp_order = new OrderMaker();
        dummyOrderMaker = new OrderMaker();
        cnf->GetSortOrders(*grp_order, *dummyOrderMaker);

        // Use FuncOperator to generate this with the help of the Function::GrowFromParseTree
        Function *grpByAggFunction = new Function();
        grpByAggFunction->GrowFromParseTree (finalFunction, *currentNode->outSchema);

        // Create OutSchema
        // First attribute will be Double, Rest wiil be the grpByAttributes from above

        // Create GroupBy Node
        currentNode->next = new GroupByNode(currentNode->outPipeID , pipeID++, Schema *outSchema, grp_order, grpByAggFunction);

    }

    // Projection  ---------- Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput

    // Duplicate removal ----------- Pipe &inPipe, Pipe &outPipe, Schema &mySchema

    // OR

    // Sum  ------------- Pipe &inPipe, Pipe &outPipe, Function &computeMe
	// inPipe -- From the previous operation in the order of the operations
    // outPipe -- Create a new pipeID
    // computeMe -- Use FuncOperator to generate this with the help of the Function::GrowFromParseTree


}

Schema* CombineSchema(Schema* schema1, Schema* schema2) {
	int total_atts = schema1->GetNumAtts() + schema2->GetNumAtts();
	Attribute* result_atts = new Attribute[total_atts];
	int index = 0;
	Attribute* atts1 = schema1->GetAtts();
	Attribute* atts2 = schema2->GetAtts();
	for (int i = 0 ; i < schema1->GetNumAtts(); i++) {
		result_atts[index++] = atts1[i];
	}
	for (int i = 0 ; i < schema2->GetNumAtts(); i++) {
		result_atts[index++] = atts2[i];
	}

	Schema *result_schema = new Schema("joined_sch", total_atts, result_atts );
	return result_schema;
}