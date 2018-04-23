
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

QueryPlanNode* CreateQueryPlan();

string getRelName(char** strPtr);

Schema* CombineSchema(Schema* schema1, Schema* schema2);

int main () {

	yyparse();
	// printFuncOperator(finalFunction, 1);

	// cout << "********   AND LIST ***********\n";
	// PrintAndList(boolean);
	// cout << "\n\n";

	// cout << "********   Table LIST ***********\n";
	// printTableList(tables);
	// cout << "\n\n";

	// cout << "********   Name LIST groupingAtts ***********\n";
	// printNameList(groupingAtts);
	// cout << "\n\n";

	// cout << "********   Name LIST attsToSelect***********\n";
	// printNameList(attsToSelect);
	// cout << "\n\n";

	// cout << "********  distinctAtts ***********\n";
	// cout << distinctAtts << "\n\n";

	// cout << "********   distinctFunc ***********\n";
	// cout << distinctFunc << endl;

	CreateQueryPlan();
	
}

string getRelName(char** strPtr){
	int strLen = strlen(*strPtr);
	char tmp[strLen+1];
	strcpy(tmp, *strPtr);
	string alias(strtok(tmp, "."));
	// *strPtr = (char*) malloc(strLen);
	// strcpy(*strPtr, strtok(NULL, "."));
	return alias;
}

AndList* nameToAndList(char * name){
	AndList* andList = new AndList();
	Operand *left = new Operand();
	left->code = NAME;
	left->value = name;
	ComparisonOp *SFCompOp = new ComparisonOp();
	SFCompOp->code = EQUALS;
	SFCompOp->left = left;
	SFCompOp->right = left;
	OrList *SFOrList = new OrList();
	SFOrList->left = SFCompOp;
	SFOrList->rightOr = NULL;
	andList->left = SFOrList;
	return andList;
}

void modifySchemaAttsNames(Schema** sch, string aliasAs){
	
	int numAtts = (*sch)->GetNumAtts();
	Attribute* atts = (*sch)->GetAtts();
	Attribute* attsRes = new Attribute[numAtts];

	for(int i = 0 ; i < numAtts; i++){
		// string attName(atts[i].name);
		string temp(aliasAs + "." + atts[i].name);
		attsRes[i].myType = atts[i].myType;
		attsRes[i].name = new char[temp.length() + 1];
		strcpy(attsRes[i].name, temp.c_str());
		//cout <<  " ********* : "<< attsRes[i].name << "  " << attsRes[i].myType << endl;
	}
	*sch = new Schema("schema", numAtts, attsRes);
}

void PrintSchema(Schema* sch){
	if(sch == NULL){
		cout << "Schema is empty\n";
		return;
	}
	int numAtts = sch->GetNumAtts();
	Attribute* attsRes = sch->GetAtts();

	for(int i = 0 ; i < numAtts; i++){
		// string attName(atts[i].name);
		cout << attsRes[i].name <<  " : " << attsRes[i].myType << endl;
	}
}

QueryPlanNode* CreateQueryPlan(){
	typedef struct OrList orList;
	AndList* andList = boolean;
	unordered_map<string, Schema*> aliasToSchema;
	vector<orList*> joinOps;
	Schema* sch;
	orList *left;
	ComparisonOp *compOp;
	unordered_map<string, vector<orList*>> sf_cnf_map ;
	unordered_map<string, string> aliasToTable;
	unordered_map<string, SelectFileNode*> aliasToSfNode;

	// c1:n1 -> c_custkey = n_nationkey
	unordered_map<string, vector<orList*>> join_map ;
	map<string, AndList*> join_cnf_map;

	while(tables != NULL){
		sch = new Schema("catalog",tables->tableName);
		string aliasName(tables->aliasAs);
		string tableName(tables->tableName);
		modifySchemaAttsNames(&sch, aliasName);
		aliasToSchema[aliasName] = sch;
		aliasToTable[aliasName] = tableName;
		tables = tables->next;
	}

	// cout << "[test.cc] Created Schema Map\n";
	
	set<string> tblNameSet; 
	string relation1;
	string relation2;
	string rel;
	stringstream joinKey;

	while(andList != NULL){

		left = andList->left;
		
		while(left != NULL){
			compOp = left->left;
			if(compOp->left->code == NAME && compOp->right->code == NAME){
				// Remove . from attribute names AND also get alias
				relation1 = getRelName(&(compOp->left->value));
				relation2 = getRelName(&(compOp->right->value));
				join_map[relation1 + ":" +relation2].push_back(andList->left);
			}
			else{
				char* attName;
				if(compOp->left->code == NAME){
					// getAttName(&(compOp->left->value));
					attName = compOp->left->value;
				}
				else if(compOp->right->code == NAME){
					// getAttName(&(compOp->right->value));
					attName = compOp->right->value;
				}

				for(pair<string, Schema*> entry : aliasToSchema){
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
			rel = *(tblNameSet.begin());
			sf_cnf_map[rel].push_back(andList->left);
		}
		andList = andList->rightAnd;
		tblNameSet.clear();
	}

	AndList* sfAndList;
	CNF *cnf;
	Record *literal;

	QueryPlanNode *dummyQueryPlanNode = new SelectFileNode();
	QueryPlanNode *currentNode = dummyQueryPlanNode;
	int pipeID = 1;
	unordered_map<string, int> aliasToPipeID;

	// cout << "[test.cc] Created SF and Join CNF Maps\n";

	for(pair<string, vector<orList*>> entry : sf_cnf_map){
		// cout << "vector size: " << entry.second.size() << endl;
		int index = 0;

		sfAndList = new AndList();
		AndList* dummy = sfAndList;

		for(orList* l : entry.second){

			dummy->rightAnd = new AndList();
			dummy->rightAnd->left = l;
			dummy = dummy->rightAnd;
		}

		// sfAndList->rightAnd;

		// PrintAndList(sfAndList->rightAnd);
		cout << endl;

		// Create CNF here
		cnf = new CNF();
		literal = new Record();
		if(aliasToSchema.find(entry.first) == aliasToSchema.end()){
			cout << "Schema not present for the relation : " << entry.first << endl;
			exit(1);
		}

		cnf->GrowFromParseTree(sfAndList->rightAnd, aliasToSchema[entry.first], *literal);
		// cnf->Print();

		// Create Select File Node
		currentNode->next = new SelectFileNode((char*)(aliasToTable[entry.first] + ".bin").c_str(), pipeID, aliasToSchema[entry.first], cnf, literal);
		aliasToPipeID[entry.first] = pipeID++;
		currentNode = currentNode->next;
		currentNode->Print();
		cout << "===============================================" << endl;
		// Adding entry in aliasToSFNode
		aliasToSfNode[entry.first] = (SelectFileNode*)currentNode;

	}

	// cout << "[test.cc] Created SF Nodes\n";
	
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

		// ** TODO split entry.first and Check whether
		// both aliases are present in aliasToSfNode map
		// if not create an SfNode using attribute in orlist
		stringstream ss(entry.first);
		string alias1, alias2;
		getline(ss, alias1, ':');
		getline(ss, alias2, ':');
		if(aliasToSfNode.find(alias1) == aliasToSfNode.end()){
			cnf = new CNF();
			literal = new Record();

			// Create ANDLIST
			// char* name = (char*)alias1.c_str();
			char* name = aliasToSchema[alias1]->GetAtts()[0].name;
			// cout << "[test.cc] jugaad char name = " << name << endl;
			sfAndList = nameToAndList(name);
			// PrintAndList(sfAndList);
			cnf->GrowFromParseTree(sfAndList, aliasToSchema[alias1], *literal);
			// cnf->Print();

			currentNode->next = new SelectFileNode((char*)(aliasToTable[alias1]+".bin").c_str(), pipeID, aliasToSchema[alias1],  cnf, literal);
			aliasToPipeID[alias1] = pipeID++;
			currentNode = currentNode->next;
			currentNode->Print();
			cout << "===============================================" << endl;
			// Adding entry in aliasToSFNode
			aliasToSfNode[alias1] = (SelectFileNode*)currentNode;
		}

		if(aliasToSfNode.find(alias2) == aliasToSfNode.end()){
			cnf = new CNF();
			literal = new Record();

			// Create ANDLIST
			char* name = aliasToSchema[alias2]->GetAtts()[0].name;
			sfAndList = nameToAndList(name);
			// PrintAndList(sfAndList);

			cnf->GrowFromParseTree(sfAndList, aliasToSchema[alias2], *literal);
			// cnf->Print();
			
			currentNode->next = new SelectFileNode((char*)(aliasToTable[alias2]+".bin").c_str(), pipeID, aliasToSchema[alias2], cnf, literal);
			aliasToPipeID[alias2] = pipeID++;
			currentNode = currentNode->next;
			currentNode->Print();
			cout << "===============================================" << endl;
			// Adding entry in aliasToSFNode
			aliasToSfNode[alias2] = (SelectFileNode*)currentNode;
		}

	}

	// Assuming text file is already present
	Statistics s;
	s.Read("Statistics.txt");
	for(pair<string, string> entry: aliasToTable){
		s.CopyRel((char*)entry.second.c_str(), (char*)entry.first.c_str());
	}

	// **TODO Call Copy Rel For getting c1 from customer

	int bestEstimate = INT_MAX;
	int size = join_map.size();
	
	set<string> joined_rel;
	string bestEntry;
	AndList* tempAndList;
	for(int i = 0 ; i < size; i++) {
		char* rel_name[2+i];
		char* best_rel_name[2+i];
		int best_index;
		bestEstimate = INT_MAX;
		for(pair<string, vector<orList*>> entry: join_map) {
			int index = 0;
			stringstream ss(entry.first);
			string item1, item2;
			getline(ss, item1, ':');
			getline(ss, item2, ':');
			
			if(joined_rel.count(item1) != 0 || joined_rel.count(item2) != 0) {
				for(auto rel:joined_rel) {
					rel_name[index] = new char[rel.length()+1];
					strcpy(rel_name[index++], rel.c_str());
				}
				if(joined_rel.count(item1) == 0) {
					rel_name[index] = new char[item1.length()+1];
					strcpy(rel_name[index++], item1.c_str());
				} else {
					rel_name[index] = new char[item2.length()+1];
					strcpy(rel_name[index++], item2.c_str());
				}
				
			} else {
				rel_name[index] = new char[item1.length()+1];
				strcpy(rel_name[index++], item1.c_str());
				rel_name[index] = new char[item2.length()+1];
				strcpy(rel_name[index++], item2.c_str());
			}
			//cout << "[test.cc] Calling Estimate for index = " << index << endl;
			int estimate = s.Estimate(join_cnf_map[entry.first], rel_name, index);
			
			if(estimate < bestEstimate) {
				bestEstimate = estimate;
				bestEntry = entry.first;
				for(int j = 0 ; j < 2+i; j++){
					best_rel_name[j] = new char[strlen(rel_name[j] + 1)];
					strcpy(best_rel_name[j], rel_name[j]);
				}
				best_index = index;
			}
		}

		s.Apply(join_cnf_map[bestEntry], best_rel_name, best_index);
		
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
			joined_rel.insert(item);
		}

	}
	
	// At this stage, join_cnf_map should have the correct order
	unordered_map<string, int> relToGroupNo;
	unordered_map<int, JoinNode*> groupToJoinNode;
	
	int groupRel1, groupRel2;
	int groupNo = 0;
	JoinNode *join_node;
	Schema *schema1, *schema2, *outSchema;
	int inPipe1ID = -1;
	int inPipe2ID = -1;
	int outPipeID = -1;

	int round = 1;

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
			inPipe1ID = aliasToSfNode[relation1]->outPipeID;
			schema1 = aliasToSfNode[relation1]->outSchema;

		} else { // Else use info from previous join
			inPipe1ID = groupToJoinNode[groupRel1]->outPipeID;
			schema1 = groupToJoinNode[groupRel1]->outSchema;
		}

		if(groupRel2 == -1) {
			inPipe2ID = aliasToSfNode[relation2]->outPipeID;
			schema2 = aliasToSfNode[relation2]->outSchema;

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
		currentNode->Print();
		cout << "===============================================" << endl;
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

	bool isGroupByPresent = false;
	
	// ************* AMARDEEP CODE **********************
	if(groupingAtts != NULL){

		isGroupByPresent = true;
        // Create AND List and OutSchema
        AndList *dummy = new AndList();
        AndList *grpByAndList = dummy;
		vector<char*> grpingAttsNames;
		vector<Type> grpingAttsTypes;

        while(groupingAtts != NULL){

            char* name = groupingAtts->name;
			grpByAndList->rightAnd = nameToAndList(name);
			grpByAndList = grpByAndList->rightAnd;
			grpingAttsNames.push_back(name);
			grpingAttsTypes.push_back(aliasToSchema[getRelName(&name)]->FindType(name));
			groupingAtts = groupingAtts->next;
        }

        // Final AndList
        grpByAndList = dummy->rightAnd;

		// PrintAndList(grpByAndList);

        // Create CNF
        cnf = new CNF();
        literal = new Record();
        cnf->GrowFromParseTree(grpByAndList, currentNode->outSchema, *literal);
		// cnf->Print();

        // Create OrderMaker
        OrderMaker *grp_order, *dummyOrderMaker;
        grp_order = new OrderMaker();
        dummyOrderMaker = new OrderMaker();
        cnf->GetSortOrders(*grp_order, *dummyOrderMaker);
		// grp_order->Print();

        // Use FuncOperator to generate this with the help of the Function::GrowFromParseTree
        Function *grpByAggFunction = new Function();
        grpByAggFunction->GrowFromParseTree (finalFunction, *currentNode->outSchema);

		// Create OutSchema
		// First attribute will be Double, Rest wiil be the grpByAttributes from above
		Attribute* atts = new Attribute[grpingAttsNames.size()+1];
		atts[0].myType = Double;
		atts[0].name = "aggResult";
		for(int i = 1; i <= grpingAttsNames.size(); i++){
			atts[i].name = grpingAttsNames[i-1];
			atts[i].myType = grpingAttsTypes[i-1];
		}

		outSchema = new Schema("grpByOutSchema", grpingAttsNames.size()+1, atts);
		// PrintSchema(outSchema);

        // Create GroupBy Node
        currentNode->next = new GroupByNode(currentNode->outPipeID , pipeID++, outSchema, grp_order, grpByAggFunction);
		currentNode = currentNode->next;
		currentNode->Print();
		cout << "===============================================" << endl;
		// cout << "[test.cc] Created Group By Node\n";
    }
	// Sum  ------------- Pipe &inPipe, Pipe &outPipe, Function &computeMe
	// inPipe -- From the previous operation in the order of the operations
    // outPipe -- Create a new pipeID
    // computeMe -- Use FuncOperator to generate this with the help of the Function::GrowFromParseTree
	else if(groupingAtts == NULL && finalFunction != NULL && distinctFunc == 0){
		
		Function *sumAggFunction = new Function();
        sumAggFunction->GrowFromParseTree (finalFunction, *currentNode->outSchema);
		Attribute* atts = new Attribute[1];
		atts[0].name = "sumResult";
		atts[0].myType = Double;

		currentNode->next = new SumNode(currentNode->outPipeID, pipeID++, new Schema("sum_schema", 1, atts), sumAggFunction );
		currentNode = currentNode->next;
		currentNode->Print();
		cout << "===============================================" << endl;
		// cout << "[test.cc] Created SUM Node\n";

	} else if(groupingAtts == NULL && finalFunction != NULL && distinctFunc == 1) {
		// For handling SELECT SUM DISTINCT (a.b + a.c) from table_a AS a
	}
	// PrintSchema(currentNode->outSchema);

	// Projection  ---------- Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput
	// in Pipe = currentNode->outPipeID, numAttsInput = currentNode->outSchem->GetNumAtts(), 

	// CreateProjectionNode(currentNode, isGroupByPresent, pipeID);
	vector<int> attsPosToKeep;
	vector<char*> attsToSelectNames;
	vector<Type> attsToSelectTypes;
	int pos;
	if(attsToSelect != NULL){

		while(attsToSelect != NULL){
			int pos = currentNode->outSchema->Find(attsToSelect->name);
			if(pos == -1){
				cout << "Not found the ATTS: " << attsToSelect->name << endl;
				exit(1);
			}
			attsToSelectNames.push_back(attsToSelect->name);
			attsToSelectTypes.push_back(currentNode->outSchema->FindType(attsToSelect->name));
			attsPosToKeep.push_back(pos);
			attsToSelect = attsToSelect->next;
		}

		if(isGroupByPresent){
			attsToSelectNames.push_back("aggResult");
			attsToSelectTypes.push_back(Double);
			attsPosToKeep.push_back(0);
		}
		

		int* keepMe = new int[attsPosToKeep.size()];
		int index = attsPosToKeep.size()-1;
		Attribute* atts = new Attribute[attsPosToKeep.size()];

		for(int attsPos: attsPosToKeep){
			atts[index].name = attsToSelectNames[attsPosToKeep.size()-1 - index];
			atts[index].myType = attsToSelectTypes[attsPosToKeep.size()-1 - index];
			keepMe[index--] = attsPos;
		}

		int numsAttsInput = currentNode->outSchema->GetNumAtts();
		int numsAttsOutput = attsPosToKeep.size();

		// Create OutSchema
		outSchema = new Schema("projectionSchema", numsAttsOutput, atts);
		// PrintSchema(outSchema);

		currentNode->next = new ProjectNode(currentNode->outPipeID, pipeID++, outSchema, keepMe, numsAttsInput, numsAttsOutput);
		currentNode = currentNode->next;
		currentNode->Print();

		// cout << "[test.cc] Created Projection Node\n";	
	}

	// Duplicate removal ----------- Pipe &inPipe, Pipe &outPipe, Schema &mySchema
	if(distinctAtts == 1){
		currentNode->next = new DuplicateRemovalNode(currentNode->outPipeID, pipeID++, currentNode->outSchema, currentNode->outSchema);
		currentNode = currentNode->next;
		currentNode->Print();
		cout << "===============================================" << endl;
		// cout << "[test.cc] Created Duplicate removal Node\n";
	}

	return dummyQueryPlanNode->next;

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

	Schema* result_schema = new Schema("joined_sch", total_atts, result_atts );
	return result_schema;
}