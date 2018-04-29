
#include <iostream>
#include "ParseTree.h"
#include <string.h>

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

extern char* tableName;
extern char* dbFileType;
extern struct AttsToCreate *colsToCreate;
extern struct SortOrder* sortOrder;
extern char* dbFileToLoad;
extern char* outputType;
extern QueryType queryType;

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

void PrintOperand(struct Operand *pOperand)
{
        if(pOperand!=NULL)
        {
                cout<<pOperand->value<<" ";
        }
        else
                return;
}

void PrintComparisonOp(struct ComparisonOp *pCom)
{
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
void PrintOrList(struct OrList *pOr)
{
        if(pOr !=NULL)
        {
                struct ComparisonOp *pCom = pOr->left;
                PrintComparisonOp(pCom);

                if(pOr->rightOr)
                {
                        cout<<" OR ";
                        PrintOrList(pOr->rightOr);
                }
        }
        else
        {
                return;
        }
}
void PrintAndList(struct AndList *pAnd)
{
        if(pAnd !=NULL)
        {
                struct OrList *pOr = pAnd->left;
                PrintOrList(pOr);
                if(pAnd->rightAnd)
                {
                        cout<<" AND ";
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

        // cout << "Table name: " << tableName << endl;
        // cout << "DbFile Type: " << dbFileType << endl;

        // while(colsToCreate != NULL){
        //         cout << "Colname: " << colsToCreate->attDetails->name << " , Type: " << colsToCreate->attDetails->type << endl;
        //         colsToCreate = colsToCreate->next;
        // }

        // if(strcmp(dbFileType, "SORTED") == 0){
        //         cout << "Cols to sort on: \n";
        //         int attCnt = 1;
        //         while(sortOrder != NULL){
        //                 cout << "Att " << attCnt++ << " : " << sortOrder->attName << endl;
        //                 sortOrder = sortOrder->next;
        //         }
        // }

        if(dbFileType != NULL){
                cout << "dbFileType : "  << dbFileType << endl;
        }

        if(outputType != NULL){
                cout << "outputType : " << outputType << endl;
        }

        if(tableName != NULL){
                cout << "tableName: " << tableName << endl;
        }

        if(dbFileToLoad != NULL){
                cout << "dbFileToLoad : "  << dbFileToLoad << endl;
        }

        cout << "QueryType : " << queryType << endl;

}