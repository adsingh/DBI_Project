
#include <iostream>
#include "ParseTree.h"

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
	printFuncOperator(finalFunction, 1);

	cout << "********   AND LIST ***********\n";
	PrintAndList(boolean);

	cout << "********   Table LIST ***********\n";
	printTableList(tables);
	cout << endl;

	cout << "********   Name LIST groupingAtts ***********\n";
	printNameList(groupingAtts);

	cout << "********   Name LIST attsToSelect***********\n";
	printNameList(attsToSelect);

	cout << "********  distinctAtts ***********\n";
	cout << distinctAtts << endl;

	cout << "********   distinctFunc ***********\n";
	cout << distinctFunc << endl;
}