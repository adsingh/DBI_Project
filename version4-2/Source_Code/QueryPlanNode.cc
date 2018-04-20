#include "QueryPlanNode.h"
#include <string.h>

void QueryPlanNode::PrintSchema(Schema *sch){
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

SelectFileNode::SelectFileNode () {
    
}
SelectFileNode::SelectFileNode (char* fileName, int outPipeID, Schema *outSchema, CNF *selOp, Record *literal) : QueryPlanNode() {
   
   this->fileName = (char*) malloc(strlen(fileName)+1);
   strcpy(this->fileName, fileName);
   this->outPipeID = outPipeID;
   this->outSchema = outSchema;
   this->selOp = selOp;
   this->literal = literal;
}
void SelectFileNode::Print () {
	cout << "SelectFile Operation Input Filename " << fileName << " Output Pipe ID " << 
            outPipeID << endl << " \nOutput Schema: \n";
    PrintSchema(outSchema);
    cout << "\nSelect File CNF:\n";
    selOp->Print();
}

SelectPipeNode::SelectPipeNode (int inPipeID, int outPipeID, Schema *outSchema, CNF *selOp, Record *literal) : QueryPlanNode() {
   this->inPipeID = inPipeID;
   this->outPipeID = outPipeID;
   this->outSchema = outSchema;
   this->selOp = selOp;
   this->literal = literal;
}

void SelectPipeNode::Print () {
	cout << "SelectPipe Operation Input PipeID " << inPipeID << " Output Pipe ID " << 
            outPipeID << endl << " \nOutput Schema: \n";
    PrintSchema(outSchema);
    cout << "\nSelect File CNF:\n";
    selOp->Print();
}

ProjectNode::ProjectNode (int inPipeID, int outPipeID, Schema *outSchema, int *keepMe, int numAttsInput, int numAttsOutput) : QueryPlanNode() {
   this->inPipeID = inPipeID;
   this->outPipeID = outPipeID;
   this->outSchema = outSchema;
   this->keepMe = keepMe;
   this->numAttsInput = numAttsInput;
   this->numAttsOutput = numAttsOutput;
}

void ProjectNode::Print () {
	cout << "Project Operation Input PipeID " << inPipeID << " Output Pipe ID " << 
            outPipeID << endl << " \nOutput Schema: \n";
    PrintSchema(outSchema);
    cout << "\nAttributes to keep:\n";
    for(int i = 0 ; i < numAttsOutput; i++){
        cout << keepMe[i] << " ";
    }
    cout << endl;
    
}

JoinNode::JoinNode (int inPipe1ID, int inPipe2ID, int outPipeID, Schema *outSchema, CNF *selOp, Record* literal) : QueryPlanNode() {
   this->inPipe1ID = inPipe1ID;
   this->inPipe2ID = inPipe2ID;
   this->outPipeID = outPipeID;
   this->outSchema = outSchema;
   this->selOp  = selOp;
   this->literal = literal;
}

void JoinNode::Print () {
	cout << "Join Operation Input PipeID1 = " << inPipe1ID 
         << " Input PipeID2 = " << inPipe2ID  << " Output Pipe ID = " << 
            outPipeID  << "\n\nOutput Schema: \n";
    PrintSchema(outSchema);
    cout << "\nJoin CNF:\n";
    selOp->Print();
}

DuplicateRemovalNode::DuplicateRemovalNode (int inPipeID, int outPipeID, Schema *outSchema, Schema *mySchema) : QueryPlanNode() {
   this->inPipeID = inPipeID;
   this->outPipeID = outPipeID;
   this->outSchema = outSchema;
   this->mySchema = mySchema;
}

void DuplicateRemovalNode::Print () {
	cout << "Duplicate removal Operation Input PipeID = " << inPipeID 
         << " Output Pipe ID = " << outPipeID  << "\n\nOutput Schema: \n";
    PrintSchema(outSchema);
}

SumNode::SumNode (int inPipeID, int outPipeID, Schema *outSchema,  Function *computeMe) : QueryPlanNode() {
   this->inPipeID = inPipeID;
   this->outPipeID = outPipeID;
   this->outSchema = outSchema;
   this->computeMe = computeMe;
}

void SumNode::Print () {
	cout << "Sum Operation Input PipeID = " << inPipeID 
         << " Output Pipe ID = " << outPipeID  << "\n\nOutput Schema: \n";
    PrintSchema(outSchema);
    cout << "\nSum Function:\n";
    computeMe->Print();
}

GroupByNode::GroupByNode (int inPipeID, int outPipeID, Schema *outSchema,  OrderMaker *groupAtts, Function *computeMe) : QueryPlanNode() {
   this->inPipeID = inPipeID;
   this->outPipeID = outPipeID;
   this->outSchema = outSchema;
   this->groupAtts = groupAtts;
   this->computeMe = computeMe;
}

void GroupByNode::Print () {
	cout << "Group By Operation Input PipeID = " << inPipeID 
         << " Output Pipe ID = " << outPipeID  << "\n\nOutput Schema: \n";
    PrintSchema(outSchema);
    cout << "\nGroupBy Ordermaker:\n";
    groupAtts->Print();
    cout << "\nAggregate Function:\n";
    computeMe->Print();
}


