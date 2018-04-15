#include "QueryPlanNode.h"

SelectFileNode::SelectFileNode (char* fileName, int outPipeID, Schema *outSchema, CNF *selOp, Record *literal) : QueryPlanNode() {
   this->fileName = fileName;
   this->outPipeID = outPipeID;
   this->outSchema = outSchema;
   this->selOp = selOp;
   this->literal = literal;
}
void SelectFileNode::Print () {
	
}

SelectPipeNode::SelectPipeNode (int inPipeID, int outPipeID, Schema *outSchema, CNF *selOp, Record *literal) : QueryPlanNode() {
   this->inPipeID = inPipeID;
   this->outPipeID = outPipeID;
   this->outSchema = outSchema;
   this->selOp = selOp;
   this->literal = literal;
}

void SelectPipeNode::Print () {
	
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
	
}

DuplicateRemovalNode::DuplicateRemovalNode (int inPipeID, int outPipeID, Schema *outSchema, Schema *mySchema) : QueryPlanNode() {
   this->inPipeID = inPipeID;
   this->outPipeID = outPipeID;
   this->outSchema = outSchema;
   this->mySchema = mySchema;
}

void DuplicateRemovalNode::Print () {
	
}

SumNode::SumNode (int inPipeID, int outPipeID, Schema *outSchema, Function *computeMe) : QueryPlanNode() {
   this->inPipeID = inPipeID;
   this->outPipeID = outPipeID;
   this->outSchema = outSchema;
   this->computeMe = computeMe;
}

void SumNode::Print () {
	
}

GroupByNode::GroupByNode (int inPipeID, int outPipeID, Schema *outSchema, OrderMaker *groupAtts, Function *computeMe) : QueryPlanNode() {
   this->inPipeID = inPipeID;
   this->outPipeID = outPipeID;
   this->outSchema = outSchema;
   this->groupAtts = groupAtts;
   this->computeMe = computeMe;
}

void GroupByNode::Print () {
	
}


