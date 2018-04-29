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

int QueryPlanNode::clear_pipe(Pipe &in_pipe, Schema *schema, bool print)
{
	Record rec;
	int cnt = 0;
	while (in_pipe.Remove(&rec))
	{
		if (print)
		{
			rec.Print(schema);
		}
		cnt++;
	}
	return cnt;
}

vector<Pipe*> *QueryPlanNode::pipesVector;
int QueryPlanNode::numPipes;

void QueryPlanNode::createPipes(){
    QueryPlanNode::pipesVector = new vector<Pipe*>(QueryPlanNode::numPipes);
    for(int i = 0 ; i < QueryPlanNode::numPipes; i++){
        (*(QueryPlanNode::pipesVector))[i] = new Pipe(PIPE_BUFF_SIZE);
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
    cout << "===============================================" << endl;
}

void SelectFileNode::Run(){
    
    cout << "[SFNODE Run] Calling DBFile Open\n";
    dbFile.Open(fileName);
    cout << "[SFNODE Run] Calling SF RUN\n";
    SF.Use_n_Pages(1);
    SF.Run(dbFile, *(*QueryPlanNode::pipesVector)[outPipeID-1], *selOp, *literal);
	
}

void SelectFileNode::WaitUntilDone(){
    int cnt;
    if(next == NULL){
        cnt = clear_pipe(*(*QueryPlanNode::pipesVector)[outPipeID-1], outSchema, true);
    }
    
    SF.WaitUntilDone();
    cout << "Finished selection for " << fileName << endl;
    if(next == NULL){
        cout << " query returned " << cnt << " records\n";
    }
    dbFile.Close();
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
    cout << "===============================================" << endl;
}

void SelectPipeNode::Run(){
    
}

void SelectPipeNode::WaitUntilDone(){
    int cnt;
    if(next == NULL){
        cnt = clear_pipe(*(*QueryPlanNode::pipesVector)[outPipeID-1], outSchema, true);
    }
    SP.WaitUntilDone();
    if(next == NULL){
        cout << " query returned " << cnt << " records\n";
    }
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
    cout << "===============================================" << endl;
}

void ProjectNode::Run(){

    P.Use_n_Pages(1);
    P.Run(*(*QueryPlanNode::pipesVector)[inPipeID-1], *(*QueryPlanNode::pipesVector)[outPipeID-1], keepMe, numAttsInput, numAttsOutput);

}

void ProjectNode::WaitUntilDone(){

    int cnt;
    if(next == NULL){
        cout << "****************** WILL PRINT PROJECTS OUTPUT ***************\n";
        cnt = clear_pipe(*(*QueryPlanNode::pipesVector)[outPipeID-1], outSchema, false);
    }
    P.WaitUntilDone();
    if(next == NULL){
        cout << " query returned " << cnt << " records\n";
    }
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
    cout << "===============================================" << endl;
}

void JoinNode::Run(){

    J.Use_n_Pages(1);
    J.Run(*(*QueryPlanNode::pipesVector)[inPipe1ID-1], *(*QueryPlanNode::pipesVector)[inPipe2ID-1], *(*QueryPlanNode::pipesVector)[outPipeID-1]
            , *selOp, *literal );    
}

void JoinNode::WaitUntilDone(){
    
    J.WaitUntilDone();
    
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
    cout << "===============================================" << endl;
}

void DuplicateRemovalNode::Run(){
    
    D.Use_n_Pages(1);
    D.Run(*(*QueryPlanNode::pipesVector)[inPipeID-1], *(*QueryPlanNode::pipesVector)[outPipeID-1], *mySchema);
    
}

void DuplicateRemovalNode::WaitUntilDone(){
    int cnt;
    if(next == NULL){
        cnt = clear_pipe(*(*QueryPlanNode::pipesVector)[outPipeID-1], outSchema, true);
    }
    D.WaitUntilDone();
    if(next == NULL){
        cout << " query returned " << cnt << " records\n";
    }
}

SumNode::SumNode (int inPipeID, int outPipeID, Schema *outSchema,  Function *computeMe, bool isDistinctSum) : QueryPlanNode() {
   this->inPipeID = inPipeID;
   this->outPipeID = outPipeID;
   this->outSchema = outSchema;
   this->computeMe = computeMe;
   this->isDistinctSum = isDistinctSum;
}

void SumNode::Print () {
	cout << "Sum Operation Input PipeID = " << inPipeID 
         << " Output Pipe ID = " << outPipeID  << "\n\nOutput Schema: \n";
    PrintSchema(outSchema);
    cout << "\nSum Function:\n";
    computeMe->Print();
    cout << "===============================================" << endl;
}

void SumNode::Run(){
    
    S.Use_n_Pages(1);
    S.Run(*(*QueryPlanNode::pipesVector)[inPipeID-1], *(*QueryPlanNode::pipesVector)[outPipeID-1], *computeMe, isDistinctSum);
}

void SumNode::WaitUntilDone(){
    int cnt;
    if(next == NULL){
        cnt = clear_pipe(*(*QueryPlanNode::pipesVector)[outPipeID-1], outSchema, true);
    }
    S.WaitUntilDone();
    if(next == NULL){
        cout << " query returned " << cnt << " records\n";
    }
}

GroupByNode::GroupByNode (int inPipeID, int outPipeID, Schema *outSchema,  OrderMaker *groupAtts, Function *computeMe,bool isDistinctGroupBy) : QueryPlanNode() {
   this->inPipeID = inPipeID;
   this->outPipeID = outPipeID;
   this->outSchema = outSchema;
   this->groupAtts = groupAtts;
   this->computeMe = computeMe;
   this->isDistinctGroupBy = isDistinctGroupBy;
}

void GroupByNode::Print () {
	cout << "Group By Operation Input PipeID = " << inPipeID 
         << " Output Pipe ID = " << outPipeID  << "\n\nOutput Schema: \n";
    PrintSchema(outSchema);
    cout << "\nGroupBy Ordermaker:\n";
    groupAtts->Print();
    cout << "\nAggregate Function:\n";
    computeMe->Print();
    cout << "===============================================" << endl;
}

void GroupByNode::Run(){
    
    G.Use_n_Pages(1);
    G.Run(*(*QueryPlanNode::pipesVector)[inPipeID-1], *(*QueryPlanNode::pipesVector)[outPipeID-1], *groupAtts, *computeMe, isDistinctGroupBy);
    
}

void GroupByNode::WaitUntilDone(){
    int cnt;
    if(next == NULL){
        cnt = clear_pipe(*(*QueryPlanNode::pipesVector)[outPipeID-1], outSchema, true);
    }
    G.WaitUntilDone();
    if(next == NULL){
        cout << " query returned " << cnt << " records\n";
    }
}