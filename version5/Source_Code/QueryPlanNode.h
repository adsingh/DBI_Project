#include "Comparison.h"
#include "Record.h"
#include "Function.h"
#include "RelOp.h"
#include <vector>
#include <iostream>

#define PIPE_BUFF_SIZE 100

using namespace std;

class QueryPlanNode {
	
	protected:
	// int outPipeID;
	// Schema *outSchema;

	public:
	QueryPlanNode *next;
	int outPipeID;
	Schema *outSchema;
	static int numPipes;
	// Vector of Pipes
	static vector<Pipe*> *pipesVector;

	static void createPipes();

	void PrintSchema(Schema * sch);
	virtual void Print () = 0;
	virtual void Run() = 0;
	virtual void WaitUntilDone() = 0;
	static int clear_pipe(QueryPlanNode* lastNode, bool print);
	
};

class SelectFileNode : public QueryPlanNode { 

	// private:
	// char *fileName;
	// CNF *selOp;
    // Record *literal;

	public:
	char *fileName;
	CNF *selOp;
    Record *literal;
	SelectFile SF;
	DBFile dbFile;

	void Print ();
	void Run();
	void WaitUntilDone();
	SelectFileNode ();
	SelectFileNode (char *fileName, int outPipeID, Schema *outSchema, CNF *selOp, Record *literal);
};

class SelectPipeNode : public QueryPlanNode {

	private:
	int inPipeID;
	CNF *selOp;
    Record *literal;

	public:
	
	SelectPipe SP;
	void Print ();
	void WaitUntilDone();
	void Run();
	SelectPipeNode (int inPipeID, int outPipeID, Schema *outSchema, CNF *selOp, Record *literal);
};

class ProjectNode : public QueryPlanNode { 

	private:
	int inPipeID;
	int *keepMe;
	int numAttsInput;
	int numAttsOutput;
	
	public:

	Project P;
	void Print();
	void Run();
	void WaitUntilDone();
	ProjectNode (int inPipeID, int outPipeID, Schema *outSchema, int *keepMe, int numAttsInput, int numAttsOutput);
};

class JoinNode : public QueryPlanNode {

	private:
	int inPipe1ID;
	int inPipe2ID;
	CNF *selOp;
    Record *literal;

	public:

	Join J;
	void Print();
	void Run();
	void WaitUntilDone();
	JoinNode (int inPipe1ID, int inPipe2ID, int outPipeID, Schema *outSchema, CNF *selOp, Record* literal);
};

class DuplicateRemovalNode : public QueryPlanNode {

	private:
	int inPipeID;
	Schema *mySchema;

	public:

	DuplicateRemoval D;
	void Print();
	void Run();
	void WaitUntilDone();
	DuplicateRemovalNode (int inPipeID, int outPipeID, Schema *outSchema, Schema *mySchema);
};

class SumNode : public QueryPlanNode {

	private:
	int inPipeID;
	Function *computeMe;
	bool isDistinctSum;
	public:

	Sum S;
	void Print();
	void Run();
	void WaitUntilDone();
	SumNode (int inPipeID, int outPipeID, Schema *outSchema,  Function *computeMe, bool isDistinctSum);
};

class GroupByNode : public QueryPlanNode {

	private:
	int inPipeID;
	OrderMaker *groupAtts;
	Function *computeMe;
	bool isDistinctGroupBy;

	public:

	GroupBy G;
	void Print();
	void Run();
	void WaitUntilDone();
	GroupByNode (int inPipeID, int outPipeID, Schema *outSchema, OrderMaker *groupAtts, Function *computeMe, bool isDistinctGroupBy);
};