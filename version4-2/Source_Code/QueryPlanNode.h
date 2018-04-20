#include "Comparison.h"
#include "Record.h"
#include "Function.h"
#include <vector>


class QueryPlanNode {
	
	protected:
	// int outPipeID;
	// Schema *outSchema;

	public:
	QueryPlanNode *next;
	int outPipeID;
	Schema *outSchema;
	
	virtual void Print () = 0;
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
	void Print ();
	SelectFileNode ();
	SelectFileNode (char *fileName, int outPipeID, Schema *outSchema, CNF *selOp, Record *literal);
};

class SelectPipeNode : public QueryPlanNode {

	private:
	int inPipeID;
	CNF *selOp;
    Record *literal;

	public:
	// int inPipeID;
	// CNF *selOp;
    // Record *literal;
	void Print ();
	SelectPipeNode (int inPipeID, int outPipeID, Schema *outSchema, CNF *selOp, Record *literal);
};

class ProjectNode : public QueryPlanNode { 

	private:
	int inPipeID;
	int *keepMe;
	int numAttsInput;
	int numAttsOutput;
	
	public:
	void Print();
	ProjectNode (int inPipeID, int outPipeID, Schema *outSchema, int *keepMe, int numAttsInput, int numAttsOutput);
};

class JoinNode : public QueryPlanNode {

	private:
	int inPipe1ID;
	int inPipe2ID;
	CNF *selOp;
    Record *literal;

	public:
	void Print();
	JoinNode (int inPipe1ID, int inPipe2ID, int outPipeID, Schema *outSchema, CNF *selOp, Record* literal);
};

class DuplicateRemovalNode : public QueryPlanNode {

	private:
	int inPipeID;
	Schema *mySchema;

	public:
	void Print();
	DuplicateRemovalNode (int inPipeID, int outPipeID, Schema *outSchema, Schema *mySchema);
};

class SumNode : public QueryPlanNode {

	private:
	int inPipeID;
	Function *computeMe;

	public:
	void Print();
	SumNode (int inPipeID, int outPipeID, Schema *outSchema,  Function *computeMe);
};

class GroupByNode : public QueryPlanNode {

	private:
	int inPipeID;
	OrderMaker *groupAtts;
	Function *computeMe;

	public:
	void Print();
	GroupByNode (int inPipeID, int outPipeID, Schema *outSchema, OrderMaker *groupAtts, Function *computeMe);
};