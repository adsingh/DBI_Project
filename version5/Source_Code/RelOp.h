#ifndef REL_OP_H
#define REL_OP_H
#include <iostream>
#include <pthread.h>
#include <cstring>
#include <sstream>
#include <stdio.h>
#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"

using namespace std;

class RelationalOp {

	protected:
	pthread_t worker;
	int totalPages;

	public:
	
	// Changes for handling DISTINCT aggregate
	bool isDistinctSum;
	bool isDistinctGroupBy;
	// Changes for handling DISTINCT aggregate

	// blocks the caller until the particular relational operator 
	// has run to completion
	virtual void WaitUntilDone () = 0;

	// tell us how much internal memory the operation can use
	virtual void Use_n_Pages (int n) = 0;
};

class SelectFile : public RelationalOp { 

	private:
	static void *workHandler(void *args);
	typedef struct{
		DBFile* inFile;
		Pipe* outPipe;
		CNF* selOp;
		Record* literal;
		int totalPages;
	}thread_data;
	//pthread_t worker;
	thread_data workerArgs;
	//int totalPages;

	public:

	void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);

};

class SelectPipe : public RelationalOp {

	static void *workHandler(void *args);
	typedef struct{
		Pipe* in;
		Pipe* out;
		CNF* selOp;
		Record* literal;
		int totalPages;
	}thread_data;
	//pthread_t worker;
	thread_data workerArgs;
	//int totalPages;

	public:
	void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};
class Project : public RelationalOp { 

	static void *workHandler(void *args);
	typedef struct{
		Pipe* inPipe;
		Pipe* outPipe;
		int* keepMe;
		int numAttsInput;
		int numAttsOutput;
		int totalPages;
	}thread_data;
	//pthread_t worker;
	thread_data workerArgs;
	//int totalPages;

	public:
	void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};
class Join : public RelationalOp {

	static void *workHandler(void *args);
	typedef struct{
		Pipe* inPipeL;
		Pipe* inPipeR;
		Pipe* outPipe;
		CNF* selOp;
		Record* literal;
		int totalPages;
	}thread_data;
	//pthread_t worker;
	thread_data workerArgs;
	//int totalPages;

	public:
	void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};
class DuplicateRemoval : public RelationalOp {

	static void *workHandler(void *args);
	typedef struct {
		Pipe  *inPipe, *outPipe;
		Schema* schema;
		int totalPages;
	}thread_data;
	thread_data workerArgs;

	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};
class Sum : public RelationalOp {

	static void *workHandler(void *args);
	typedef struct {
		Pipe  *inPipe, *outPipe;
		Function *function;
		int totalPages;
		bool isDistinctSum;
	}thread_data;
	thread_data workerArgs;

	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe, bool isDistinctSum);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};

class GroupBy : public RelationalOp {

	static void *workHandler(void *args);

	typedef struct {
		Pipe  *inPipe, *outPipe;
		Function *function;
		OrderMaker *orderMaker;
		int totalPages;
		bool isDistinctGroupBy;
	}thread_data;

	thread_data workerArgs;

	public:
		void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe, bool isDistinctGroupBy);
		void WaitUntilDone ();
		void Use_n_Pages (int n);
};

class WriteOut : public RelationalOp {

	static void *workHandler(void *args);

	typedef struct {
		Pipe  *inPipe;
		FILE *file;
		Schema *recSchema;
		int totalPages;
	}thread_data;

	thread_data workerArgs;

	public:
		void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema);
		void WaitUntilDone ();
		void Use_n_Pages (int n);
};
#endif
