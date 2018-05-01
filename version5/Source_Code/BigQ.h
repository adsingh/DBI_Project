#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include "DBFile.h"

using namespace std;

class BigQ {

private:

	static int numOfInstances;
	static void *externalSortWorker(void* args);
	static bool myComparator (int i,int j) ;
	typedef struct{
		string filename;
		Pipe* in;
		Pipe* out;
		OrderMaker* sortorder;
		int runlen;
	}thread_data;

	thread_data worker_args;
	static pthread_mutex_t bigQMutex;
	

public:

	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();
	static void initializeMutex();
	pthread_t * worker;
};

#endif
