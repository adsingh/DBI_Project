#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"

using namespace std;

class BigQ {

private:

	static void *externalSortWorker(void* args);
	typedef struct{
		Pipe* in;
		Pipe* out;
		OrderMaker sortorder;
		int runlen;
	}thread_data;

	thread_data worker_args;

public:

	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();
};

#endif
