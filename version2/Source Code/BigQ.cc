#include "BigQ.h"
#include <unistd.h>


BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
	
	// Initializing the worker Thread
	pthread_t worker;
	pthread_attr_t attr;
	int retVal;
    void *status;

	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

	worker_args.in = &in;
	worker_args.out = &out;
	worker_args.sortorder = sortorder;
	worker_args.runlen = runlen;

	cout << "Creating thread\n";
	retVal = pthread_create(&worker, &attr, externalSortWorker, (void *) &worker_args);

	pthread_attr_destroy(&attr);

	// read data from in pipe sort them into runlen pages
    // construct priority queue over sorted runs and dump sorted data 
 	// into the out pipe
	
    // finally shut down the out pipe
	out.ShutDown ();
	pthread_exit(NULL);
}

void *BigQ :: externalSortWorker(void* args){

	thread_data* worker_args = (thread_data *) args;
	cout << "Hello, I am worker thread with runlen = " << worker_args->runlen << endl;

}

BigQ::~BigQ () {
}
