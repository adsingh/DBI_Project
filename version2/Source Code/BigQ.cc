#include "BigQ.h"
#include <unistd.h>
#include <vector>


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
	DBFile file;
	file.Create("sortedRuns.bin", heap, NULL);
	Record currentRecord;
	Page* page = new Page();
	vector<Record> recordArray;

	int currentNoOfPages = 0;
	int currentPageSize = sizeof(int);

	struct Comparator {
		OrderMaker orderMaker;
		ComparisonEngine cnf;
		Comparator(OrderMaker orderMaker) {this->orderMaker = orderMaker;}

		bool operator () (Record i, Record j) {
			return this->cnf.Compare(&i, &j, &(this->orderMaker)) > 0 ? true : false;
		}

	};

	// Continue until the pipe is not done
	while(worker_args->in->Remove(&currentRecord)){

		recordArray.push_back(currentRecord);
		//currentNoOfPages < worker_args->runlen
		int insertStatus = page->Append(&currentRecord);
		
		
		// Check if page is full
		if(insertStatus == 0){
			page->EmptyItOut();
			currentNoOfPages++;
			page->Append(&currentRecord);
		}

		Schema s("catalog", "customer");
		if(currentNoOfPages == worker_args->runlen) {
			Record lastRecord = recordArray[recordArray.size() - 1];
			sort(recordArray.begin(), recordArray.end(), Comparator(worker_args->sortorder));
			file.Open("sortedRuns.bin");
			// Excluding last record
			for(int i = 0 ; i < recordArray.size() - 1; i++) {
				recordArray[i].Print(&s);
				file.Add(recordArray[i]);
			}
			file.Close();
			recordArray.clear();
			recordArray.push_back(lastRecord);
			currentNoOfPages = 0;

		}

	}

	if(recordArray.size() > 0) {
		sort(recordArray.begin(), recordArray.end(), Comparator(worker_args->sortorder));
		file.Open("sortedRuns.bin");
		for(int i = 0 ; i < recordArray.size() ; i++) {
			file.Add(recordArray[i]);
		}
		file.Close();
		recordArray.clear();
	}

	cout << "Hello, I am worker thread with runlen = " << worker_args->runlen << endl;

}


BigQ::~BigQ () {
}
