#include "BigQ.h"
#include <unistd.h>
#include <vector>
#include <list>
#include <algorithm>


BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
	
	// Initializing the worker Thread
	pthread_t worker;
	pthread_attr_t attr;
	int retVal;
    void *status;

	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

	worker_args.in = &in;
	worker_args.out = &out;
	worker_args.sortorder = &sortorder;
	worker_args.runlen = runlen;

	cout << "Creating thread in BigQ\n";
	retVal = pthread_create(&worker, &attr, externalSortWorker, (void *) &worker_args);

	pthread_attr_destroy(&attr);
	pthread_join(worker, &status);
	// read data from in pipe sort them into runlen pages
    // construct priority queue over sorted runs and dump sorted data 
 	// into the out pipe
	
    // finally shut down the out pipe
	out.ShutDown ();
	// pthread_exit(NULL);
}

void *BigQ :: externalSortWorker(void* args){

	
	
	Record currentRecord;
	thread_data* worker_args = (thread_data *) args;
	
	DBFile file;

	
	file.Create("sortedRuns.bin", heap, NULL);
	file.Close();
	Page* page = new Page();
	// vector<Record> recordArray;
	list<Record> recordArray;

	int currentNoOfPages = 0;
	int currentPageSize = sizeof(int);

	struct Comparator {
		OrderMaker* orderMaker;
		ComparisonEngine cnf;
		Comparator(OrderMaker* orderMaker) {this->orderMaker = orderMaker;}

		bool operator () (const Record & i, const Record & j) {
			//cout << "Sorting in proc\n";
			return this->cnf.Compare((Record*)&i, (Record*)&j, (this->orderMaker)) > 0 ? true : false;
			//return true;
		}

	};

	int count = 0;
	Schema s("catalog", "orders");
	// Continue until the pipe is not done
	while(worker_args->in->Remove(&currentRecord)){

		
		count++;
	
		recordArray.push_back(currentRecord);
		//currentRecord.Print(&s);
		int insertStatus = page->Append(&currentRecord);
		
		// Check if page is full
		if(insertStatus == 0){
			cout << "Inside first if\n";
			page->EmptyItOut();
			currentNoOfPages++;
			page->Append(&currentRecord);
		}

		
		if(currentNoOfPages == worker_args->runlen) {

			cout << "Inside second if\n size of record array = " << recordArray.size() <<endl;
			Record lastRecord = recordArray.back();
			recordArray.pop_back();
			// sort(recordArray.begin(), recordArray.end(), Comparator(worker_args->sortorder));
			recordArray.sort(Comparator(worker_args->sortorder));
			cout << "List sorted\n";
			file.Open("sortedRuns.bin");
			cout << "File opened\n";
			// Excluding last record
			// vector<Record>::iterator it;
			list<Record>::iterator it;
			for(it = recordArray.begin() ; it != recordArray.end(); it++) {
				(*it).Print(&s);
				//file.Add(temp);
			}
			file.Close();
			recordArray.clear();
			//recordArray.push_back(lastRecord);
			currentNoOfPages = 0;

		}

	}

	cout << "Number of records read from the pipe = " << count << endl;

	if(recordArray.size() > 0) {
		ComparisonEngine comp;
		// sort(recordArray.begin(), recordArray.end(), Comparator(worker_args->sortorder));
		recordArray.sort(Comparator(worker_args->sortorder));
		
		file.Open("sortedRuns.bin");
		// vector<Record>::iterator it;
		list<Record>::iterator it;
		for(it = recordArray.begin() ; it != recordArray.end(); it++) {
			(*it).Print(&s);
			file.Add(*it);
		}
		file.Close();
		recordArray.clear();
	}

	cout << "Hello, I am worker thread with runlen = " << worker_args->runlen << endl;

}

BigQ::~BigQ () {
}