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

	
	
	Record* currentRecord = new Record();
	thread_data* worker_args = (thread_data *) args;
	
	DBFile file;

	
	file.Create("sortedRuns.bin", heap, NULL);
	file.Close();
	Page* page = new Page();
	// vector<Record> recordArray;
	vector<Record*> recordArray;

	int currentNoOfPages = 0;
	int currentPageSize = sizeof(int);

	struct Comparator {
		OrderMaker* orderMaker;
		ComparisonEngine cnf;
		Comparator(OrderMaker* orderMaker) {this->orderMaker = orderMaker;}

		bool operator () (const Record* i, const Record* j) {
			//cout << "Sorting in proc\n";
			return this->cnf.Compare((Record*)i, (Record*)j, (this->orderMaker)) > 0 ? true : false;
			//return true;
		}

	};

	int count = 0;
	Schema s("catalog", "nation");

	// Continue until the pipe is not done
	while(worker_args->in->Remove(currentRecord)){

		
		count++;

		if(currentPageSize + currentRecord->GetSize() > PAGE_SIZE){
			currentNoOfPages++;
			currentPageSize = sizeof(int);
		}
		
		if(currentNoOfPages == worker_args->runlen) {

			cout << "Inside second if\n size of record array = " << recordArray.size() <<endl;
			sort(recordArray.begin(), recordArray.end(), Comparator(worker_args->sortorder));
			//recordArray.sort(Comparator(worker_args->sortorder));
			cout << "List sorted\n";
			file.Open("sortedRuns.bin");
			cout << "File opened\n";
			// Excluding last record
			
			for(auto it : recordArray) {
				it->Print(&s);
				file.Add(*it);
			}
			file.Close();
			recordArray.clear();
			//recordArray.push_back(lastRecord);
			currentNoOfPages = 0;

		}

		recordArray.push_back(currentRecord);
		currentPageSize += currentRecord->GetSize();
		currentRecord = new Record();

	}

	cout << "Number of records read from the pipe = " << count << endl;

	if(recordArray.size() > 0) {
		ComparisonEngine comp;
		sort(recordArray.begin(), recordArray.end(), Comparator(worker_args->sortorder));
		//recordArray.sort(Comparator(worker_args->sortorder));
		
		file.Open("sortedRuns.bin");
		
		for(vector<Record*>::iterator it = recordArray.begin(); it != recordArray.end(); ++it) {
			//cout << "Printing records\n";
			(*it)->Print(&s);
			//cout << "Printed records\n";
			file.Add(**it);
		}
		file.Close();
		recordArray.clear();
	}

	cout << "Hello, I am worker thread with runlen = " << worker_args->runlen << endl;

}

BigQ::~BigQ () {
}