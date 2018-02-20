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
	int count = 0;
	File file_temp;
	DBFile file;
	vector<Record*> recordArray;
	int currentNoOfPages = 0;
	int currentPageSize = sizeof(int);
	int numberOfRuns = 0;

	struct Comparator {
		OrderMaker* orderMaker;
		ComparisonEngine cnf;
		Comparator(OrderMaker* orderMaker) {this->orderMaker = orderMaker;}

		bool operator () (const Record* i, const Record* j) {
			//cout << "Sorting in proc\n";
			return this->cnf.Compare((Record*)i, (Record*)j, (this->orderMaker)) > 0 ? true : false;
			// return false;
		}

	};

	file_temp.Open(0, "sortedRuns.bin");
	file.Create("sortedRuns.bin", heap, NULL);
	file.Close();

	
	Schema s("catalog", "part");

	// Continue until the pipe is not done
	while(worker_args->in->Remove(currentRecord)){

		
		count++;
		
		if(currentPageSize + currentRecord->GetSize() > PAGE_SIZE){
			currentNoOfPages++;
			currentPageSize = sizeof(int);
		}
		
		if(currentNoOfPages == worker_args->runlen) {

			numberOfRuns++;
			cout << "Inside second if\n size of record array = " << recordArray.size() <<endl;
			stable_sort(recordArray.begin(), recordArray.end(), Comparator(worker_args->sortorder));
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

	if(recordArray.size() > 0) {

		numberOfRuns++;
		ComparisonEngine comp;
		cout << "Records sorting started!\n";
		stable_sort(recordArray.begin(), recordArray.end(), Comparator(worker_args->sortorder));
		cout << "Records sorted!\n";

		file.Open("sortedRuns.bin");
		
		for(vector<Record*>::iterator it = recordArray.begin(); it != recordArray.end(); ++it) {
			
			file.Add(**it);
		}
		file.Close();
		recordArray.clear();
	}

	Page* pageArr[numberOfRuns];
	
	for(int i = 0 ; i < numberOfRuns ; i++)
		pageArr[i] = new Page();

	

	
	cout << "Number of records read from the pipe = " << count << endl;
	cout << "Hello, I am worker thread with runlen = " << worker_args->runlen << endl;

}

BigQ::~BigQ () {
}