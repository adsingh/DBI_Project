#include "BigQ.h"
#include <unistd.h>
#include <vector>
#include <list>
#include <algorithm>
#include <queue>

using namespace std;
int BigQ :: numOfInstances = 0;

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
	
	// Initializing the worker Thread
	numOfInstances++;

	pthread_t worker;
	pthread_attr_t attr;
	int retVal;
    // void *status;

	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

	worker_args.in = &in;
	worker_args.out = &out;
	worker_args.sortorder = &sortorder;
	worker_args.runlen = runlen;
	worker_args.filename = to_string(numOfInstances);
	
	// Creating Worker Thread
	// retVal = pthread_create(&worker, NULL, externalSortWorker, (void *) &worker_args);
	retVal = pthread_create(&worker, &attr, externalSortWorker, (void *) &worker_args);    
}

void *BigQ :: externalSortWorker(void* args){

	
	
	Record* currentRecord = new Record();
	
	thread_data* worker_args = (thread_data *) args;
	if(worker_args->runlen <= 0){
		cout << "Invalid runlength, using default value of 1\n";
		worker_args->runlen = 1;
	}
	
	int recordCount = 0;
	
	File file;
	Page page;
	
	vector<Record*> recordArray;
	
	int currentNoOfPages = 0, currentPageNo = 0;
	int currentPageSize = sizeof(int);
	int numberOfRuns = 0;
	int totalPages;
	int pagesAppended = 0;
	bool pipeEmpty;

	// Vector to hold offset of runs in the file in unit of pages
	vector<int> runOffset;
	
	runOffset.push_back(0);

	// Comparator to compare two records
	struct Comparator {
		OrderMaker* orderMaker;
		ComparisonEngine cnf;
		Comparator(OrderMaker* orderMaker) {this->orderMaker = orderMaker;}

		bool operator () (const Record* i, const Record* j) {
			return this->cnf.Compare((Record*)i, (Record*)j, (this->orderMaker)) < 0 ? true : false;
		}

	};

	// Element for Priority Queue
	struct QueueElement {
		int index;
		Record* record;

		QueueElement(int index, Record* rec){
			this->index = index;
			this->record = rec;
		}
	};

	// Comparator to compare two Queue Elements
	struct QueueComparator{
		ComparisonEngine comp;
		OrderMaker* orderMaker;

		QueueComparator(OrderMaker* orderMaker){
			this->orderMaker = orderMaker;
		}

		bool operator () (const QueueElement* a, const QueueElement* b){
			return comp.Compare((Record*)(a->record), (Record*)(b->record), this->orderMaker ) > 0 ? true : false;
		}
	};
	
	Record* tempRecord;
	typedef priority_queue<QueueElement* , vector<QueueElement*>, QueueComparator> pq;
	QueueElement* ele;

	file.Open(0, (char*)(worker_args->filename).c_str());
	file.Close();
	
	// Continue until the pipe is not done
	while(!(pipeEmpty = worker_args->in->Remove(currentRecord) == 0) || (recordArray.size() > 0)){
		
		recordCount += pipeEmpty ? 0 : 1;
		
		// Increment page Count once a page fills
		if(!pipeEmpty && currentPageSize + currentRecord->GetSize() > PAGE_SIZE){

			currentNoOfPages++;
			currentPageSize = sizeof(int);
		}
		
		// Flush records to file once a Run is formed or its the last run needs
		if(currentNoOfPages == worker_args->runlen || (pipeEmpty && recordArray.size() > 0)) {

			numberOfRuns++;

			stable_sort(recordArray.begin(), recordArray.end(), Comparator(worker_args->sortorder));

			file.Open(1, (char*)(worker_args->filename).c_str());
			
			for(Record* it : recordArray) {
				if(page.Append(it) == 0){
					
					pagesAppended++;
					file.AddPage(&page, currentPageNo++);
					page.EmptyItOut();
					page.Append(it);
				}
			}

			if(page.GetNumRecs() > 0){
				pagesAppended++;
				file.AddPage(&page, currentPageNo++);
				page.EmptyItOut();
			}

			file.Close();
			recordArray.clear();
			currentNoOfPages = 0;
			runOffset.push_back(runOffset.back() + pagesAppended);
			pagesAppended = 0;

			if(pipeEmpty){
				break;
			}
		}
		

		recordArray.push_back(currentRecord);
		currentPageSize += currentRecord->GetSize();
		currentRecord = new Record();

	}

	cout << "[BigQ.cc][worker"<< worker_args->filename << "] Records read from the pipe = " << recordCount << endl;	
	// cout << "[BigQ.cc][worker"<< worker_args->filename << "] Number of runs = " << numberOfRuns << endl;

	Page* pageArr[numberOfRuns];
	int activePages[numberOfRuns];

	pq RecordPQ(worker_args->sortorder);
	file.Open(1, (char*)(worker_args->filename).c_str());


	// Initialize the priority queue
	for(int i = 0 ; i < numberOfRuns ; i++){

		pageArr[i] = new Page();
		activePages[i] = runOffset[i];
		file.GetPage(pageArr[i], activePages[i]++);
		tempRecord = new Record();
		pageArr[i]->GetFirst(tempRecord);
		ele = new QueueElement(i, tempRecord);
		RecordPQ.push(ele);
	}

	recordCount = 0;

	// Merge the Runs now
	while(!RecordPQ.empty()){
		ele = RecordPQ.top();
		RecordPQ.pop();
		recordCount++;
		worker_args->out->Insert(ele->record);
		int index = ele->index;
		tempRecord = new Record();

		if(pageArr[index]->GetFirst(tempRecord) == 0){
			if(activePages[index] < runOffset[index+1]){
				file.GetPage(pageArr[index], activePages[index]++);
				pageArr[index]->GetFirst(tempRecord);
				ele->record = tempRecord;
				RecordPQ.push(ele);
			}
		}
		else{
			ele->record = tempRecord;
			RecordPQ.push(ele);
		}

	}

	file.Close();
	// finally shut down the out pipe
	worker_args->out->ShutDown ();
	// Free unwanted memory
	for(int i = 0 ; i < numberOfRuns ; i++)
		delete pageArr[i];
	
	cout << "[BigQ.cc][worker"<< worker_args->filename << "] Number of records read from the temp file = " << recordCount << endl;
}

BigQ::~BigQ () {
}