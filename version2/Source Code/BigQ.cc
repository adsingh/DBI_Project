#include "BigQ.h"
#include <unistd.h>
#include <vector>
#include <list>
#include <algorithm>
#include <queue>


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
}

void *BigQ :: externalSortWorker(void* args){

	
	
	Record* currentRecord = new Record();
	thread_data* worker_args = (thread_data *) args;
	if(worker_args->runlen <= 0){
		cout << "Invalid runlength, using default value of 1\n";
		worker_args->runlen = 1;
	}
	int count = 0;
	File file_temp;
	Page page_temp;
	vector<Record*> recordArray;
	int currentNoOfPages = 0, currentPageNo = 0;
	int currentPageSize = sizeof(int);
	int numberOfRuns = 0;
	int totalPages;
	vector<int> runOffset;
	
	runOffset.push_back(0);

	struct Comparator {
		OrderMaker* orderMaker;
		ComparisonEngine cnf;
		Comparator(OrderMaker* orderMaker) {this->orderMaker = orderMaker;}

		bool operator () (const Record* i, const Record* j) {
			return this->cnf.Compare((Record*)i, (Record*)j, (this->orderMaker)) < 0 ? true : false;
		}

	};

	file_temp.Open(0, "sortedRuns.bin");
	file_temp.Close();
	
	int pagesAppended = 0;

	// Continue until the pipe is not done
	while(worker_args->in->Remove(currentRecord)){

		
		count++;
		
		if(currentPageSize + currentRecord->GetSize() > PAGE_SIZE){

			currentNoOfPages++;
			//cout << "Page NO : " << currentNoOfPages << " ----  No of records = " << count - prevCnt - 1 << endl;
			currentPageSize = sizeof(int);
		}
		
		if(currentNoOfPages == worker_args->runlen) {

			numberOfRuns++;

			stable_sort(recordArray.begin(), recordArray.end(), Comparator(worker_args->sortorder));

			file_temp.Open(1, "sortedRuns.bin");
			
			// Excluding last record
			
			for(Record* it : recordArray) {
				if(page_temp.Append(it) == 0){
					
					pagesAppended++;
					file_temp.AddPage(&page_temp, currentPageNo++);
					page_temp.EmptyItOut();
					page_temp.Append(it);
				}
			}
			if(page_temp.GetNumRecs() > 0){
				pagesAppended++;
				file_temp.AddPage(&page_temp, currentPageNo++);
				page_temp.EmptyItOut();
			}

			file_temp.Close();
			recordArray.clear();
			currentNoOfPages = 0;
			runOffset.push_back(runOffset.back() + pagesAppended);
			pagesAppended = 0;
		}
		

		recordArray.push_back(currentRecord);
		currentPageSize += currentRecord->GetSize();
		currentRecord = new Record();

	}

	cout << "Records read from the pipe = " << count << endl;

	if(recordArray.size() > 0) {
		numberOfRuns++;
		ComparisonEngine comp;
		stable_sort(recordArray.begin(), recordArray.end(), Comparator(worker_args->sortorder));
		
		file_temp.Open(1, "sortedRuns.bin");
		for(Record* it : recordArray) {
			if(page_temp.Append(it) == 0){
				pagesAppended++;
				file_temp.AddPage(&page_temp, currentPageNo++);
				page_temp.EmptyItOut();
				page_temp.Append(it);
			}
		}

		if(page_temp.GetNumRecs() > 0){
			pagesAppended++;
			file_temp.AddPage(&page_temp, currentPageNo++);
			page_temp.EmptyItOut();
		}

		file_temp.Close();
		recordArray.clear();
		runOffset.push_back(runOffset.back() + pagesAppended);
	}	
	
	cout << "Number of runs = " << numberOfRuns << endl;

	struct QueueElement {
		int index;
		Record* record;

		QueueElement(int index, Record* rec){
			this->index = index;
			this->record = rec;
		}
	};

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

	Page* pageArr[numberOfRuns];
	int activePages[numberOfRuns];
	Record* tempRecord;
	typedef priority_queue<QueueElement* , vector<QueueElement*>, QueueComparator> pq;
	QueueElement* ele;
	pq RecordPQ(worker_args->sortorder);
	file_temp.Open(1, "sortedRuns.bin");

	for(int i = 0 ; i < numberOfRuns ; i++){
		pageArr[i] = new Page();
		activePages[i] = runOffset[i];
		file_temp.GetPage(pageArr[i], activePages[i]++);
		tempRecord = new Record();
		pageArr[i]->GetFirst(tempRecord);
		ele = new QueueElement(i, tempRecord);
		RecordPQ.push(ele);
	}

	count = 0;
	while(!RecordPQ.empty()){
		ele = RecordPQ.top();
		RecordPQ.pop();
		count++;
		worker_args->out->Insert(ele->record);
		int index = ele->index;
		tempRecord = new Record();

		if(pageArr[index]->GetFirst(tempRecord) == 0){
			if(activePages[index] < runOffset[index+1]){
				file_temp.GetPage(pageArr[index], activePages[index]++);
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

	file_temp.Close();

	for(int i = 0 ; i < numberOfRuns ; i++)
		delete pageArr[i];
	
	// cout << "Number of records read from the file = " << count << endl;
}

BigQ::~BigQ () {
}