#include "RelOp.h"

void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {

	cout << "[SelectPipe][Run] Start" << endl;
	pthread_attr_t attr;
	int retVal;

	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

	workerArgs.in = &inPipe;
	workerArgs.out = &outPipe;
	workerArgs.selOp = &selOp;
	workerArgs.literal = &literal;
	workerArgs.totalPages = totalPages == 0 ? 1 : totalPages;

	retVal = pthread_create(&worker, &attr, selectPipeWorker, (void *) &workerArgs);
	cout << "[SelectPipe][Run] Created new thread" << endl;
	if(retVal) {
		cout << "[SelectPipe][Run] Error in pthread_create" << endl;
		exit(1);
	}
	pthread_attr_destroy(&attr);
	cout << "[SelectPipe][Run] End" << endl;
}
void SelectPipe::WaitUntilDone () {
	void* status;
	int retVal;
	retVal = pthread_join(worker, &status);
	if(retVal) {
		cout << "[SelectPipe][WaitUntilDone] Error in pthread_join" << endl;
		exit(1);
	}
}
void SelectPipe::Use_n_Pages (int n) {
	totalPages = n;
}

void *SelectPipe::selectPipeWorker(void *args) {
	cout << "[SelectPipe][selectPipeWorker] Start" << endl;
	thread_data *worker_args = (thread_data *) args;
	Page* pageArr[worker_args->totalPages]; 
	// Initialize all Pages
	for(int i = 0; i < worker_args->totalPages; i++) {
		pageArr[i] = new Page();
	}

	// Indicates the no of pages filled
	int currentPageNo = 0;
	Record* record = new Record();
	Record* tempRecord;
	ComparisonEngine comp;
	while(worker_args->in->Remove(record) == 1) {

		// Check CNF condition
		if(comp.Compare(record, worker_args->literal, worker_args->selOp)) {
			// Add to current page in page array
			if(pageArr[currentPageNo]->Append(record) == 0) {
				currentPageNo++;
				// If all pages in page array are full, flush into output pipe
				if(currentPageNo == worker_args->totalPages) {
					for(int i = 0; i < worker_args->totalPages; i++) {
						while(pageArr[i]->GetFirst(tempRecord) == 1) {
							worker_args->out->Insert(tempRecord);
						}
					}
					currentPageNo = 0;
				}
				pageArr[currentPageNo]->Append(record);
			}

			// Creating new record only if it is appended to page
			record = new Record();
		}
		
	}
	// Insert the remaining records into output pipe
	for(int i = 0; i <= currentPageNo; i++) {
		while(pageArr[i]->GetFirst(tempRecord) == 1) {
			worker_args->out->Insert(tempRecord);
		}
	}

	for(int i = 0; i < worker_args->totalPages; i++) {
		delete pageArr[i];
	}
	
	worker_args->out->ShutDown();

	cout << "[SelectPipe][selectPipeWorker] Start" << endl;
}

void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
	cout << "[SelectFile][Run] Start" << endl;
	pthread_attr_t attr;
	int retVal;

	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
	
	workerArgs.inFile = &inFile;
	workerArgs.outPipe = &outPipe;
	workerArgs.selOp = &selOp;
	workerArgs.literal = &literal;
	workerArgs.totalPages = totalPages == 0 ? 1 : totalPages;

	retVal = pthread_create(&worker, &attr, selectFileWorker, (void *) &workerArgs);
	cout << "[SelectFile][Run] Created new thread" << endl;
	if(retVal) {
		cout << "[SelectFile][Run] Error in pthread_create" << endl;
		exit(1);
	}
	pthread_attr_destroy(&attr);
	cout << "[SelectFile][Run] End" << endl;
}

void SelectFile::WaitUntilDone () {
	void* status;
	int retVal;
	retVal = pthread_join(worker, &status);
	if(retVal) {
		cout << "[SelectFile][WaitUntilDone] Error in pthread_join" << endl;
		exit(1);
	}
}

void SelectFile::Use_n_Pages (int runlen) {
	totalPages = runlen;
}

void *SelectFile::selectFileWorker(void *args) {
	cout << "[SelectFile][selectFileWorker] Start" << endl;

	thread_data *worker_args = (thread_data *) args;
	Page* pageArr[worker_args->totalPages]; 
	// Initialize all Pages
	for(int i = 0; i < worker_args->totalPages; i++) {
		pageArr[i] = new Page();
	}

	worker_args->inFile->MoveFirst();
	// Indicates the no of pages filled
	int currentPageNo = 0;
	Record* record = new Record();
	Record* tempRecord = new Record();
	ComparisonEngine comp;


	while(worker_args->inFile->GetNext(*record, *worker_args->selOp, *worker_args->literal) == 1) {
		// Add to current page in page array
		if(pageArr[currentPageNo]->Append(record) == 0) {
			currentPageNo++;
			// If all pages in page array are full, flush into output pipe
			if(currentPageNo == worker_args->totalPages) {
				for(int i = 0; i < worker_args->totalPages; i++) {
					while(pageArr[i]->GetFirst(tempRecord) == 1) {
						worker_args->outPipe->Insert(tempRecord);
					}
				}
				currentPageNo = 0;
			}
			pageArr[currentPageNo]->Append(record);
		}
	}
	
	// Insert the remaining records into output pipe
	for(int i = 0; i <= currentPageNo; i++) {
		while(pageArr[i]->GetFirst(tempRecord) == 1) {
			worker_args->outPipe->Insert(tempRecord);
		}
	}

	for(int i = 0; i < worker_args->totalPages; i++) {
		delete pageArr[i];
	}
	delete record;
	delete tempRecord;
	
	worker_args->outPipe->ShutDown();

	cout << "[SelectFile][selectFileWorker] End" << endl;
}


