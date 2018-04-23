#include "RelOp.h"
#include "BigQ.h"
#include <set>

#define PIPE_BUFF_SIZE 100

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

	retVal = pthread_create(&worker, &attr, workHandler, (void *) &workerArgs);
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

void *SelectPipe::workHandler(void *args) {
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
	Record* tempRecord = new Record();
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
	delete record;
	delete tempRecord;

	worker_args->out->ShutDown();

	cout << "[SelectPipe][selectPipeWorker] End" << endl;
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

	retVal = pthread_create(&worker, &attr, workHandler, (void *) &workerArgs);
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

void *SelectFile::workHandler(void *args) {
	cout << "[SelectFile][selectFileWorker] Start" << endl;

	thread_data *worker_args = (thread_data *) args;
	Page* pageArr[worker_args->totalPages]; 
	// Initialize all Pages
	for(int i = 0; i < worker_args->totalPages; i++) {
		pageArr[i] = new Page();
	}
	cout << "[SelectFile][selectFileWorker] moving first\n";
	worker_args->inFile->MoveFirst();
	cout << "[SelectFile][selectFileWorker] moved first\n";
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
	cout << "[SelectFile][selectFileWorker] while finished\n";
	
	// Insert the remaining records into output pipe
	for(int i = 0; i <= currentPageNo; i++) {
		while(pageArr[i]->GetFirst(tempRecord) == 1) {
			//cout << "[SelectFile][workHandler] Attributes found after retrieving from Page : " << record->GetNoOfAttributes() << endl;
			worker_args->outPipe->Insert(tempRecord);
		}
	}

	for(int i = 0; i < worker_args->totalPages; i++) {
		delete pageArr[i];
	}
	delete record;
	delete tempRecord;
	
	worker_args->outPipe->ShutDown();
	cout << "[SelectFile][selectFileWorker] Pipe Shutdown" << endl;
	cout << "[SelectFile][selectFileWorker] End" << endl;
}

void Project::Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) {
	cout << "[Project][Run] Start" << endl;
	pthread_attr_t attr;
	int retVal;

	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
	
	workerArgs.inPipe = &inPipe;
	workerArgs.outPipe = &outPipe;
	workerArgs.keepMe = keepMe;
	workerArgs.numAttsInput = numAttsInput;
	workerArgs.numAttsOutput = numAttsOutput;
	workerArgs.totalPages = totalPages == 0 ? 1 : totalPages;

	retVal = pthread_create(&worker, &attr, workHandler, (void *) &workerArgs);
	cout << "[Project][Run] Created new thread" << endl;
	if(retVal) {
		cout << "[Project][Run] Error in pthread_create" << endl;
		exit(1);
	}
	pthread_attr_destroy(&attr);
	cout << "[Project][Run] End" << endl;
}

void Project::WaitUntilDone () {
	void* status;
	int retVal;
	retVal = pthread_join(worker, &status);
	if(retVal) {
		cout << "[Project][WaitUntilDone] Error in pthread_join" << endl;
		exit(1);
	}
}

void Project::Use_n_Pages(int n) {
	totalPages = n;
}

void *Project::workHandler(void *args) {
	cout << "[Project][projectWorker] Start" << endl;
	thread_data *worker_args = (thread_data *) args;
	Page* pageArr[worker_args->totalPages]; 
	// Initialize all Pages
	for(int i = 0; i < worker_args->totalPages; i++) {
		pageArr[i] = new Page();
	}

	// Indicates the no of pages filled
	int currentPageNo = 0;
	Record* record = new Record();
	Record* tempRecord = new Record();

	while(worker_args->inPipe->Remove(record) == 1) {
		// Project on required attributes
		record->Project(worker_args->keepMe, worker_args->numAttsOutput, worker_args->numAttsInput);
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
		// cout << "[Project][projectWorker]Adding remaining Pages " << endl;
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
	cout << "[Project][projectWorker] End" << endl;
}

void Join::Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) {
	cout << "[Join][Run] Start" << endl;
	pthread_attr_t attr;
	int retVal;

	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
	
	workerArgs.inPipeL = &inPipeL;
	workerArgs.inPipeR = &inPipeR;
	workerArgs.outPipe = &outPipe;
	workerArgs.selOp = &selOp;
	workerArgs.literal = &literal;
	workerArgs.totalPages = totalPages == 0 ? 1 : totalPages;

	retVal = pthread_create(&worker, &attr, workHandler, (void *) &workerArgs);
	cout << "[Join][Run] Created new thread" << endl;
	if(retVal) {
		cout << "[Join][Run] Error in pthread_create" << endl;
		exit(1);
	}
	pthread_attr_destroy(&attr);
	cout << "[Join][Run] End" << endl;
}

void Join::WaitUntilDone () {
	void* status;
	int retVal;
	retVal = pthread_join(worker, &status);
	if(retVal) {
		cout << "[Join][WaitUntilDone] Error in pthread_join" << endl;
		exit(1);
	}
}

void Join::Use_n_Pages(int n) {
	totalPages = n;
}

void *Join::workHandler(void *args) {
	cout << "[Join][joinWorker] Start" << endl;
	thread_data *worker_args = (thread_data *) args;
	
	// Can do this later in merging stage ?
	Page* pageArr[worker_args->totalPages]; 
	// Initialize all Pages
	for(int i = 0; i < worker_args->totalPages; i++) {
		pageArr[i] = new Page();
	}

	OrderMaker *order_left = new OrderMaker();
	OrderMaker *order_right = new OrderMaker();

	ComparisonEngine comp;
	int numAttsLeft, numAttsRight, numAttsToKeep ;
	int *attsToKeep;
	Record *mergedRecord;

	if(worker_args->selOp->GetSortOrders(*order_left, *order_right) != 0) {
		cout << "[Join][workHandler] Order Maker can be used" << endl;
		// Initilization for BigQ
		Pipe l_sorted(PIPE_BUFF_SIZE);
		Pipe r_sorted(PIPE_BUFF_SIZE);
		int runlen = 10;

		// Hack to get number of records before sorting
				
		cout << "[Join][workHandler] BigQs Started" << endl;
		BigQ bqL (*(worker_args->inPipeL), l_sorted, *order_left ,10);
		BigQ bqR (*(worker_args->inPipeR), r_sorted, *order_right,10);

		cout << "[Join][workHandler] BigQs Done" << endl;
		Record *leftRecord = new Record();
		Record *rightRecord = new Record();
		
		// cout << "[Join][workHandler] *********** bef *********\n";
		bool leftPipeEmpty = l_sorted.Remove(leftRecord) == 0;
		bool rightPipeEmpty = r_sorted.Remove(rightRecord) == 0;

		// Get the number of attributes

		if(!leftPipeEmpty){
			numAttsLeft = ((int *)leftRecord->bits)[1] / sizeof(int) - 1;
		}

		if(!rightPipeEmpty){
			numAttsRight = ((int *)rightRecord->bits)[1] / sizeof(int) - 1;
		}

		cout << "[Join][workHandler] numAttsLeft : " << numAttsLeft << endl;
		cout << "[Join][workHandler] numAttsRight : " << numAttsRight << endl;

		int compResult;
		
		numAttsToKeep = numAttsLeft + numAttsRight ;

		if(numAttsToKeep == 0){
			worker_args->outPipe->ShutDown();
			cout << "[Join][workHandler] Zero Attributes Given\n";
			exit(1);
		}

		attsToKeep = new int[numAttsToKeep];
		int i;
		for(i = 0; i < numAttsLeft; i++) {
			attsToKeep[i] = i;
		}
		
		for(int j = 0; j < numAttsRight; j++) {
			attsToKeep[i] = j;
			i++;
		}

		Record *tempLeftRecord, *tempRightRecord;
		
		vector<Record*> leftPipeRecords;
		vector<Record*> rightPipeRecords;
		
		while(!leftPipeEmpty && !rightPipeEmpty) {
			compResult = comp.Compare(leftRecord, order_left, rightRecord, order_right);

			if(compResult == 0) {
				tempLeftRecord = new Record();
				tempRightRecord = new Record();
				tempLeftRecord->Copy(leftRecord);
				tempRightRecord->Copy(rightRecord);
				leftPipeRecords.push_back(tempLeftRecord);
				rightPipeRecords.push_back(tempRightRecord);

				tempLeftRecord = new Record();
				tempRightRecord = new Record();
				leftPipeEmpty = l_sorted.Remove(tempLeftRecord) == 0;
				rightPipeEmpty = r_sorted.Remove(tempRightRecord) == 0;

				while(!leftPipeEmpty && (comp.Compare(leftRecord, tempLeftRecord, order_left) == 0)) {
					leftPipeRecords.push_back(tempLeftRecord);
					tempLeftRecord = new Record();
					leftPipeEmpty = l_sorted.Remove(tempLeftRecord) == 0;
				}

				if(!leftPipeEmpty)
					leftRecord->Copy(tempLeftRecord);

				while(!rightPipeEmpty && (comp.Compare(rightRecord, tempRightRecord, order_right)== 0)) {
					rightPipeRecords.push_back(tempRightRecord);
					tempRightRecord = new Record();
					rightPipeEmpty = r_sorted.Remove(tempRightRecord) == 0;
				}

				if(!rightPipeEmpty)
					rightRecord->Copy(tempRightRecord);

				// cout << "[Join][workHandler] vector sizes : " << leftPipeRecords.size() << ", " << rightPipeRecords.size() << endl;
				for(Record* outer: leftPipeRecords) {
					for(Record* inner: rightPipeRecords) {
						if(comp.Compare(outer, inner, worker_args->literal, worker_args->selOp) != 0){
							mergedRecord = new Record();
							mergedRecord->MergeRecords(outer, inner, 
							numAttsLeft, numAttsRight, attsToKeep, numAttsToKeep, numAttsLeft);
							worker_args->outPipe->Insert(mergedRecord);
						}
					}
				}
				leftPipeRecords.clear();
				rightPipeRecords.clear();
				// leftPipeEmpty = l_sorted.Remove(leftRecord) == 0;
				// rightPipeEmpty = r_sorted.Remove(rightRecord) == 0;

			} // Left is smaller. Take a new record from left pipe 
			else if(compResult < 0) {
				leftPipeEmpty = l_sorted.Remove(leftRecord) == 0;
			} // Right is smaller. Take a new record from right pipe  
			else {
				rightPipeEmpty = r_sorted.Remove(rightRecord) == 0;
			}
		}

	} else { // Cannot get OrderMakers. Use Block nested Joins

		cout << "[Join][Worker] Using Block nested Joins\n";

		attsToKeep = NULL;

		// fetch the records from first pipe and write into a temp file

		Record rec;
		DBFile tempFile;
		const char* filename = "leftPipeRecords.tmp";

		tempFile.Create(filename, heap, NULL);

		int noOfRecordsAddedToFile = 0;
		while(worker_args->inPipeL->Remove(&rec)){
			tempFile.Add(rec);
			noOfRecordsAddedToFile++;
		}

		// ***************  Temp code  **************
		// int tmpCnt=0;
		// tempFile.MoveFirst();
		// Record temp;
		// Attribute IA = {"Integer",Int};
		// Attribute SA = {"String",String};
		// Attribute DA = {"Double",Double};
		// Attribute atts[] = {IA, SA,SA,IA, SA,DA,SA};
		// Schema tmpSch("tmp", 7, atts);
		// while(tempFile.GetNext(temp)){
		// 	if(++tmpCnt < 400)
		// 		temp.Print(&tmpSch);
		// }
		// tempFile.Close();

		// ***************  Temp code ends  **************

		cout << "[Join][Worker] temp file for left pipe created with " << noOfRecordsAddedToFile << " records\n";

		// Fill the buffer with records from right pipe and proceed to nested-loop join, repeat the process

		int pageCnt = 0, currentPageSize = sizeof(int);
		vector<Record*> recVec;
		Page containerPage;
		Record* recPointer = new Record();
		bool pipeEmpty;

		Record leftRec;
		Record* rightRec;

		// Starting the processing
		cout << "[Join][Worker] Nested-Loop Join started\n";
		int blockCount = 0;

		while(!(pipeEmpty = worker_args->inPipeR->Remove(recPointer) == 0) || (recVec.size() > 0)){

			
			if(!pipeEmpty && currentPageSize + recPointer->GetSize() > PAGE_SIZE){
				pageCnt++;
				currentPageSize = sizeof(int);
			}

			// Buffer is full, proceed with nested-loop join
			if(pageCnt == worker_args->totalPages || (pipeEmpty && recVec.size() > 0)){
				
				blockCount++;

				cout << "[Join][Worker] Processing block " << blockCount << " with record count = " << recVec.size() << endl;

				tempFile.MoveFirst();

				while(tempFile.GetNext(leftRec)){
					for(auto rightRec : recVec){
						if(comp.Compare(&leftRec, rightRec, worker_args->literal, worker_args->selOp) != 0){
							
							if(attsToKeep == NULL){
								numAttsLeft  = ((int *)leftRec.bits)[1]  / sizeof(int) - 1;
								numAttsRight = ((int *)rightRec->bits)[1] / sizeof(int) - 1;

								numAttsToKeep = numAttsLeft + numAttsRight;

								attsToKeep = new int[numAttsToKeep];
								
								int i;
								for(i = 0; i < numAttsLeft; i++) {
									attsToKeep[i] = i;
								}
								
								for(int j = 0; j < numAttsRight; j++) {
									attsToKeep[i] = j;
									i++;
								}
							}

							mergedRecord = new Record();
							mergedRecord->MergeRecords(&leftRec, rightRec, numAttsLeft, numAttsRight,
														attsToKeep, numAttsToKeep, numAttsLeft);
							worker_args->outPipe->Insert(mergedRecord);
						}
					}
				}

				//reset the page count, record vector
				pageCnt = 0;
				recVec.clear();
				if(pipeEmpty){
					break;
			}
			}

			recVec.push_back(recPointer);
			currentPageSize += recPointer->GetSize();
			recPointer = new Record();
		}
		
		tempFile.Close();

	}

	worker_args->outPipe->ShutDown();
	cout << "[Join][workHandler] End" << endl;
}

void DuplicateRemoval::Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema) {
	
	cout << "[DuplicateRemoval][Run] Start" << endl;
	pthread_attr_t attr;
	int retVal;

	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
	
	workerArgs.inPipe = &inPipe;
	workerArgs.outPipe = &outPipe;
	workerArgs.schema = &mySchema;
	workerArgs.totalPages = totalPages == 0 ? 1 : totalPages;

	retVal = pthread_create(&worker, &attr, workHandler, (void *) &workerArgs);
	cout << "[DuplicateRemoval][Run] Created new thread" << endl;
	if(retVal) {
		cout << "[DuplicateRemoval][Run] Error in pthread_create" << endl;
		exit(1);
	}
	pthread_attr_destroy(&attr);
	cout << "[DuplicateRemoval][Run] End" << endl;
}

void DuplicateRemoval::WaitUntilDone () {
	void* status;
	int retVal;
	retVal = pthread_join(worker, &status);
	if(retVal) {
		cout << "[DuplicateRemoval][WaitUntilDone] Error in pthread_join" << endl;
		exit(1);
	}
}

void DuplicateRemoval::Use_n_Pages (int runlen) {
	totalPages = runlen;
}

void * DuplicateRemoval::workHandler(void * args){

	cout << "[DuplicateRemoval][workHandler] Start" << endl;
	thread_data *worker_args = (thread_data *) args;
	OrderMaker orderMaker(worker_args->schema);

	Pipe sortedOutputPipe(PIPE_BUFF_SIZE);

	BigQ queue(*(worker_args->inPipe), sortedOutputPipe, orderMaker, 1);

	Record currentRecord, prevRecord;

	ComparisonEngine comp;


	while(sortedOutputPipe.Remove(&currentRecord)){

		if(!prevRecord.IsNull() && comp.Compare(&currentRecord, &prevRecord, &orderMaker) == 0)
			continue;
		
		prevRecord.Copy(&currentRecord);
		worker_args->outPipe->Insert(&currentRecord);

	}

	worker_args->outPipe->ShutDown();
	cout << "[DuplicateRemoval][workHandler] End" << endl;

}

void Sum::Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe) {
	
	cout << "[Sum][Run] Start" << endl;
	pthread_attr_t attr;
	int retVal;

	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
	
	workerArgs.inPipe = &inPipe;
	workerArgs.outPipe = &outPipe;
	workerArgs.function = &computeMe;
	workerArgs.totalPages = totalPages == 0 ? 1 : totalPages;

	retVal = pthread_create(&worker, &attr, workHandler, (void *) &workerArgs);
	cout << "[Sum][Run] Created new thread" << endl;
	if(retVal) {
		cout << "[Sum][Run] Error in pthread_create" << endl;
		exit(1);
	}
	pthread_attr_destroy(&attr);
	cout << "[Sum][Run] End" << endl;
}

void Sum::WaitUntilDone () {
	void* status;
	int retVal;
	retVal = pthread_join(worker, &status);
	if(retVal) {
		cout << "[Sum][WaitUntilDone] Error in pthread_join" << endl;
		exit(1);
	}
}

void Sum::Use_n_Pages (int runlen) {
	totalPages = runlen;
}

void * Sum::workHandler(void * args){

	cout << "[Sum][workHandler] Start" << endl;

	// Fetch the passed arguments
	thread_data *worker_args = (thread_data *) args;

	double result = 0;

	int intResult;
	double doubleResult;
	Type resultType;

	Record rec;
	int recCnt = 0;
	// One by one get all the tuples from the inpt pipe and apply the function to each one, 
	// aggregate the result in a variable
	while(worker_args->inPipe->Remove(&rec)){
		recCnt++;
		resultType = worker_args->function->Apply(rec, intResult, doubleResult);
		if(resultType == Int){
			result += intResult;
		}
		else if(resultType == Double){
			result += doubleResult;
		}
	}

	cout << "[Sum][worker] total records = " << recCnt << endl;
	// Create the output record

	Attribute att;
	if(resultType == Int){
		att.myType = Int;
		att.name = "Integer";
	}
	else{
		att.myType = Double;
		att.name = "Double";
	}
	
	Schema recSchema("RecordSchema", 1, &att);

	char* recBits = new char[PAGE_SIZE];

	strcpy(recBits, (char*) to_string(result).c_str());
	strcat(recBits, "|");
	rec.ComposeRecord(&recSchema, recBits);
	
	worker_args->outPipe->Insert(&rec);
	worker_args->outPipe->ShutDown();
	
	cout << "[Sum][workHandler] End" << endl;

}

// *************  GROUPBY ****************
void GroupBy::Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) { 
	
	cout << "[GroupBy][Run] Start" << endl;
	pthread_attr_t attr;
	int retVal;

	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
	
	workerArgs.inPipe = &inPipe;
	workerArgs.outPipe = &outPipe;
	workerArgs.function = &computeMe;
	workerArgs.orderMaker = &groupAtts;
	workerArgs.totalPages = totalPages == 0 ? 1 : totalPages;

	retVal = pthread_create(&worker, &attr, workHandler, (void *) &workerArgs);
	cout << "[GroupBy][Run] Created new thread" << endl;
	if(retVal) {
		cout << "[GroupBy][Run] Error in pthread_create" << endl;
		exit(1);
	}
	pthread_attr_destroy(&attr);
	cout << "[GroupBy][Run] End" << endl;
}

void GroupBy::WaitUntilDone () { 
	void* status;
	int retVal;
	retVal = pthread_join(worker, &status);
	if(retVal) {
		cout << "[GroupBy][WaitUntilDone] Error in pthread_join" << endl;
		exit(1);
	}
}

void GroupBy::Use_n_Pages (int runlen) { 
	totalPages = runlen;
}

void * GroupBy::workHandler(void * args){

	cout << "[GroupBy][workHandler] Start\n";

	// Attribute for integer, string and double data types
	Attribute intAttr = {"Integer",Int};
	Attribute strAttr = {"String",String};
	Attribute doubleAttr = {"Double",Double};

	// Fetch the passed arguments
	thread_data *worker_args = (thread_data *) args;

	Pipe sortedOutputPipe(PIPE_BUFF_SIZE);

	BigQ queue(*(worker_args->inPipe), sortedOutputPipe, *(worker_args->orderMaker), 100);
	cout << "[GroupBy][workHandler] Got sorted output\n";

	Record currRecord, prevRecord;
	int runningIntSum, res1= 0;
	double runningDoubleSum, res2 = 0;
	Type resType;
	ComparisonEngine comp;
	Record *outputRecord;
	bool processingFinished = false, pipeEmpty;
	char *recBits;

	int cnt = 0;

	while(!(pipeEmpty = sortedOutputPipe.Remove(&currRecord)==0) || !processingFinished){

		
		if(prevRecord.IsNull()){
			if(pipeEmpty)
				break;
			prevRecord.Copy(&currRecord);
			worker_args->function->Apply(currRecord, res1, res2);
			runningIntSum = res1;
			runningDoubleSum = res2;
			continue;
		}
		
		if(!pipeEmpty)
			resType = worker_args->function->Apply(currRecord, res1, res2);

		if(!pipeEmpty && comp.Compare(&currRecord, &prevRecord, worker_args->orderMaker) == 0){

			switch(resType){
				case Int:

					runningIntSum += res1;
					break;
				
				case Double:

					runningDoubleSum += res2;
					break;
			}	
		}
		else{
			cnt++;
			// Create record
			recBits = new (std::nothrow) char[PAGE_SIZE];
			if(recBits == NULL){
				cout << "[GroupBy][workHandler] Not enough memory!! Exiting....\n";
				exit(1);
			}

			int n = worker_args->orderMaker->GetNumAtts();

			Attribute atts[n+1];
			// char arr[100];
			char* valToStr;

			if(resType == Int){
				//snprintf(arr, sizeof arr, "%d", runningIntSum);
				valToStr = (char*)to_string(runningIntSum).c_str();
				atts[0] = intAttr;
			}
			else{
				//snprintf(arr, sizeof arr, "%10f", runningDoubleSum);
				valToStr = (char*)to_string(runningDoubleSum).c_str();
				atts[0] = doubleAttr;
			}

			strcpy(recBits, valToStr);
			
			strcat(recBits, "|");


			for(int i = 0; i < n; i++){

				int offsetNumber = worker_args->orderMaker->whichAtts[i];
				Type type = worker_args->orderMaker->whichTypes[i];

				// Get the value from the record and put into the recBits
				// Also, build the attributes array for the schema
				int offset = ((int *) prevRecord.bits)[offsetNumber+1];
				int intValue;
				double dbValue;
				char* strValue;

				switch(type){
					case Int:

						intValue = *((int *) &prevRecord.bits[offset]);
						valToStr = (char*)to_string(intValue).c_str();
						// snprintf(arr, sizeof arr, "%d", intValue);
						strcat(recBits, valToStr);
						atts[i+1] = intAttr;
						break;
					
					case String:

						strValue = &(prevRecord.bits[offset]);
						strcat(recBits, strValue);
						atts[i+1] = strAttr;
						break;

					case Double:
						dbValue = *((double *) &prevRecord.bits[offset]);
						valToStr = (char*)to_string(dbValue).c_str();
						// snprintf(arr, sizeof arr, "%10f", dbValue);
						strcat(recBits, valToStr);
						atts[i+1] = doubleAttr;
						break;
				}
				
				strcat(recBits, "|");

			}

			Schema composeRecSchema("composeRecSchema", n+1, atts);

			outputRecord = new Record();
			outputRecord->ComposeRecord(&composeRecSchema, recBits);
		
			worker_args->outPipe->Insert(outputRecord);

			// get result from current record
			switch(resType){
				case Int:

					runningIntSum = res1;
					runningDoubleSum = 0;
					break;
				
				case Double:

					runningDoubleSum = res2;
					runningIntSum = 0;
					break;
			}

			// delete [] recBits;
		}

		if(pipeEmpty)
			processingFinished = true;
		else{
			prevRecord.Copy(&currRecord);
		}

	}

	cout << "[GroupBy][workHandler] No of groups = " << cnt << endl;

	worker_args->outPipe->ShutDown();
	cout << "[GroupBy][workHandler] End\n";
}

// ***********  WriteOut  ***************

void WriteOut::Run (Pipe &inPipe, FILE *outFile, Schema &mySchema){
	cout << "[WriteOut][Run] Start" << endl;
	pthread_attr_t attr;
	int retVal;

	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
	
	workerArgs.inPipe = &inPipe;
	workerArgs.file = outFile;
	workerArgs.recSchema = &mySchema;
	workerArgs.totalPages = totalPages == 0 ? 1 : totalPages;

	retVal = pthread_create(&worker, &attr, workHandler, (void *) &workerArgs);
	cout << "[WriteOut][Run] Created new thread" << endl;
	if(retVal) {
		cout << "[WriteOut][Run] Error in pthread_create" << endl;
		exit(1);
	}
	pthread_attr_destroy(&attr);
	cout << "[WriteOut][Run] End" << endl;
}

void WriteOut::WaitUntilDone (){
	void* status;
	int retVal;
	retVal = pthread_join(worker, &status);
	if(retVal) {
		cout << "[WriteOut][WaitUntilDone] Error in pthread_join" << endl;
		exit(1);
	}
}

void WriteOut::Use_n_Pages (int n){
	totalPages = n;
}

void *WriteOut::workHandler(void *args){

	cout << "[WriteOut][workHandler] Start\n";

	// Fetch the passed arguments
	thread_data *worker_args = (thread_data *) args;

	stringstream ss;

	int n = worker_args->recSchema->GetNumAtts();
	Attribute *atts = worker_args->recSchema->GetAtts();
	Record rec;
	int pointer;

	while(worker_args->inPipe->Remove(&rec)){

		char* bits = rec.bits;

		for(int i = 0; i < n ; i++){

			// print the attribute name
			ss << atts[i].name << ": ";

			// use the i^th slot at the head of the record to get the
			// offset to the correct attribute in the record
			pointer = ((int *) bits)[i + 1];

			// here we determine the type, which given in the schema;
			// depending on the type we then print out the contents
			ss << "[";

			// first is integer
			if (atts[i].myType == Int) {
				int *myInt = (int *) &(bits[pointer]);
				ss << *myInt;	

			// then is a double
			} else if (atts[i].myType == Double) {
				double *myDouble = (double *) &(bits[pointer]);
				ss << *myDouble;	

			// then is a character string
			} else if (atts[i].myType == String) {
				char *myString = (char *) &(bits[pointer]);
				ss << myString;	
			} 

			ss << "]";

			// print out a comma as needed to make things pretty
			if (i != n - 1) {
				ss << ", ";
			}

		}

		ss << "\n";

		fputs(ss.str().c_str(), worker_args->file);

		ss.str(std::string());
	}

	fclose(worker_args->file);
	cout << "[WriteOut][workHandler] End\n";
}