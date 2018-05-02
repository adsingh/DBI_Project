#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "HeapFile.h"
#include "Defs.h"
#include <algorithm>
#include <iostream>
#include <fstream>  
#include <sstream>
#include <iterator>
#include <string.h>
#include <stdlib.h>

HeapFile::HeapFile (string confgFile_name) : GenericDBFile() {
    prevState = start;  
    pageSizesArr = new vector<int>;
    configFile_name = confgFile_name;
}

HeapFile::~HeapFile(){
    delete pageSizesArr;
}

HeapFile::HeapFile (HeapFileInfo* configInfo) : GenericDBFile() {
    
    currRecordNo = configInfo->currRecordNo;
    currentPageNo = configInfo->currentPageNo;
    totalPages = configInfo->totalPages;
    pageSizesArr = &configInfo->pageSizesArr;
    prevState = configInfo->prevState;
    configFile_name = configInfo->configFile_name;
}

int HeapFile::Create (const char *f_path, fType f_type, void *startup) {

    myFile.Open(0, (char *)f_path);

    //Initializing state
    totalPages = 0;
    currentPageNo = 0;
    currRecordNo = 1;
    return 1;

}

void HeapFile::Load (Schema &f_schema, const char *loadpath) {

    FILE *tableFile = fopen(loadpath, "r");

    Record temp;
    
    while (temp.SuckNextRecord(&f_schema, tableFile) == 1){
        Add(temp);
    }
} 

int HeapFile::Open (const char *f_path) {

    // Passing non zero value
    myFile.Open(1, (char *)f_path);

    // Checking if last page was incomplete
    if(currentPageNo == totalPages){
        ////cout << "Open currentPageNo == totalPages\n";
        currentPageNo = --totalPages;
        pageSizesArr->pop_back();
    }

    // Getting the current Page
    myFile.GetPage(&myPage, currentPageNo);
    gettingRecordForFirstTime = true;
    
    return 1;
}

void HeapFile::MoveFirst () {

    currRecordNo = 1;
}

int HeapFile::Close () {
    
    // Last Page has not been written
    if (currentPageNo == totalPages) {
        
        int prevSum = pageSizesArr->size() > 0 ? pageSizesArr->back() : 0;
        pageSizesArr->push_back(myPage.GetNumRecs()+ prevSum);
        myFile.AddPage(&myPage, totalPages++);
        currentPageNo = totalPages;
        myPage.EmptyItOut();
    }

    //Convert Page Sizes Array into String of space separated values
    ostringstream pageSizesStr;
    if(pageSizesArr->size() > 1) {
        copy(pageSizesArr->begin(), pageSizesArr->end() - 1, ostream_iterator<int>(pageSizesStr, " "));
    }
    pageSizesStr << pageSizesArr->back();
    // DO WE NEED TO OVERWRITE THE CONFIG FILE
    // Open and Write to configuration File

    ofstream configFile (configFile_name.c_str());
    if(!configFile.is_open()) {
        //cout << "Unable to open config file";
        return 0;
    }
    else {

        configFile << "heap\n";

        configFile << to_string(currRecordNo) + "\n";
        
        configFile << to_string(currentPageNo) + "\n";
        ////cout << "currPagedNo="<<currentPageNo<<endl;
        configFile << to_string(totalPages) + "\n";
        ////cout << "totalPages="<<totalPages<<endl;
        configFile << pageSizesStr.str() + "\n";
        configFile << to_string(static_cast<int>(prevState)) + "\n";

        configFile.close();
    }
    
    // Close file
    myFile.Close();
    return 1;
}

void HeapFile::Add (Record &rec) {

    // Set page to the last page if any other page than last page was being used
    if(totalPages != currentPageNo){
        myPage.EmptyItOut();
        myFile.GetPage(&myPage, --totalPages);
        currentPageNo = totalPages;
        pageSizesArr->pop_back();
    }

    int isPageFull = myPage.Append(&rec);
    if(isPageFull == 0){

        // Since page is full, flush/write page to myFile
        myFile.AddPage(&myPage, totalPages++);
        int prevCount = pageSizesArr->size() > 0 ? pageSizesArr->back() : 0 ;
        pageSizesArr->push_back(prevCount + myPage.GetNumRecs());
        currentPageNo = totalPages;
        myPage.EmptyItOut();
        myPage.Append(&rec);
    }
    prevState = add;

}

int HeapFile::GetNext (Record &fetchme) {

    off_t pageNo;
    vector<int>::iterator it;
    int numRecordsInFile, offsetInPage;
    Record* rec = nullptr;
    int totalRecords;
    
    numRecordsInFile = pageSizesArr->size() > 0 ? pageSizesArr->back() : 0;
    
    totalRecords = numRecordsInFile;

    if(currentPageNo == totalPages){
        totalRecords += myPage.GetNumRecs();
    }

    if(currRecordNo > totalRecords){
        ////cout << "Get next end of file: currentPgNo = " << currentPageNo << " totalPages = " << totalPages << endl;
        //cout << "[HeapFile][GetNext] Reached end of file\n";
        return 0;
    }

    it = lower_bound(pageSizesArr->begin(), pageSizesArr->end(), currRecordNo);
    pageNo = it - pageSizesArr->begin();
    offsetInPage = (pageNo > 0 ? currRecordNo - (*pageSizesArr)[pageNo - 1] : currRecordNo) - 1;

    switch(prevState){

        case start:

            //cout << "[HeapFile][GetNext] Reached end of file\n";
            return 0;
            break;

        case add:

            // Get record from the current page in memory
            if(currRecordNo > numRecordsInFile){
                rec = myPage.GetRecord(currRecordNo - numRecordsInFile-1);
            }
            else{

                // flush current page to end of file
                myFile.AddPage(&myPage, totalPages++);
                pageSizesArr->push_back(numRecordsInFile + myPage.GetNumRecs());

                // empty out the current page
                myPage.EmptyItOut();

                // get the page 
                myFile.GetPage(&myPage, pageNo);
                currentPageNo = pageNo;

                rec = myPage.GetRecord(offsetInPage);

            }

            break;

        case getnext:

            // Record was read from the same page, hence just get the next record
            if(currentPageNo == pageNo && !gettingRecordForFirstTime){
                ////cout << "[HeapFile][GetNext1] Getting next record " << currRecordNo << "\n";
                rec = myPage.GetNextRecord();
                ////cout << "[HeapFile][GetNext1] Got next record\n";
            }
            else{

                // empty out the current page
                myPage.EmptyItOut();

                // get the page 
                myFile.GetPage(&myPage, pageNo);
                currentPageNo = pageNo;

                rec = myPage.GetRecord(offsetInPage);
            }
            break;

    }

    fetchme.Copy(rec);
    prevState = getnext;
    currRecordNo++;
    gettingRecordForFirstTime = false;
    return 1;

}

int HeapFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {

    ComparisonEngine comp;

    int counter = 0;
    do{
        if(GetNext(fetchme)==0){
            // //cout << "total records read = " << counter << endl;
            return 0;
        }
    }
    while(!comp.Compare(&fetchme, &literal, &cnf));
    
    return 1;

}
