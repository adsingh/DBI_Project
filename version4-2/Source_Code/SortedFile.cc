#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "SortedFile.h"
#include "Defs.h"
#include "MergeStructures.h"
#include <algorithm>
#include <iostream>
#include <fstream>  
#include <sstream>
#include <iterator>
#include <string.h>
#include <stdlib.h>

SortedFile:: SortedFile (string confgFile_name) : GenericDBFile(){
    bigQ = nullptr;
    configFile_name = confgFile_name;
    queryOrderMaker = nullptr;
    startingPage = 0;
} 

SortedFile:: SortedFile (string confgFile_name, int runlen, OrderMaker* orderMaker) : GenericDBFile(){
    bigQ = nullptr;
    configFile_name = confgFile_name;
    sortInfo = new SortInfo(runlen, orderMaker);
    queryOrderMaker = nullptr;
    startingPage = 0;
} 

SortedFile::~SortedFile(){

}

int SortedFile :: Create (const char *fpath, fType file_type, void *startup){
    
    sortInfo = (struct SortInfo*) startup;
    filePath = string(fpath);
    myFile.Open(0, (char *)fpath);
    currentPageNo = 0;
    return 1;

}

int SortedFile :: Open (const char *fpath){

    // Passing non zero value
    myFile.Open(1, (char *)fpath);
    filePath = string(fpath);
    return 1;

}

int SortedFile :: Close (){

    if(bigQ != nullptr) {
        Merge();
    }
    ofstream configFile (configFile_name.c_str());
    if(!configFile.is_open()) {
        cout << "Unable to open config file";
        return 0;
    }
    else {

        configFile << "sorted\n";
        configFile << sortInfo->runLength << endl;
        configFile << sortInfo->orderMaker->ToString() << endl;
        configFile.close();
    }
    
    // Close file
    myFile.Close();
    return 1;
}

void SortedFile :: MoveFirst (){

    if(bigQ != nullptr) {
        Merge();
    }
    myPage.EmptyItOut();
    currentPageNo = 0;
    myFile.GetPage(&myPage, currentPageNo);
    queryOrderMaker = nullptr;

}

void SortedFile :: Load (Schema &myschema, const char *loadpath){
    
    FILE *tableFile = fopen(loadpath, "r");

    Record temp;
    
    while (temp.SuckNextRecord(&myschema, tableFile) == 1){
        Add(temp);
    }
    
}

void SortedFile :: Add (Record &addme){

    if(bigQ == nullptr){
        input = new Pipe(BUFF_SIZE);
        output = new Pipe(BUFF_SIZE);
        bigQ = new BigQ(*input, *output, *sortInfo->orderMaker, sortInfo->runLength);
        queryOrderMaker = nullptr;
    }

    input->Insert(&addme);

}

int SortedFile :: GetNext (Record &fetchme){ 

    if(bigQ != nullptr) {
        Merge();
    }
    if(myPage.GetFirst(&fetchme) == 0){
        if(currentPageNo+1 == myFile.GetLength()-1){
            cout << "[SortedFile][GetNext] Reached end of file\n";
            return 0;
        }
        myFile.GetPage(&myPage, ++currentPageNo);
        myPage.GetFirst(&fetchme);
    }
    
    return 1;
}

int SortedFile :: GetNext (Record &fetchme, CNF &cnf, Record &literal){

    // Exit if file is empty
    if(myFile.GetLength()==0){
        return 0;
    }

    // Merge Records in bigQ before GetNext
    if(bigQ != nullptr){
        Merge();
    }

    ComparisonEngine comp;
    // Construct queryOrderMaker based on File sort order and input CNF
    // queryOrderMaker is created only once in multiple GetNext calls
    // Create ordermaker for Literal based on input cnf
    if(queryOrderMaker == nullptr){
        queryOrderMaker = new OrderMaker();
        literalOrderMaker = new OrderMaker();
        cnf.GetQueryOrderMaker(*(sortInfo->orderMaker), *queryOrderMaker, *literalOrderMaker);

        // Binary Search is applied only when queryOrdermaker is not empty
        if(queryOrderMaker->GetNumAtts() > 0){

            int totalPages = myFile.GetLength()-1;
            int left, right, mid;
            left = 0;
            right = totalPages-1;
            Record temp;
            while(left < right){
                
                mid = left + (right-left)/2;
                myPage.EmptyItOut();
                myFile.GetPage(&myPage, mid);
                
                myPage.GetFirst(&temp);
                
                int result = comp.Compare(&temp, queryOrderMaker, &literal, literalOrderMaker);
                
                // First record in current Page is GREATER than queryOrderMaker
                if(result > 0){
                    right = mid-1;
                }
                // First record in current Page is SMALLER than queryOrderMaker
                else if(result < 0){
                    
                    if(mid+1 < totalPages){
                        myPage.EmptyItOut();
                        myFile.GetPage(&myPage, mid+1);
                        myPage.GetFirst(&temp);
                        if(comp.Compare(&temp, queryOrderMaker, &literal, literalOrderMaker) >= 0){
                            left = mid;
                            break;
                        }   
                        else{
                            left = mid+1;
                        }
                    }
                    else{
                        break;
                    }
                }
                // First record in current Page is EQUAL to the queryOrderMaker
                else{
                    
                    if(mid-1 >= 0){
                        myPage.EmptyItOut();
                        myFile.GetPage(&myPage, mid-1);
                        myPage.GetFirst(&temp);
                        if(comp.Compare(&temp, queryOrderMaker, &literal, literalOrderMaker) == 0){
                            right = mid-1;
                        }
                        else{
                            left = mid-1;
                            break;
                        }   
                    }
                    else{
                        break;
                    }
                }
            }
            cout << "[SortedFile.cc] Page No after Binary Search = " << left << endl;
            startingPage = left;
        }
        // Get Appropriate Page for Linear Scan
        myPage.EmptyItOut();
        myFile.GetPage(&myPage, startingPage);
        currentPageNo = startingPage;
    }

    int count = 0;
    // Linearly Scan Records in Page
    while(GetNext(fetchme) != 0){
        count++;
        int result = -1;
        // When Binary Search is used, the record should match the queryOrderMaker
        if(queryOrderMaker->GetNumAtts() > 0){
            // cout << "************** Number of atrributes is > 0 \n";
            result = comp.Compare(&fetchme, queryOrderMaker, &literal, literalOrderMaker);
            if(result == 1){
                return 0;
            }
        }
        // Used to match with input CNF when
        // 1. There was no Binary Search
        // 2. Binary Search was applied and current record matches queryOrderMaker
        if(queryOrderMaker->GetNumAtts() == 0 || result == 0){
            if(comp.Compare(&fetchme, &literal, &cnf) == 1){
                return 1;
            }
        }
    }
    queryOrderMaker->Print();
    return 0;

}

void SortedFile :: Merge(){
    // Close Pipe to sort Records present in BigQ
    input->ShutDown();

    Record* tempRecord = new Record();
    Page* tempPage = new Page();
    int currFilePageNo = 0;
    int tempFilePageNo = 0;
    bool pipeEmptied = false;
    bool fileEmptied = false;
    pq RecordPQ(sortInfo->orderMaker);
    // Creating a Temp File to write merged records
    File tempFile;
    tempFile.Open(0, const_cast<char*>((filePath+"_temp").c_str()));

    // No records in File. Read from output pipe
    if(myFile.GetLength() == 0) {
        fileEmptied = true;

    } 
    // Initialise PQ and start 2 way merge
    // Records from old file are read into myPage
    // Records are inserted into new file using tempPage
    else if(output->Remove(tempRecord) != 0) {
        myFile.GetPage(&myPage, currFilePageNo++);
        QueueElement* ele;
        
        RecordPQ.push(new QueueElement(0, tempRecord));
        tempRecord = new Record();
        myPage.GetFirst(tempRecord);
        RecordPQ.push(new QueueElement(1, tempRecord));
        while(true) {
            // Pop from PQ
            ele = RecordPQ.top();
            RecordPQ.pop();

            // Add to tempPage. If tempPage is full, write tempPage to temp file
            if(tempPage->Append(ele->record) == 0) {
                
                tempFile.AddPage(tempPage, tempFilePageNo++);
                tempPage->EmptyItOut();
                tempPage->Append(ele->record);
            }
            tempRecord = new Record();
            // Index 0 indicates the record popped was obtained from Pipe
            if(ele->index == 0) {
                // Remove next Record from Pipe. Break if empty
                if(output->Remove(tempRecord) == 0) {
                    pipeEmptied = true;
                    break;
                }
                
            }
            // Index 1 indicates the record popped was obtained from File
            else {
                // Read next record from myPage. If myPage is empty, bring new Page from old file
                if(myPage.GetFirst(tempRecord) == 0) {
                    // Break when all Pages from the old file have been added into the temp file
                    if(myFile.GetLength()-1 == currFilePageNo) {
                        fileEmptied = true;
                        break;
                    }
                    myFile.GetPage(&myPage, currFilePageNo++);
                    myPage.GetFirst(tempRecord);
                }
            }
            // Add the record from appropriate Source to PQ
            ele->record = tempRecord;
            RecordPQ.push(ele);
        }

    }

    // Add 1 remaining record from PS into tempPage
    if((pipeEmptied || fileEmptied) && RecordPQ.size() > 0) {
        if(tempPage->Append(RecordPQ.top()->record)== 0) {
            tempFile.AddPage(tempPage, tempFilePageNo++);
            tempPage->EmptyItOut();
            tempPage->Append(RecordPQ.top()->record);
        }
        
    } 

    // If File was emptied, keep adding all records from output pipe into new file
    if(fileEmptied) {
        while(output->Remove(tempRecord) == 1) {
            if(tempPage->Append(tempRecord) == 0) {
                tempFile.AddPage(tempPage, tempFilePageNo++);
                tempPage->EmptyItOut();
                tempPage->Append(tempRecord);
            }
            tempRecord = new Record();
        }

        if(tempPage->GetNumRecs() > 0) {
            tempFile.AddPage(tempPage, tempFilePageNo);
        }
    }

    // If Pipe was emptied, add all records from old file into new file
    if(pipeEmptied) {
        while(true) {    
            if(myPage.GetFirst(tempRecord) == 0) {
                if(myFile.GetLength()-1 == currFilePageNo) {
                    break;
                }
                myFile.GetPage(&myPage, currFilePageNo++);
                myPage.GetFirst(tempRecord);
            } 
            if(tempPage->Append(tempRecord) == 0) {
                tempFile.AddPage(tempPage, tempFilePageNo++);
                tempPage->EmptyItOut();
                tempPage->Append(tempRecord);
            }
            tempRecord = new Record();
        }

        if(tempPage->GetNumRecs() > 0) {
            tempFile.AddPage(tempPage, tempFilePageNo);
        }

    }

    tempFile.Close();
    myFile.Close();
    // Replace original file by temp file
    if(rename((filePath+"_temp").c_str(), filePath.c_str())){
        cout << "[SortedFile.cc] Couldn't rename the file\n";
    }
    myFile.Open(1, const_cast<char*>(filePath.c_str()));

    delete tempPage;

    delete input;
    delete output;
    delete bigQ;
    bigQ = nullptr;
    currentPageNo = 0;
}