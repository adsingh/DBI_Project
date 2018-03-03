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
} 

SortedFile:: SortedFile (string confgFile_name, int runlen, OrderMaker* orderMaker) : GenericDBFile(){
    bigQ = nullptr;
    configFile_name = confgFile_name;
    sortInfo = new SortInfo(runlen, orderMaker);
    queryOrderMaker = nullptr;
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
        configFile << sortInfo->orderMaker->ToString();
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
            cout << "Reached end of file\n";
            return 0;
        }
        myFile.GetPage(&myPage, ++currentPageNo);
        myPage.GetFirst(&fetchme);
    }
    return 1;
}

int SortedFile :: GetNext (Record &fetchme, CNF &cnf, Record &literal){

    if(myFile.GetLength()==0){
        return 0;
    }
    if(bigQ != nullptr){
        Merge();
    }

    cout << "[SortedFile.cc] inside GetNext\n";
    if(queryOrderMaker == nullptr){
        queryOrderMaker = new OrderMaker();
        cnf.GetQueryOrderMaker(*(sortInfo->orderMaker), *queryOrderMaker);

        if(queryOrderMaker->GetNumAtts() != 0){
            int totalPages = myFile.GetLength()-1;
            int left, right, mid;
            left = 0;
            right = totalPages-1;
            cout << "[SortedFile.cc] left = " << left << " right = " << right << " --- TotalPages = " << totalPages << endl;
            Record temp;
            ComparisonEngine comp;

            while(left < right){
                mid = left + (right-left)/2;
                myPage.EmptyItOut();
                myFile.GetPage(&myPage, mid);
                myPage.GetFirst(&temp); 
                int result = comp.Compare(&temp, &literal, queryOrderMaker);
                cout << "[SortedFile.cc] Compare result = " << result << endl;
                if(result > 0){
                    right = mid-1;
                }
                else if(result < 0){
                    if(mid+1 < totalPages){
                        myPage.EmptyItOut();
                        myFile.GetPage(&myPage, mid+1);
                        myPage.GetFirst(&temp);
                        if(comp.Compare(&temp, &literal, queryOrderMaker) >= 0){
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
                else{
                    if(mid-1 >= 0){
                        myPage.EmptyItOut();
                        myFile.GetPage(&myPage, mid-1);
                        myPage.GetFirst(&temp);
                        if(comp.Compare(&temp, &literal, queryOrderMaker) == 0){
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
        }
    }



    queryOrderMaker->Print();
    return 0;

}

void SortedFile :: Merge(){

    cout << "Merge\n";
    input->ShutDown();

    cout << "Pipe closed\n";
    Record* tempRecord = new Record();

    Page* tempPage = new Page();
    int currFilePageNo = 0;
    int tempFilePageNo = 0;
    bool pipeEmptied = false;
    bool fileEmptied = false;
    pq RecordPQ(sortInfo->orderMaker);
    File tempFile;
    tempFile.Open(0, const_cast<char*>((filePath+"_temp").c_str()));
    // No records in File. Read from output pipe
    if(myFile.GetLength() == 0) {
        fileEmptied = true;

    } else if(output->Remove(tempRecord) != 0) {
        myFile.GetPage(&myPage, currFilePageNo++);
        cout << "[SortedFile] Got Page\n";
        QueueElement* ele;
        
        RecordPQ.push(new QueueElement(0, tempRecord));
        tempRecord = new Record();
        myPage.GetFirst(tempRecord);
        RecordPQ.push(new QueueElement(1, tempRecord));
        cout << "[SortedFile] PQ ready\n";
        // Check
        while(true) {
            ele = RecordPQ.top();
            if(ele == NULL){
                cout << "[SortedFile] PQ empty\n";
            }
            RecordPQ.pop();
            if(tempPage->Append(ele->record) == 0) {
                cout << "[SortedFile] Flush Page\n";
                tempFile.AddPage(tempPage, tempFilePageNo++);
                tempPage->EmptyItOut();
                tempPage->Append(ele->record);
            }
            tempRecord = new Record();
            if(ele->index == 0) {
                cout << "[SortedFile] From Pipe\n";
                if(output->Remove(tempRecord) == 0) {
                    // Output Pipe is empty.
                    // Sequentially scan other data from file
                    pipeEmptied = true;
                    cout << "[SortedFile] Pipe Emptied\n";
                    break;
                }
                
            } else {
                cout << "[SortedFile] From File\n";
                if(myPage.GetFirst(tempRecord) == 0) {
                    cout << "[SortedFile] Get New Page\n";
                    if(myFile.GetLength()-1 == currFilePageNo) {
                        fileEmptied = true;
                        cout << "[SortedFile] File emptied\n";
                        break;
                    }
                    myFile.GetPage(&myPage, currFilePageNo++);
                    myPage.GetFirst(tempRecord);
                }
            }

            ele->record = tempRecord;
            RecordPQ.push(ele);
        }

    }

    cout << "[SortedFile] Outside while\n";

    if((pipeEmptied || fileEmptied) && RecordPQ.size() > 0) {
        if(tempPage->Append(RecordPQ.top()->record)== 0) {
            tempFile.AddPage(tempPage, tempFilePageNo++);
            tempPage->EmptyItOut();
            tempPage->Append(RecordPQ.top()->record);
        }
        
    } 

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

    cout << "[SortedFile.cc] Merging complete\n";
    cout << "[SortedFile.cc] Filepath : " << filePath << endl;
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