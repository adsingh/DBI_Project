#include "DBFile.h"
#include <iostream>
#include <fstream>  
#include <sstream>
#include <string.h>
#include <stdlib.h>
#include "HeapFile.h"
#include "SortedFile.h"

GenericDBFile::GenericDBFile(){

}

GenericDBFile :: ~GenericDBFile(){

}

DBFile::DBFile () {
    
}

// Used to parse the file path and extract the name of table (eg. filepath = heapfiles/Customer.bin; output string = Customer)
string DBFile::GetFileName(const char* f_path){

    char * dup = strdup(f_path);
    char * token = strtok(dup, "/");
    char* temp;
    while(token != NULL){
        temp = token;
        token = strtok(NULL, "/");
    }
    //temp = strtok(temp, ".");
    char *file_name = (char*) malloc(sizeof(char)*100);
    strcpy(file_name, temp);
    strcat(file_name, "_config.txt");
    free(dup);
    string str(file_name);
    return str;
    
}

int DBFile::Create (const char *f_path, fType f_type, void *startup) {

    configFile_name = GetFileName(f_path);
    switch(f_type){

        case heap:

            myInternalVar = new HeapFile(configFile_name);
            break;

        case sorted:

            myInternalVar = new SortedFile(configFile_name);
            break;

        default:
            cout << "Invalid file type\n";
            exit(1);

    }

    return myInternalVar->Create(f_path, f_type, startup);

}

void DBFile::Load (Schema &f_schema, const char *loadpath) {

    myInternalVar->Load(f_schema, loadpath);

} 

int DBFile::Open (const char *f_path) {

    configFile_name = GetFileName(f_path);
    
    // Reading Data from Auxiliary File
    string line;
    string fileType;
    ifstream configFile (configFile_name.c_str());
    if(!configFile.is_open()) {
        cout << "Unable to open auxiliary file" << endl;
        return 0;
    } 
    else {

        getline (configFile,line);
        stringstream line1(line);
        line1 >> fileType;
        // Create a HeapFile if first line is "heap"
        if(fileType.compare("heap") == 0) {
            HeapFileInfo* configInfo = new HeapFileInfo();
            
            getline (configFile,line);
            stringstream currentRecord(line);
            currentRecord >> configInfo->currRecordNo;
    
            getline (configFile,line);
            stringstream currentPage(line);
            currentPage >> configInfo->currentPageNo;

            getline (configFile,line);
            stringstream pages(line);
            pages >> configInfo->totalPages;

            getline (configFile,line);
            stringstream pageSizes(line);
            int number;
            
            while(pageSizes >> number) {
                configInfo->pageSizesArr.push_back(number);
            }
        
            getline (configFile,line);
            stringstream getState(line);
            int index;
            getState >> index;
            configInfo->prevState = static_cast<state>(index);

            configInfo->configFile_name = configFile_name;
            myInternalVar = new HeapFile(configInfo);

        } 
        // Create a SortedFile if first line is "sorted"
        else {

            int runlen;
            int numAtts = 100;
        
            getline(configFile, line);
            stringstream runlen_str(line);
            runlen_str >> runlen;

            getline(configFile, line);
            stringstream numAttsString(line);
            numAttsString >> numAtts;

            int whichAtts[numAtts];
            Type typeAtts[numAtts];

            getline(configFile, line);
            stringstream whichAttsString(line);
            int num;
            int i = 0;

            while(whichAttsString >> num){
                whichAtts[i++] = num;
            }

            getline(configFile, line);
            stringstream whichTypesString(line);

            i = 0;
            while(whichTypesString >> num){
                typeAtts[i++] = static_cast<Type>(num);
            }
    
            OrderMaker *orderMaker = new OrderMaker(numAtts, whichAtts, typeAtts);
            myInternalVar = new SortedFile(configFile_name, runlen, orderMaker);
        }
    }

    configFile.close();

    if(myInternalVar == NULL) {
        cout << "[DBFile.cc] myInternalVar NOT initialized\n" ;
        return 0;
    }

    return myInternalVar->Open(f_path);
}

void DBFile::MoveFirst () {

    myInternalVar->MoveFirst();
}

int DBFile::Close () {
    
    return myInternalVar->Close();
    
}

void DBFile::Add (Record &rec) {

    myInternalVar->Add(rec);

}

int DBFile::GetNext (Record &fetchme) {

    return myInternalVar->GetNext(fetchme);

}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {

    return myInternalVar->GetNext(fetchme, cnf, literal);

}
