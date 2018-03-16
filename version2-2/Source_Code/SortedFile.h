#ifndef SORTEDFILE_H
#define SORTEDFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include <vector>
#include <string>
#include "DBFile.h"
#include "BigQ.h"

#define BUFF_SIZE 100

class SortedFile: public GenericDBFile {

private:
    Page myPage;
    File myFile;
    int currRecordNo, currentPageNo;
    string configFile_name;
    OrderMaker* queryOrderMaker, *literalOrderMaker;
    BigQ* bigQ;

    struct SortInfo{
        OrderMaker *orderMaker;
        int runLength;

        SortInfo(int runlen, OrderMaker *orderMaker){
            this->runLength = runlen;
            this->orderMaker = orderMaker;
        }
    };

    struct SortInfo *sortInfo;
    string filePath;
    Pipe *input, *output;

    // Contains page number and offset of the record found by binary search
    int startingPage;

    // Utility method. Merges all records in the BigQ with the existing File
    // Used when File switches to Read Mode
    void Merge();

public:
	SortedFile (string confgFile_name); 
	SortedFile (string confgFile_name, int runlen, OrderMaker* orderMaker); 
    ~SortedFile();
    
	int Create (const char *fpath, fType file_type, void *startup);
	int Open (const char *fpath);
	int Close ();

	void Load (Schema &myschema, const char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

};
#endif