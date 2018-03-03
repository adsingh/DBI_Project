#ifndef HeapFile_H
#define HeapFile_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include <vector>
#include <string>
#include "DBFile.h"

// stub HeapFile header..replace it with your own HeapFile.h 

class HeapFile: public GenericDBFile {

    private:
        Page myPage;
        File myFile;
        off_t totalPages;
        int currentPageNo, currRecordNo;
        vector<int>* pageSizesArr;
        state prevState;
		string configFile_name;
		bool gettingRecordForFirstTime;
		string GetFileName(const char* f_path);

public:
	HeapFile (string configFile_name); 
    ~HeapFile();
    HeapFile (HeapFileInfo* configFile);
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