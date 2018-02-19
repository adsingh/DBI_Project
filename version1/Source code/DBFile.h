#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include <vector>
#include <string>

using namespace std;

typedef enum {heap, sorted, tree} fType;
typedef enum {start, add, getnext} state;

// stub DBFile header..replace it with your own DBFile.h 

class DBFile {

    private:
        Page myPage;
        File myFile;
        off_t totalPages;
        int currentPageNo, currRecordNo;
        vector<int> pageSizesArr;
        state prevState;
		string configFile_name;
		bool advance;
		string GetFileName(const char* f_path);

public:
	DBFile (); 

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
