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

struct HeapFileInfo{
	int currRecordNo;
	int currentPageNo;
	off_t totalPages;
	vector<int> pageSizesArr;
	state prevState;
	string configFile_name;
};

class GenericDBFile{
	public:
		GenericDBFile();
		~GenericDBFile();

		virtual int Create (const char *fpath, fType file_type, void *startup) = 0;
		virtual int Open (const char *fpath) = 0;
		virtual int Close () = 0;
		virtual void Load (Schema &myschema, const char *loadpath) = 0;
		virtual void MoveFirst () = 0;
		virtual void Add (Record &addme) = 0;
		virtual int GetNext (Record &fetchme) = 0;
		virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal) = 0;

};

class DBFile {

    private:
        string configFile_name;
		string GetFileName(const char* f_path);
		GenericDBFile* myInternalVar;

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