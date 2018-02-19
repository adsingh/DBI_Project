#include <iostream>
#include "Record.h"
#include "DBFile.h"
#include <stdlib.h>
#include <math.h>

using namespace std;



extern "C" {
int yyparse(void); // defined in y.tab.c
}

extern struct AndList *final;

int main()
{

	// try to parse the CNF
	cout << "Enter in your CNF: ";
	if (yyparse() != 0)
	{
		cout << "Can't parse your CNF.\n";
		exit(1);
	}

	// suck up the schema from the file
	Schema lineitem("catalog", "lineitem");

	// grow the CNF expression from the parse tree
	CNF myComparison;
	Record literal;
	
	myComparison.GrowFromParseTree(final, &lineitem, literal);
	// print out the comparison to the screen
	myComparison.Print();

	// now open up the text file and start procesing it
	FILE *tableFile = fopen("/home/asiglani/DBI_project/tpch-dbgen/lineitem.tbl", "r");
	
	Record temp;
	Schema mySchema("catalog", "lineitem");

	//char *bits = literal.GetBits ();
	//cout << " numbytes in rec " << ((int *) bits)[0] << endl;
	//literal.Print (&supplier);

	DBFile* db = new DBFile();
	db->Create("dbi_MOT", heap, NULL);
	// read in all of the records from the text file and see if they match
	// the CNF expression that was typed in
	int counter = 0;
	ComparisonEngine comp;
	while (temp.SuckNextRecord(&mySchema, tableFile) == 1)
	{
		counter++;
		// if (counter % 10000 == 0)
		// {
		// 	cerr << counter << "\n";
		// }

		db->Add(temp);

		// if (comp.Compare(&temp, &literal, &myComparison))
		// 	temp.Print(&mySchema);
	}
	cout << "total records = " << counter << endl;
	// if(db->GetNext(temp, myComparison, literal)==0)
	// 	cout << "Record with given conditions not found\n";
	// else
	// 	temp.Print(&mySchema);
	// if(db->GetNext(temp)!=0)
	// 	temp.Print(&mySchema);
	// if(db->GetNext(temp)!=0)
	// 	temp.Print(&mySchema);
	// if(db->GetNext(temp)!=0)
	// 	temp.Print(&mySchema);
	// if(db->GetNext(temp)!=0)
	// 	temp.Print(&mySchema);

	counter  = 0;
	while(db->GetNext(temp) != 0)
		counter++;
	cout << "total records read = " << counter << endl;
	db->Close();
}
