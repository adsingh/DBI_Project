#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include <string>
#include <unordered_map>

using namespace std;

class Statistics
{

private:
	int unassignedGroupNo;
	unordered_map<string, int> rel_to_group;
	unordered_map<int, unordered_map<string, double> > group_to_info;
	int GetGroupNo(unordered_map<int, int>, string att_name, Statistics& stats);
	double EstimationHelper(struct AndList *parseTree, char *relNames[], int numToJoin, Statistics &stats);

public:
	Statistics();
	Statistics(Statistics &copyMe);	 // Performs deep copy
	~Statistics();


	void AddRel(char *relName, int numTuples);
	void AddAtt(char *relName, char *attName, int numDistincts);
	void CopyRel(char *oldName, char *newName);
	
	void Read(char *fromWhere);
	void Write(char *fromWhere);

	void  Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
	double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);

};

#endif
