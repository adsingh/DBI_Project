#include "Statistics.h"
#include <iostream>

Statistics::Statistics()
{
    unassignedGroupNo = 0;
}
Statistics::Statistics(Statistics &copyMe)
{
}
Statistics::~Statistics()
{
}

void Statistics::AddRel(char *relName, int numTuples)
{
    string relation_name(relName);
    string num_tuples_str("num_tuples");
    // Add entries in 2 maps if relation does not exist
    if(rel_to_group.find(relation_name) == rel_to_group.end()) {
        rel_to_group[relation_name] = unassignedGroupNo;
        unordered_map<string, int> info;
        // Signifies that the group is a singleton
        string num_relation_str("num_relations");
        group_to_info[unassignedGroupNo] = info; // I think this adds a copy of the map
        group_to_info[unassignedGroupNo][num_relation_str] = 1;
        unassignedGroupNo++;
    }
    int group_no = rel_to_group[relation_name];
    group_to_info[group_no][num_tuples_str] = numTuples;
}
void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{
    string relation_name(relName);
    if(rel_to_group.find(relation_name) == rel_to_group.end()) {
        cout << "Relation DOES NOT exist. Cannot add attribute" << endl;
        exit(1);
    }
    string att_name_str(attName);
    int group_no = rel_to_group[relation_name];
    group_to_info[group_no][att_name_str] = numDistincts;
}
void Statistics::CopyRel(char *oldName, char *newName)
{
    string old_relation_name(oldName);
    if(rel_to_group.find(old_relation_name) == rel_to_group.end()) {
        cout << "Relation DOES NOT exist. Cannot create copy" << endl;
        exit(1);
    }
    int old_group_no = rel_to_group[old_relation_name];
    string new_relation_name(newName);
    rel_to_group[new_relation_name] = unassignedGroupNo;
    group_to_info[unassignedGroupNo] = group_to_info[old_group_no]; // This MUST create a copy. TEST
    unassignedGroupNo++;
}
	
void Statistics::Read(char *fromWhere)
{
}
void Statistics::Write(char *fromWhere)
{
}

void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
    // Check whether Join can be applied
    unordered_map<int, int> input_groups;
    int group_no = -1;
    for(int i = 0 ; i < numToJoin; i++) {
        string relname_str(relNames[i]);
        group_no = rel_to_group[relname_str];
        if(input_groups.find(group_no) == input_groups.end()) {
            input_groups[group_no] = 0;
        }
        input_groups[group_no]++;
    }
    int num_relations = -1;
    string num_relation_str("num_relations");
    for (auto it=input_groups.begin(); it!=input_groups.end(); ++it) {
        group_no = it->first;
        if(group_to_info[group_no][num_relation_str] != it->second) {
            cout << "Cannot perform operation" << endl;
            exit(1);
        }
    }

}
double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{
}

