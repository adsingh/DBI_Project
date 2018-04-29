#include "Statistics.h"
#include <iostream>
#include <vector>
#include <fstream>

Statistics::Statistics()
{
    unassignedGroupNo = 0;
}
Statistics::Statistics(Statistics &copyMe)
{
    unassignedGroupNo = copyMe.unassignedGroupNo;
    rel_to_group = copyMe.rel_to_group;
    for(pair<int,unordered_map<string, double>> entry: copyMe.group_to_info){
        group_to_info[entry.first] = entry.second;
    }
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
        unordered_map<string, double> info;
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
    ifstream statsFile(fromWhere);

    // Reading Map 1 (rel_to_grp)
    string relGroupsCntStr;
    getline(statsFile, relGroupsCntStr);

    string relName, grpNoStr;
    for(int i = 0 ; i < stoi(relGroupsCntStr); i++){
        getline(statsFile, relName);
        getline(statsFile, grpNoStr);

        rel_to_group[relName] = stoi(grpNoStr);
    }
    // Reading Map 2 (grp_to_info)
    string grpNoCntStr;
    int grpNo;
    getline(statsFile, grpNoCntStr);

    string entriesCntStr, attrName, attrValStr;

    for(int i = 0 ; i < stoi(grpNoCntStr) ; i++){
        
        getline(statsFile, grpNoStr);
        grpNo = stoi(grpNoStr);

        unordered_map<string, double> info;

        getline(statsFile, entriesCntStr);

        for(int j = 0 ; j < stoi(entriesCntStr); j++){
            getline(statsFile, attrName);
            getline(statsFile, attrValStr);
            info[attrName] = stoi(attrValStr);
        }

        group_to_info[grpNo] = info;
    }

    statsFile.close();
}

void Statistics::Write(char *fromWhere)
{
    ofstream statsFile(fromWhere);

    // Writing Map 1

    // Size of Map 2
    statsFile << to_string(rel_to_group.size());

    // Writing the relation, group_no pair per line
    for( pair<string, int> entry : rel_to_group){
        statsFile << endl << entry.first << "\n" << to_string(entry.second);
    }

    // Writing Map 2

    //Size of Map 2
    statsFile << endl << to_string(group_to_info.size());

    for( pair<int, unordered_map<string, double>> group_info_pair : group_to_info){

        // Writing the group no
        statsFile << endl << to_string(group_info_pair.first);

        // Writing the size of the unordered_map associated with the current group no
        statsFile << endl << to_string(group_info_pair.second.size());

        // Writing the attributes info for the current group
        for(pair<string, int> entry : group_info_pair.second){
            statsFile << endl << entry.first << endl << to_string(entry.second);
        }
    }

    statsFile.close();
}

void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
    EstimationHelper(parseTree, relNames, numToJoin, *this);   
}

void parseAttributeName(string& att_name){

    size_t found = att_name.find(".");

    if(found != string::npos){
        att_name = att_name.substr(found+1);
    }
}

double Statistics::EstimationHelper(struct AndList *parseTree, char *relNames[], int numToJoin, Statistics &stats) {

    // Check whether all relations in a set are included
    unordered_map<int, int> input_groups;
    int group_no = -1;
    for(int i = 0 ; i < numToJoin; i++) {
        string relname_str(relNames[i]);
        if(stats.rel_to_group.find(relname_str) == stats.rel_to_group.end()) {
            cout << "Unknown Relation " << relNames[i] << " at index "<< i << ". Exiting Application" << endl;
            exit(1);
        }
        group_no = stats.rel_to_group[relname_str];
        if(input_groups.find(group_no) == input_groups.end()) {
            input_groups[group_no] = 0;
        }
        input_groups[group_no]++;
    }
    int num_relations = -1;

    string num_relation_str("num_relations");

    // Verifying that all the relations belonging to a group are being used
    for (pair<int, int> entry : input_groups) {

        group_no = entry.first;
        if(stats.group_to_info[group_no][num_relation_str] != entry.second) {
            cout << "Cannot perform operation" << endl;
            exit(1);
        }
    }

    AndList *and_list = parseTree;
    OrList *or_list = NULL;
    ComparisonOp *comparison_op = NULL;
    Statistics copy;
    Statistics pointedAt;
    // Iterate over Andlist
    while(and_list != NULL) {
        or_list = and_list->left;

        // This will be used in combining or results
        Statistics *prev_or_result = NULL;
        // Iterate over the OrList
        while(or_list != NULL) {

            comparison_op = or_list->left;
            // Perform a join
            if(comparison_op->left->code == NAME & comparison_op->right->code == NAME) {
                
                
                string att1_str(comparison_op->left->value);  // s.s_nationkey
                string att2_str(comparison_op->right->value); // ps.s_nationkey

                int group1 = GetGroupNo(input_groups, att1_str, stats);
                int group2 = GetGroupNo(input_groups, att2_str, stats);

                parseAttributeName(att1_str);
                parseAttributeName(att2_str);

                // If groups are same, do nothing
                if(group1 == group2) {
                    or_list = or_list->rightOr;
                    continue;
                }

                unordered_map<string, double> updated_info;
                string num_tuples_str("num_tuples");
                string num_relation_str("num_relations");

                // Assuming it will automatically truncate division result
                double ratio1 = (1.0*stats.group_to_info[group1][num_tuples_str]) / stats.group_to_info[group1][att1_str];
                double ratio2 = (1.0*stats.group_to_info[group2][num_tuples_str]) / stats.group_to_info[group2][att2_str];

                // cout << "[Stats.cc] Ratios found \n";
                double unique_count = min(stats.group_to_info[group1][att1_str], stats.group_to_info[group2][att2_str]);

                // Updated no of tuples
                double updated_num_tuples = ratio1 * ratio2 * unique_count;

                // Add attributes from group 1
                for (pair<string, double> entry : stats.group_to_info[group1]) {
                    updated_info[entry.first] = min(updated_num_tuples, entry.second);
                }

                // Add attributes from group 2
                for (pair<string, double> entry : stats.group_to_info[group2]) {
                    updated_info[entry.first] = min(updated_num_tuples, entry.second);
                }

                // Update join attributes. This will always be the smaller value
                // If 1 value is larger, then some values will not appear in the join
                updated_info[att1_str] = unique_count;
                updated_info[att2_str] = unique_count;

                // Update no of tuples
                updated_info[num_tuples_str] = updated_num_tuples;

                // Update no of relations
                updated_info[num_relation_str] = stats.group_to_info[group1][num_relation_str] + stats.group_to_info[group2][num_relation_str];

                // Update maps. Delete larger group. Keep smaller group
                int min_group = min(group1, group2);
                int max_group = max(group1, group2);

                // Update rel_to_group and delete entry from group_to_info
                stats.group_to_info.erase(max_group);

                input_groups.erase(max_group);

                for (pair<string, int> entry : stats.rel_to_group) {
                    // There can be multiple such relations
                    if(entry.second == max_group) {
                        stats.rel_to_group[entry.first] = min_group;
                    }
                }

                // Add the new map to group_to_info
                stats.group_to_info[min_group] = updated_info;

            } else { // Perform selection

                // Create a copy of original
                copy = stats;
                // stats.Write("testStats.txt");

                // Perform operation on the copy
                group_no = -1;
                string attr_str(comparison_op->left->value);


                string num_tuples_str("num_tuples");
                if(comparison_op->left->code == NAME) {
                    group_no = GetGroupNo(input_groups, attr_str, stats);
                }

                parseAttributeName(attr_str);
                // cout << "[Stats.cc] Not a Join - Grp No = " << group_no << " \nAttr_name = " << attr_str <<" \n";

                double ratio = -1;
                if(comparison_op->code == LESS_THAN || comparison_op->code == GREATER_THAN) {
                    ratio = 1.0/3;
                } else {
                    // cout << "[Stats.cc] NO of distinct values = " << copy.group_to_info[group_no][attr_str] << endl;
                    ratio = 1.0/copy.group_to_info[group_no][attr_str];
                }

                double updated_num_tuples = ratio * copy.group_to_info[group_no][num_tuples_str];
                // cout << "updated No ********** " << updated_num_tuples << endl;
                // cout << "Ratio = " << ratio << endl;
                
                // Modify attributes
                for (pair<string, double> entry : copy.group_to_info[group_no]) {
                    copy.group_to_info[group_no][entry.first] =  min(entry.second, updated_num_tuples);
                }

                copy.group_to_info[group_no][num_tuples_str] = updated_num_tuples;
                copy.group_to_info[group_no][attr_str] = ratio * copy.group_to_info[group_no][attr_str];

                // Merge with previous or result
                if(prev_or_result != NULL) {

                    // Calculate  1. if the attribute distinct count should be added. 2. Intersection
                    double intersection = 0;
                    double attr_distinct_count = max(copy.group_to_info[group_no][attr_str],
                                                prev_or_result->group_to_info[group_no][attr_str]);
                    if(prev_or_result->group_to_info[group_no][attr_str] != stats.group_to_info[group_no][attr_str]) {
                        attr_distinct_count = copy.group_to_info[group_no][attr_str] + prev_or_result->group_to_info[group_no][attr_str];
                    } else { // This was not used previously
                        intersection = prev_or_result->group_to_info[group_no][num_tuples_str] * ratio;
                        
                    }
                    // cout << "Num tuples from prev = " << prev_or_result->group_to_info[group_no][num_tuples_str]
                    //             <<  "\nIntersection value = " << intersection << endl;

                    // Calculate resultant tuples
                    updated_num_tuples = copy.group_to_info[group_no][num_tuples_str] + 
                                         prev_or_result->group_to_info[group_no][num_tuples_str] - intersection;

                    for (pair<string, double> entry : copy.group_to_info[group_no]) {
                        prev_or_result->group_to_info[group_no][entry.first] = max(prev_or_result->group_to_info[group_no][entry.first],
                                                                                  entry.second); 
                    }

                    prev_or_result->group_to_info[group_no][num_tuples_str] = updated_num_tuples;
                    prev_or_result->group_to_info[group_no][attr_str] = attr_distinct_count;
                    
                } else {
                    pointedAt = copy;
                    prev_or_result = &pointedAt;
                }

            }

            or_list = or_list->rightOr;
            
        }

        if(prev_or_result != NULL) {
            stats.rel_to_group = prev_or_result->rel_to_group;
            for(pair<int, unordered_map<string, double> > entry : prev_or_result->group_to_info) {
                stats.group_to_info[entry.first] = entry.second;
            }
        }
        
        and_list = and_list->rightAnd;
        
    }

}

int Statistics::GetGroupNo(unordered_map<int, int> input_groups, string att_name, Statistics& stats) {
    int group_no = -1;

    size_t found = att_name.find(".");

    if(found != string::npos){
        string rel_name = att_name.substr(0 , found);
        att_name = att_name.substr(found+1);
        return stats.rel_to_group[rel_name];
    }

    for (auto it=input_groups.begin(); it!=input_groups.end(); ++it) {
        group_no = it->first;
        if(stats.group_to_info[group_no].find(att_name) != stats.group_to_info[group_no].end()) {
            return group_no;
        }
    }
    cout << "Group NOT FOUND for attribute : " << att_name << endl;
    exit(1);
}

double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{
    Statistics copy = *this;
    EstimationHelper(parseTree, relNames, numToJoin, copy);
    //copy.Write("testStats.txt");
    if(numToJoin > 0) {
        string relname_str(relNames[0]);
        string num_tuples_str("num_tuples");
        int group_no = copy.rel_to_group[relname_str];
        return copy.group_to_info[group_no][num_tuples_str];
    } 
    return 0;
    
}

