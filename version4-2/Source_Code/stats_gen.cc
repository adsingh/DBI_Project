#include "Statistics.h"

int main(){

    Statistics s;

    s.AddRel("supplier",10000);
	s.AddAtt("supplier", "s_suppkey",10000);
	s.AddAtt("supplier", "s_nationkey",25);

	s.AddRel("partsupp",800000);
	s.AddAtt("partsupp", "ps_suppkey", 10000);	
	s.AddAtt("partsupp", "ps_partkey", 200000);	

    s.AddRel("lineitem", 6001215);

    s.AddAtt("lineitem", "l_returnflag", 3);
    s.AddAtt("lineitem", "l_discount",11);
    s.AddAtt("lineitem","l_shipmode", 7);
    s.AddAtt("lineitem", "l_orderkey",1500000);
    s.AddAtt("lineitem", "l_receiptdate",6001215);
    s.AddAtt("lineitem", "l_partkey",200000);
    s.AddAtt("lineitem", "l_shipinstruct",4);

    s.AddRel("orders", 1500000);
    s.AddAtt("orders", "o_custkey",150000);
    s.AddAtt("orders", "o_orderkey",1500000);
    s.AddAtt("orders", "o_orderdate",1500000);

    s.AddRel("customer", 150000);
    s.AddAtt("customer", "c_custkey", 150000);
    s.AddAtt("customer", "c_nationkey", 25);
    s.AddAtt("customer", "c_mktsegment",5);

    s.AddRel("nation", 25);
    s.AddAtt("nation", "n_nationkey",25);
    s.AddAtt("nation", "n_regionkey",5);
    s.AddAtt("nation", "n_name", 25);

    s.AddRel("part", 200000);
    s.AddAtt("part", "p_partkey",200000);
    s.AddAtt("part", "p_size",50);
    s.AddAtt("part", "p_name", 199996);
    s.AddAtt("part", "p_container",40);

    s.AddRel("region", 5);
    s.AddAtt("region", "r_regionkey", 5);
    s.AddAtt("region", "r_name", 5);

    s.Write("Statistics.txt");

    return 0;
}