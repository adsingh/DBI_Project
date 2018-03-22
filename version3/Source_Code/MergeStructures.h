#ifndef MERGESTRUCTURES_H
#define MERGESTRUCTURES_H

#include "Record.h"
#include "Comparison.h"
#include <queue>

// Element for Priority Queue
struct QueueElement {
    int index;
    Record* record;

    QueueElement(int index, Record* rec){
        this->index = index;
        this->record = rec;
    }
};

// Comparator to compare two Queue Elements
struct QueueComparator{
    ComparisonEngine comp;
    OrderMaker* orderMaker;

    QueueComparator(OrderMaker* orderMaker){
        this->orderMaker = orderMaker;
    }

    bool operator () (const QueueElement* a, const QueueElement* b){
        return comp.Compare((Record*)(a->record), (Record*)(b->record), this->orderMaker ) > 0 ? true : false;
    }
};

typedef priority_queue<QueueElement* , vector<QueueElement*>, QueueComparator> pq;

#endif