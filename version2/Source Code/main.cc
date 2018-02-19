#include "main.h"
#include "BigQ.h"
#include <pthread.h>

int main (int argc, char *argv[]) {

    int buffsz = 100;
    Pipe input (buffsz);
    OrderMaker o;
    BigQ b(input, input, o, 10);

    return 0;

}