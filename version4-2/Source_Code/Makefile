
CC = g++ -std=c++14 -O2 -Wno-deprecated

tag = -i

ifdef linux
tag = -n
endif

test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o BigQ.o RelOp.o HeapFile.o SortedFile.o Function.o Statistics.o QueryPlanNode.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o test.o
	$(CC) -o a4-2.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o BigQ.o RelOp.o HeapFile.o SortedFile.o Function.o Statistics.o QueryPlanNode.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o test.o -lfl -lpthread

a4-1.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o BigQ.o RelOp.o HeapFile.o SortedFile.o Function.o Statistics.o QueryPlanNode.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o stats_gen.o
	$(CC) -o a4-1.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o BigQ.o RelOp.o HeapFile.o SortedFile.o Function.o Statistics.o QueryPlanNode.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o stats_gen.o -lfl -lpthread

a2-2test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o a2-2test.o
	$(CC) -o a2-2test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o a2-2test.o -lfl -lpthread
	
a2test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o a2-test.o
	$(CC) -o a2test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o a2-test.o -lfl -lpthread
	
a1test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o y.tab.o lex.yy.o a1-test.o
	$(CC) -o a1test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o y.tab.o lex.yy.o a1-test.o -lfl
	
main.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o BigQ.o RelOp.o HeapFile.o SortedFile.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o main.o
	$(CC) -o main.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o BigQ.o RelOp.o HeapFile.o SortedFile.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o main.o -lfl -lpthread

test.o: test.cc
	$(CC) -g -c test.cc
	
stats_gen.o: stats_gen.cc
	$(CC) -g -c stats_gen.cc

a2-2test.o: a2-2test.cc
	$(CC) -g -c a2-2test.cc

a2-test.o: a2-test.cc
	$(CC) -g -c a2-test.cc

a1-test.o: a1-test.cc
	$(CC) -g -c a1-test.cc

main.o: main.cc
	$(CC) -g -c main.cc

Statistics.o: Statistics.cc
	$(CC) -g -c Statistics.cc

QueryPlanNode.o: QueryPlanNode.cc
	$(CC) -g -c QueryPlanNode.cc

Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc
	
ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc
	
DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc
	
HeapFile.o: HeapFile.cc
	$(CC) -g -c HeapFile.cc

SortedFile.o: SortedFile.cc
	$(CC) -g -c SortedFile.cc

Pipe.o: Pipe.cc
	$(CC) -g -c Pipe.cc

BigQ.o: BigQ.cc
	$(CC) -g -c BigQ.cc

RelOp.o: RelOp.cc
	$(CC) -g -c RelOp.cc

Function.o: Function.cc
	$(CC) -g -c Function.cc

File.o: File.cc
	$(CC) -g -c File.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc
	
y.tab.o: Parser.y
	yacc -d Parser.y
	#sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c y.tab.c
		
yyfunc.tab.o: ParserFunc.y
	yacc -p "yyfunc" -b "yyfunc" -d ParserFunc.y
	#sed $(tag) yyfunc.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c yyfunc.tab.c
	
lex.yy.o: Lexer.l
	lex Lexer.l
	gcc  -c lex.yy.c

lex.yyfunc.o: LexerFunc.l
	lex -Pyyfunc LexerFunc.l
	gcc  -c lex.yyfunc.c


clean: 
	rm -f *.o
	rm -f *.out
	rm -f y.tab.*
	rm -f yyfunc.tab.*
	rm -f lex.yy.*
	rm -f lex.yyfunc*
