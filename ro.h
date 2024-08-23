#ifndef RO_H
#define RO_H
#include "db.h"

void init();
void release();

// utility
int hash(const int input);
void freePageTuple(const int bid);

// compute extended table meta
void computeTableMeta();

// print buffer status
void printFileBuffer();
void printPageBuffer();

// buffer slot manager
int availFileBufferSlot();
int availPageBufferSlot();

// buffer manager
void releasePage(const int bid);
int requestPage(const char* table_name, const int ipid);
int requestFile(const char* table_name, const int ipid);

// read from disk operations
int readPageFromDisk(const char* table_name, const int ipid);
int readPageFromFileBuffer(const int fid, const int ipid);

// equality test for one attribute
// idx: index of the attribute for comparison, 0 <= idx < nattrs
// cond_val: the compared value
// table_name: table name
_Table* sel(const UINT idx, const INT cond_val, const char* table_name);

_Table* join(const UINT idx1, const char* table1_name, const UINT idx2, const char* table2_name);
#endif