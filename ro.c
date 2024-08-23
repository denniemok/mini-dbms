#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "ro.h"
#include "db.h"

#define min(x,y) (((x)<(y))?(x):(y))
#define max(x,y) (((x)>(y))?(x):(y))

typedef struct FileDesc { // file = table
    // maintain meta info of table
    UINT isempty;
    UINT oid;
    UINT nattrs;
    UINT ntuples;
    UINT npages;
    char name[10];
    char path[120];
    FILE* file; // file pointer
} FileDesc;

typedef struct PageDesc { // page = collection of tuples
    // maintain meta info of page
    UINT isempty;
    UINT pageid;
    UINT ipid; // internal page id, count from 0, increment 1 (ie nth page)
    UINT oid; // table oid
    char name[10];
    UINT nattrs; 
    UINT ntuples; // number of tuples in page (different from file's ntuples)
    UINT pin;
    UINT use;
    INT** tuple; // array of int32 data
} PageDesc;

// extended table meta
typedef struct exTable{
    UINT oid;
    char name[10];
    UINT nattrs;
    UINT ntuples;
    UINT npages;
    UINT ntpp;
} exTable;

FileDesc** fileBuffer; // array of file descriptors, size depends on conf value
PageDesc** pageBuffer; // array of pages, size depends on conf value
exTable* extmeta;

int NVF = 0; // next victim file to evict in fbuffer
int NVP = 0; // next victim page to evict in pbuffer

Conf* conf;
Database* dbase;

// initialisation
void init() {

    conf = get_conf();
    dbase = get_db();

    extmeta = malloc(sizeof(exTable) * dbase->ntables);
    computeTableMeta();

    fileBuffer = malloc(sizeof(FileDesc*) * conf->file_limit);
    pageBuffer = malloc(sizeof(PageDesc*) * conf->buf_slots);

    for (int i = 0; i < conf->file_limit; i++) {
        fileBuffer[i] = malloc(sizeof(FileDesc));
        fileBuffer[i]->isempty = 1;
    }

    for (int i = 0; i < conf->buf_slots; i++) {
        pageBuffer[i] = malloc(sizeof(PageDesc));
        pageBuffer[i]->isempty = 1;
    }
    
    printf("\ninit() is invoked.\n");

    printf("\n====================\n");
    printPageBuffer();
    printf("====================\n");
    printFileBuffer();
    printf("====================\n");

}


// end tasks
void release() {

    // free space to avoid memory leak

    for (int i = 0; i < conf->file_limit; i++) {
        free(fileBuffer[i]);
    }

    for (int i = 0; i < conf->buf_slots; i++) {
        freePageTuple(i); // if empty then will have segmentation fault
        free(pageBuffer[i]);
    }

    free(fileBuffer);
    free(pageBuffer);
    free(extmeta);

    printf("\nrelease() is invoked.\n");

}


int hash(const int input) {
    return input % 2;
}


// clean tuples in page buffer
void freePageTuple(const int bid) {
    if (pageBuffer[bid]->isempty) return;
    for (int y = 0; y < pageBuffer[bid]->ntuples; y++) { // for each tuple
        free(pageBuffer[bid]->tuple[y]);
    }
    free(pageBuffer[bid]->tuple);
}


// return specific table meta, with table name as input
exTable* getTableMeta(const char* table_name) {
    for (int i = 0; i < dbase->ntables; i++) {
        if (strcmp(table_name, extmeta[i].name) == 0) {
            return &extmeta[i];
        }
    }
    return NULL;
}


// compute extended table meta, global action
void computeTableMeta() {

    printf("\nTABLE META\n");

    for (int i = 0; i < dbase->ntables; i++) {
        extmeta[i].oid = dbase->tables[i].oid;
        strcpy(extmeta[i].name, dbase->tables[i].name);
        extmeta[i].nattrs = dbase->tables[i].nattrs;
        extmeta[i].ntuples = dbase->tables[i].ntuples;

        // compute the number of pages involved in the selection
        int ntpp = (conf->page_size - 8) / (dbase->tables[i].nattrs * 4); // number of tuples per page
        int npages = dbase->tables[i].ntuples / ntpp;
        if (dbase->tables[i].ntuples % ntpp != 0) {
            npages++;
        }

        extmeta[i].ntpp = ntpp;
        extmeta[i].npages = npages;

        printf("name: %s | oid: %u | nattrs: %u | ntuples: %u | ntpp: %i | npages: %i\n", extmeta[i].name, extmeta[i].oid, extmeta[i].nattrs, extmeta[i].ntuples, extmeta[i].ntpp, extmeta[i].npages);

    }

}


// page buffer slot manager 
int availPageBufferSlot() {

    // first-traversal: find free space
    for (int i = 0; i < conf->buf_slots; i++) {
        if (pageBuffer[i]->isempty) return i;
    }

    // second-traversal: find page to evict if full
    // clock-sweep replacement policy
    int evict = 0;
    int pNVP;

    while (!evict) {

        for (int i = NVP; i < conf->buf_slots; i++) {
            
            // if both pin_count and usage_count are 0, execute eviction
            if (pageBuffer[i]->pin == 0 && pageBuffer[i]->use == 0) {
                
                log_release_page(pageBuffer[i]->pageid);
                
                freePageTuple(i);
                free(pageBuffer[i]);
                pageBuffer[i] = malloc(sizeof(PageDesc));
                //pageBuffer[i]->isempty = 1;

                NVP = i;
                pNVP = NVP;
                NVP = (NVP+1) % conf->buf_slots;
                
                evict = 1;
                break;

            }
            
            if (pageBuffer[i]->use > 0) pageBuffer[i]->use--;
        
        }

        if (!evict) NVP = 0;

    } 

    printf("CLOCK SWEEP [NVP: %i | Next NVP: %i]\n", pNVP, NVP);
    return pNVP;

}


// file buffer slot manager
int availFileBufferSlot() {

    // find free space
    for (int i = 0; i < conf->file_limit; i++) {
        if (fileBuffer[i]->isempty) return i;
    }

    // if no empty slot, replacement policy takes place
    fclose(fileBuffer[NVF]->file);
    log_close_file(fileBuffer[NVF]->oid);

    free(fileBuffer[NVF]);
    fileBuffer[NVF] = malloc(sizeof(FileDesc));
    //fileBuffer[NVF]->isempty = 1;

    int pNVF = NVF; // because NVF will increment
    NVF = (NVF + 1) % conf->file_limit; // cycle back

    return pNVF;

}


// read page to page buffer from file buffer
// return page buffer id (ie buffer tag) of the nth page of target table
int readPageFromFileBuffer(const int fid, const int ipid) {

    printf("\nREAD FROM FBUFFER\n");

    // open file from existing pointer in file desc
    FILE* file = fileBuffer[fid]->file;

    // compute the number of tuples in page
    int ntip = (conf->page_size - 8) / (fileBuffer[fid]->nattrs * 4);

    // if it is the last page, in case the last page as less fewer tuples
    if (ipid == fileBuffer[fid]->npages - 1) {
        ntip = fileBuffer[fid]->ntuples - ntip * (fileBuffer[fid]->npages - 1);
    }

    // compute page offset in file
    int offset = ipid * conf->page_size;
    //printf("ntip: %i | offset: %i\n", ntip, offset);

    // move file seeker
    fseek(file, offset, SEEK_SET);

    // read pageid
    long pageid; // 64 bits or 8 bytes
    fread(&pageid, sizeof pageid, 1, file);

    // get free page buffer slot from page buffer slot manager
    int bid = availPageBufferSlot();

    // complete page desc info
    pageBuffer[bid]->isempty = 0;
    pageBuffer[bid]->pageid = pageid;
    pageBuffer[bid]->ipid = ipid;
    pageBuffer[bid]->oid = fileBuffer[fid]->oid;
    strcpy(pageBuffer[bid]->name, fileBuffer[fid]->name);
    pageBuffer[bid]->nattrs = fileBuffer[fid]->nattrs;
    pageBuffer[bid]->ntuples = ntip;
    pageBuffer[bid]->pin = 1;
    pageBuffer[bid]->use = 1;

    // convert binary data into integers and store in array
    pageBuffer[bid]->tuple = malloc(sizeof(INT*) * ntip);

    for (int y = 0; y < ntip; y++) { // for each tuple

        pageBuffer[bid]->tuple[y] = malloc(sizeof(INT) * fileBuffer[fid]->nattrs);
        
        for (int x = 0; x < fileBuffer[fid]->nattrs; x++) { // for each attr

            int data; // 32 bits or 4 bytes
            fread(&data, sizeof data, 1, file);
            
            pageBuffer[bid]->tuple[y][x] = data;
            printf("%i ", data);

        }

        printf("\n");

    }

    // read page from disk
    log_read_page(pageid); 

    return bid;

}


// read page to page buffer from disk, costruct file desc to file buffer
// return page buffer id (ie buffer tag) of the nth page of target table
int readPageFromDisk(const char* table_name, const int ipid) {

    exTable* tmeta = getTableMeta(table_name);
    if (tmeta == NULL) return -1;

    printf("\nREAD FROM DISK\n");

    // compose file path
    // path is 100 bytes, / is 1 byte, 10 bytes for 32 bit oid, 9 byte for buffer
    char t_path[120];
    sprintf(t_path, "%s/%u", dbase->path, tmeta->oid);

    // open file from disk
    FILE* file = fopen(t_path, "rb"); // read bytes
    log_open_file(tmeta->oid); // read from disk

    // compute the number of tuples in page
    int ntip = tmeta->ntpp;
    
    // if it is the last page, in case the last page as less fewer tuples
    if (ipid == tmeta->npages - 1) {
        ntip = tmeta->ntuples - ntip * (tmeta->npages - 1);
    }

    // compute page offset in file
    int offset = ipid * conf->page_size;
    //printf("ntip: %i | offset: %i\n", ntip, offset);

    // move file seeker
    fseek(file, offset, SEEK_SET);

    // read pageid
    long pageid; // 64 bits or 8 bytes
    fread(&pageid, sizeof pageid, 1, file);

    // get free page buffer slot from page buffer slot manager
    int bid = availPageBufferSlot();

    // complete page desc info
    pageBuffer[bid]->isempty = 0;
    pageBuffer[bid]->pageid = pageid;
    pageBuffer[bid]->ipid = ipid;
    pageBuffer[bid]->oid = tmeta->oid;
    strcpy(pageBuffer[bid]->name, tmeta->name);
    pageBuffer[bid]->nattrs = tmeta->nattrs;
    pageBuffer[bid]->ntuples = ntip;
    pageBuffer[bid]->pin = 1;
    pageBuffer[bid]->use = 1;

    // convert binary data into integers and store in array
    pageBuffer[bid]->tuple = malloc(sizeof(INT*) * ntip);

    for (int y = 0; y < ntip; y++) { // for each tuple

        pageBuffer[bid]->tuple[y] = malloc(sizeof(INT) * tmeta->nattrs);
        
        for (int x = 0; x < tmeta->nattrs; x++) { // for each attr

            int data; // 32 bits or 4 bytes
            fread(&data, sizeof data, 1, file);

            pageBuffer[bid]->tuple[y][x] = data;
            printf("%i ", data);

        }

        printf("\n");

    }

    // read page from disk
    log_read_page(pageid);

    // get free file buffer slot from file buffer slot manager
    int fid = availFileBufferSlot();

    // complete file desc info
    fileBuffer[fid]->isempty = 0;
    fileBuffer[fid]->oid = tmeta->oid;
    fileBuffer[fid]->nattrs = tmeta->nattrs;
    fileBuffer[fid]->ntuples = tmeta->ntuples;
    fileBuffer[fid]->npages = tmeta->npages;
    strcpy(fileBuffer[fid]->name, tmeta->name);
    strcpy(fileBuffer[fid]->path, t_path);
    fileBuffer[fid]->file = file; // keep file pointer

    return bid;

}


// entrance of file buffer manager
// traverse through the file buffer pool to look for target file
// return page buffer id (ie buffer tag) of the nth page of target table
int requestFile(const char* table_name, const int ipid) {

    // for each file desc in file buffer pool
    for (int fid = 0; fid < conf->file_limit; fid++) {
        
        if (fileBuffer[fid]->isempty) continue;
        
        // if found
        if (strcmp(fileBuffer[fid]->name, table_name) == 0) {
            // read file from buffer
            return readPageFromFileBuffer(fid, ipid);
        }

    }

    // if not found
    // read file from disk 
    return readPageFromDisk(table_name, ipid);

}


// entrance of page buffer manager
// traverse through the page buffer pool to look for target page
// return page buffer id (ie buffer tag) of the nth page of target table
int requestPage(const char* table_name, const int ipid) {

    // for each page desc in page buffer pool
    for (int i = 0; i < conf->buf_slots; i++) {
        
        if (pageBuffer[i]->isempty) continue;
        
        // if found
        if (strcmp(pageBuffer[i]->name, table_name) == 0 && pageBuffer[i]->ipid == ipid) {
            
            printf("\nREAD FROM PBUFFER\n");
            
            pageBuffer[i]->pin = 1;
            pageBuffer[i]->use++;

            // print tuple values
            for (int y = 0; y < pageBuffer[i]->ntuples; y++) {
                for (int x = 0; x < pageBuffer[i]->nattrs; x++) {
                    printf("%i ", pageBuffer[i]->tuple[y][x]);
                }
                printf("\n");
            }

            return i;

        }

    }

    // if no match found, get page from file buffer manager
    // by default set pin = 1 and use = 1;
    return requestFile(table_name, ipid);

}


void releasePage(const int bid) {
    pageBuffer[bid]->pin = 0;
}


_Table* sel(const UINT idx, const INT cond_val, const char* table_name) {

    // invoke log_read_page() every time a page is read from the hard drive.
    // invoke log_release_page() every time a page is released from the memory.

    // invoke log_open_file() every time a page is read from the hard drive.
    // invoke log_close_file() every time a page is released from the memory.
    
    printf("\nsel() is invoked.\n");
    printf("\nSEL\nidx: %u | cond_val: %i | table_name: %s\n", idx, cond_val, table_name); 

    exTable* tmeta = getTableMeta(table_name);
    if (tmeta == NULL) return NULL;
    
    int res_ntuples = 0;
    INT temp[tmeta->ntuples][tmeta->nattrs]; // temp array holding results
    // results will not be more than ntuples
    
    // for each page in table
    for (int ipid = 0; ipid < tmeta->npages; ipid++) {
        
        // get buffer id
        int bid = requestPage(table_name, ipid);
        if (bid == -1) return NULL;

        // do equality comparison
        printf("results:\n");

        for (int y = 0; y < pageBuffer[bid]->ntuples; y++) {
            // equality search on single attr
            if (cond_val == pageBuffer[bid]->tuple[y][idx]) { 
                for (int x = 0; x < pageBuffer[bid]->nattrs; x++) {
                    printf("%i ", pageBuffer[bid]->tuple[y][x]);
                    temp[res_ntuples][x] = pageBuffer[bid]->tuple[y][x];
                }
                printf("\n");
                res_ntuples++;
            }
        }

        // finished reading page, release page
        releasePage(bid);

    }

    printf("\nres_ntuples: %i\n", res_ntuples);

    // compose result table
    _Table* result = malloc(sizeof(_Table) + res_ntuples * sizeof(Tuple));
    result->nattrs = tmeta->nattrs;
    result->ntuples = res_ntuples;

    for (int i = 0; i < result->ntuples; i++) {
        Tuple t = malloc(sizeof(INT) * result->nattrs);
        result->tuples[i] = t;
        for (int j = 0; j < result->nattrs; j++) {
            t[j] = temp[i][j];
        }
    }

    printf("\n====================\n");
    printPageBuffer();
    printf("====================\n");
    printFileBuffer();
    printf("====================\n");
    
    return result;

}


_Table* join(const UINT idx1, const char* table1_name, const UINT idx2, const char* table2_name) {

    // invoke log_read_page() every time a page is read from the hard drive.
    // invoke log_release_page() every time a page is released from the memory.

    // invoke log_open_file() every time a page is read from the hard drive.
    // invoke log_close_file() every time a page is released from the memory.

    printf("\njoin() is invoked.\n");
    printf("\nJOIN\nidx1: %u | table1_name: %s | idx2: %u | table2_name: %s\n", idx1, table1_name, idx2, table2_name); 

    // get table meta
    exTable* tmeta1 = getTableMeta(table1_name);
    exTable* tmeta2 = getTableMeta(table2_name);
    if (tmeta1 == NULL || tmeta2 == NULL) return NULL;
    
    int res_ntuples = 0;
    // temp array holding results
    INT temp[tmeta1->ntuples * tmeta2->ntuples][tmeta1->nattrs + tmeta2->nattrs];

    // naive nested loop join
    if (tmeta1->npages + tmeta2->npages > conf->buf_slots) {

        printf("\nBLOCK NESTED LOOP JOIN\n");
        
        // compute performance cost
        // table 1 as outer table
        int a = tmeta1->npages / (conf->buf_slots - 1);
        if (tmeta1->npages % (conf->buf_slots - 1) != 0) a++;
        int plan1 = tmeta1->npages + tmeta2->npages * a;

        // table 2 as outer table
        int b = tmeta2->npages / (conf->buf_slots - 1);
        if (tmeta2->npages % (conf->buf_slots - 1) != 0) b++;
        int plan2 = tmeta2->npages + tmeta1->npages * b;

        printf("COST [plan1: %i | plan2: %i]\n", plan1, plan2);

        int nchunks;
        int outer_npages;
        int inner_npages;
        char outer_tname[10];
        char inner_tname[10];
        int outer_idx;
        int inner_idx;
        
        // performance evaluator
        if (plan1 <= plan2) { // if both plan has same cost, always use table1 as outer
            nchunks = a;
            outer_npages = tmeta1->npages;
            inner_npages = tmeta2->npages;
            strcpy(outer_tname, tmeta1->name);
            strcpy(inner_tname, tmeta2->name);
            outer_idx = idx1;
            inner_idx = idx2;
            printf("plan1 is chosen: table1 as outer, table2 as inner\n");
        } else {
            nchunks = b;
            outer_npages = tmeta2->npages;
            inner_npages = tmeta1->npages;
            strcpy(outer_tname, tmeta2->name);
            strcpy(inner_tname, tmeta1->name);
            outer_idx = idx2;
            inner_idx = idx1;
            printf("plan2 is chosen: table2 as outer, table1 as inner\n");
        }

        // number of outer page to read in chunk
        int outer_nPiC = min(outer_npages, conf->buf_slots - 1);

        // for each outer chunk
        for (int i = 0; i < nchunks; i++) { 

            // temporarily stores bid of processed outer chunk pages
            int outerL[outer_nPiC];

            // read a chunk of outer page
            printf("\n... reading %i/%i outer chunk (outer_nPiC: %i) ...\n", i+1, nchunks, outer_nPiC);
            for (int j = 0; j < outer_nPiC; j++) { 
                outerL[j] = requestPage(outer_tname, j + i * (conf->buf_slots - 1));
                if (outerL[j] == -1) return NULL;
            }
            printf("\n... finished reading %i/%i outer chunk ...\n", i+1, nchunks);

            printf("\n... reading inner pages, computing result tuples ...\n");

            // for each inner page
            for (int k = 0; k < inner_npages; k++) { 
                
                int inner_bid = requestPage(inner_tname, k);
                if (inner_bid == -1) return NULL;

                printf("results:\n");

                // for each outer page in chunk
                for (int m = 0; m < outer_nPiC; m++) { 
                    
                    //int outer_bid = requestPage(outer_tname, m + i * (conf->buf_slots - 1));
                    //if (outer_bid == -1) return NULL;
                    //outerL[m] = outer_bid;
                    int outer_bid = outerL[m];

                    // for each tuple in outer page
                    for (int oy = 0; oy < pageBuffer[outer_bid]->ntuples; oy++) {
                        
                        // for each tuple in inner page
                        for (int iy = 0; iy < pageBuffer[inner_bid]->ntuples; iy++) { 
                            
                            // do comparison, join test
                            if (pageBuffer[outer_bid]->tuple[oy][outer_idx] == pageBuffer[inner_bid]->tuple[iy][inner_idx]) {
                                
                                int s;
                                int v;

                                if (plan1 <= plan2) { 
                                    s = 0;
                                    v = pageBuffer[outer_bid]->nattrs;
                                } else {
                                    s = pageBuffer[inner_bid]->nattrs;
                                    v = 0;
                                }

                                // store result tuple to temp
                                // reverse print order depending on plan
                                for (int ox = 0; ox < pageBuffer[outer_bid]->nattrs; ox++) {
                                    printf("%i ", pageBuffer[outer_bid]->tuple[oy][ox]);
                                    temp[res_ntuples][ox+s] = pageBuffer[outer_bid]->tuple[oy][ox];
                                }

                                for (int ix = 0; ix < pageBuffer[inner_bid]->nattrs; ix++) {
                                    printf("%i ", pageBuffer[inner_bid]->tuple[iy][ix]);
                                    temp[res_ntuples][ix+v] = pageBuffer[inner_bid]->tuple[iy][ix];
                                }
                                
                                printf("\n");
                                res_ntuples++;

                            }
                        }
                    }
                }

                // release inner page
                releasePage(inner_bid);

            }

            //release all outer pages in chunk
            for (int p = 0; p < outer_nPiC; p++) {
                releasePage(outerL[p]);
            }

            printf("\n... finished computing result tuples ...\n");

            // recompute number of pages to read in next chunk
            outer_nPiC = min(outer_npages - outer_nPiC * (i+1), conf->buf_slots - 1);

        }

        
    // simple hash join
    } else {

        printf("\nSIMPLE HASH JOIN\n");
        printf("table1 as outer, table2 as inner\n");

        // always treat table1 as outer table, table2 as inner table

        // compute hash table for outer table
        // hash table is temporary and will be destroyed once hash join is completed
        // partition based on even or odd numbers on join attrs
        int hashtable[2][tmeta1->ntuples][tmeta1->nattrs];

        // number of entries in parition
        int nEiP[2] = {0, 0};

        // table1, outer table
        // scan through each page in outer table, hash all tuples into paritions
        printf("\n... hashing table1 into 2 partitions ...\n");

        // for each page in table
        for (int i = 0; i < tmeta1->npages; i++) { 
            
            // request page
            int bid = requestPage(table1_name, i);
            if (bid == -1) return NULL;
            
            // for each tuples in page
            for (int y = 0; y < pageBuffer[bid]->ntuples; y++) { 
                
                // check divisibility on join attrs
                int prt = hash(pageBuffer[bid]->tuple[y][idx1]);

                // for each attr in tuple
                for (int x = 0; x < pageBuffer[bid]->nattrs; x++) { 

                    // make a copy to hash table
                    hashtable[prt][nEiP[prt]][x] = pageBuffer[bid]->tuple[y][x];

                }

                nEiP[prt]++;

            }

            // release page
            releasePage(bid);

        }

        printf("\n... finished hashing table1 into 2 partitions ...\n");

        // table2, inner table
        // scan through each page in inner table, compare with the corresponding parition in hash table
        printf("\n... scanning table2, computing result tuples ...\n");

        // for each page in table
        for (int i = 0; i < tmeta2->npages; i++) { 
            
            // request page
            int bid = requestPage(table2_name, i); 
            if (bid == -1) return NULL;

            printf("results:\n");
            
            // for each tuples in page
            for (int y = 0; y < pageBuffer[bid]->ntuples; y++) { 
                
                // check divisibility on join attrs, derive hash parition to check
                int prt = hash(pageBuffer[bid]->tuple[y][idx2]);

                // for each entry in hash, in corresponse to the derived hash parition
                for (int h = 0; h < nEiP[prt]; h++) { 

                    // do comparison, join test
                    if (pageBuffer[bid]->tuple[y][idx2] == hashtable[prt][h][idx1]) {
                        
                        // store result tuple to temp
                        // do not need to reverse print order since table1 is always outer
                        int ox = 0;

                        for (ox = 0; ox < tmeta1->nattrs; ox++) {
                            printf("%i ", hashtable[prt][h][ox]);
                            temp[res_ntuples][ox] = hashtable[prt][h][ox];
                        }

                        for (int ix = 0; ix < tmeta2->nattrs; ix++) {
                            printf("%i ", pageBuffer[bid]->tuple[y][ix]);
                            temp[res_ntuples][ox+ix] = pageBuffer[bid]->tuple[y][ix];
                        }

                        printf("\n");
                        res_ntuples++;

                    }

                }

            }

            // release page
            releasePage(bid);

        }

        printf("\n... finished computing result tuples ...\n");

    }

    printf("\nres_ntuples: %i\n", res_ntuples);

    // compose result table
    _Table* result = malloc(sizeof(_Table) + res_ntuples * sizeof(Tuple));
    result->nattrs = tmeta1->nattrs + tmeta2->nattrs;
    result->ntuples = res_ntuples;

    for (int i = 0; i < result->ntuples; i++) {
        Tuple t = malloc(sizeof(INT) * result->nattrs);
        result->tuples[i] = t;
        for (int j = 0; j < result->nattrs; j++) {
            t[j] = temp[i][j];
        }
    }

    printf("\n====================\n");
    printPageBuffer();
    printf("====================\n");
    printFileBuffer();
    printf("====================\n");

    return result;

}


void printPageBuffer() {
    printf("PAGE BUFFER\n\n");
    for (int q = 0; q < conf->buf_slots; q++) {
        printf("#%i\n", q);
        printf("isempty: %u\n", pageBuffer[q]->isempty);
        if (pageBuffer[q]->isempty) {
            continue;
        }
        printf("pageid:  %u\n", pageBuffer[q]->pageid);
        printf("ipid:    %u\n", pageBuffer[q]->ipid);
        printf("oid:     %u\n", pageBuffer[q]->oid);
        printf("name:    %s\n", pageBuffer[q]->name);
        printf("nattrs:  %u\n", pageBuffer[q]->nattrs);
        printf("ntuples: %u\n", pageBuffer[q]->ntuples);
        printf("pin:     %u\n", pageBuffer[q]->pin);
        printf("use:     %u\n", pageBuffer[q]->use);
        for (int j = 0; j < pageBuffer[q]->ntuples; j++) {
            for (int k = 0; k < pageBuffer[q]->nattrs; k++) {
                printf("%i ", pageBuffer[q]->tuple[j][k]);
            }
        }
        printf("\n\n");
    }
}


void printFileBuffer() {
    printf("FILE BUFFER\n\n");
    for (int q = 0; q < conf->file_limit; q++) {
        printf("#%i\n", q);
        printf("isempty: %u\n", fileBuffer[q]->isempty);
        if (fileBuffer[q]->isempty) {
            continue;
        }
        printf("oid:     %u\n", fileBuffer[q]->oid);
        printf("name:    %s\n", fileBuffer[q]->name);
        printf("nattrs:  %u\n", fileBuffer[q]->nattrs);
        printf("ntuples: %u\n", fileBuffer[q]->ntuples);
        printf("npages:  %u\n", fileBuffer[q]->npages);
        printf("path:    %s\n\n", fileBuffer[q]->path);
    }
}