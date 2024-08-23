# Simple DBMS

## Overview

This project implements a simplified yet functional model of a DBMS, focusing on core mechanics such as memory management, file handling, data structures, join algorithms, and query processing / optimisation. It supports two primary relational operators: equi-select and inner equi-join. Logging functions are included to track file and page operations for performance analysis.

## Assumptions

- The server hosts only one database.
- The database is read-only with no updates.
- A maximum number of open files and buffer slots is specified at runtime.
- All attributes are fixed-size integers (fixed tuple size).
- Each table is stored in a single file with no overflow files.
- The select condition is always an equality test on a single attribute.
- The inner join condition is always an equality test between attributes from two tables.
- Clock-sweep strategy is used as the buffer replacement policy. 
- Additional space outside buffer slots can be used for sorting tables.
- PageIDs may not be consecutive but are unique within each table.

## File Structure

```
|--- README.md // log for recent updates
|--- db.c // global database information
|--- db.h // definitions for all data types
|--- main.c // main entry
|--- run.sh // script to run the code
|--- ro.c // relational operators
|--- ro.h // definitions for ro.c
|--- Makefile // compile rules
|--- test1/ // directory containing testing files
     |--- data_1.txt // testing data
     |--- query_1.txt // testing queries
     |--- expected_log_1.txt // expected results
```

## Usage

To compile the program, run `make` or use:
```shell
gcc -c db.c
gcc -c ro.c
gcc -c main.c
gcc -o main main.o ro.o db.o
```

To run the command, use:
```shell
./main page_size buffer_slots max_opened_files buffer_replacement_policy database_folder input_data queries output_log
```

Where:

- `page_size` is the size of each page.
- `buffer_slots` is the number of memory buffer slots.
- `max_opened_files` is the maximum allowed number of open files.
- `buffer_replacement_policy` is always CLS for this project.
- `database_folder` is the folder for storing database files.
- `input_data` is a .txt file containing data schemas (see Schema Language).
- `queries` is a .txt file containing queries (see Query Language).
- `output_log` is a .txt file for tracing outputs.

To run sample queries, use:
```shell
./main 50 3 3 CLS ./data ./test/test1/data_1.txt ./test/test1/query_1.txt ./test/test1/log_1.txt
```
Refer to the test files to understand the layout of data and query text files.

To run the test script, use:
```shell
./autotest
```

## Schema Language

### Database Schema

Syntax:
```
database_meta number_of_tables
```

Example:
```
database_meta 2
```

This creates a database with 2 tables.

### Table Schema

Syntax:
```
table_meta table_oid table_name number_of_attributes

data_row_1
data_row_2
...
```

Example:
```
table_meta 100 t1 4 

1 5 23 54
1 2 3 4
5 78 8 2
```

This creates a table with objectID `100`, named `t1`, with 4 attribute columns. It then loads 3 rows of data into the table. Attributes are separated by spaces.


## Query Language

`nattrs` = number of attributes
`attribute_index` ranges from 0 to `nattrs-1`

### Equi-Select

Syntax:
```
sel attribute_index compared_value operator table_name
```

Example:
```
sel 0 1314 = t1
```

Equivalent SQL:
```sql
SELECT * FROM t1 WHERE column0 = 1314;
```

### Inner Equil-Join

Syntax:
```
join table1_attribute_index table1_name table2_attribute_index table2_name
```

Example:
```
join 3 t1 1 t2
```

Equivalent SQL:
```sql
SELECT * FROM t1 INNER JOIN t2 ON t1.column3 = t2.column1;
```

## Storage Details

Each table file is named after its objectID, similar to PostgreSQL. For each page, an INT64 is added to represent the pageID, followed by tuples until the space is insufficient to hold another tuple. Each tuple contains a sequence of INT32 integers as attribute values. Unused space in a page is filled with trailing 0s.

```
 INT64 | Tuple 1 | Tuple 2 | 0 ... || INT64 | Tuple 1 | Tuple 2 | 0 ...
       | INT32, INT32, ... |
|<-------------Page 1------------->||<-------------Page 2------------->|
|<----------------------------Table File------------------------------>|
```

## Query Processing Lifecycle

1. **Initialisation**:
   - Initialise the `fileBuffer` and `pageBuffer`.
   - Define `fileDesc` and `pageDesc` to represent files and pages.
   - Gather and compute extended metadata for all tables.

2. **Query Invocation**:
   - Invoke a query and iteratively request pages from the involved tables.

3. **Data Reading**:
   - If the page is not in `pageBuffer`, read it from the `fileBuffer`.
   - If the file is not in `fileBuffer`, read it from disk.
     - If the buffer is full, evict the least recently used file.
     - Read the required table file and store it in `fileBuffer` as `fileDesc`.
   - If the file is in `fileBuffer`, check for available slots in `pageBuffer`.
     - If the buffer is full, apply the clock-sweep replacement policy to evict pages that are no longer needed.
     - Read the page from the file and save it in `pageBuffer` as `pageDesc`.
   - If the page is in `pageBuffer`, pin it.
   - Repeat this process for every page request.

4. **Query Execution**:
   - For selection queries (`sel()`):
     - Iterate through the pages of the target table.
     - Perform equality comparisons on the specified attribute to filter results based on the query conditions.
   - For join queries (`join()`):
     - Determine whether to use Block Nested Loop Join or Simple Hash Join based on the sizes of the tables and available buffer slots.
     - For Block Nested Loop Join:
       - Calculate the cost of two plans: one with Table1 as the outer table and the other with Table2 as the outer table.
       - Choose the plan with the lower cost.
       - Read chunks of the outer table into the buffer and compare each chunk with the pages of the inner table to find matching records.
     - For Simple Hash Join:
       - Create a hash table for the outer table (Table1).
       - Partition the outer table based on the hash of the join attribute (e.g., even/odd).
       - Scan the inner table (Table2) and compare its records with the corresponding partitions in the hash table to find matches.

5. **Result Processing**:
   - Store the progressive results from each iterative step of query execution in a temporary array.
   - Unpin and release processed pages from the buffer for each page request iteration.
   - Populate the final result structure for output.

6. **Finalisation**:
   - Free any allocated memory and close all open files.
