
typedef struct Row Row;
typedef struct Cell Cell;
typedef struct Table Table;
typedef struct Db Db;
struct Cell
{
	char* data;
	int ver;
	Cell* next_ver;
	Cell* pev_ver;
	Cell* latest;
	Cell* next_col;
};

struct Row
{
	int id;
	Cell* cell;
	int curr, comm;
	Row* next_row;

};
struct Table
{
	int no_rows, no_cols;
	char* table_name;
	Row* row;
	char** col_names;
};

struct Db
{
	int no_tables;
	Table** table;
};
