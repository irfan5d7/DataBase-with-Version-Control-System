#include"Header.h"


Table* create_table(char*query);
int str_cmp(char *s1, char *s2);
Table* put(Table* table, int id, int cols, int* cols_indx, char** vals, int ver);
int colm_indx(Table* table, char* col_name, int id);
Row* get(Table*table, int id);
Table* delet(Table* table, int id);
void display(Table* table, Row* g);
void flushto(Db* db);
void meta_data_write(Db* db);
Db* read_metadata();
char* get_token(char *query, int* i);
char* file_name_cre(char* table, char* col);
char** col_load(Table* table, char* file_name);
Db* load();
Table* add(Table* table, char** rows);

int colm_indx(Table* table, char* col_name, int id)
{
	int i = 0;
	for (i = 0; i < table->no_cols; i++)
	{
		if (str_cmp(table->col_names[i], col_name))
			return i;
	}
	return -1;
}


int str_cmp(char *s1, char *s2)
{
	int i = 0;
	while (s1[i] != '\0' && s2[i] != '\0')
	{
		if (s1[i] != s2[i])
			return 0;
		i++;
	}
	if ((s1[i] == '\0' && s2[i] == '\0') || ((s1[i] == ' ' && s2[i] == ' ')))
		return 1;
	else
		return 0;
}



int main()
{

	int c, id, i;
	Row* g_row;
	Db* db = (Db*)malloc(sizeof(Db));
	db->table = (Table**)malloc(sizeof(Table*));
	db->no_tables = 0;
	char query[128];
	while (1)
	{
		printf("1.Put\n2.Get\n3.Delete\n4.Create Tables\n5.Flush\n6.Write Meta Data\n7.Read Meta Data\n0.Exit\n\n");
		scanf("%d", &c);
		if (c == 0)
			break;
		else if (c == 1)
		{
			int no_cols, id, ver, ret, t;
			char name[16];
			char table_name[16];
			printf("Enter Table Name: ");
			scanf("%s", table_name);
			for (t = 0; t < db->no_tables; t++)
			{
				if (str_cmp(db->table[t]->table_name, table_name))
					break;
			}
			if (t >= db->no_tables)
			{
				printf("\nTable not found");
			}
			else
			{
				printf("\nEnter row_id: ");
				scanf("%d", &id);
				printf("\nEnter version : ");
				scanf("%d", &ver);
				printf("\nNo.of Columns to Put : ");
				scanf("%d", &no_cols);
				int* cols_indx = (int *)malloc(sizeof(int)*db->table[t]->no_cols);
				for (i = 0; i < db->table[t]->no_cols; i++)
				{
					cols_indx[i] = 0;
				}
				char** vals = (char**)malloc(sizeof(char*)* db->table[t]->no_cols);
				for (int i = 0; i < db->table[t]->no_cols; i++)
				{
					vals[i] = (char*)malloc(sizeof(char) * 16);
				}

				for (int i = 0; i < no_cols; i++)
				{
					printf("\nEnter a  Column name: ");
					scanf("%s", name);
					ret = colm_indx(db->table[t], name, id);
					cols_indx[ret] = 1;
					printf("\nEnter  value for %s : ", name);
					scanf("%s", vals[ret]);
				}
				db->table[t] = put(db->table[t], id, db->table[t]->no_cols, cols_indx, vals, ver);
			}

		}
		else if (c == 2)
		{

			char table_name[16];
			printf("Enter Table Name: ");
			scanf("%s", table_name);
			for (i = 0; i < db->no_tables; i++)
			{
				if (str_cmp(db->table[i]->table_name, table_name))
					break;
			}
			if (i == db->no_tables)
			{
				printf("Table not found");
			}
			else
			{
				printf("\nEnter row_id: ");
				scanf("%d", &id);
				g_row = get(db->table[i], id);
				display(db->table[i], g_row);
			}

		}
		else if (c == 3)
		{
			char table_name[16];
			printf("Enter Table Name: ");
			scanf("%s", table_name);
			for (i = 0; i < db->no_tables; i++)
			{
				if (str_cmp(db->table[i]->table_name, table_name))
					break;
			}
			if (i == db->no_tables)
			{
				printf("Table not found");
			}
			else
			{
				printf("\nEnter row_id: ");
				scanf("%d", &id);
				db->table[i] = delet(db->table[i], id);
			}

		}
		else if (c == 4)
		{
			printf("Enter Create query > \n");
			scanf("%s", query);
			db->table[db->no_tables++] = create_table(query);
		}
		else if (c == 5)
		{
			flushto(db);
		}
		else if (c == 6)
		{
			meta_data_write(db);
		}
		else if (c == 7)
		{
			db = load();
		}
	}
}


void display(Table* table, Row* g)
{
	int cols, i;
	cols = table->no_cols;
	Cell* c = g->cell;
	for (i = 0; i < cols; i++)
	{
		printf("%15s", table->col_names[i]);
	}
	for (i = 0; i < cols; i++)
	{
		printf("%15s", c->data);
		c = c->next_col;
	}
}











