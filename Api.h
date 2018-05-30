#include "Structure.h"

char* get_token(char *query, int* i)
{
	int j = 0;
	char* tmp = (char*)malloc(sizeof(char) * 32);
	while (query[*i] != ',' && query[*i] != ' ' && query[*i] != '\0'  && query[*i] != '\n')
	{
		tmp[j++] = query[(*i)++];
	}
	tmp[j] = '\0';
	return tmp;
}

void meta_data_write(Db* db)
{
	int i, j;
	FILE*fp = fopen("metadata.txt", "w");
	fprintf(fp, "%d\n", db->no_tables);
	for (i = 0; i < db->no_tables; i++)
	{
		fprintf(fp, "%s,", db->table[i]->table_name);
		fprintf(fp, "%d,", db->table[i]->no_cols);
		for (j = 0; j < db->table[i]->no_cols - 1; j++)
		{
			fprintf(fp, "%s,", db->table[i]->col_names[j]);
		}
		fprintf(fp, "%s\n", db->table[i]->col_names[j]);
		fprintf(fp, "%d\n", db->table[i]->no_rows);
	}
	//fprintf(fp, "\0");
	fclose(fp);
}

char** col_load(Table* table, char* file_name)
{
	int off = 3;
	int i, j, k, val;
	char c;
	char* str = (char*)malloc(sizeof(char) * 128);
	char** row = (char**)malloc(sizeof(char*) * table->no_rows);
	char* tmp = (char*)malloc(sizeof(char) * 128);
	for (i = 0; i < table->no_rows; i++)
	{
		row[i] = (char*)malloc(sizeof(char) * 128);
	}
	FILE* fp = fopen(file_name, "r");
	int latest_row = 1;
	while (1 && latest_row != 0)
	{
		fseek(fp, -off, SEEK_END);
		off++;
		c = fgetc(fp);
		while (c != '\n')
		{
			fseek(fp, -off, SEEK_END);
			off++;
			c = fgetc(fp);
		}
		fgets(str, 128, fp);
		i = 0;
		tmp = get_token(str, &i);
		val = atoi(tmp);
		strcpy(row[val - 1], str);
		latest_row = val - 1;
		off++;
	}
	return row;
}

char* file_name_cre(char* table, char* col)
{
	strcat(table, "_");
	strcat(table, col);
	strcat(table, ".txt");
	return table;
}
Table* put(Table* table, int id, int cols, int* cols_indx, char** vals, int ver)
{
	int i = 0;
	if (table->no_rows == 0)
	{
		Cell* cell = (Cell*)malloc(sizeof(Cell));
		cell->data = (char*)malloc(sizeof(char) * 16);
		Cell* cellhead;
		Cell* temp;
		cellhead = temp = cell;
		strcpy(cell->data, vals[i]);
		cell->next_col = NULL;
		cell->next_ver = NULL;
		cell->pev_ver = NULL;
		cell->latest = NULL;
		cell->ver = 1;
		i++;
		while (i < cols)
		{
			Cell* c = (Cell*)malloc(sizeof(Cell));
			c->data = (char*)malloc(sizeof(char) * 16);
			if (cols_indx[i] == 0)
				strcpy(c->data, "NaN");
			else
				strcpy(c->data, vals[i]);
			c->next_col = NULL;
			c->next_ver = NULL;
			c->pev_ver = NULL;
			c->ver = 1;
			c->latest = NULL;
			while (temp->next_col != NULL)
				temp = temp->next_col;
			temp->next_col = c;
			i++;
		}
		Row* head;
		head = table->row;
		if (head != NULL)
		{
			while (head->next_row != NULL)
				head = head->next_row;
			Row* r = (Row*)malloc(sizeof(Row));
			r->cell = cellhead;
			r->id = id;
			r->comm = table->row->curr = 1;
			r->next_row = NULL;
			head->next_row = r;
		}
		else
		{
			table->row = (Row*)malloc(sizeof(Row));
			table->row->cell = cellhead;
			table->row->cell->ver = 1;
			table->row->id = id;
			table->row->comm = table->row->curr = 1;
			table->row->next_row = NULL;
		}

		table->no_rows++;
		return table;
	}


	else
	{
		Row* tmp = (Row*)malloc(sizeof(Row));
		tmp = table->row;
		while (tmp->next_row != NULL && tmp->id != id)
			tmp = tmp->next_row;
		if (tmp->id == id)
		{
			if (tmp->comm != ver && ver != -1)
				return table;
			int j = 0;
			i = 0;
			while (i < cols)
			{
				Cell* cell = (Cell*)malloc(sizeof(Cell));
				cell->data = (char*)malloc(sizeof(char) * 16);
				Cell* temp;
				if (cols_indx[i] == 0)
					strcpy(cell->data, "NaN");
				else
					strcpy(cell->data, vals[i]);
				cell->ver = tmp->comm + 1;
				cell->next_col = NULL;
				cell->next_ver = NULL;
				temp = tmp->cell;
				Cell* prev = NULL;
				j = 0;
				while (j < i)
				{
					prev = temp;
					temp = temp->next_col;
					j++;
				}
				if (prev == NULL)
				{
					cell->next_col = tmp->cell->next_col;
					cell->pev_ver = tmp->cell;
					tmp->cell->next_ver = cell;
					tmp->cell->next_col = NULL;
					if (tmp->cell->latest == NULL)
						cell->latest = tmp->cell;
					else
						cell->latest = tmp->cell->latest;
					tmp->cell = cell;
				}
				else
				{
					prev->next_col = cell;
					cell->pev_ver = temp;
					temp->next_ver = cell;
					cell->next_col = temp->next_col;
					if (temp->latest == NULL)
						cell->latest = temp;
					else
						cell->latest = temp->latest;
					temp->next_col = NULL;
				}

				i++;

			}
			tmp->comm++;
			tmp->curr++;
			return table;
		}
		else
		{
			Cell* cell = (Cell*)malloc(sizeof(Cell));
			cell->data = (char*)malloc(sizeof(char) * 16);
			Cell* cellhead;
			Cell* temp;
			cellhead = temp = cell;
			strcpy(cell->data, vals[i]);
			cell->next_col = NULL;
			cell->next_ver = NULL;
			cell->pev_ver = NULL;
			cell->latest = NULL;
			cell->ver = 1;
			i++;
			while (i < cols)
			{
				Cell* c = (Cell*)malloc(sizeof(Cell));
				c->data = (char*)malloc(sizeof(char) * 16);
				if (cols_indx[i] == 0)
					strcpy(c->data, "NULL");
				else
					strcpy(c->data, vals[i]);
				c->next_col = NULL;
				c->next_ver = NULL;
				c->pev_ver = NULL;
				c->ver = 1;
				c->latest = NULL;
				while (temp->next_col != NULL)
					temp = temp->next_col;
				temp->next_col = c;
				i++;
			}
			Row* row = (Row*)malloc(sizeof(Row));
			row->id = id;
			row->next_row = NULL;
			row->comm = row->curr = 1;
			row->cell = cellhead;
			tmp->next_row = row;
		}
		table->no_rows++;
		return table;

	}

}

Row* get(Table*table, int id)
{
	int i;
	Row* tmp = (Row*)malloc(sizeof(Row));
	tmp = table->row;
	while (tmp->next_row != NULL && tmp->id != id)
		tmp = tmp->next_row;
	if (tmp->id != id)
		return NULL;
	else
		return tmp;

}

Table* delet(Table* table, int id)
{
	Row* r = table->row;
	Row* prev = NULL;
	while (r->id != id && r->next_row != NULL)
	{
		prev = r;
		r = r->next_row;
	}
	if (r->id == id)
	{
		if (prev == NULL)
		{
			table->row = table->row->next_row;
		}
		else
		{
			prev->next_row = r->next_row;
			free(r);
		}
		table->no_rows--;
	}
	return table;

}

Table* create_table(char* query)
{
	int i, j, val;
	i = j = 0;
	char *tmp = (char*)malloc(sizeof(char) * 64);
	Table* table = (Table*)malloc(sizeof(Table));
	table->no_rows = 0;
	table->table_name = (char*)malloc(sizeof(char) * 32);
	tmp = get_token(query, &i);
	strcpy(table->table_name, tmp);
	i++;
	tmp = get_token(query, &i);
	i++;
	val = atoi(tmp);
	table->col_names = (char**)malloc(sizeof(char*)*val);
	table->no_cols = val;
	for (j = 0; j < val; j++)
	{
		table->col_names[j] = (char*)malloc(sizeof(char) * 32);
		tmp = get_token(query, &i);
		i++;
		strcpy(table->col_names[j], tmp);
	}
	table->no_rows = 0;
	table->row = (Row*)malloc(sizeof(Row));
	table->row = NULL;
	return table;
}

void flushto(Db* db)
{
	int i, j;
	char* file_name = (char*)malloc(sizeof(char) * 32);
	char* table_name = (char*)malloc(sizeof(char) * 32);
	char* col_name = (char*)malloc(sizeof(char) * 32);
	FILE*fp;
	for (i = 0; i < db->no_tables; i++)
	{
		Row* r = db->table[i]->row;
		int rc = 0;
		while (rc <db->table[i]->no_rows)
		{
			Cell* c_last = r->cell;
			Cell* c_temp;
			if (r->cell->latest != NULL)
				c_temp = r->cell->latest;
			else
				c_temp = r->cell;
			for (j = 0; j < db->table[i]->no_cols; j++)
			{
				strcpy(table_name, db->table[i]->table_name);
				strcpy(col_name, db->table[i]->col_names[j]);
				file_name = file_name_cre(table_name, col_name);
				fp = fopen(file_name, "a");
				while (c_temp != c_last)
				{
					fprintf(fp, "%d,%d,%s\n", r->id, c_temp->ver, c_temp->data);
					c_temp = c_temp->next_ver;
				}
				fprintf(fp, "%d,%d,%s\n", r->id, c_temp->ver, c_temp->data);
				if (c_temp->next_col != NULL)
				{
					c_last = c_last->next_col;
					if (c_temp->next_col->latest != NULL)
						c_temp = c_temp->next_col->latest;
					else
						c_temp = c_temp->next_col;
				}
				fclose(fp);
			}
			r = r->next_row;
			rc++;
		}
	}

	meta_data_write(db);
}

Table* add(Table* table, char** rows)
{
	int j, val;
	char* tok = (char*)malloc(sizeof(char) * 32);
	if (table->row == NULL)
	{

		Row* rhead = (Row*)malloc(sizeof(Row));
		rhead = NULL;
		Row* temp;
		for (int i = 0; i < table->no_rows; i++)
		{

			j = 0;
			tok = get_token(rows[i], &j);
			val = atoi(tok);
			Row* r = (Row*)malloc(sizeof(Row));
			if (rhead == NULL)
				rhead = r;
			else
			{
				temp = rhead;
				while (temp->next_row != NULL)
					temp = temp->next_row;
				temp->next_row = r;
			}
			r->id = val;
			j++;
			tok = get_token(rows[i], &j);
			j++;
			val = atoi(tok);
			Cell* c = (Cell*)malloc(sizeof(Cell));
			c->data = (char*)malloc(sizeof(char) * 32);
			c->ver = val;
			c->next_col = NULL;
			c->next_ver = NULL;
			c->pev_ver = NULL;
			tok = get_token(rows[i], &j);
			strcpy(c->data, tok);
			r->cell = c;
			r->next_row = NULL;
		}
		table->row = rhead;
	}
	else
	{
		Row* r = table->row;
		Cell* c;
		for (int i = 0; i < table->no_rows; i++)
		{
			c = r->cell;
			while (c->next_col != NULL)
				c = c->next_col;
			Cell* cell = (Cell*)malloc(sizeof(Cell));
			c->next_col = cell;
			cell->data = (char*)malloc(sizeof(char) * 32);
			j = 0;
			tok = get_token(rows[i], &j);
			j++;
			tok = get_token(rows[i], &j);
			val = atoi(tok);
			cell->ver = val;
			j++;
			tok = get_token(rows[i], &j);;
			strcpy(cell->data, tok);
			c->next_col = NULL;
			c->next_ver = NULL;
			c->pev_ver = NULL;
			r = r->next_row;
		}
	}
	return table;
}

Db* read_metadata()
{
	int val, i, j, k;
	i = 0;
	char *tmp = (char*)malloc(sizeof(char) * 128);
	FILE* fp = fopen("metadata.txt", "r");
	fgets(tmp, 128, fp);
	val = atoi(tmp);
	Db* db = (Db*)malloc(sizeof(Db));
	db->no_tables = val;
	db->table = (Table**)malloc(sizeof(Table*)*val);
	for (j = 0; j < db->no_tables; j++)
	{
		fgets(tmp, 128, fp);
		db->table[j] = create_table(tmp);
		fgets(tmp, 128, fp);
		val = atoi(tmp);
		db->table[j]->no_rows = val;
	}
	return db;
}

Db* load()
{
	int i, j;
	char* file_name = (char*)malloc(sizeof(char) * 32);
	char* table_name = (char*)malloc(sizeof(char) * 32);
	char* col_name = (char*)malloc(sizeof(char) * 32);
	char **rows;
	Db* db = read_metadata();
	for (i = 0; i < db->no_tables; i++)
	{

		Table *table = db->table[i];
		for (j = 0; j < table->no_cols; j++)
		{

			strcpy(table_name, db->table[i]->table_name);
			strcpy(col_name, db->table[i]->col_names[j]);
			file_name = file_name_cre(table_name, col_name);
			rows = col_load(table, file_name);
			db->table[i] = add(db->table[i], rows);
		}

	}
	return db;
}


