Final targets: BedBaseConf, get_bedbase_cfg
<script>
document.addEventListener('DOMContentLoaded', (event) => {
  document.querySelectorAll('h3 code').forEach((block) => {
    hljs.highlightBlock(block);
  });
});
</script>

<style>
h3 .content { 
    padding-left: 22px;
    text-indent: -15px;
 }
h3 .hljs .content {
    padding-left: 20px;
    margin-left: 0px;
    text-indent: -15px;
    martin-bottom: 0px;
}
h4 .content, table .content, p .content, li .content { margin-left: 30px; }
h4 .content { 
    font-style: italic;
    font-size: 1em;
    margin-bottom: 0px;
}

</style>


# Package `bbconf` Documentation

## <a name="BedBaseConf"></a> Class `BedBaseConf`
This class provides is an in-memory representation of the configuration file for the *BEDBASE* project. Additionally it implements multiple convenience methods for interacting with the database backend, i.e. [PostgreSQL](https://www.postgresql.org/)


```python
def __init__(self, filepath)
```

Create the config instance with a filepath
#### Parameters:

- `filepath` (`str`):  a path to the YAML file to read




```python
def check_bedfiles_table_exists(self)
```

Check if the bedfiles table exists
#### Returns:

- `bool`:  whether the bedfiles table exists




```python
def check_bedset_bedfiles_table_exists(self)
```

Check if the bedset_bedfiles table exists
#### Returns:

- `bool`:  whether the bedset_bedfiles table exists




```python
def check_bedsets_table_exists(self)
```

Check if the bedsets table exists
#### Returns:

- `bool`:  whether the bedsets table exists




```python
def check_connection(self)
```

Check whether a PostgreSQL connection has been established
#### Returns:

- `bool`:  whether the connection has been established




```python
def close_postgres_connection(self)
```

Close connection and remove client bound



```python
def count_bedfiles(self)
```

Count rows in the bedfiles table
#### Returns:

- `int`:  number of rows in the bedfiles table




```python
def count_bedsets(self)
```

Count rows in the bedsets table
#### Returns:

- `int`:  number of rows in the bedsets table




```python
def create_bedfiles_table(self, columns)
```

Create a bedfiles table
#### Parameters:

- `columns` (`str | list[str]`):  columns definition list,for instance: ['name VARCHAR(50) NOT NULL']




```python
def create_bedset_bedfiles_table(self)
```

Create a bedsets table, id column is defined by default



```python
def create_bedsets_table(self, columns)
```

Create a bedsets table
#### Parameters:

- `columns` (`str | list[str]`):  columns definition list,for instance: ['name VARCHAR(50) NOT NULL']




```python
def db_cursor(self)
```

Establish connection and get a PostgreSQL database cursor, commit and close the connection afterwards
#### Returns:

- `DictCursor`:  Database cursor object




```python
def drop_bedfiles_table(self)
```

Remove bedfiles table from the database



```python
def drop_bedset_bedfiles_table(self)
```

Remove bedsets table from the database



```python
def drop_bedsets_table(self)
```

Remove bedsets table from the database



```python
def establish_postgres_connection(self, suppress=False)
```

Establish PostgreSQL connection using the config data
#### Parameters:

- `suppress` (`bool`):  whether to suppress any connection errors


#### Returns:

- `bool`:  whether the connection has been established successfully




```python
def file_path(self)
```

Return the path to the config file or None if not set
#### Returns:

- `str | None`:  path to the file the object will would to




```python
def insert_bedfile_data(self, values)
```


#### Parameters:

- `values` (`dict`):  a mapping of pairs of table column names andrespective values to bne inserted to the database


#### Returns:

- `int`:  id of the row just inserted




```python
def insert_bedset_bedfiles_data(self, values)
```


#### Parameters:

- `values` (`dict`):  a mapping of pairs of table column names andrespective values to bne inserted to the database




```python
def insert_bedset_data(self, values)
```


#### Parameters:

- `values` (`dict`):  a mapping of pairs of table column names andrespective values to bne inserted to the database


#### Returns:

- `int`:  id of the row just inserted




```python
def select(self, table_name, columns=None, condition=None)
```

Get all the contents from the selected table
#### Parameters:

- `table_name` (`str`):  name of the table to list contents for
- `columns` (`str | list[str]`):  columns to select
- `condition` (`str`):  condition to restrict the results with


#### Returns:

- `list[psycopg2.extras.DictRow]`:  all table contents




```python
def select_bedfiles_for_bedset(self, query, bedfile_col=None)
```

Select bedfiles that are part of a bedset that matches the query
#### Parameters:

- `query` (`str`):  bedsets table query to restrict the results with,for instance "name='bedset1'"
- `bedfile_col` (`list[str] | str`):  bedfile columns to include in theresult, if none specified all columns will be included


#### Returns:

- `list[psycopg2.extras.DictRow]`:  matched bedfiles table contents




```python
def writable(self)
```

Return writability flag or None if not set
#### Returns:

- `bool | None`:  whether the object is writable now




```python
def get_bedbase_cfg(cfg=None)
```

Determine path to the bedbase configuration file

The path can be either explicitly provided
or read from a $BEDBASE environment variable
#### Parameters:

- `cfg` (`str`):  path to the config file.Optional, the $BEDBASE config env var will be used if not provided


#### Returns:

- `str`:  configuration file path







*Version Information: `bbconf` v0.1.0, generated by `lucidoc` v0.4.3*
