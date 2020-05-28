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
A class that extends AttMap to provide yaml reading and race-free writing in multi-user contexts.

The YacAttMap class is a YAML Configuration Attribute Map. Think of it as a python representation of your YAML
configuration file, that can do a lot of cool stuff. You can access the hierarchical YAML attributes with dot
notation or dict notation. You can read and write YAML config files with easy functions. It also retains memory
of the its source filepath. If both a filepath and an entries dict are provided, it will first load the file
and then updated it with values from the dict.


```python
def __init__(self, filepath)
```

Create the config instance by with a filepath
#### Parameters:

- `filepath` (`str`):  a path to the YAML file to read




```python
def assert_connection(self)
```

Check whether an Elasticsearch connection has been established
#### Raises:

- `BedBaseConnectionError`:  if there is no active connection




```python
def count_bedfiles_docs(self)
```

Get the total number of the documents in the bedfiles index
#### Returns:

- `int`:  number of documents




```python
def count_bedsets_docs(self)
```

Get the total number of the documents in the bedsets index
#### Returns:

- `int`:  number of documents




```python
def delete_bedfiles_index(self)
```

Delete bedfiles index from Elasticsearch



```python
def delete_bedsets_index(self)
```

Delete bedsets index from Elasticsearch



```python
def establish_elasticsearch_connection(self, host=None)
```

Establish Elasticsearch connection using the config data
#### Returns:

- `elasticsearch.Elasticsearch`:  connected client




```python
def file_path(self)
```

Return the path to the config file or None if not set
#### Returns:

- `str | None`:  path to the file the object will would to




```python
def get_bedfiles_doc(self, doc_id)
```

Get a document from bedfiles index by its ID
#### Parameters:

- `doc_id` (`str`):  document ID to return


#### Returns:

- `Mapping`:  matched document




```python
def get_bedfiles_mapping(self, just_data=True, **kwargs)
```

Get mapping definitions for the bedfiles index
#### Returns:

- `dict`:  bedfiles mapping definitions




```python
def get_bedsets_doc(self, doc_id)
```

Get a document from bedsets index by its ID
#### Parameters:

- `doc_id` (`str`):  document ID to return


#### Returns:

- `Mapping`:  matched document




```python
def get_bedsets_mapping(self, just_data=True, **kwargs)
```

Get mapping definitions for the bedsets index
#### Returns:

- `dict`:  besets mapping definitions




```python
def insert_bedfiles_data(self, data, doc_id=None, **kwargs)
```

Insert data to the bedfile index a Elasticsearch DB or create it and the insert in case it does not exist.

Document ID argument is optional. If not provided, a random ID will
be assigned. If provided the document will be inserted only if no
documents with this ID are present in the DB. However, the document
overwriting can be forced if needed.
#### Parameters:

- `data` (`dict`):  data to insert
- `doc_id` (`str`):  unique identifier for the document, optional




```python
def insert_bedsets_data(self, data, doc_id=None, **kwargs)
```

Insert data to the bedset index in a Elasticsearch DB or create it and the insert in case it does not exist.

Document ID argument is optional. If not provided, a random ID will
be assigned.
If provided the document will be inserted only if no documents with
this ID are present in the DB.
However, the document overwriting can be forced if needed.
#### Parameters:

- `data` (`dict`):  data to insert
- `doc_id` (`str`):  unique identifier for the document, optional




```python
def search_bedfiles(self, query, just_data=True, **kwargs)
```

Search selected Elasticsearch bedset index with selected query
#### Parameters:

- `query` (`dict`):  query to search the DB against
- `just_data` (`bool`):  whether just the hits should be returned


#### Returns:

- `dict | Iterable[dict]`:  search results




```python
def search_bedsets(self, query, just_data=True, **kwargs)
```

Search selected Elasticsearch bedfiles index with selected query
#### Parameters:

- `query` (`dict`):  query to search the DB against
- `just_data` (`bool`):  whether just the hits should be returned


#### Returns:

- `dict | Iterable[dict]`:  search results




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







*Version Information: `bbconf` v0.0.2-dev, generated by `lucidoc` v0.4.3*
