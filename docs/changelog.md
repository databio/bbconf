# Changelog

This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html) and [Keep a Changelog](https://keepachangelog.com/en/1.0.0/) format.

## [0.1.0] - 2020-0X-XX
### Changed 
- `BedBaseConf` backend (database) to [PostgreSQL](https://www.postgresql.org/) 

## [0.0.2] - 2020-05-28
### Added
- index deleting methods:
	- `delete_bedsets_index`
	- `delete_bedfiles_index`
- multiple new keys constants

### Changed 
- make `search_bedfiles` and `search_bedsets` methods return all hits by default instead of just 10. Parametrize it. 
- added more arguments to `insert_bedfiles_data` and `insert_bedsets_data` method interfaces: `doc_id` and `force_update`
- Elasticsearch documents are inserted into the indices more securily, `insert_*` methods prevent documents duplication


## [0.0.1] - 2020-02-05
### Added 
- initial project release