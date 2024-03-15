# How to setup the test environment

### To create a test database for testing :

```
docker run --rm -it --name bedbase-test \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=docker\
  -e POSTGRES_DB=bedbase \
  -p 5432:5432 postgres
```