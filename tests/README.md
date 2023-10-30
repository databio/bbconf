# How to setup the test environment

### To create a test database for testing :

```
docker run --rm -it --name pipestat-test \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=dockerpassword \
  -e POSTGRES_DB=pipestat-test \
  -p 5432:5432 postgres
```