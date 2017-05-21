# MSSQL2PG

**PRE ALPHA, DON'T USE**

### Requirements:

- mustang-shared (https://github.com/mjhagen/mustang-shared)
- Commandbox
- Framework One
- Testbox

### Configuration:

Set the path to mustang-shared in `Application.cfc`.

Add a json file containing two keys for your source (mssql) and destination (postgres) datasources to the `/config` dir.

Example:

```
{
  "source": {
    "class": "net.sourceforge.jtds.jdbc.Driver",
    "connectionString": "jdbc:jtds:sqlserver://example-mssql-server.com.:1433/mySourceDatabase",
    "username": "sa",
    "password": "encrypted:{your-encrypted-password-here}"
  },
  "destination": {
    "class": "org.postgresql.Driver",
    "connectionString": "jdbc:postgresql://example-postgres-server.com:5432/myDestinationDatabase",
    "username": "postgres",
    "password": "encrypted:{your-encrypted-password-here}"
  }
}
```

### Installation:

`cd` into the `/webroot` dir, run `box install`, then `box start`.

### Usage:

Click the "Go" button. Wait.