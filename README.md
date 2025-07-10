# object-store-connector

## Testing locally

For testing locally, run the following command to download the dependencies

```
mvn dependency:copy-dependencies -DrepoUrl=http://repo1.maven.org/maven2/ -DexcludeTrans -DoutputDirectory=libs
```

then run

```
pytest
```

## Building the connector

Run the following command to build the connector package

```
poetry run package
```