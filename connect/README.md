## Configs

In the`etc/worker.properties`, `etc/github-events-connector.properties` needs to change `plugin.path` to locate the connector jar file.

## Build
`./gradlew jar`

## Standalone
`<kafka path>bin/connect-standalone.sh <repo path>connect/etc/worker.properties  <repo path>/connect/etc/github-events-connector.properties`

## Distributed
`<kafka path>bin/connect-distributed.sh <project path>/etc/github-events-distributed.properties`

Submit the connector config:
```
curl -i -X POST \
-H "Content-Type: application/json" \
-d '{"name": "dist-connect-github-events-connector","config": {"connector.class":"com.kugmax.learn.github.events.connect.sink.FileSinkConnector","tasks.max": 3,"topics": "github-events-1","github.events.file.name": "dist-github-events-sink-connector.txt"}}' \
http://localhost:8083/connectors 
```