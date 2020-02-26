# Gentics Mesh Cluster Chaos Test

This repository contains code to run the Gentics Mesh chaos tests.

## Chaos Actions

The chaos test will randomly invoke various actions on the cluster until the actions limit has been reached.

Implemented actions:

* Add instance
* Remove instance
* Disconnect instance
* Reconnect instance
* Kill instance (-9)
* Utilize instance (Write)
* Invoke schema migration
* Split Brain
* Merge Brain

