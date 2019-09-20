## Introduction
This driver can connect to multiple different databases and also supports connection routing using customizable script.
## Usage
While adding driver in SQLWorkbench, refer to tensult-multidb-jdbc-drvier-*.jar file inside the target folder 
and select "[com.tensult.jdbc.MultiDatabasesDriver](https://github.com/tensult/multidb-jdbc-driver/blob/master/src/main/java/com/tensult/jdbc/MultiDatabasesDriver.java)" as Driver class name. 

Set following values as extend properties for Redshift with ADFS authentication:
```
multi_databases_driver_config_path:	/pathTo/multi-clusters-redshift.json
idp_port:	443
plugin_name:	com.amazon.redshift.plugin.AdfsCredentialsProvider
idp_host:	<IDP hostname>
preferred_role:	<IAM-Role-ARN>
```
## [Samples](https://github.com/tensult/multidb-jdbc-driver/tree/master/src/main/java/com/tensult/samples)

## Generation of driver
To generate the jar use `maven install` command.
