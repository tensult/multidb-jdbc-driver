==Introduction==
This driver can connect to multiple different databases and also supports connection routing using customizable script.
==Usage==
While adding driver in SQLWorkbench, refer to tensult-multidb-jdbc-drvier-*.jar file inside the target folder 
and select "com.tensult.jdbc.MultiDatabasesDriver" as Driver class name. 

Set following values as extend properties for Redshift with ADFS authentication
multi_databases_driver_config_path:	/pathTo/multi-clusters-redshift.json
idp_port:	443
plugin_name:	com.amazon.redshift.plugin.AdfsCredentialsProvider
idp_host:	<IDP hostname>
preferred_role:	<IAM-Role-ARN>

==Samples==

==Generation==
When you update the code, to regenerate the jar use `maven install` command.