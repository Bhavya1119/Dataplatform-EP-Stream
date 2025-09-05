# DP-EP-Stream

1. Streaming application that consumes data from post purifier kafka topic -> check schema from schema registry -> write data in delta format at intervals of 2 minutes.
2. Databricks API Create utility -> creates delta tables in uc external location