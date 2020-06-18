# Insight-Data-Engineering-Project

# System Design Diagram
<img src="https://github.com/AddyZhang/Insight-Data-Engineering-Project/blob/master/myimage/system_design_1.png">

# Workflow
1. Produce data (ingestion/prdocuer.py) on Lambda 
2. Ingest data on Kinesis Data Stream (kds)
3. Check if there is/are attribute missing or extra attributes (data-processing/kds_error_handle.py) on Lambda
4. Process streaming data (data-processing/streaming_query.sql) on Kinesis Data Analytics (kda)
5. Push data from kds to elasticsearch for storage (database-scripts)
6. Visulize on Kibana (no code needed)

