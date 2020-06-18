# Insight-Data-Engineering-Project
The project businssdfsldfkdmf
ksdnfsndf

# System Design Diagram
<img src="https://github.com/AddyZhang/Insight-Data-Engineering-Project/blob/master/myimage/system_design_1.png">

# Workflow
1. Produce data on Lambda `ingestion/prdocuer.py`
2. Ingest data on Kinesis Data Stream (kds)
3. Check if there is missing attribute or extra attributes on Lambda (data-processing/kds_error_handle.py)
4. Process streaming data (data-processing/streaming_query.sql) on Kinesis Data Analytics (kda)
5. Push data from kds to elasticsearch for storage (database-scripts)
6. Visulize on Kibana (no code needed)

