# Insight-Data-Engineering-Project
`Situation`  Global e-commerce sales are expected to top $4.2 trillion USD in 2020, but 80% e-commerce businesses failed in the past. 

`Challenge`  E-commerce managements don’t know how to turn site visits into value. In plain language, they don’t know what customers want and when they want.

`Solution`   The management team will be able to monitor customer clicks  in real-time to understand what and when customers want. 

`Scope of Project`   Provide real-time dashboard showing trending items for each category in specific time  interval.

`Benefits for Management`   Instantly generate customer shopping patterns & recommend trending items to users


### System Design Diagram
<img src="https://github.com/AddyZhang/Insight-Data-Engineering-Project/blob/master/myimage/system_design_1.png">

### Workflow
1. Produce data on Lambda `ingestion/prdocuer.py`
2. Ingest data on Kinesis Data Stream (kds)
3. Check if there is missing attribute or extra attributes on Lambda `data-processing/kds_error_handle.py`
4. Process streaming data on Kinesis Data Analytics (kda) `data-processing/streaming_query.sql`
5. Push data from kds to elasticsearch for storage on Lambda `database-scripts/kds-to-es.py`
6. Visulize on Kibana (no code needed)

