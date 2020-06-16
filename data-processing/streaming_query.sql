-- ** Continuous Filter ** 
-- Performs a continuous filter based on a WHERE condition.
--          .----------.   .----------.   .----------.              
--          |  SOURCE  |   |  INSERT  |   |  DESTIN. |              
-- Source-->|  STREAM  |-->| & SELECT |-->|  STREAM  |-->Destination
--          |          |   |  (PUMP)  |   |          |              
--          '----------'   '----------'   '----------'               
-- STREAM (in-application): a continuously updated entity that you can SELECT from and INSERT into like a TABLE
-- PUMP: an entity used to continuously 'SELECT ... FROM' a source STREAM, and INSERT SQL results into an output STREAM
-- Create output stream, which can be used to send to a destination

-- Approximate top-K items - Finds the most frequently occurring values in a stream using the Space Saving algorithm.
-- Returns the approximate top-K most frequently
-- occurring values in a specified column over a
-- tumbling window
CREATE OR REPLACE STREAM "DESTINATION_DELIVERY_STREAM" (CURRENT_ROW_TIMESTAMP TIMESTAMP, ITEM VARCHAR(1024), ITEM_COUNT INT);

CREATE OR REPLACE STREAM "DESTINATION_DELIVERY_STREAM" (CURRENT_ROW_TIMESTAMP TIMESTAMP, ITEM VARCHAR(1024), ITEM_COUNT INT);

CREATE OR REPLACE STREAM "DESTINATION_DELIVERY_STREAM" (CURRENT_ROW_TIMESTAMP TIMESTAMP, ITEM VARCHAR(1024), ITEM_COUNT INT);

----------------------------------ELECTRONICS CATEGORY---------------------------------
CREATE OR REPLACE PUMP "test1_STREAM_PUMP" AS INSERT INTO "DESTINATION_DELIVERY_STREAM"
SELECT STREAM CURRENT_ROW_TIMESTAMP, ITEM, ITEM_COUNT FROM TABLE(TOP_K_ITEMS_TUMBLING(
  CURSOR(SELECT STREAM * FROM "SOURCE_SQL_STREAM_001" WHERE CATEGORY = 'ELECTRONICS'),
  'ITEM_ID', -- name of column in single quotes
  3, -- number of top items
  20 -- tumbling window size in seconds
  )
);

----------------------------------FURNITURE CATEGORY---------------------------------
CREATE OR REPLACE PUMP "test2_STREAM_PUMP" AS INSERT INTO "DESTINATION_DELIVERY_STREAM"
SELECT STREAM CURRENT_ROW_TIMESTAMP, ITEM, ITEM_COUNT FROM TABLE(TOP_K_ITEMS_TUMBLING(
  CURSOR(SELECT STREAM * FROM "SOURCE_SQL_STREAM_001" WHERE CATEGORY = 'FURNITURE'),
  'ITEM_ID', -- name of column in single quotes
  3, -- number of top items
  20 -- tumbling window size in seconds
  )
);


----------------------------------SPORTS CATEGORY-------------------------------------
CREATE OR REPLACE PUMP "test3_STREAM_PUMP" AS INSERT INTO "DESTINATION_DELIVERY_STREAM"
SELECT STREAM CURRENT_ROW_TIMESTAMP, ITEM, ITEM_COUNT FROM TABLE(TOP_K_ITEMS_TUMBLING(
  CURSOR(SELECT STREAM * FROM "SOURCE_SQL_STREAM_001" WHERE CATEGORY = 'SPORTS'),
  'ITEM_ID', -- name of column in single quotes
  3, -- number of top items
  20 -- tumbling window size in seconds
  )
);
