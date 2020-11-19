# JSONloader

Toolset to migrate from MongoDB to Oracle Autonomous JSON database

# Tools

## How to drop all existing SODA collections?

```
set serveroutput on size unlimited
DECLARE
    coll_list  SODA_COLLNAME_LIST_T;
    status  NUMBER := 0;
BEGIN
    coll_list := DBMS_SODA.list_collection_names;
    DBMS_OUTPUT.put_line('Number of collections: ' || to_char(coll_list.count));
    IF (coll_list.count > 0) THEN
        -- Loop over the collection name list
        FOR i IN
            coll_list.first .. coll_list.last
        LOOP
            status := DBMS_SODA.drop_collection(coll_list(i));
        END LOOP;  
    ELSE   
        DBMS_OUTPUT.put_line('No collections found');
    END IF;
END;
/
```

## How to query existing documents from a collection?

```
select id as SODA_ID, c.json_document."_id" as MONGODB_ID, 
       json_query(json_document,'$' returning CLOB pretty) as JSON_DOCUMENT 
  from mycollection c;
```

