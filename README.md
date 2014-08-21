# Vert.x Couchbase Persistor

This module allows you to perform CRUD operations in a Couchbase server instance in a JSON format.

####To be able to use this module you will need a Couchbase instance running on your network.

This is a multi-threaded worker module.

## Dependencies

This module requires a Couchbase server available on the network.

## Name

The module name is `couchbase-persistor`.

## Configuration

The couchbase-persistor module takes the following configuration:

    {
        "address": <address>,
		"init": <init>,
        "host": <host>,
        "port": <port>,
        "bucket_name": <bucket_name>,
        "bucket_password": <bucket_password>,
		"bucket_memory_size": <bucket_memory_size>,
		"views": <views>,
		"username": <username>,
        "password": <password>,
        "operation_timeout": <operation_timeout>,
        "view_timeout": <view_timeout>,
        "observer_timeout": <observer_timeout>,
		"view_conns_per_node": <view_conns_per_node>
    }
    
 For example:

    {
        "address": "couchbase.persistor",
        "host": "127.0.0.1",
        "port": 8091,
        "bucket_name": "vertx",
        "bucket_password": "vertx123",
        
        // the following settings should be specified just for couchbase bucket/views auto generation
        // when starting the module
        "init": true,
        "username": "john",
        "password": "doe",
        "bucket_memory_size": 1024,
        "views":
        [
        	{
        		"name": "vertx_view",
        		"designDoc": "vertx_doc",
        		"function": "function (doc, meta) {emit(meta.id, null);}",
        		"mode": "production", //optional, development is the default one
        		"reduce": "_count" //optional
        	}
        ]
    }
    
 Let's explain each field:

* `address` The main address for the module. Every module has a main address. Defaults to `vertx.couchbasepersistor`.
* `host` Host name or ip address of the Couchbase instance. Defaults to `localhost`.
* `port` Port at which the Couchbase instance is listening. Defaults to `8091`.
* `bucket_name` Name of the bucket in the Couchbase instance to use. Defaults to `default`.
* `bucket_password` The password of the bucket in the Couchbase instance for client authentication. Defaults to `empty`.
* `init` If enabled this will create/update bucket configuration besides views. Defaults to `false`.
* `username` If init param is enabled we should specify Couchbase instance username to perform cluster manager operations. Defaults to `empty`.
* `password` If init param is enabled we should specify Couchbase instance password to perform cluster manager operations. Defaults to `empty`.
* `bucket_memory_size` If init param is enabled we can specify bucket memory size that will be set when the bucket is created/updated. Defaults to `512MB`. 
* `views` If init param is enabled this option allows the module to autogenerate the specified views in the couchbase instance.

## Operations

The module supports the following operations

### Insert

Inserts a document in the database. Returns an error whether the document already exists.

To insert a document send a JSON message to the module main address:

    {
        "action": "insert", //mandatory
        "document": <json_document>, //mandatory
        "persistTo": <ZERO|ONE|TWO|THREE|FOUR>, //optional, default ZERO
        "replicatTo": <ZERO|ONE|TWO|THREE|FOUR>, //optional, default ZERO
        "expiration": <time> //optional, default 0 - non expire
    }

### Update

Updates a document in the database. Returns an error whether the document doesn't exists.

To update a document send a JSON message to the module main address:

    {
        "action": "update", //mandatory
        "document": <json_document>, //mandatory
        "persistTo": <ZERO|ONE|TWO|THREE|FOUR>, //optional, default ZERO
        "replicatTo": <ZERO|ONE|TWO|THREE|FOUR>, //optional, default ZERO
        "expiration": <time> //optional, default 0 - non expire
    }
    
### Save

Saves a document in the database. Creates or updates the document whether already exists.

To save a document send a JSON message to the module main address:

    {
        "action": "save", //mandatory
        "document": <json_document>, //mandatory
        "persistTo": <ZERO|ONE|TWO|THREE|FOUR>, //optional, default ZERO
        "replicatTo": <ZERO|ONE|TWO|THREE|FOUR>, //optional, default ZERO
        "expiration": <time> //optional, default 0 - non expire
    }
    
### Delete

Deletes a document in the database. Returns an error whether the document doesn't exists.

To delete a document send a JSON message to the module main address:

    {
        "action": "delete", //mandatory
        "document": <json_document>, //mandatory
        "persistTo": <ZERO|ONE|TWO|THREE|FOUR>, //optional, default ZERO
        "replicatTo": <ZERO|ONE|TWO|THREE|FOUR> //optional, default ZERO
    }

When the insert/save/update/delete completes successfully, a reply message is sent back to the sender with the following data:

    {
        "status": "ok"
    }
    
The reply will also contain a field `id` if the document that was saved didn't specify an id, this will be an automatically generated UUID, for example:

    {
        "status": "ok"
        "id": "ffeef2a7-5658-4905-a37c-cfb19f70471d"
    }
     
If an error occurs in saving the document a reply is returned:

    {
        "status": "error",
        "message": <message>
    }
    
Where
* `message` is an error message.

### Find by id

Finds matching document in the database by id.

To find the document send a JSON message to the module main address:

    {
        "action": "find_by_id", //mandatory
        "id": <document_id> //mandatory
    }

When the find completes successfully, a reply message is sent back to the sender with the following data:

    {
        "status": "ok",
        "document_property_1": <document_value_1>,
        .........................................,
        "document_property_n": <document_value_n>
    }
    
If an error occurs in finding the document a reply is returned:

    {
        "status": "error",
        "message": <message>
    }
    
Where
*`message` is an error message.


### Find by view

Finds matching documents in the database by view.

To find the documents send a JSON message to the module main address:

    {
        "action": "find_by_view", //mandatory
        "designDoc": <design_doc_name>, //mandatory
        "viewName": <view_name>, //mandatory
        "query": <query_document> //mandatory
    }

Where:
* `designDoc` is the design doc name where the Couchbase view is located.
* `viewName` is the name of the view to be queried.
* `query` is a json object to specify the query parameters, see http://docs.couchbase.com/couchbase-manual-2.5/cb-rest-api/#views-rest-api.

Example:

    {
        "action": "find_by_view",
        "designDoc": "vertx_doc",
        "viewName": "vertx_view",
        "query": 
        	{
        		"key": "some_key_to_query",
        		"stale": "OK|FALSE|UPDATE_AFTER",
        		"limit": 10
        	}
    }


When the find completes successfully, a reply message is sent back to the sender with the following data:

	{
        "status": "ok",
        "results": <results>
    } 
    
If an error occurs in finding the document a reply is returned:

    {
        "status": "error",
        "message": <message>
    }
    
Where
*`message` is an error message.