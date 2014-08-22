package org.vertx.mods;

import static org.apache.commons.beanutils.MethodUtils.invokeMethod;
import static org.apache.commons.lang.StringUtils.capitalize;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import net.spy.memcached.CASValue;
import net.spy.memcached.PersistTo;
import net.spy.memcached.ReplicateTo;
import net.spy.memcached.internal.OperationFuture;

import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import com.couchbase.client.protocol.views.Query;
import com.couchbase.client.protocol.views.Stale;
import com.couchbase.client.protocol.views.View;
import com.couchbase.client.protocol.views.ViewResponse;
import com.couchbase.client.protocol.views.ViewRow;

/**
 * Couchbase Persistor Bus Module<p>
 * Please see the README.md for a full descrition<p>
 *
 * @author Juan Manuel Musacchio
 */
public class CouchbasePersistor extends CouchbaseGenerator implements Handler<Message<JsonObject>> {

  // used to specify whether the couchbase server should be initialized
  protected boolean init;
  // event bus address
  protected String address;
    
  @Override
  public void start() {
    super.start();

    init = getOptionalBooleanConfig("init", false);
    address = getOptionalStringConfig("address", "vertx.couchbasepersistor");
    host = getOptionalStringConfig("host", "localhost");
    port = getOptionalIntConfig("port", 8091);
        
    bucketName = getOptionalStringConfig("bucket_name", "default");
    bucketPassword = getOptionalStringConfig("bucket_password", null);
    
    operationTimeout = getOptionalIntConfig("operation_timeout", 5000);
    viewTimeout = getOptionalIntConfig("view_timeout", 75000);
    observerTimeout = getOptionalIntConfig("observer_timeout", 5000);
    viewConnsPerNode = getOptionalIntConfig("view_conns_per_node", 10);
    
    // if init param is specified means that couchbase should be started up
    // which can include bucket creation/update plus views
    if (init) {
      initializeCouchbase();
    }
    // otherwise instance the couchbase client
    else {
      createClient();
    }
       
    eb.registerHandler(address, this);
  }
  
  @Override
  public void stop() {
    if (client != null) {
    	client.shutdown();
    }
  }

  @Override
  public void handle(Message<JsonObject> message) {
    String action = message.body().getString("action");

    if (action == null) {
      sendError(message, "action must be specified");
      return;
    }

    try {
      switch (action) {
        case "insert":
          store(message, Store.INSERT);
          break;
        case "save":
          store(message, Store.SAVE);
          break;
        case "update":
          store(message, Store.UPDATE);
          break;
        case "delete":
          store(message, Store.DELETE);
          break;
        case "find_by_id":
          findById(message);
          break;
        case "find_by_view":
          findByView(message);
          break;
        case "find_by_ids":
          findByIds(message);
          break;
        
        default:
          sendError(message, "Invalid action: " + action);
      }
    } catch (Exception e) {
      sendError(message, e.getMessage(), e);
    }
  }
  
  private void store(Message<JsonObject> message, Store store) throws InterruptedException, ExecutionException {
    JsonObject doc = getMandatoryObject("document", message);
    
    if (doc == null) {
      sendError(message, "document must be specified");
      return;
    }

    if (doc.getField("id") == null) {
      doc.putString("id", UUID.randomUUID().toString());
    }

    JsonObject json = message.body();
    
    PersistTo persistTo = PersistTo.valueOf(json.getString("persistTo", "ZERO"));
    ReplicateTo replicateTo = ReplicateTo.valueOf(json.getString("replicatTo", "ZERO"));
    int expiration = json.getInteger("expiration", 0);

    OperationFuture<Boolean> addFuture = null;
    switch (store) {
      case INSERT:
        addFuture = client.add(doc.getString("id"), expiration, doc.toString(), persistTo, replicateTo);
      break;
      
      case SAVE:
        addFuture = client.set(doc.getString("id"), expiration, doc.toString(), persistTo, replicateTo);
      break;
      
      case UPDATE:
        addFuture = client.replace(doc.getString("id"), expiration, doc.toString(), persistTo, replicateTo);
      break;
      
      case DELETE:
        addFuture = client.delete(doc.getString("id"), persistTo, replicateTo);
    }
    
    boolean result = addFuture.get();
    if(result == false) {
      sendError(message, addFuture.getStatus().getMessage());
    }
    else {
      JsonObject reply = new JsonObject();
      reply.putString("id", addFuture.getKey());
      sendOK(message, reply);
    }
  }
  
  private void findById(Message<JsonObject> message) {
    String id = getMandatoryString("id", message);

    if (id == null) {
      sendError(message, "id must be specified");
      return;
    }
    
    String mode = message.body().getString("mode", "standard");
    int exp = message.body().getInteger("expiration", 0);
        
    CASValue<Object> object = null;
    if (mode.equals("lock")) {
      object = client.getAndLock(id, exp);
    }
    else if (mode.equals("touch")) {
      object = client.getAndTouch(id, exp);
    }
    else if (mode.equals("standard")) {
      object = client.gets(id);
    }
     
    if (object != null) {
    	sendOK(message, new JsonObject((String)object.getValue()));
    }
    else {
    	sendError(message, "not found");
    }
  }
  
  private void findByView(Message<JsonObject> message) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
    String designDoc = getMandatoryString("designDoc", message);
    String viewName = getMandatoryString("viewName", message);
    JsonObject query = getMandatoryObject("query", message);

    if (designDoc == null || viewName == null || query == null) {
      sendError(message, "designDoc and viewName and query must be specified");
      return;
    }	
    
    final Query q = new Query();
    for(Entry<String, Object> entry : query.toMap().entrySet()) {
      String setterMethod = "set" + capitalize(entry.getKey());
      Object value = entry.getValue();
      
      if(entry.getKey().equals("stale")) {
        invokeMethod(q, setterMethod, Stale.valueOf((String)value));
      }
      else {
        invokeMethod(q, setterMethod, value);
      }
    }
    
    final View view = client.getView(designDoc, viewName);
    final ViewResponse response = client.query(view, q);
    
    final JsonArray result = new JsonArray();
    for (final ViewRow row : response) {
      CASValue<Object> object = client.gets(row.getId());
      result.add(new JsonObject((String)object.getValue()));
    }
    
    sendOK(message, new JsonObject().putArray("result", result));
  }
  
  private void findByIds(Message<JsonObject> message) {
    JsonArray ids = message.body().getArray("ids");

    if (ids == null) {
      sendError(message, "ids must be specified");
      return;
    }
    
    @SuppressWarnings("unchecked")
    Map<String, Object> object = client.getBulk(ids.toList());
         
    if (object != null) {
      sendOK(message, new JsonObject(object));
    }
    else {
      sendError(message, "not found");
    }
  }
  
  enum Store {
	  INSERT,
	  SAVE,
	  UPDATE,
	  DELETE;
  }
}
