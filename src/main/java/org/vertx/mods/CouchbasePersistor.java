package org.vertx.mods;

import static org.apache.commons.beanutils.MethodUtils.invokeMethod;
import static org.apache.commons.lang.StringUtils.capitalize;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import net.spy.memcached.CASResponse;
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
        case "cas":
          cas(message);
          break;
        case "counter":
          counter(message);
          break;
        case "unlock":
          unlock(message);
          break;
        case "touch":
          touch(message);
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
    Long cas = json.getLong("cas");

    OperationFuture<Boolean> opFuture = null;
    OperationFuture<CASResponse> casFuture = null;
    
    switch (store) {
      case INSERT:
        opFuture = client.add(doc.getString("id"), expiration, doc.toString(), persistTo, replicateTo);
      break;
      
      case SAVE:
        if (cas == null) {
          opFuture = client.set(doc.getString("id"), expiration, doc.toString(), persistTo, replicateTo);
        }
        else {
          casFuture = client.asyncCas(doc.getString("id"), cas, expiration, doc.toString(), persistTo, replicateTo);
        }
      break;
      
      case UPDATE:
        if (cas == null) {
          opFuture = client.replace(doc.getString("id"), expiration, doc.toString(), persistTo, replicateTo);
        }
        else {
          casFuture = client.asyncCas(doc.getString("id"), cas, expiration, doc.toString(), persistTo, replicateTo);
        }
      break;
      
      case DELETE:
        opFuture = client.delete(doc.getString("id"), persistTo, replicateTo);
    }
    
    boolean result = false;
    String msg = null;
    String key = null;
    
    if (opFuture != null) {
      result = opFuture.get();
      msg = opFuture.getStatus().getMessage();
      key = opFuture.getKey();
    }
    else if(casFuture != null) {
      result = casFuture.get() == CASResponse.OK;
      msg = casFuture.getStatus().getMessage();
      key = casFuture.getKey();
    }
    
    if(result) {
      JsonObject reply = new JsonObject();
      reply.putString("id", key);
      sendOK(message, reply);
    }
    else {
      sendError(message, msg);
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
  
  private void cas(Message<JsonObject> message) throws InterruptedException, ExecutionException {
    JsonObject json = message.body(); 
    
    String key = getMandatoryString("key", message);   
    Long cas = json.getLong("cas");
    
    if (key == null || cas == null) {
      sendError(message, "key and cas must be specified");
      return;
    }
    
    PersistTo persistTo = PersistTo.valueOf(json.getString("persistTo", "ZERO"));
    ReplicateTo replicateTo = ReplicateTo.valueOf(json.getString("replicatTo", "ZERO"));
    int expiration = json.getInteger("expiration", 0);
    Object value = json.getValue("value");
    
    OperationFuture<CASResponse> casFuture = client.asyncCas(key, cas, expiration, value, persistTo, replicateTo);
    CASResponse response = casFuture.get();
    
    if (response == CASResponse.OK) {
      JsonObject reply = new JsonObject();
      reply.putString("key", casFuture.getKey());
      sendOK(message, reply);
    }
    else {
      sendError(message, casFuture.getStatus().getMessage());
    }
  }
  
  private void counter(Message<JsonObject> message) throws InterruptedException, ExecutionException {
    JsonObject json = message.body(); 
    
    String key = getMandatoryString("key", message);
    String operation = getMandatoryString("operation", message);
    Long by = json.getLong("by");
    
    if (key == null || operation == null || by == null) {
      sendError(message, "key and operation and by must be specified");
      return;
    }
    
    int expiration = json.getInteger("expiration", 0);
    long def = json.getLong("default", 0);
    OperationFuture<Long> opFuture = null;
    
    if (operation.equals("increment")) {
      opFuture = client.asyncIncr(key, by, def, expiration);
    }
    else if (operation.equals("decrement")){
      opFuture = client.asyncDecr(key, by, def, expiration);
    }
    
    if (opFuture != null) {
      Long counter = opFuture.get();
      if (opFuture.getStatus().isSuccess()) {
        JsonObject reply = new JsonObject();
        reply.putString("key", opFuture.getKey());
        reply.putNumber("counter", counter);
        
        sendOK(message, reply);
      }
      else {
        sendError(message, opFuture.getStatus().getMessage());
      }
    }
    else {
      sendError(message, "invalid operation should be increment or decrement");
    }
  }
  
  private void unlock(Message<JsonObject> message) throws InterruptedException, ExecutionException {
    JsonObject json = message.body(); 
    
    String key = getMandatoryString("key", message);   
    Long cas = json.getLong("cas");
    
    if (key == null || cas == null) {
      sendError(message, "key and cas must be specified");
      return;
    }
        
    OperationFuture<Boolean> unlockFuture = client.asyncUnlock(key, cas);
    boolean response = unlockFuture.get();
    
    if (response) {
      JsonObject reply = new JsonObject();
      reply.putString("key", unlockFuture.getKey());
      sendOK(message, reply);
    }
    else {
      sendError(message, unlockFuture.getStatus().getMessage());
    }
  }
  
  private void touch(Message<JsonObject> message) throws InterruptedException, ExecutionException {
    JsonObject json = message.body(); 
    
    String key = getMandatoryString("key", message);   
    int expiration = json.getInteger("expiration", 0);
    
    if (key == null) {
      sendError(message, "key must be specified");
      return;
    }
        
    OperationFuture<Boolean> touchFuture = client.touch(key, expiration);
    boolean response = touchFuture.get();
    
    if (response) {
      JsonObject reply = new JsonObject();
      reply.putString("key", touchFuture.getKey());
      sendOK(message, reply);
    }
    else {
      sendError(message, touchFuture.getStatus().getMessage());
    }
  }
  
  enum Store {
	  INSERT,
	  SAVE,
	  UPDATE,
	  DELETE;
  }
}
