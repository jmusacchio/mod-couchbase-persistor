package org.vertx.mods.couchbase.test.integration.java;

import static org.vertx.testtools.VertxAssert.assertEquals;
import static org.vertx.testtools.VertxAssert.testComplete;

import org.junit.Test;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;

/**
 * Example Java integration test
 *
 * You should extend TestVerticle.
 *
 * We do a bit of magic and the test will actually be run _inside_ the Vert.x container as a Verticle.
 *
 * You can use the standard JUnit Assert API in your test by using the VertxAssert class
 */
public class PersistorTest extends TestVerticle {

  private EventBus eb;

  @Override
  public void start() {
    eb = vertx.eventBus();
    
    JsonObject config = new JsonObject();
    config.putBoolean("init", true);
    config.putString("address", "couchbase.persistor");
    config.putString("bucket_name", "vertx");
    config.putString("bucket_password", "vertx123");
    config.putString("host", "localhost");
    config.putNumber("port", 8091);
    config.putString("username", "john");
    config.putString("password", "doe");
    
    JsonObject view = new JsonObject()
    .putString("name", "vertx_view")
    .putString("function", "function (doc, meta) {emit(meta.id, null);}")
    .putString("designDoc", "vertx_doc");
    
    config.putArray("views", new JsonArray().add(view));
    
    container.deployModule("io.vertx~mod-couchbase-persistor~0.0.1-SNAPSHOT", config, 1, new AsyncResultHandler<String>() {
      public void handle(AsyncResult<String> ar) {
        if (ar.succeeded()) {
          PersistorTest.super.start();
        } else {
          ar.cause().printStackTrace();
        }
      }
    });
  }

  @Test
  public void testPersistor() throws Exception {
	final String id = "jmusacchio";
    JsonObject json = new JsonObject()
    .putString("action", "save")
    .putObject("document", new JsonObject().putString("id", id).putString("name", "Juan Manuel Musacchio"));
    
    eb.send("couchbase.persistor", json, new Handler<Message<JsonObject>>() {
      public void handle(Message<JsonObject> reply) {
        assertEquals("ok", reply.body().getString("status"));
        
        JsonObject json = new JsonObject()
        .putString("action", "find_by_id")
        .putString("id", id);
        
        eb.send("couchbase.persistor", json, new Handler<Message<JsonObject>>() {
          public void handle(Message<JsonObject> reply) {
            assertEquals("ok", reply.body().getString("status"));
            
            JsonObject json = new JsonObject()
            .putString("action", "find_by_view")
            .putString("designDoc", "dev_vertx_doc")
            .putString("viewName", "vertx_view")
            .putObject("query", new JsonObject().putString("key", id).putString("stale", "FALSE"));
            
            eb.send("couchbase.persistor", json, new Handler<Message<JsonObject>>() {
              public void handle(Message<JsonObject> reply) {
                assertEquals("ok", reply.body().getString("status"));
                testComplete();
              }
            });
          }
        });
      }
    });
  }
}
