package com.services;

import com.utils.constants.Fields;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import org.apache.log4j.Logger;

public class message extends AbstractVerticle {
    private final Logger logger = Logger.getLogger(message.class);
    public static final String COLLECTION = "Messages";
    private MongoClient mongo;

    @Override
    public void start(Future<Void> future) {
        mongo = MongoClient.createShared(vertx, config().getJsonObject("db"), "GestionControle");
        vertx.eventBus().consumer("EnvoyerMessage", this::sauvegarder);
        vertx.eventBus().consumer("ListerMessage", this::recuperer);
        future.complete();
    }

    private void sauvegarder(Message message) {
        logger.debug("MESSANGER");
        JsonObject body = (JsonObject) message.body();
        mongo.insert(COLLECTION, body, r -> {
            if (r.succeeded()) {
                message.reply(r.result());
                System.out.println("success");
            } else {
                JsonObject resp = new JsonObject()
                        .put(Fields.RESPONSE_FLUX_SUCCEEDED, false)
                        .put(Fields.RESPONSE_FLUX_MESSAGE, "KO");
                message.reply(resp);
            }
        });
    }
    private void recuperer(Message message) {
        JsonObject body = (JsonObject) message.body();
        String user = body.getString("user");
        JsonObject Query =  new JsonObject()
                .put("destinataire",user);
        mongo.find(COLLECTION, Query, hdlr -> {
            if (hdlr.succeeded()) {
                if(hdlr.result() == null){
                    System.out.println("fail");
                    message.fail(1, "document not found");
                }else{
                    JsonArray reply = new JsonArray(hdlr.result());
                    message.reply(reply);
                    System.out.println("success");
                }
            } else {
                System.out.println("fail");
                message.fail(1, hdlr.cause().getMessage());
            }
        });
    }

}
