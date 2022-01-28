package com.services;

import com.utils.constants.Fields;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import org.apache.log4j.Logger;

public class Elements extends AbstractVerticle {
    private final Logger logger = Logger.getLogger(Elements.class);
    public static final String COLLECTION = "elements";
    private MongoClient mongo;
    @Override
    public void start(Future<Void> future) {
        mongo = MongoClient.createShared(vertx, config().getJsonObject("db"),"GestionControle");
        vertx.eventBus().consumer("AjouterElement", this::AddElement);
        vertx.eventBus().consumer("ListerAllElements", this::GetAllElement);
        vertx.eventBus().consumer("ModifierElement", this::UpdateElement);
        vertx.eventBus().consumer("SupprimerElement", this::DeleteElement);
        vertx.eventBus().consumer("ListerElementsByModule", this::ListerElementByModule);
        future.complete();
    }
    private void AddElement(Message message) {
        logger.debug("inside Ajouter Un ELement");
        JsonObject body = (JsonObject) message.body();
        mongo.insert(COLLECTION, body, hdlr ->{
            if (hdlr.succeeded()) {
                message.reply(hdlr.result());
            }else
            {
                JsonObject resp = new JsonObject()
                        .put(Fields.RESPONSE_FLUX_SUCCEEDED, false)
                        .put(Fields.RESPONSE_FLUX_MESSAGE, "KO");
                message.reply(resp);
            }
        });
    }
    private void GetAllElement(Message message) {
        logger.debug("inside Lister Les Elements");
        mongo.find(COLLECTION,new JsonObject(),hdlr ->{
            if (hdlr.succeeded()) {
                JsonArray reply = new JsonArray(hdlr.result());
                message.reply(reply);
            } else {
                JsonObject resp = new JsonObject()
                        .put(Fields.RESPONSE_FLUX_SUCCEEDED, false)
                        .put(Fields.RESPONSE_FLUX_MESSAGE, "KO");
                message.reply(resp);
            }
        });
    }
    private void UpdateElement(Message message) {
        logger.debug("inside Modifier Un Element");
        JsonObject Body = (JsonObject)message.body();
        String id = Body.getString("_id");
        mongo.updateCollection(COLLECTION,  new JsonObject().put("_id",id),
                new JsonObject().put("$set",Body), ar -> {
                    if (ar.failed()) {
                        JsonObject resp = new JsonObject()
                                .put(Fields.RESPONSE_FLUX_SUCCEEDED, false)
                                .put(Fields.RESPONSE_FLUX_MESSAGE, "KO");
                        message.reply(resp);
                    } else {
                        message.reply(ar.succeeded());
                    }
                });
    }
    private void DeleteElement(Message message) {
        logger.debug("inside Supprimer Un Element");
        JsonObject Body = (JsonObject)message.body();
        String id = Body.getString("_id");
        mongo.removeDocument(COLLECTION, new JsonObject().put("_id", id),ar -> {
            if (ar.failed()) {
                JsonObject resp = new JsonObject()
                        .put(Fields.RESPONSE_FLUX_SUCCEEDED, false)
                        .put(Fields.RESPONSE_FLUX_MESSAGE,"KO");
                message.reply(resp);
            } else {
                message.reply(ar.succeeded());
            }
        });
    }

    private void ListerElementByModule(Message message) {
        logger.debug("inside LISTER ELEMENTS BY MODULE");
        JsonObject body = (JsonObject) message.body();
        String module = body.getString("module");
        mongo.find(COLLECTION,new JsonObject().put("module.nom",module),hdlr ->{
            if (hdlr.succeeded()) {
                JsonArray reply = new JsonArray(hdlr.result());
                message.reply(reply);
            } else {
                JsonObject resp = new JsonObject()
                        .put(Fields.RESPONSE_FLUX_SUCCEEDED, false)
                        .put(Fields.RESPONSE_FLUX_MESSAGE,"KO");
                message.reply(resp);
            }
        });
    }

}
