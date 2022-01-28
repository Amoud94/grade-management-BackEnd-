package com.services;

import com.utils.constants.Fields;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.FindOptions;
import io.vertx.ext.mongo.MongoClient;
import org.apache.log4j.Logger;

public class Departements extends AbstractVerticle {
    private final Logger logger = Logger.getLogger(Departements.class);
    public static final String COLLECTION = "departement";
    private MongoClient mongo;
    @Override
    public void start(Future<Void> future) {
        mongo = MongoClient.createShared(vertx, config().getJsonObject("db"),"GestionControle");
        vertx.eventBus().consumer("AjouterDepartement", this::AddDepartement);
        vertx.eventBus().consumer("SupprimerDepartement", this::DeleteDepartement);
        vertx.eventBus().consumer("ListerDepartement", this::GetAllDepartement);
        vertx.eventBus().consumer("ModifierDepartement", this::UpdateDepartement);
        future.complete();


    }
    private void AddDepartement(Message message) {
        logger.debug("inside Ajouter Un Departement");
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
    private void GetAllDepartement(Message message) {
        mongo.find(COLLECTION,  new JsonObject(), hdlrFind -> {
            if (hdlrFind.succeeded()) {
                JsonArray reply = new JsonArray(hdlrFind.result());
                message.reply(reply);
            } else {
                logger.debug("1");
                JsonObject resp = new JsonObject()
                        .put(Fields.RESPONSE_FLUX_SUCCEEDED, false)
                        .put(Fields.RESPONSE_FLUX_MESSAGE,"KO");
                message.reply(resp);
            }
        });
    }
    private void UpdateDepartement(Message message) {
        logger.debug("inside Update Departement");
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
    private void DeleteDepartement(Message message) {
        logger.debug("inside Delete Departement");
        JsonObject Body = (JsonObject)message.body();
        String id = Body.getString("_id");
        mongo.removeDocument(COLLECTION, new JsonObject().put("_id", id),ar -> {
            if (ar.failed()) {
                logger.debug("1");
                JsonObject resp = new JsonObject()
                        .put(Fields.RESPONSE_FLUX_SUCCEEDED, false)
                        .put(Fields.RESPONSE_FLUX_MESSAGE,"KO");
                message.reply(resp);
            } else {
                message.reply(ar.succeeded());
            }
        });
    }

}
