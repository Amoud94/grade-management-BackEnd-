package com.services;

import com.utils.constants.Fields;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import org.apache.log4j.Logger;

public class Filieres extends AbstractVerticle {
    private final Logger logger = Logger.getLogger(Filieres.class);
    public static final String COLLECTION = "filieres";
    private MongoClient mongo;
    @Override
    public void start(Future<Void> future) {
        mongo = MongoClient.createShared(vertx, config().getJsonObject("db"),"GestionControle");
        vertx.eventBus().consumer("AjouterFiliere", this::AddFiliere);
        vertx.eventBus().consumer("SupprimerFiliere", this::DeleteFiliere);
        vertx.eventBus().consumer("ListerFilieres", this::GetAllFiliere);
        vertx.eventBus().consumer("ModifierFiliere", this::UpdateFiliere);
        future.complete();
    }

    private void AddFiliere(Message message) {
        logger.debug("inside Ajouter Filiere");
        JsonObject body = (JsonObject) message.body();
        mongo.insert(COLLECTION, body, hdlr -> {
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
    private void GetAllFiliere(Message message) {
        logger.debug("inside Lister Les Filieres");
        mongo.find(COLLECTION,  new JsonObject(), hdlr -> {
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
    private void UpdateFiliere(Message message) {
        logger.debug("inside Modifier Filiere");
        JsonObject Body = (JsonObject)message.body();
        String id = Body.getString("_id");
        mongo.updateCollection(COLLECTION,  new JsonObject().put("_id",id),
                new JsonObject().put("$set",Body), hdlr -> {
                    if (hdlr.succeeded()) {
                        message.reply(hdlr.succeeded());
                    } else {
                        JsonObject resp = new JsonObject()
                                .put(Fields.RESPONSE_FLUX_SUCCEEDED, false)
                                .put(Fields.RESPONSE_FLUX_MESSAGE, "KO");
                        message.reply(resp);
                    }
                });
    }
    private void DeleteFiliere(Message message) {
        logger.debug("inside Supprimer Filiere");
        JsonObject Body = (JsonObject)message.body();
        String id = Body.getString("_id");
        mongo.removeDocument(COLLECTION, new JsonObject().put("_id", id),
                hdlr -> {
                    if (hdlr.succeeded()) {
                        message.reply(hdlr.succeeded());
                    } else {
                        JsonObject resp = new JsonObject()
                                .put(Fields.RESPONSE_FLUX_SUCCEEDED, false)
                                .put(Fields.RESPONSE_FLUX_MESSAGE, "KO");
                        message.reply(resp);
                    }
                });
    }
}
