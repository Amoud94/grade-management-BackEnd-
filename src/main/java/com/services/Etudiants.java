package com.services;


import com.utils.constants.Fields;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import org.apache.log4j.Logger;

import java.util.ArrayList;

public class Etudiants extends AbstractVerticle {
    private final Logger logger = Logger.getLogger(Etudiants.class);
    public static final String COLLECTION = "etudiants";
    private MongoClient mongo;
    @Override
    public void start(Future<Void> future) {
        mongo = MongoClient.createShared(vertx, config().getJsonObject("db"),"GestionControle");
        vertx.eventBus().consumer("AjouterEtudiant", this::AddEtudiant);
        vertx.eventBus().consumer("ListerAllEtudiants", this::GetAllEtudiant);
        vertx.eventBus().consumer("ModifierEtudiant", this::UpdateEtudiant);
        vertx.eventBus().consumer("SupprimerEtudiant", this::DeleteEtudiant);
        vertx.eventBus().consumer("ListerEtudiantsByFiliere", this::ListerEtudiantByFiliere);
        vertx.eventBus().consumer("NOTES_ETD", this::NOTES_ETD);

        future.complete();

    }

    private void AddEtudiant(Message message) {
        logger.debug("inside Ajouter Etudiant");
        JsonObject body = (JsonObject) message.body();
        mongo.insert(COLLECTION, body, hdlr -> {
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
    private void GetAllEtudiant(Message message) {
        logger.debug("inside Lister Les Etudiant");
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

    private void ListerEtudiantByFiliere(Message message) {
        JsonObject body = (JsonObject) message.body();
        int limit = body.getInteger("limit");
        int page = body.getInteger("page");
        String filiere = body.getString("filiere");
        page = page != 0 ? page - 1 : page;
        int skip = page * limit;

        // get stages
        JsonArray pipeline = new JsonArray()
                .add(new JsonObject().put("$match", new JsonObject().put("filiere",filiere)))
                .add(new JsonObject().put("$facet", new JsonObject()
                                // count aggregation stages
                                .put("count", new JsonArray()
                                        .add(new JsonObject().put("$count", "total"))
                                )
                                // docs list aggregation stages
                                .put("docs", new JsonArray()
                                        .add(new JsonObject().put("$skip", skip))
                                        .add(new JsonObject().put("$limit", limit)))
                        )
                )
                .add(new JsonObject().put("$unwind", "$count"))
                .add(new JsonObject().put("$project", new JsonObject()
                        .put("count", "$count.total")
                        .put("docs", "$docs"))
                );

        JsonObject command = new JsonObject()
                .put("aggregate", COLLECTION)
                .put("pipeline", pipeline)
                .put("allowDiskUse", true)
                .put("explain", false);

        mongo.runCommand("aggregate", command, hdlr ->{
            if (hdlr.succeeded()) {
                JsonArray firstBatch = hdlr.result().getJsonObject("cursor").getJsonArray("firstBatch");
                // init replay
                JsonObject reply = new JsonObject().put("count", 0).put("docs", new JsonArray());
                if(firstBatch.size() > 0){
                    reply = firstBatch.getJsonObject(0);
                }
                message.reply(reply);
            } else {
                logger.error(hdlr.cause(), hdlr.cause());
                message.fail(1, hdlr.cause().getMessage());
            }

        });
    }
    private void DeleteEtudiant(Message message) {
        logger.debug("inside Supprimer Un Etudiant");
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
    private void UpdateEtudiant(Message message) {
        logger.debug("inside Modifier Un Etudiant");
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
    private void NOTES_ETD(Message message) {
        logger.debug("inside Modifier Un Etudiant");
        JsonObject Body = (JsonObject) message.body();
        JsonArray notes = Body.getJsonArray("notes");
        ArrayList<Future> calls = new ArrayList<Future>();
        notes.forEach(e -> {
            logger.debug(e+"@@@@@@");
            Promise promise = Promise.promise();
            calls.add(promise.future());
            JsonObject note = (JsonObject) e;
            mongo.updateCollection(COLLECTION, new JsonObject().put("_id", note.getString("_id")),
                    new JsonObject().put("$addToSet", new JsonObject().put("notes", note)), hdlr -> {
                        if (hdlr.succeeded()) {
                            promise.complete();
                        } else {
                            promise.fail(hdlr.cause());
                        }
                    });
        });
        CompositeFuture.all(calls).onComplete(hdlr ->{
            if (hdlr.succeeded()){
                message.reply(hdlr.succeeded());
            } else {
                message.fail(1, hdlr.cause().getMessage());
            }

        });
    }
}
