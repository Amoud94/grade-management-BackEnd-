package com.services;

import com.utils.constants.Fields;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import org.apache.log4j.Logger;

public class Note_distributeur extends AbstractVerticle {
    private final Logger logger = Logger.getLogger(Note_distributeur.class);
    public static final String COLLECTION = "Archive";
    private MongoClient mongo;

    @Override
    public void start(Future<Void> future) {
        mongo = MongoClient.createShared(vertx, config().getJsonObject("db"), "GestionControle");
        vertx.eventBus().consumer("Save", this::sauvegarder);
        vertx.eventBus().consumer("Check", this::recuperer);
        vertx.eventBus().consumer("ListerModuleNote", this::GetModuleByNote);
        vertx.eventBus().consumer("ListerNoteByAdmin", this::GetNoteByAdmin);

        future.complete();
    }

    private void sauvegarder(Message message) {
        logger.debug("NOTE MODULE X");
        JsonObject body = (JsonObject) message.body();
        JsonArray tab = body.getJsonArray("notes");
        for (Object o : tab) {
            mongo.insert(COLLECTION, (JsonObject) o, r -> {
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

    }

    private void recuperer(Message message) {
        logger.debug("inside GET GRADES GIVEN BY TEACHER X");
        JsonObject body = (JsonObject) message.body();
        String module = body.getString("module");
        String filiere = body.getString("filiere");
        String semester = body.getString("semester");
        String session = body.getString("session");
        JsonObject Query = new JsonObject()
                .put("module", module)
                .put("filiere", filiere)
                .put("semester", semester)
                .put("session", session);
        mongo.find(COLLECTION, Query, hdlr -> {
            if (hdlr.succeeded()) {
                if (hdlr.result() == null) {
                    System.out.println("fail");
                    message.fail(1, "document not found");
                } else {
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

    private void GetModuleByNote(Message message) {
        JsonObject body = (JsonObject) message.body();
        JsonObject query = body.getJsonObject("query");
        JsonArray pipeline = new JsonArray()
                .add(new JsonObject().put("$match", query))
                .add(new JsonObject().put("$group", new JsonObject()
                        .put("_id", new JsonObject()
                                .put("module", "$module"))));

        JsonObject command = new JsonObject()
                .put("aggregate", COLLECTION)
                .put("cursor", new JsonObject())
                .put("pipeline", pipeline);
        mongo.runCommand("aggregate", command, res -> {
            if (res.succeeded()) {
                JsonArray reply = res.result().getJsonObject("cursor").getJsonArray("firstBatch");
                logger.debug("@@@" + reply);
                message.reply(reply);
                System.out.println("success");
            } else {
                res.cause().printStackTrace();
            }
        });

    }

    private void GetNoteByAdmin(Message message) {
        JsonObject body = (JsonObject) message.body();
        JsonObject query = body.getJsonObject("query");
        query.put("proved",true);
        mongo.find(COLLECTION, query, hdlr -> {
            if (hdlr.succeeded()) {
                if (hdlr.result() == null) {
                    System.out.println("fail");
                    message.fail(1, "document not found");
                } else {
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
