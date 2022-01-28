package com.services;

import com.utils.constants.Fields;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import org.apache.log4j.Logger;

public class Modules extends AbstractVerticle {
    private final Logger logger = Logger.getLogger(Modules.class);
    public static final String COLLECTION = "modules";
    private MongoClient mongo;
    @Override
    public void start(Future<Void> future) {
        mongo = MongoClient.createShared(vertx, config().getJsonObject("db"),"GestionControle");
        vertx.eventBus().consumer("AjouterModule", this::AddModule);
        vertx.eventBus().consumer("ListerAllModules", this::GetModules);
        vertx.eventBus().consumer("ModifierModule", this::UpdateModule);
        vertx.eventBus().consumer("SupprimerModule", this::DeleteModule);
        vertx.eventBus().consumer("ListerModulesByFiliere", this::ListerModulesByFiliere);
        future.complete();
    }

    private void AddModule(Message message) {
        logger.debug("inside AddModule");
        JsonObject body = (JsonObject) message.body();
        mongo.insert(COLLECTION, body, hdlr ->{
            if (hdlr.succeeded()) {
                message.reply(hdlr.succeeded());
            }else {
                JsonObject resp = new JsonObject()
                        .put(Fields.RESPONSE_FLUX_SUCCEEDED, false)
                        .put(Fields.RESPONSE_FLUX_MESSAGE, "KO");
                message.reply(resp);
            }
        });
    }
    private void GetModules(Message message) {
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
    private void UpdateModule(Message message) {
        logger.debug("inside Update Module");
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
    private void DeleteModule(Message message) {
        logger.debug("inside Delete Module");
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


    private void ListerModulesByFiliere(Message message) {
        logger.debug("inside Lister Module Element By Filiere");
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

}
