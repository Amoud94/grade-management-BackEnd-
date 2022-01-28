package com.services;


import com.utils.constants.Fields;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import org.apache.log4j.Logger;

public class Enseignants extends AbstractVerticle {
    private final Logger logger = Logger.getLogger(Enseignants.class);
    public static final String COLLECTION = "enseignants";
    private MongoClient mongo;
    @Override
    public void start(Future<Void> future) {
        mongo = MongoClient.createShared(vertx, config().getJsonObject("db"),"GestionControle");
        vertx.eventBus().consumer("AjouterEnseignant", this::AddEnseignant);
        vertx.eventBus().consumer("ListerAllEnseignant", this::GetEnseignants);
        vertx.eventBus().consumer("ModifierEnseignant", this::UpdateEnseignant);
        vertx.eventBus().consumer("SupprimerEnseignant", this::DeleteEnseignant);

        vertx.eventBus().consumer("ListerChefsDepartement", this::GetChefDepartement);
        vertx.eventBus().consumer("ListerChefsFiliere", this::GetChefFiliere);

        vertx.eventBus().consumer("ListerEnseignantsByDepartement", this::ListerEnseignantsByDepartement);


        vertx.eventBus().consumer("LOGIN_ENS", this::LOGIN_ENS);
        future.complete();
    }


    private void LOGIN_ENS(Message message) {
        JsonObject body = (JsonObject) message.body();
        String email = body.getString("username");
        String CNE = body.getString("login");

        JsonObject query = new JsonObject()
                .put("emailInstitutionnel",email)
                .put("cin",CNE);

        mongo.findOne(COLLECTION, query, new JsonObject(), hdlr -> {
            if (hdlr.succeeded()) {
                JsonObject doc = hdlr.result();
                if(doc == null){
                    System.out.println("fail");
                    message.fail(1, "document not found");
                }else{
                    System.out.println("success");
                    message.reply(doc);
                }
            } else {
                System.out.println("fail");
                message.fail(1, hdlr.cause().getMessage());
            }
        });

    }

    private void ListerEnseignantsByDepartement(Message message) {
        logger.debug("inside Lister Module Element By Filiere");
        JsonObject body = (JsonObject) message.body();
        int limit = body.getInteger("limit");
        int page = body.getInteger("page");
        JsonObject query = body.getJsonObject("query");
        page = page != 0 ? page - 1 : page;
        int skip = page * limit;

        // get stages
        JsonArray pipeline = new JsonArray()
                .add(new JsonObject().put("$match", query))
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


    private void GetChefDepartement(Message message) {
        mongo.find(COLLECTION,new JsonObject().put("position","Chef de departement"),hdlr ->{
            if (hdlr.succeeded()) {
                JsonArray reply = new JsonArray(hdlr.result());
                message.reply(reply);
            } else {
                System.out.println("fail");
                message.fail(1, hdlr.cause().getMessage());
            }
        });
    }
    private void GetChefFiliere(Message message) {
        mongo.find(COLLECTION,new JsonObject().put("position","Chef de filiere"),hdlr ->{
            if (hdlr.succeeded()) {
                JsonArray reply = new JsonArray(hdlr.result());
                message.reply(reply);
            } else {
                System.out.println("fail");
                message.fail(1, hdlr.cause().getMessage());
            }
        });
    }


    private void AddEnseignant(Message message) {
        logger.debug("inside Ajouter Un Enseignant");
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
    private void GetEnseignants(Message message) {
        logger.debug("inside Lister Les Enseignants");
        int limit = ( (JsonObject) message.body() ).getInteger("limit", -1);
        int page = ( (JsonObject) message.body() ).getInteger("page", 0);
        page = page != 0 ? page - 1 : page;
        int skip = page * limit;

        // get stages
        JsonArray pipeline = new JsonArray()
                .add(new JsonObject().put("$match", new JsonObject()))
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
    private void DeleteEnseignant(Message message) {
        logger.debug("inside Supprimer Un Enseignant");
        JsonObject Body = (JsonObject)message.body();
        String id = Body.getString("_id");
        mongo.removeDocument(COLLECTION, new JsonObject().put("_id", id),hdlr -> {
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
    private void UpdateEnseignant(Message message) {
        logger.debug("inside Modifier Un Enseignant");
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
}
