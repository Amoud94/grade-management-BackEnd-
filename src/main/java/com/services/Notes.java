package com.services;

import com.sun.org.apache.xpath.internal.operations.Mod;
import com.utils.constants.Fields;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import org.apache.log4j.Logger;



public class Notes extends AbstractVerticle {
    private final Logger logger = Logger.getLogger(Notes.class);
    public static final String COLLECTION = "notes";
    private MongoClient mongo;
    @Override
    public void start(Future<Void> future){
        mongo = MongoClient.createShared(vertx, config().getJsonObject("db"),"GestionControle");
        vertx.eventBus().consumer("AjouterNote", this::AddNote);
        vertx.eventBus().consumer("ModifierNote", this::UpdateNote);
        vertx.eventBus().consumer("SupprimerNote", this::DeleteNote);

        vertx.eventBus().consumer("ListerNoteByEnseignant", this::GetNoteByEnseignant);
        vertx.eventBus().consumer("ListerNoteByModule", this::GetNoteByModule);

        vertx.eventBus().consumer("ListerNoteX", this::GetNote);

        vertx.eventBus().consumer("ListerNoteEtudiant", this::GetNoteForStudent);
        future.complete();
    }

    private void GetNote(Message message) {
        JsonObject body = (JsonObject) message.body();
        String module = body.getString("module");
        String Etd_nom = body.getString("nom");
        String Etd_prenom = body.getString("prenom");
        String semester = body.getString("semester");
        String anneeScolaire = body.getString("anneeScolaire");
        String filiere = body.getString("filiere");
        String session = body.getString("session");

        JsonObject query = new JsonObject()
                .put("element.module.nom",module)
                .put("element.module.filiere",filiere)
                .put("element.module.semester",semester)
                .put("element.module.anneeScolaire",anneeScolaire)
                .put("etudiant.nom",Etd_nom)
                .put("etudiant.prenom",Etd_prenom)
                .put("session",session);

        mongo.find(COLLECTION, query, hdlr -> {
            if (hdlr.succeeded()) {
                JsonArray reply = new JsonArray(hdlr.result());
                message.reply(reply);
                System.out.println("success");
            }else{
                System.out.println("fail");
                message.fail(1, "document not found");
            }
        });

    }
    private void AddNote(Message message) {
        logger.debug("inside ADD NEW GRADE FOR ELEMENT X AND STUDENT X");
        JsonObject body = (JsonObject) message.body();
        JsonArray tab = body.getJsonArray("notes");
        for(Object o : tab) {
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
    private void UpdateNote(Message message) {
        logger.debug("inside UPDATE GRADE");
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
    private void DeleteNote(Message message) {
        logger.debug("inside DELETE GRADE");
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
    private void GetNoteByEnseignant(Message message) {
        logger.debug("inside GET GRADES GIVEN BY TEACHER X");
        JsonObject body = (JsonObject) message.body();
        String ID = body.getString("enseignant");
        String filiere = body.getString("filiere");
        String element = body.getString("element");
        String session = body.getString("session");
        String semester = body.getString("semester");
        JsonObject Query =  new JsonObject()
                .put("enseignant.id",ID)
                .put("element.nom",element)
                .put("element.module.filiere",filiere)
                .put("element.module.semester",semester)
                .put("session",session);
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
    private void GetNoteByModule(Message message) {
        logger.debug("inside GET GRADES GIVEN BY TEACHER X");
        JsonObject body = (JsonObject) message.body();
        String Module = body.getString("Module");
        String session = body.getString("session");
        String semester = body.getString("semester");
        JsonArray pipeline = new JsonArray()
                .add(new JsonObject().put("$match", new JsonObject()
                        .put("element.module.nom", Module)
                        .put("session", session)
                        .put("element.module.semester", semester)))
                .add(new JsonObject().put("$group", new JsonObject()
                        .put("_id",new JsonObject()
                                .put("module","$element.module.nom")
                                .put("filiere","$element.module.filiere")
                                .put("session","$session")
                                .put("semester","$element.module.semester")
                                .put("anneeScolaire","$element.module.anneeScolaire")
                                .put("etudiant",new JsonObject()
                                        .put("id","$etudiant.id")
                                        .put("cin","$etudiant.cin")
                                        .put("nom","$etudiant.nom")
                                        .put("prenom","$etudiant.prenom")
                                )
                        )
                                .put("note", new JsonObject()
                                                .put("$sum", new JsonObject()
                                                    .put("$multiply", new JsonArray()
                                                            .add("$note")
                                                            .add("$element.coefficient")
                                                    )
                                                )
                                        )
                                .put("coefficient", new JsonObject().put("$sum","$element.coefficient"))
                        )


                )
                .add(new JsonObject().put("$project",new JsonObject()
                        .put("_id",0)
                        .put("etudiant","$_id.etudiant")
                        .put("semester","$_id.semester")
                        .put("session","$_id.session")
                        .put("anneeScolaire","$_id.anneeScolaire")
                        .put("module","$_id.module")
                        .put("filiere","$_id.filiere")
                        .put("note",new JsonObject().put("$divide",new JsonArray()
                                        .add("$note")
                                        .add("$coefficient")
                                )
                        )
                ));
        JsonObject command = new JsonObject()
                .put("aggregate", COLLECTION)
                .put("cursor", new JsonObject())
                .put("pipeline", pipeline);
        mongo.runCommand("aggregate", command, res -> {
            if (res.succeeded()) {
                JsonArray reply = res.result().getJsonObject("cursor").getJsonArray("firstBatch");
                logger.debug("@@@"+reply);
                message.reply(reply);
                System.out.println("success");
            } else {
                res.cause().printStackTrace();
            }
        });
    }

    private void GetNoteForStudent(Message message) {
        logger.debug("inside GET GRADES FOR STUDENT X");
        JsonObject body = (JsonObject) message.body();
        String ID = body.getString("id");
        String session = body.getString("session");
        JsonObject Query = new JsonObject()
                .put("etudiant.id",ID)
                .put("session",session);
        mongo.find(COLLECTION, Query , hdlr ->{
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
                JsonObject resp = new JsonObject()
                        .put(Fields.RESPONSE_FLUX_SUCCEEDED, false)
                        .put(Fields.RESPONSE_FLUX_MESSAGE, "KO");
                message.reply(resp);
            }
        });
    }


}
