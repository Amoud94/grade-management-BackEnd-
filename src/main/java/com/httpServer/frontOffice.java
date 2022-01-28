package com.httpServer;

import com.utils.constants.*;
import io.vertx.core.*;
import io.vertx.core.http.*;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.JWTOptions;
import io.vertx.ext.auth.PubSecKeyOptions;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.api.contract.RouterFactoryOptions;
import io.vertx.ext.web.api.contract.openapi3.OpenAPI3RouterFactory;
import io.vertx.ext.web.handler.*;
import org.apache.log4j.Logger;

public class frontOffice extends AbstractVerticle {

	private JWTAuth JWTAuthProvider;

	private final Logger logger = Logger.getLogger(frontOffice.class);

	private OpenAPI3RouterFactory routerFactory;

	private boolean isProduction;
	private int serverPort;
	private int clientPort;

	@Override
	public void start(Promise<Void> startFuture) {
		try {

			// load the application config file
			JsonObject config = config();

			isProduction = config.getBoolean("isProduction", true);
			serverPort = config.getInteger("serverPort");
			clientPort = config.getInteger("clientPort");

			// START JWT
			JsonObject options = new JsonObject()
					.put("algorithm", "RS256")
					.put("secretKey", config.getJsonObject("jwt").getString("secretKey"))
					.put("publicKey", config.getJsonObject("jwt").getString("publicKey"));

			JWTAuthOptions JWTAuthProviderConfig = new JWTAuthOptions()
					.addPubSecKey(new PubSecKeyOptions(options));

			JWTAuthProvider = JWTAuth.create(vertx, JWTAuthProviderConfig);

			// creating the router routerFactory + start httpServer
			OpenAPI3RouterFactory.create(vertx, "openAPI3.json", ar -> {
				try {
					if (ar.failed()) {
						// Something went wrong during router factory initialization
						logger.error(ar.cause(), ar.cause());
						startFuture.fail(ar.cause());
					} else {

						// Spec loaded with success
						routerFactory = ar.result();

						// serve vue application
						StaticHandler vueProjectHandler = StaticHandler.create()
								.setWebRoot("web/dist/")
								.setIndexPage("index.html")
								.setCachingEnabled(isProduction)
								.setDefaultContentEncoding("UTF-8");

						// add handlers
						routerFactory
								.addHandlerByOperationId("LOGIN", this::Authentification)
								
								.addHandlerByOperationId("GET", vueProjectHandler)
								.addHandlerByOperationId("POST", BodyHandler.create())
								.addHandlerByOperationId("GET", this::globalHandler)
								.addHandlerByOperationId("POST", this::globalHandler)

								.addHandlerByOperationId("privateGET", this::privateControl)
								.addHandlerByOperationId("privatePOST", this::privateControl)
								.addHandlerByOperationId("privateDELETE", this::privateControl)
								.addHandlerByOperationId("privatePUT", this::privateControl)

								.addHandlerByOperationId("Lister_Modules", this::ListerModules)
								.addHandlerByOperationId("Ajouter_Module", this::AjouterModule)
								.addHandlerByOperationId("Modifier_Module", this::ModifierModule)
								.addHandlerByOperationId("Supprimer_Module", this::SupprimerModule)

								.addHandlerByOperationId("Lister_Elements", this::ListerElements)
								.addHandlerByOperationId("Ajouter_Element", this::AjouterElement)
								.addHandlerByOperationId("Modifier_Element", this::ModifierElement)
								.addHandlerByOperationId("Supprimer_Element", this::SupprimerElement)

								.addHandlerByOperationId("Ajouter_Departement", this::AjouterDepartement)
								.addHandlerByOperationId("Lister_Departements", this::ListerDepartements)
								.addHandlerByOperationId("Supprimer_Departement", this::SupprimerDepartement)
								.addHandlerByOperationId("Modifier_Departement", this::ModifierDepartement)

								.addHandlerByOperationId("Ajouter_Filiere", this::AjouterFiliere)
								.addHandlerByOperationId("Lister_Filieres", this::ListerFilieres)
								.addHandlerByOperationId("Supprimer_Filiere", this::SupprimerFiliere)
								.addHandlerByOperationId("Modifier_Filiere", this::ModifierFiliere)

								.addHandlerByOperationId("Ajouter_Enseignant", this::AjouterEnseignant)
								.addHandlerByOperationId("Lister_Enseignants", this::ListerEnseignants)
								.addHandlerByOperationId("Supprimer_Enseignant", this::SupprimerEnseignant)
								.addHandlerByOperationId("Modifier_Enseignant", this::ModifierEnseignant)

								.addHandlerByOperationId("Ajouter_Etudiant", this::AjouterEtudiant)
								.addHandlerByOperationId("Lister_Etudiants", this::ListerEtudiants)
								.addHandlerByOperationId("Supprimer_Etudiant", this::SupprimerEtudiant)
								.addHandlerByOperationId("Modifier_Etudiant", this::ModifierEtudiant)

								.addHandlerByOperationId("SAISIE_NOTES", this::SaisirNote)
								.addHandlerByOperationId("SUPPRIMER_NOTE", this::SupprimerNote)
								.addHandlerByOperationId("MODIFIER_NOTE", this::ModifierNote)
								.addHandlerByOperationId("LISTER_NOTES", this::ListerNoteByEnseignant)

								.addHandlerByOperationId("LISTER_MODULE_NOTE", this::ListerModuleByNote)
								.addHandlerByOperationId("LISTER_NOTES_MODULE", this::ListerNoteByModule)
								.addHandlerByOperationId("LISTER_NOTES_ADMIN", this::ListerNoteByAdmin)

								.addHandlerByOperationId("Lister_Chef_Departement", this::GetChefDepartement)
								.addHandlerByOperationId("Lister_Chef_Filiere", this::GetChefFiliere)

								.addHandlerByOperationId("Lister_Modules_By_Filiere", this::GetModulesByFiliere)
								.addHandlerByOperationId("Lister_Elements_By_Module", this::GetElementsByModule)
								.addHandlerByOperationId("Lister_Etudiant_By_Filiere", this::ListerStudentByFiliere)
								.addHandlerByOperationId("Lister_Enseignant_By_Departement", this::ListerEnseignantByDepartement)

								.addHandlerByOperationId("GET_NOTE", this::GET_NOTE)

								.addHandlerByOperationId("DISTRIBUTOR", this::DISTRIBUTOR)
								.addHandlerByOperationId("CHECK_IF_EXIST", this::CHECK_IF_EXIST)

								.addHandlerByOperationId("ENVOYER_MESSAGE", this::SAVE_MESSAGE)
								.addHandlerByOperationId("RECEVOIR_MESSAGE", this::GET_MESSAGE)

								.addHandlerByOperationId("NOTES_ETD_CTRL", this::NOTES_ETD_CTRL)







						;

						//remove CORS
						if(!isProduction)
							routerFactory.addGlobalHandler(
									CorsHandler.create("http://localhost:"+ clientPort)
											.allowedMethod(HttpMethod.GET)
											.allowedMethod(HttpMethod.POST)
											.allowedMethod(HttpMethod.DELETE)
											.allowedMethod(HttpMethod.PUT)
											.allowedMethod(HttpMethod.OPTIONS)
											.allowCredentials(true)
											.allowedHeader("Access-Control-Allow-Method")
											.allowedHeader("Access-Control-Allow-Origin")
											.allowedHeader("Access-Control-Allow-Credentials")
											.allowedHeader("Content-Type")
											.allowedHeader("Content-Encoding")
											.allowedHeader("authorization")
							);

						// generate the router
						Router router = routerFactory.setOptions(new RouterFactoryOptions()).getRouter();

						// create the server
						HttpServerOptions httpServerOptions = new HttpServerOptions()
								.setPort(serverPort)
								.setCompressionSupported(true);

						vertx.createHttpServer(httpServerOptions).requestHandler(router).listen(asyncResult -> {
							if (asyncResult.succeeded()) {
								logger.info("Server is listening : http://localhost:" + serverPort);
								startFuture.complete();
							} else {
								logger.error(asyncResult.cause(), asyncResult.cause());
								startFuture.fail(asyncResult.cause());
							}
						});

					}
				} catch (Exception e) {
					logger.error(e, e);
					startFuture.fail(e);
				}
			});

		} catch (Exception e) {
			logger.error(e, e);
			startFuture.fail(e);
		}
	}


//	JWT AUTORISATION AND AUTHENTIFICATION CONTROLE
	private void privateControl(RoutingContext routingContext) {
		try {
			logger.debug("Handler `privateControl`");
			String token = routingContext.request().getHeader(HttpHeaders.AUTHORIZATION);
			JsonObject authInfo = new JsonObject().put("jwt", token);
			logger.debug("authenticate...");
			JWTAuthProvider.authenticate(authInfo, ar -> {
				if (ar.failed()) {
					logger.debug("ko.");
					JsonObject resp = new JsonObject()
							.put(Fields.RESPONSE_FLUX_SUCCEEDED, false)
							.put(Fields.RESPONSE_FLUX_MESSAGE,"failed to authenticate.");
					sendResponse(resp, routingContext.response());
				} else {
					logger.debug("ok.");
					routingContext.next();
				}
			});
		} catch (Exception e) {
			logger.error(e, e);
			JsonObject resp = new JsonObject()
					.put(Fields.RESPONSE_FLUX_SUCCEEDED, false)
					.put(Fields.RESPONSE_FLUX_MESSAGE, Exceptions.TECHNICAL_ERROR.failureCode());
			sendResponse(resp, routingContext.response());
		}
	}
	private void Authentification(RoutingContext routingContext) {
		logger.debug("Handler `Authentification`");

		JsonObject responseContent = new JsonObject();

		JsonObject module = routingContext.getBodyAsJson();

		vertx.eventBus().request("LOGIN_ENS", module, ar -> {
			if (ar.failed()){
				logger.error(ar.cause(), ar.cause());
				// todo send http response
			} else {
				JsonObject user = (JsonObject) ar.result().body();
				String JWTToken = JWTAuthProvider.generateToken(user, new JWTOptions().setAlgorithm("RS256"));
				responseContent
						.put(Fields.RESPONSE_FLUX_SUCCEEDED, true)
						.put(Fields.RESPONSE_FLUX_DATA, new JsonObject()
								.put("user", user)
								.put("TOKEN", JWTToken)
						);
			}
			sendResponse(responseContent, routingContext.request().response());
		});
	}
	private void AutorisationFILTER(RoutingContext routingContext,String uriConsumer,String permission) {
		String token = routingContext.request().getHeader(HttpHeaders.AUTHORIZATION);
		JsonObject authInfo = new JsonObject().put("jwt", token);
		JWTAuthProvider.authenticate(authInfo, ar -> {
			if (ar.failed()) {
				logger.debug("1");
				JsonObject resp = new JsonObject()
						.put(Fields.RESPONSE_FLUX_SUCCEEDED, false)
						.put(Fields.RESPONSE_FLUX_MESSAGE,"authenticate failed");
				sendResponse(resp, routingContext.response());
			} else {
				User user = ar.result();
				user.isAuthorized(permission, res -> {
					if (res.succeeded() && res.result()) {
						JsonObject responseContent = new JsonObject();

						JsonObject module = routingContext.getBodyAsJson();

						vertx.eventBus().request(uriConsumer, module, hdlr -> {
							if (hdlr.failed()){
								responseContent
										.put(Fields.RESPONSE_FLUX_SUCCEEDED, false)
										.put(Fields.RESPONSE_FLUX_MESSAGE,"KO");
							} else {
								responseContent
										.put(Fields.RESPONSE_FLUX_SUCCEEDED, true)
										.put(Fields.RESPONSE_FLUX_DATA, hdlr.result().body());
							}
							sendResponse(responseContent, routingContext.response());
						});
					}else{
						logger.debug("KO");
						JsonObject resp = new JsonObject()
								.put(Fields.RESPONSE_FLUX_SUCCEEDED, false)
								.put(Fields.RESPONSE_FLUX_MESSAGE,"not authorized.");
						sendResponse(resp, routingContext.response());

					}
				});
			}
		});
	}
//	THE END

	private void sendResponse(JsonObject responseContent, HttpServerResponse response) {
		try {
			logger.debug("sending http Response, content: " + responseContent+"\n\n");
			response.putHeader("content-type", "application/json; charset=utf-8");
			response.end(responseContent.encode());
		} catch (Exception e) {
			logger.error(e, e);
			JsonObject rs = new JsonObject()
					.put(Fields.RESPONSE_FLUX_SUCCEEDED, false)
					.put(Fields.RESPONSE_FLUX_MESSAGE, Exceptions.TECHNICAL_ERROR.failureCode());
			response.end(rs.encode());
		}
	}
	private void globalHandler(RoutingContext routingContext) {
		logger.debug("Handler `globalHandler`, path: " + routingContext.request().path() + ", body: " + routingContext.getBody());
		routingContext.next();
	}

//	CRUD OPERATIONS
	private void AjouterDepartement(RoutingContext routingContext) {
		logger.debug("Handler `AjouterDepartement`");
		AutorisationFILTER(routingContext,"AjouterDepartement","PRMSS_EE");
	}
	private void ListerDepartements(RoutingContext routingContext) {
		logger.debug("Handler `Lister Departements`");
		AutorisationFILTER(routingContext,"ListerDepartement","PRMSS_EE");
	}
	private void ModifierDepartement(RoutingContext routingContext) {
		logger.debug("Handler `Modifier Departement`");
		AutorisationFILTER(routingContext,"ModifierDepartement","PRMSS_EE");
	}
	private void SupprimerDepartement(RoutingContext routingContext) {
		logger.debug("Handler `DELETE DEPARTEMENT`");
		AutorisationFILTER(routingContext,"SupprimerDepartement","PRMSS_EE");
	}

	private void AjouterFiliere(RoutingContext routingContext) {
		logger.debug("Handler `Ajouter Filiere`");
		AutorisationFILTER(routingContext,"AjouterFiliere","PRMSS_EE");
	}
	private void ListerFilieres(RoutingContext routingContext) {
		logger.debug("Handler `Lister Filieres`");
		AutorisationFILTER(routingContext,"ListerFilieres","PRMSS_EE");
	}
	private void ModifierFiliere(RoutingContext routingContext) {
		logger.debug("Handler `Modifier Filiere`");
		AutorisationFILTER(routingContext,"ModifierFiliere","PRMSS_EE");
	}
	private void SupprimerFiliere(RoutingContext routingContext) {
		logger.debug("Handler `Supprimer Filiere`");
		AutorisationFILTER(routingContext,"SupprimerFiliere","PRMSS_EE");
	}

	private void AjouterEnseignant(RoutingContext routingContext) {
		logger.debug("Handler `Ajouter Enseignant`");
		AutorisationFILTER(routingContext,"AjouterEnseignant","PRMSS_EE");
	}
	private void ListerEnseignantByDepartement(RoutingContext routingContext) {
		logger.debug("Handler `LISTER ENSEIGNANTS BY DEPARTEMENT X`");
		AutorisationFILTER(routingContext,"ListerEnseignantsByDepartement","PRMSS_EE");
	}
	private void ListerEnseignants(RoutingContext routingContext) {
		logger.debug("Handler `LISTER` ENSEIGNANT`");
		int page = Integer.parseInt(routingContext.pathParam("page"));
		int limit = Integer.parseInt(routingContext.pathParam("limit"));
		String token = routingContext.request().getHeader(HttpHeaders.AUTHORIZATION);
		JsonObject authInfo = new JsonObject().put("jwt", token);
		JWTAuthProvider.authenticate(authInfo, ar -> {
			if (ar.failed()) {
				logger.debug("1");
				JsonObject resp = new JsonObject()
						.put(Fields.RESPONSE_FLUX_SUCCEEDED, false)
						.put(Fields.RESPONSE_FLUX_MESSAGE,"authenticate failed");
				sendResponse(resp, routingContext.response());
			} else {
				User user = ar.result();
				logger.debug("user"+ user.principal());
				user.isAuthorized("PRMSS_EE", res -> {
					if (res.succeeded() && res.result()) {
						JsonObject responseContent = new JsonObject();
						JsonObject msg = new JsonObject()
								.put("page", page)
								.put("limit", limit);
						vertx.eventBus().request("ListerAllEnseignant", msg, hdlr -> {
							if (hdlr.failed()){
								responseContent
										.put(Fields.RESPONSE_FLUX_SUCCEEDED, false)
										.put(Fields.RESPONSE_FLUX_MESSAGE,"KO");
							} else {
								responseContent
										.put(Fields.RESPONSE_FLUX_SUCCEEDED, true)
										.put(Fields.RESPONSE_FLUX_DATA, hdlr.result().body());
							}
							sendResponse(responseContent, routingContext.response());
						});
					}else{
						logger.debug("KO");
						JsonObject resp = new JsonObject()
								.put(Fields.RESPONSE_FLUX_SUCCEEDED, false)
								.put(Fields.RESPONSE_FLUX_MESSAGE,"not authorized.");
						sendResponse(resp, routingContext.response());

					}
				});
			}
		});
	}
	private void ModifierEnseignant(RoutingContext routingContext) {
		logger.debug("Handler `Modifier Enseignant`");
		AutorisationFILTER(routingContext,"ModifierEnseignant","PRMSS_EE");
	}
	private void SupprimerEnseignant(RoutingContext routingContext) {
		logger.debug("Handler `DELETE ENSIEGNANT`");
		AutorisationFILTER(routingContext,"SupprimerEnseignant","PRMSS_EE");
	}

	private void AjouterEtudiant(RoutingContext routingContext) {
		logger.debug("Handler `Ajouter Etudiant`");
		AutorisationFILTER(routingContext,"AjouterEtudiant","PRMSS_EE");
	}
	private void ModifierEtudiant(RoutingContext routingContext) {
		logger.debug("Handler `Modifier Etudiant`");
		AutorisationFILTER(routingContext,"ModifierEtudiant","PRMSS_EE");
	}
	private void ListerEtudiants(RoutingContext routingContext) {
		logger.debug("Handler `LISTER ETUDIANTS`");
		AutorisationFILTER(routingContext,"ListerAllEtudiants","PRMSS_EE");
	}
	private void SupprimerEtudiant(RoutingContext routingContext) {
		logger.debug("Handler `DELETE ETUDIANT`");
		AutorisationFILTER(routingContext,"SupprimerEtudiant","PRMSS_EE");
	}
	private void ListerStudentByFiliere(RoutingContext routingContext) {
		logger.debug("Handler `GET ELEMENT BY FILIERE`");
		AutorisationFILTER(routingContext,"ListerEtudiantsByFiliere","PRMSS_EE");
	}

	private void AjouterModule(RoutingContext routingContext) {
		logger.debug("Handler `AJOUTER Module`");
		AutorisationFILTER(routingContext,"AjouterModule","PRMSS_EE");
	}
	private void ModifierModule(RoutingContext routingContext) {
		logger.debug("Handler `MODIFIER Module`");
		AutorisationFILTER(routingContext,"ModifierModule","PRMSS_EE");
	}
	private void ListerModules(RoutingContext routingContext) {
		logger.debug("Handler `LISTER MODULE`");
		AutorisationFILTER(routingContext,"ListerAllModules","PRMSS_EE");
	}
	private void SupprimerModule(RoutingContext routingContext) {
		logger.debug("Handler `DELETE Module`");
		AutorisationFILTER(routingContext,"SupprimerModule","PRMSS_EE");
	}
	private void GetModulesByFiliere(RoutingContext routingContext) {
		logger.debug("Handler `GET ELEMENT BY FILIERE`");
		AutorisationFILTER(routingContext,"ListerModulesByFiliere","PRMSS_EE");
	}

	private void GetElementsByModule(RoutingContext routingContext) {
		logger.debug("Handler `GET ELEMENT BY FILIERE`");
		AutorisationFILTER(routingContext,"ListerElementsByModule","PRMSS_EE");
	}

	private void AjouterElement(RoutingContext routingContext) {
		logger.debug("Handler `AJOUTER Module`");
		AutorisationFILTER(routingContext,"AjouterElement","PRMSS_EE");
	}
	private void ModifierElement(RoutingContext routingContext) {
		logger.debug("Handler `MODIFIER Module`");
		AutorisationFILTER(routingContext,"ModifierElement","PRMSS_EE");
	}
	private void ListerElements(RoutingContext routingContext) {
		logger.debug("Handler `GET Module`");
		AutorisationFILTER(routingContext,"ListerAllElements","PRMSS_EE");
	}
	private void SupprimerElement(RoutingContext routingContext) {
		logger.debug("Handler `DELETE Module`");
		AutorisationFILTER(routingContext,"SupprimerElement","PRMSS_EE");
	}

	private void SaisirNote(RoutingContext routingContext) {
		logger.debug("Handler `SAISIR LES NOTES`");
		AutorisationFILTER(routingContext,"AjouterNote","PRMSS_EE");
	}
	private void ModifierNote(RoutingContext routingContext) {
		logger.debug("Handler `MODIFIER NOTE`");
		AutorisationFILTER(routingContext,"ModifierNote","PRMSS_EE");
	}
	private void SupprimerNote(RoutingContext routingContext) {
		logger.debug("Handler `SUPPRIMER NOTE`");
		AutorisationFILTER(routingContext,"SupprimerNote","PRMSS_EE");
	}
//	END OF CRUD OPERATIONS

	private void ListerNoteByEnseignant(RoutingContext routingContext) {
		logger.debug("Handler `LISTER LES NOTES ENSEIGNANT X ELEMENT X`");
		AutorisationFILTER(routingContext,"ListerNoteByEnseignant","PRMSS_EE");
	}
	private void ListerNoteByAdmin(RoutingContext routingContext) {
		logger.debug("Handler `LISTER LES NOTES DU MODULE X`");
		AutorisationFILTER(routingContext,"ListerNoteByAdmin","PRMSS_EE");
	}
	private void ListerModuleByNote(RoutingContext routingContext) {
		logger.debug("Handler `LISTER MODULES BY NOTES`");
		AutorisationFILTER(routingContext,"ListerModuleNote","PRMSS_EE");
	}
	private void ListerNoteByModule(RoutingContext routingContext) {
		logger.debug("Handler `LISTER LES NOTES DU MODULE`");
		AutorisationFILTER(routingContext,"ListerNoteByModule","PRMSS_EE");
	}

	private void GetChefDepartement(RoutingContext routingContext) {
		logger.debug("Handler `GET CHEFS DEPARTEMENT`");
		AutorisationFILTER(routingContext,"ListerChefsDepartement","PRMSS_EE");
	}
	private void GetChefFiliere(RoutingContext routingContext) {
		logger.debug("Handler `GET CHEFS FILIERE`");
		AutorisationFILTER(routingContext,"ListerChefsFiliere","PRMSS_EE");
	}

	private void SAVE_MESSAGE(RoutingContext routingContext) {
		logger.debug("Handler `GET CHEFS DEPARTEMENT`");
		AutorisationFILTER(routingContext,"EnvoyerMessage","PRMSS_EE");
	}
	private void GET_MESSAGE(RoutingContext routingContext) {
		logger.debug("Handler `LISTER LES NOTES ENSEIGNANT X ELEMENT X`");
		AutorisationFILTER(routingContext,"ListerMessage","PRMSS_EE");
	}

	private void GET_NOTE(RoutingContext routingContext) {
		logger.debug("Handler `GET CHEFS DEPARTEMENT`");
		AutorisationFILTER(routingContext,"ListerNoteX","PRMSS_EE");
	}

	private void DISTRIBUTOR(RoutingContext routingContext) {
		logger.debug("Handler `GET CHEFS DEPARTEMENT`");
		AutorisationFILTER(routingContext,"Save","PRMSS_EE");
	}
	private void CHECK_IF_EXIST(RoutingContext routingContext) {
		logger.debug("Handler `LISTER LES NOTES ENSEIGNANT X ELEMENT X`");
		AutorisationFILTER(routingContext,"Check","PRMSS_EE");
	}

	private void NOTES_ETD_CTRL(RoutingContext routingContext) {
		AutorisationFILTER(routingContext,"NOTES_ETD","PRMSS_EE");
	}


}
