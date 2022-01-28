package com;

import com.utils.DeploimentUtils;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.log4j.Logger;

public class main extends AbstractVerticle {

	private Logger logger = Logger.getLogger(main.class);

	@Override
	public void start(Promise<Void> startPromise) throws Exception {
		try {
			JsonObject config = config();
			logger.info("config.json: " + config.encodePrettily());

			String webServer = config.getString("webServer");
			JsonArray standardVerticles = config.getJsonArray("standardVerticles");
			JsonArray workerVerticles = config.getJsonArray("workerVerticles");

			if (standardVerticles == null) {
				logger.error("property `services` not fount in config file");
				startPromise.fail(new NullPointerException());
				return;
			}

			// deploy services
			logger.info("deploying Standard Verticles");
			DeploimentUtils.deployVerticles(vertx, standardVerticles, config, false)
					.compose(v -> {
						if (workerVerticles != null && workerVerticles.size() > 0) {
							logger.info("deploying Workers Verticles");
							return DeploimentUtils.deployVerticles(vertx, workerVerticles, config, true);
						} else {
							Promise<Void> promise = Promise.promise();
							promise.complete();
							return promise.future();
						}
					})
					.setHandler(asyncResult -> {
						if (asyncResult.failed()) {
							logger.error(asyncResult.cause(), asyncResult.cause());
							startPromise.fail(asyncResult.cause());
						} else {
							if (webServer != null && !webServer.equals("")) {
								vertx.deployVerticle(webServer, new DeploymentOptions().setConfig(config), ar -> {
									if (ar.failed()) {
										logger.error(ar.cause(), ar.cause());
										startPromise.fail(ar.cause());
									} else {
										logger.info("httpServer `" + webServer + "` deployed");
										startPromise.complete();
									}
								});
							} else {
								logger.info("no httpServer deployed");
								startPromise.complete();
							}
						}
					});

		} catch (Exception e) {
			logger.error(e, e);
		}
	}
}
