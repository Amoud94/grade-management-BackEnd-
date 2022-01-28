package com.utils;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.log4j.Logger;

public class DeploimentUtils {

	private static Logger logger = Logger.getLogger(DeploimentUtils.class);

	public static Future<Void> deployVerticles(Vertx vertx, JsonArray verticalArray, JsonObject config, Boolean isWorker) {
		Promise<Void> promise = Promise.promise();
		deployVerticlesIndex(0, verticalArray, promise, config, vertx, isWorker);
		return promise.future();
	}

	private static void deployVerticlesIndex(int indexCurrentDeploy, JsonArray verticalArray, Promise<Void> promise, JsonObject config, Vertx vertx, Boolean isWorker) {
		try {
			if (verticalArray.size() > 0) {
				String currentVertical = verticalArray.getString(indexCurrentDeploy);
				DeploymentOptions options = new DeploymentOptions().setConfig(config);
				if (isWorker)
					options.setWorker(true);
				vertx.deployVerticle(currentVertical, options, ar -> {
					try {
						if (ar.failed()) {
							logger.error("FAILED to deploy verticle `" + currentVertical + "`");
							promise.fail(ar.cause());
						} else {
							logger.info("verticle `" + currentVertical + "` deployed");
							if (indexCurrentDeploy + 1 < verticalArray.size()) {
								deployVerticlesIndex(indexCurrentDeploy + 1, verticalArray, promise, config, vertx, isWorker);
							} else {
								promise.complete();
							}
						}
					} catch (Exception e) {
						logger.error(e, e);
						promise.fail(e);
					}
				});
			} else {
				promise.complete();
			}
		} catch (Exception e) {
			logger.error(e, e);
			promise.fail(e);
		}

	}

}
