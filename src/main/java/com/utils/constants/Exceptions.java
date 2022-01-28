package com.utils.constants;

import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.eventbus.ReplyFailure;

public class Exceptions {

    public static final ReplyException TECHNICAL_ERROR = new ReplyException(ReplyFailure.RECIPIENT_FAILURE, 0, "TECHNICAL_ERROR");

}
