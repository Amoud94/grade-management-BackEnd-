package com.utils;

import com.utils.constants.Exceptions;
import io.vertx.core.Promise;
import io.vertx.core.file.FileSystem;
import io.vertx.ext.web.FileUpload;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class StaticFunctions {

    public static String formatDate(long millis,String format){
        try {
            Date date = new Date(millis);
            DateFormat dateFormat = new SimpleDateFormat(format);
            return dateFormat.format(date);
        }catch (Exception e){
            return "";
        }
    }

}
