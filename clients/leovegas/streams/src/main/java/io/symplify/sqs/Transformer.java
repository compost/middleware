package io.symplify.sqs;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;

public class Transformer {
  public static ObjectMapper getObjectMapper() {
    var objMp = new ObjectMapper();
    objMp.registerModules(new Jdk8Module());
    objMp.setSerializationInclusion(Include.NON_ABSENT);
    return objMp;
  }

  public static String truncateOrKeep(String v) {
    return v.length() > 10 ? v.substring(0, 10) : v;
  }

}
