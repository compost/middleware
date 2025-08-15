package ch.qos.logback.contrib.json.classic;

import java.util.Map;

import ch.qos.logback.classic.spi.ILoggingEvent;

public class CustomJsonLayout extends ch.qos.logback.contrib.json.classic.JsonLayout {

  protected void addCustomDataToJsonMap(
      java.util.Map<String, Object> map,
      ILoggingEvent event) {

    if (event.getArgumentArray() != null) {
      for (int i = 0; i < event.getArgumentArray().length; i++) {
        Object v = event.getArgumentArray()[i];
        if (v instanceof java.util.Map) {
          Map<String, Object> kv = (Map<String, Object>) v;
          kv.forEach(
              (k, value) -> {
                if (value != null) {
                  if(value instanceof java.util.Map) {
                    addMap(k, true, (Map<String, Object>)value, map);
                  } else {
                    add(k, true, value.toString(), map);
                  }
                }
              }
          );
        }

      }
    }
  }

}
