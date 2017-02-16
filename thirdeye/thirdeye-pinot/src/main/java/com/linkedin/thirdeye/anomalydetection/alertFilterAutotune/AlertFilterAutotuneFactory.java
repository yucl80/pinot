package com.linkedin.thirdeye.anomalydetection.alertFilterAutotune;

import com.linkedin.thirdeye.datalayer.dto.AnomalyFeedbackDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;
import com.linkedin.thirdeye.detector.function.BaseAnomalyFunction;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by ychung on 2/8/17.
 */
public class AlertFilterAutotuneFactory {
  private static Logger LOGGER = LoggerFactory.getLogger(AlertFilterAutotuneFactory.class);
  private final Properties props;

  public AlertFilterAutotuneFactory(String functionConfigPath) {
    props = new Properties();
    try {
      InputStream input = new FileInputStream(functionConfigPath);
      loadPropertiesFromInputStream(input);
    } catch (FileNotFoundException e) {
      LOGGER.error("File {} not found", functionConfigPath, e);
    }

  }

  public AlertFilterAutotuneFactory(InputStream input) {
    props = new Properties();
    loadPropertiesFromInputStream(input);
  }

  private void loadPropertiesFromInputStream(InputStream input) {
    try {
      props.load(input);
    } catch (IOException e) {
      LOGGER.error("Error loading the functions from config", e);
    } finally {
      IOUtils.closeQuietly(input);
    }

    LOGGER.info("Found {} entries in alert filter autotune configuration file {}", props.size());
    for (Map.Entry<Object, Object> entry : props.entrySet()) {
      LOGGER.info("{}: {}", entry.getKey(), entry.getValue());
    }
  }

  public AlertFilterAutoTune fromSpec(AnomalyFunctionDTO anomalyFunctionSpec) throws Exception {
    AlertFilterAutoTune alertFilterAutoTune = null;
    String type = anomalyFunctionSpec.getType();
    if (!props.containsKey(type)) {
      throw new IllegalArgumentException("Unsupported type " + type);
    }
    String className = props.getProperty(type);
    alertFilterAutoTune = (AlertFilterAutoTune) Class.forName(className).newInstance();

    return alertFilterAutoTune;
  }

}
