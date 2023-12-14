package KSPOnStorm.util;

import org.apache.storm.Config;

import java.io.*;
import java.util.*;

/**
 * Created by yestin on 2017/1/13.
 */
public class Configuration {


    private boolean debug = false;

    private List<String> serializationList = new ArrayList<>();

    private Map<String, Object> stormConfigMap = new HashMap<>();

    private Map<String, Integer> parallelismMap = new HashMap<>();

    public boolean isDebug() {
        return debug;
    }

    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    public List<String> getSerializationList() {
        return serializationList;
    }

    public void setSerializationList(List<String> serializationList) {
        this.serializationList = serializationList;
    }

    public Map<String, Object> getStormConfigMap() {
        return stormConfigMap;
    }

    public void setStormConfigMap(Map<String, Object> stormConfigMap) {
        this.stormConfigMap = stormConfigMap;
    }

    public Map<String, Integer> getParallelismMap() {
        return parallelismMap;
    }

    public void setParallelismMap(Map<String, Integer> parallelismMap) {
        this.parallelismMap = parallelismMap;
    }

    public static Configuration getConfiguration(String appName) throws IOException {
        return getConfiguration(ClassLoader.getSystemClassLoader()
                        .getResourceAsStream(appName + ".properties"));
    }

    public static Configuration getConfiguration(String appName, String filePath) throws IOException {
        return getConfiguration(new FileInputStream(new File(filePath)));
    }

    public static Configuration getConfiguration(InputStream inputStream) throws IOException {
        Configuration appConfiguration = new Configuration();
        Properties properties = new Properties();
        properties.load(inputStream);
        inputStream.close();
        Set<Object> keySet = properties.keySet();
        for (Object keyObj : keySet) {
            String key = (String) keyObj;
            if (key.equals("mode.debug")) {
                String value = properties.getProperty(key);
                if (value != null) {
                    appConfiguration.debug = Boolean.valueOf(value);
                }
            } else if (((String) key).startsWith("parallelism.")) {
                String value = properties.getProperty(key);
                if (value != null) {
                    appConfiguration.getParallelismMap().put(key.substring("parallelism.".length()), Integer.valueOf(value));
                }
            } else if (((String) key).startsWith("config.")) {
                String value = properties.getProperty(key);
                if (value != null) {
                    try {
                        appConfiguration.getStormConfigMap().put(key.substring("config.".length()), Integer.valueOf(value));
                    } catch (Exception e) {
                        appConfiguration.getStormConfigMap().put(key.substring("config.".length()), value);
                    }

                }
            }
        }

        // read serialization file.

        appConfiguration.serializationList.addAll(readLines(
                ClassLoader.getSystemClassLoader()
                        .getResourceAsStream("serialization.list")
        ));


        /*appConfiguration.serializationList.addAll(readLines(
                ClassLoader.getSystemClassLoader()
                        .getResourceAsStream("grandland/glits/storm/serialization/" + appName + ".list")
        ));*/

        return appConfiguration;
    }

    private static List<String> readLines(InputStream inputStream) throws IOException {
        InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
        List<String> lines = new ArrayList<>();
        String line = null;
        while ((line = bufferedReader.readLine()) != null) {
            if (line.startsWith("#") || line.isEmpty()) continue;
            lines.add(line.trim());
        }
        bufferedReader.close();
        inputStream.close();
        inputStream.close();
        return lines;
    }

    public Config createStormConfig() throws ClassNotFoundException {
        Config conf = new Config();
        conf.setDebug(isDebug());
        conf.putAll(getStormConfigMap());
        //将Storm传递的对象注册序列化
        for (String clazzName : getSerializationList()) {
            conf.registerSerialization(Class.forName(clazzName));
        }
        return conf;
    }
}
