package com.fanx.kafka.monitorbyjmx;

import javax.management.*;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.net.MalformedURLException;

public class JmxConnectionDemo {
    private MBeanServerConnection conn;
    private String jmxURL;
    private String ipAndPort;

    public JmxConnectionDemo(String ipAndPort) {
        this.ipAndPort = ipAndPort;
    }

    public boolean init() {
        jmxURL = "service:jmx:rmi:///jndi/rmi://" + ipAndPort + "/jmxrmi";
        try {
            JMXServiceURL serviceURL = new JMXServiceURL(jmxURL);
            JMXConnector connector = JMXConnectorFactory.connect(serviceURL, null);
            conn = connector.getMBeanServerConnection();
            if (conn != null) {
                return true;
            }
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    public double getMsbInPerSec() {
        Object val =getAttribute(
                "kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec",
                "OneMinuteRate");
        if (val != null) {
            return (double) val;
        }
        return 0.0;
    }

    public Object getAttribute(String objName, String objAttr) {
        try {
            ObjectName objectName = new ObjectName(objName);
            return conn.getAttribute(objectName, objAttr);
        } catch (MalformedObjectNameException | MBeanException |
                IOException | AttributeNotFoundException |
                InstanceNotFoundException | ReflectionException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) {
        JmxConnectionDemo jmxConnectionDemo = new JmxConnectionDemo("192.168.12.174:9999");
        boolean init = jmxConnectionDemo.init();
        if (!init) {
            System.out.println("配置错误");
        }
        System.out.println(jmxConnectionDemo.getMsbInPerSec());
    }
}
