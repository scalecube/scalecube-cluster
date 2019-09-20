package io.scalecube.cluster;

import java.lang.management.ManagementFactory;
import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.StandardMBean;

public abstract class AbstractMonitorMBean {

  /**
   * Registers monitor mbean.
   *
   * @param other monitor mbean instance
   * @throws Exception in case of error
   * @return object instance
   */
  public static ObjectInstance register(AbstractMonitorMBean other) throws Exception {
    Object bean = other.getBeanType().cast(other);
    //noinspection unchecked
    StandardMBean standardMBean = new StandardMBean(bean, other.getBeanType());
    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    ObjectName objectName = new ObjectName(other.getObjectName());
    return server.registerMBean(standardMBean, objectName);
  }

  /**
   * Returns bean type.
   *
   * @return bean type
   */
  protected abstract Class getBeanType();

  /**
   * Returns jmx object name.
   *
   * @return object name
   */
  protected abstract String getObjectName();
}
