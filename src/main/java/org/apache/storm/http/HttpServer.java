/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.http;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.BindException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;

import org.apache.storm.util.ReflectionUtils;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.ContextHandlerCollection;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.DefaultServlet;
import org.mortbay.jetty.servlet.FilterHolder;
import org.mortbay.jetty.servlet.FilterMapping;
import org.mortbay.jetty.servlet.ServletHandler;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.jetty.webapp.WebAppContext;
import org.mortbay.thread.QueuedThreadPool;
import org.mortbay.util.MultiException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Create a Jetty embedded server to answer http requests. The primary goal is
 * to serve up status information for the server. There are three contexts:
 * "/logs/" -> points to the log directory
 * 
 * "/static/" -> points to common static files (src/webapps/static)
 * 
 * "/" -> the jsp server code from (src/webapps/<name>)
 */
public class HttpServer implements FilterContainer {
  public static final Logger LOG = LoggerFactory.getLogger(HttpServer.class);

  // The ServletContext attribute where the daemon Map
  // gets stored.
  public static final String CONF_CONTEXT_ATTRIBUTE = "storm.conf";

  protected final Server webServer;
  protected final Connector listener;
  protected final WebAppContext webAppContext;
  protected final boolean findPort;
  protected final Map<Context, Boolean> defaultContexts =
      new HashMap<Context, Boolean>();
  protected final List<String> filterNames = new ArrayList<String>();
  private static final int MAX_RETRIES = 10;
  static final String STATE_DESCRIPTION_ALIVE = " - alive";
  static final String STATE_DESCRIPTION_NOT_LIVE = " - not live";

  /** Same as this(name, bindAddress, port, findPort, null); */
  public HttpServer(String name, String bindAddress, int port, boolean findPort)
      throws IOException {
    this(name, bindAddress, port, findPort, new HashMap<Object, Object>());
  }

  @SuppressWarnings("unchecked")
  public HttpServer(String name, String bindAddress, int port,
      boolean findPort, @SuppressWarnings("rawtypes") Map conf)
      throws IOException {
    webServer = new Server();
    this.findPort = findPort;
    listener = createBaseListener(conf);
    listener.setHost(bindAddress);
    listener.setPort(port);
    webServer.addConnector(listener);

    int maxThreads = 254;
    // TODO
    // ServerUtils.parseInt(conf.get(Config.UI_HTTP_THREAD_MAX), 254);
    QueuedThreadPool threadPool =
        maxThreads == -1 ? new QueuedThreadPool() : new QueuedThreadPool(
            maxThreads);
    webServer.setThreadPool(threadPool);

    final String appDir = getWebAppsPath(name);
    ContextHandlerCollection contexts = new ContextHandlerCollection();
    webServer.setHandler(contexts);

    webAppContext = new WebAppContext();
    webAppContext.setDisplayName("WepAppsContext");
    webAppContext.setContextPath("/");
    webAppContext.setWar(appDir + "/" + name);
    webAppContext.getServletContext()
        .setAttribute(CONF_CONTEXT_ATTRIBUTE, conf);
    webAppContext.setDescriptor("/WEB-INFO/web.xml");
    webServer.addHandler(webAppContext);

    addDefaultApps(contexts, appDir, conf);
    // addDefaultServlets();
  }

  // private void addDefaultServlets() {
  // addServlet("stacks", "/stacks", StackServlet.class);
  // addServlet("logLevel", "/logLevel", LogLevel.Servlet.class);
  // }

  /**
   * Create a required listener for the Jetty instance listening on the port
   * provided. This wrapper and all subclasses must create at least one
   * listener.
   */
  protected Connector createBaseListener(Map<Object, Object> conf)
      throws IOException {
    SelectChannelConnector ret = new SelectChannelConnector();
    ret.setLowResourceMaxIdleTime(10000);
    ret.setAcceptQueueSize(128);
    ret.setResolveNames(false);
    ret.setUseDirectBuffers(false);
    return ret;
  }

  /**
   * Add default apps.
   * 
   * @param appDir The application directory
   * @throws IOException
   */
  protected void addDefaultApps(ContextHandlerCollection parent,
      final String appDir, Map<Object, Object> conf) throws IOException {
    // set up the context for "/logs/" if "storm.log.dir" property is
    // defined.
    String logDir = System.getProperty("storm.log.dir");
    if (logDir != null) {
      Context logContext = new Context(parent, "/logs");
      logContext.setResourceBase(logDir);
      logContext.addServlet(DefaultServlet.class, "/*");
      logContext.setDisplayName("logs");
      setContextAttributes(logContext, conf);
      defaultContexts.put(logContext, true);
    }
    // set up the context for "/static/*"
    Context staticContext = new Context(parent, "/static");
    staticContext.setResourceBase(appDir + "/static");
    staticContext.addServlet(DefaultServlet.class, "/*");
    staticContext.setDisplayName("static");
    setContextAttributes(staticContext, conf);
    defaultContexts.put(staticContext, true);
  }

  private void setContextAttributes(Context context, Map<Object, Object> conf) {
    context.getServletContext().setAttribute(CONF_CONTEXT_ATTRIBUTE, conf);
  }

  public void addContext(Context ctxt, boolean isFiltered) throws IOException {
    webServer.addHandler(ctxt);
    defaultContexts.put(ctxt, isFiltered);
  }

  public static boolean hasAdministratorAccess(ServletContext servletContext,
      HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    // Configuration conf =
    // (Configuration) servletContext.getAttribute(CONF_CONTEXT_ATTRIBUTE);
    //
    // // If there is no authorization, anybody has administrator access.
    // if
    // (!conf.getBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION,
    // false)) {
    // return true;
    // }
    //
    // String remoteUser = request.getRemoteUser();
    // if (remoteUser == null) {
    // return true;
    // }
    // AccessControlList adminsAcl =
    // (AccessControlList) servletContext.getAttribute(ADMINS_ACL);
    // UserGroupInformation remoteUserUGI =
    // UserGroupInformation.createRemoteUser(remoteUser);
    // if (adminsAcl != null) {
    // if (!adminsAcl.isUserAllowed(remoteUserUGI)) {
    // response.sendError(HttpServletResponse.SC_UNAUTHORIZED, "User "
    // + remoteUser + " is unauthorized to access this page. " + "Only \""
    // + adminsAcl.toString() + "\" can access this page.");
    // return false;
    // }
    // }
    return true;
  }

  /**
   * Add a context
   * 
   * @param pathSpec The path spec for the context
   * @param dir The directory containing the context
   * @param isFiltered if true, the servlet is added to the filter path mapping
   * @throws IOException
   */
  protected void addContext(String pathSpec, String dir, boolean isFiltered)
      throws IOException {
    if (0 == webServer.getHandlers().length) {
      throw new RuntimeException("Couldn't find handler");
    }
    WebAppContext webAppCtx = new WebAppContext();
    webAppCtx.setContextPath(pathSpec);
    webAppCtx.setWar(dir);
    addContext(webAppCtx, true);
  }

  public void setAttribute(String name, Object value) {
    webAppContext.setAttribute(name, value);
  }

  public void addServlet(String name, String pathSpec,
      Class<? extends HttpServlet> clazz) {
    addInternalServlet(name, pathSpec, clazz);
    addFilterPathMapping(pathSpec, webAppContext);
  }

  public void addInternalServlet(String name, String pathSpec,
      Class<? extends HttpServlet> clazz) {
    ServletHolder holder = new ServletHolder(clazz);
    if (name != null) {
      holder.setName(name);
    }
    webAppContext.addServlet(holder, pathSpec);
  }

  public void addFilter(String name, String classname,
      Map<String, String> parameters) {

    final String[] USER_FACING_URLS = { "*.html", "*.jsp" };
    defineFilter(webAppContext, name, classname, parameters, USER_FACING_URLS);
    LOG.info("Added filter " + name + " (class=" + classname + ") to context "
        + webAppContext.getDisplayName());
    final String[] ALL_URLS = { "/*" };
    for (Map.Entry<Context, Boolean> e : defaultContexts.entrySet()) {
      if (e.getValue()) {
        Context ctx = e.getKey();
        defineFilter(ctx, name, classname, parameters, ALL_URLS);
        LOG.info("Added filter " + name + " (class=" + classname
            + ") to context " + ctx.getDisplayName());
      }
    }
    filterNames.add(name);
  }

  public void addGlobalFilter(String name, String classname,
      Map<String, String> parameters) {
    final String[] ALL_URLS = { "/*" };
    defineFilter(webAppContext, name, classname, parameters, ALL_URLS);
    for (Context ctx : defaultContexts.keySet()) {
      defineFilter(ctx, name, classname, parameters, ALL_URLS);
    }
    LOG.info("Added global filter" + name + " (class=" + classname + ")");
  }

  protected void defineFilter(Context ctx, String name, String classname,
      Map<String, String> parameters, String[] urls) {

    FilterHolder holder = new FilterHolder();
    holder.setName(name);
    holder.setClassName(classname);
    holder.setInitParameters(parameters);
    FilterMapping fmap = new FilterMapping();
    fmap.setPathSpecs(urls);
    fmap.setDispatches(Handler.ALL);
    fmap.setFilterName(name);
    ServletHandler handler = ctx.getServletHandler();
    handler.addFilter(holder, fmap);
  }

  protected void addFilterPathMapping(String pathSpec, Context webAppCtx) {
    ServletHandler handler = webAppCtx.getServletHandler();
    for (String name : filterNames) {
      FilterMapping fmap = new FilterMapping();
      fmap.setPathSpec(pathSpec);
      fmap.setFilterName(name);
      fmap.setDispatches(Handler.ALL);
      handler.addFilterMapping(fmap);
    }
  }

  public Object getAttribute(String name) {
    return webAppContext.getAttribute(name);
  }

  /**
   * Get the pathname to the webapps files.
   * 
   * @param appName eg ui or conf or log
   * @return the pathname as a URL
   * @throws FileNotFoundException if 'webapps' directory cannot be found on
   *           CLASSPATH.
   */
  private String getWebAppsPath(String appName) throws FileNotFoundException {
    URL url = getClass().getClassLoader().getResource("webapps/" + appName);
    if (url == null)
      throw new FileNotFoundException("webapps/" + appName
          + " not found in CLASSPATH");
    String urlString = url.toString();
    return urlString.substring(0, urlString.lastIndexOf('/'));
  }

  /**
   * Get the port that the server is on
   * 
   * @return the port
   */
  public int getPort() {
    return webServer.getConnectors()[0].getLocalPort();
  }

  /**
   * Set the min, max number of worker threads (simultaneous connections).
   */
  public void setThreads(int min, int max) {
    QueuedThreadPool pool = (QueuedThreadPool) webServer.getThreadPool();
    pool.setMinThreads(min);
    pool.setMaxThreads(max);
  }

  /**
   * Start the server. Does not wait for the server to start.
   */
  public void start() throws IOException {
    try {
      int port = 0;
      int oriPort = listener.getPort(); // The original requested port
      while (true) {
        try {
          port = webServer.getConnectors()[0].getLocalPort();
          LOG.info("Port returned by webServer.getConnectors()[0]."
              + "getLocalPort() before open() is " + port
              + ". Opening the listener on " + oriPort);
          listener.open();
          port = listener.getLocalPort();
          LOG.info("listener.getLocalPort() returned "
              + listener.getLocalPort()
              + " webServer.getConnectors()[0].getLocalPort() returned "
              + webServer.getConnectors()[0].getLocalPort());
          // Workaround to handle the problem reported in HADOOP-4744
          if (port < 0) {
            Thread.sleep(100);
            int numRetries = 1;
            while (port < 0) {
              LOG.warn("listener.getLocalPort returned " + port);
              if (numRetries++ > MAX_RETRIES) {
                throw new Exception(" listener.getLocalPort is returning "
                    + "less than 0 even after " + numRetries + " resets");
              }
              for (int i = 0; i < 2; i++) {
                LOG.info("Retrying listener.getLocalPort()");
                port = listener.getLocalPort();
                if (port > 0) {
                  break;
                }
                Thread.sleep(200);
              }
              if (port > 0) {
                break;
              }
              LOG.info("Bouncing the listener");
              listener.close();
              Thread.sleep(1000);
              listener.setPort(oriPort == 0 ? 0 : (oriPort += 1));
              listener.open();
              Thread.sleep(100);
              port = listener.getLocalPort();
            }
          } // Workaround end
          LOG.info("Jetty bound to port " + port);
          webServer.start();
          break;
        } catch (IOException ex) {
          // if this is a bind exception,
          // then try the next port number.
          if (ex instanceof BindException) {
            if (!findPort) {
              BindException be =
                  new BindException("Port in use: " + listener.getHost() + ":"
                      + listener.getPort());
              be.initCause(ex);
              throw be;
            }
          } else {
            LOG.info("HttpServer.start() threw a non Bind IOException");
            throw ex;
          }
        } catch (MultiException ex) {
          LOG.info("HttpServer.start() threw a MultiException");
          throw ex;
        }
        listener.setPort((oriPort += 1));
      }
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException("Problem starting http server", e);
    }
  }

  /**
   * stop the server
   */
  public void stop() throws Exception {
    listener.close();
    webServer.stop();
  }

  public void join() throws InterruptedException {
    webServer.join();
  }

  /**
   * Test for the availability of the web server
   * 
   * @return true if the web server is started, false otherwise
   */
  public boolean isAlive() {
    return webServer != null && webServer.isStarted();
  }

  /**
   * Return the host and port of the HttpServer, if live
   * 
   * @return the classname and any HTTP URL
   */
  @Override
  public String toString() {
    return listener != null ? ("HttpServer at http://" + listener.getHost()
        + ":" + listener.getLocalPort() + "/" + (isAlive() ? STATE_DESCRIPTION_ALIVE
        : STATE_DESCRIPTION_NOT_LIVE))
        : "Inactive HttpServer";
  }

  public static class StackServlet extends HttpServlet {
    private static final long serialVersionUID = -6284183679759467039L;

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response)
        throws ServletException, IOException {
      // Do the authorization
      if (!HttpServer.hasAdministratorAccess(getServletContext(), request,
          response)) {
        return;
      }

      PrintWriter out =
          new PrintWriter(HtmlQuoting.quoteOutputStream(response
              .getOutputStream()));
      ReflectionUtils.printThreadInfo(out, "");
      out.close();
      ReflectionUtils.logThreadInfo(LOG, "jsp requested", 1);
    }

  }

  public static class QuotingInputFilter implements Filter {

    public static class RequestQuoter extends HttpServletRequestWrapper {
      private final HttpServletRequest rawRequest;

      public RequestQuoter(HttpServletRequest rawRequest) {
        super(rawRequest);
        this.rawRequest = rawRequest;
      }

      /**
       * Return the set of parameter names, quoting each name.
       */
      @SuppressWarnings("unchecked")
      @Override
      public Enumeration<String> getParameterNames() {
        return new Enumeration<String>() {
          private Enumeration<String> rawIterator = rawRequest
              .getParameterNames();

          @Override
          public boolean hasMoreElements() {
            return rawIterator.hasMoreElements();
          }

          @Override
          public String nextElement() {
            return HtmlQuoting.quoteHtmlChars(rawIterator.nextElement());
          }
        };
      }

      /**
       * Unquote the name and quote the value.
       */
      @Override
      public String getParameter(String name) {
        return HtmlQuoting.quoteHtmlChars(rawRequest.getParameter(HtmlQuoting
            .unquoteHtmlChars(name)));
      }

      @Override
      public String[] getParameterValues(String name) {
        String unquoteName = HtmlQuoting.unquoteHtmlChars(name);
        String[] unquoteValue = rawRequest.getParameterValues(unquoteName);
        String[] result = new String[unquoteValue.length];
        for (int i = 0; i < result.length; ++i) {
          result[i] = HtmlQuoting.quoteHtmlChars(unquoteValue[i]);
        }
        return result;
      }

      @SuppressWarnings("unchecked")
      @Override
      public Map<String, String[]> getParameterMap() {
        Map<String, String[]> result = new HashMap<String, String[]>();
        Map<String, String[]> raw = rawRequest.getParameterMap();
        for (Map.Entry<String, String[]> item : raw.entrySet()) {
          String[] rawValue = item.getValue();
          String[] cookedValue = new String[rawValue.length];
          for (int i = 0; i < rawValue.length; ++i) {
            cookedValue[i] = HtmlQuoting.quoteHtmlChars(rawValue[i]);
          }
          result.put(HtmlQuoting.quoteHtmlChars(item.getKey()), cookedValue);
        }
        return result;
      }

      /**
       * Quote the url so that users specifying the HOST HTTP header can't
       * inject attacks.
       */
      @Override
      public StringBuffer getRequestURL() {
        String url = rawRequest.getRequestURL().toString();
        return new StringBuffer(HtmlQuoting.quoteHtmlChars(url));
      }

      /**
       * Quote the server name so that users specifying the HOST HTTP header
       * can't inject attacks.
       */
      @Override
      public String getServerName() {
        return HtmlQuoting.quoteHtmlChars(rawRequest.getServerName());
      }
    }

    @Override
    public void init(FilterConfig config) throws ServletException {
    }

    @Override
    public void destroy() {
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response,
        FilterChain chain) throws IOException, ServletException {
      HttpServletRequestWrapper quoted =
          new RequestQuoter((HttpServletRequest) request);
      final HttpServletResponse httpResponse = (HttpServletResponse) response;
      // set the default to UTF-8 so that we don't need to worry about IE7
      // choosing to interpret the special characters as UTF-7
      httpResponse.setContentType("text/html;charset=utf-8");
      chain.doFilter(quoted, response);
    }

  }
}
