/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.hadoop.yarn.server.resourcemanager;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.ams.ApplicationMasterServiceProcessor;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationAttemptNotFoundException;
import org.apache.hadoop.yarn.exceptions.ApplicationMasterNotRegisteredException;
import org.apache.hadoop.yarn.exceptions.InvalidApplicationMasterRequestException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AMLivelinessMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.processor.AbstractPlacementProcessor;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.processor.DisabledPlacementProcessor;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.processor.PlacementConstraintProcessor;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.processor.SchedulerPlacementProcessor;
import org.apache.hadoop.yarn.server.resourcemanager.security.AMRMTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.authorize.RMPolicyProvider;
import org.apache.hadoop.yarn.server.security.MasterKeyData;
import org.apache.hadoop.yarn.server.utils.AMRMClientUtils;
import org.apache.hadoop.yarn.server.utils.YarnServerSecurityUtils;
import org.apache.hadoop.yarn.util.ASPair;
import org.apache.hadoop.yarn.util.MapOutputSize;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.util.ReduceSize;
import org.apache.hadoop.yarn.util.ReduceInfo2;

import com.google.common.annotations.VisibleForTesting;
import sun.awt.image.ImageWatched;

@SuppressWarnings("unchecked")
@Private
public class ApplicationMasterService extends AbstractService implements
    ApplicationMasterProtocol {
  private static final Log LOG = LogFactory.getLog(ApplicationMasterService.class);
  private static final int PRE_REGISTER_RESPONSE_ID = -1;

  private final AMLivelinessMonitor amLivelinessMonitor;
  private YarnScheduler rScheduler;
  protected InetSocketAddress masterServiceAddress;

  protected InetSocketAddress reduceScheduleAddress;

  protected Server server;
  protected final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);
  private final ConcurrentMap<ApplicationAttemptId, AllocateResponseLock> responseMap =
      new ConcurrentHashMap<ApplicationAttemptId, AllocateResponseLock>();
  protected final RMContext rmContext;
  private final AMSProcessingChain amsProcessingChain;

  //public static ConcurrentHashMap<String, ReduceSize> appReduceSize;
  //public ConcurrentHashMap<String, ReduceInfo2> appReduceSize;
  public ConcurrentHashMap<String, MapOutputSize> appReduceSize;
  public ASPair[] coflow_table;
  public int coflow_table_index;
  public int coflow_table_max;

  public ApplicationMasterService(RMContext rmContext,
      YarnScheduler scheduler) {
    this(ApplicationMasterService.class.getName(), rmContext, scheduler);
  }

  public ApplicationMasterService(String name, RMContext rmContext,
      YarnScheduler scheduler) {
    super(name);
    this.amLivelinessMonitor = rmContext.getAMLivelinessMonitor();
    this.rScheduler = scheduler;
    this.rmContext = rmContext;
    this.amsProcessingChain = new AMSProcessingChain(new DefaultAMSProcessor());

  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    masterServiceAddress = conf.getSocketAddr(
        YarnConfiguration.RM_BIND_HOST,
        YarnConfiguration.RM_SCHEDULER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_PORT);
    initializeProcessingChain(conf);
    /*
    boolean use_max_reducer = conf.getBoolean(YarnConfiguration.USE_MAX_REDUCER, YarnConfiguration.DEFAULT_USE_MAX_REDUCER);
    if(use_max_reducer) {
        reduceScheduleAddress = new InetSocketAddress("172.16.100.1", 50000);
        ReduceScheduleThread reduceScheduleThread = new ReduceScheduleThread();
        reduceScheduleThread.start();
        appReduceSize = new ConcurrentHashMap<>();
    }
     */
    boolean use_coflow = conf.getBoolean(YarnConfiguration.USE_COFLOW, YarnConfiguration.DEFAULT_USE_COFLOW);
    if(use_coflow){
      appReduceSize = new ConcurrentHashMap<>();
      coflow_table_max = 50;
      coflow_table_index = 0;
      coflow_table = new ASPair[coflow_table_max]; //assume there are 50 jobs at most
      reduceScheduleAddress = new InetSocketAddress("172.16.100.1", 60000);
      //ReduceScheduleThread2 reduceScheduleThread = new ReduceScheduleThread2();
      //reduceScheduleThread.start();
    }
  }

  public void AddASPair(ASPair newPair){
    LOG.info("AddASPair = " + newPair.ApplicationID);
    synchronized (coflow_table){
      assert coflow_table_index < coflow_table_max - 1;
      if(coflow_table_index == 0){
        coflow_table[coflow_table_index++] = newPair;
      }else{
        int idx =coflow_table_index - 1;
        while(idx >= 0 && coflow_table[idx].size > newPair.size){//insert sort
          coflow_table[idx+1] = coflow_table[idx];
          idx--;
        }
        coflow_table[idx+1] = newPair;
        coflow_table_index++;
      }

      assert coflow_table_index >= 0;
      String rte = "";
      int min = Math.min(6, coflow_table_index);
      if(min == 0) return;
      int i;
      for(i=0; i < min - 1; i++){
        rte += coflow_table[i].ApplicationID+";";
      }
      rte += coflow_table[i].ApplicationID;
      rmContext.getResourceManager().UpdateDscp(rte);
    }
  }

  public void UpdateASPair(String ID, int reduce_task_id, long diff_size){
    LOG.info("UpdateASPair = " + ID + ";" + reduce_task_id + ";" + diff_size);
    synchronized (coflow_table){

      assert coflow_table_index > 0;
      /*
      String coflow_table_rte = "";
      for(int i=0; i < coflow_table_index; i++){
        coflow_table_rte += coflow_table[i].ApplicationID+"; size : " + coflow_table[i].size+";\n";
      }
      LOG.info("coflow_table : " + coflow_table_rte);
      */

      int idx;
      for(idx = 0; idx < coflow_table_index; idx++)
        if(coflow_table[idx].ApplicationID.equals(ID))  break;

      assert idx < coflow_table_index;
      LOG.info("Application " + ID + " : idx = " + idx);
      coflow_table[idx].size -= diff_size;
      boolean update = false;
      if(coflow_table[idx].size == 0){
        appReduceSize.remove(ID);
        for(int after_idx = idx; after_idx < coflow_table_index - 1; after_idx++)
          coflow_table[after_idx] = coflow_table[after_idx + 1];
        coflow_table[--coflow_table_index] = null;
        if(coflow_table_index != 0)
          update = true;
      }else{

        ASPair asp = coflow_table[idx];
        int before_idx = idx - 1;;
        while(before_idx >=0 && coflow_table[before_idx].size > asp.size){
          coflow_table[before_idx + 1] = coflow_table[before_idx];
          before_idx--;
          update = true;
        }
        coflow_table[before_idx + 1] = asp;
      }
      if(update){
        String rte = "";
        int min = Math.min(6, coflow_table_index);
        if(min == 0) return;
        int i;
        for(i=0; i < min - 1; i++){
          rte += coflow_table[i].ApplicationID+";";
        }
        rte += coflow_table[i].ApplicationID;
        LOG.info("Update Coflow Order : " + rte);
        rmContext.getResourceManager().UpdateDscp(rte);
      }else{
        LOG.info("No need to update the order of coflow");
      }

    }
  }


  private void addPlacementConstraintHandler(Configuration conf) {
    String placementConstraintsHandler =
        conf.get(YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_HANDLER,
            YarnConfiguration.DISABLED_RM_PLACEMENT_CONSTRAINTS_HANDLER);
    if (placementConstraintsHandler
        .equals(YarnConfiguration.DISABLED_RM_PLACEMENT_CONSTRAINTS_HANDLER)) {
      LOG.info(YarnConfiguration.DISABLED_RM_PLACEMENT_CONSTRAINTS_HANDLER
          + " placement handler will be used, all scheduling requests will "
          + "be rejected.");
      amsProcessingChain.addProcessor(new DisabledPlacementProcessor());
    } else if (placementConstraintsHandler
        .equals(YarnConfiguration.PROCESSOR_RM_PLACEMENT_CONSTRAINTS_HANDLER)) {
      LOG.info(YarnConfiguration.PROCESSOR_RM_PLACEMENT_CONSTRAINTS_HANDLER
          + " placement handler will be used. Scheduling requests will be "
          + "handled by the placement constraint processor");
      amsProcessingChain.addProcessor(new PlacementConstraintProcessor());
    } else if (placementConstraintsHandler
        .equals(YarnConfiguration.SCHEDULER_RM_PLACEMENT_CONSTRAINTS_HANDLER)) {
      LOG.info(YarnConfiguration.SCHEDULER_RM_PLACEMENT_CONSTRAINTS_HANDLER
          + " placement handler will be used. Scheduling requests will be "
          + "handled by the main scheduler.");
      amsProcessingChain.addProcessor(new SchedulerPlacementProcessor());
    }
  }

  private void initializeProcessingChain(Configuration conf) {
    amsProcessingChain.init(rmContext, null);
    addPlacementConstraintHandler(conf);

    List<ApplicationMasterServiceProcessor> processors = getProcessorList(conf);
    if (processors != null) {
      Collections.reverse(processors);
      for (ApplicationMasterServiceProcessor p : processors) {
        // Ensure only single instance of PlacementProcessor is included
        if (p instanceof AbstractPlacementProcessor) {
          LOG.warn("Found PlacementProcessor=" + p.getClass().getCanonicalName()
              + " defined in "
              + YarnConfiguration.RM_APPLICATION_MASTER_SERVICE_PROCESSORS
              + ", however PlacementProcessor handler should be configured "
              + "by using " + YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_HANDLER
              + ", this processor will be ignored.");
          continue;
        }
        this.amsProcessingChain.addProcessor(p);
      }
    }
  }

  protected List<ApplicationMasterServiceProcessor> getProcessorList(
      Configuration conf) {
    return conf.getInstances(
        YarnConfiguration.RM_APPLICATION_MASTER_SERVICE_PROCESSORS,
        ApplicationMasterServiceProcessor.class);
  }

  @Override
  protected void serviceStart() throws Exception {
    Configuration conf = getConfig();
    YarnRPC rpc = YarnRPC.create(conf);

    Configuration serverConf = conf;
    // If the auth is not-simple, enforce it to be token-based.
    serverConf = new Configuration(conf);
    serverConf.set(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        SaslRpcServer.AuthMethod.TOKEN.toString());
    this.server = getServer(rpc, serverConf, masterServiceAddress,
        this.rmContext.getAMRMTokenSecretManager());
    // TODO more exceptions could be added later.
    this.server.addTerseExceptions(
        ApplicationMasterNotRegisteredException.class);

    // Enable service authorization?
    if (conf.getBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, 
        false)) {
      InputStream inputStream =
          this.rmContext.getConfigurationProvider()
              .getConfigurationInputStream(conf,
                  YarnConfiguration.HADOOP_POLICY_CONFIGURATION_FILE);
      if (inputStream != null) {
        conf.addResource(inputStream);
      }
      refreshServiceAcls(conf, RMPolicyProvider.getInstance());
    }

    this.server.start();

    this.masterServiceAddress =
        conf.updateConnectAddr(YarnConfiguration.RM_BIND_HOST,
                               YarnConfiguration.RM_SCHEDULER_ADDRESS,
                               YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS,
                               server.getListenerAddress());
    super.serviceStart();
  }

  protected Server getServer(YarnRPC rpc, Configuration serverConf,
      InetSocketAddress addr, AMRMTokenSecretManager secretManager) {
    return rpc.getServer(ApplicationMasterProtocol.class, this, addr,
        serverConf, secretManager,
        serverConf.getInt(YarnConfiguration.RM_SCHEDULER_CLIENT_THREAD_COUNT,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_CLIENT_THREAD_COUNT));
  }

  protected AMSProcessingChain getProcessingChain() {
    return this.amsProcessingChain;
  }

  @Private
  public InetSocketAddress getBindAddress() {
    return this.masterServiceAddress;
  }

  @Override
  public RegisterApplicationMasterResponse registerApplicationMaster(
      RegisterApplicationMasterRequest request) throws YarnException,
      IOException {

    AMRMTokenIdentifier amrmTokenIdentifier =
        YarnServerSecurityUtils.authorizeRequest();
    ApplicationAttemptId applicationAttemptId =
        amrmTokenIdentifier.getApplicationAttemptId();

    ApplicationId appID = applicationAttemptId.getApplicationId();
    AllocateResponseLock lock = responseMap.get(applicationAttemptId);
    if (lock == null) {
      RMAuditLogger.logFailure(this.rmContext.getRMApps().get(appID).getUser(),
          AuditConstants.REGISTER_AM, "Application doesn't exist in cache "
              + applicationAttemptId, "ApplicationMasterService",
          "Error in registering application master", appID,
          applicationAttemptId);
      throwApplicationDoesNotExistInCacheException(applicationAttemptId);
    }

    // Allow only one thread in AM to do registerApp at a time.
    synchronized (lock) {
      AllocateResponse lastResponse = lock.getAllocateResponse();
      if (hasApplicationMasterRegistered(applicationAttemptId)) {
        // allow UAM re-register if work preservation is enabled
        ApplicationSubmissionContext appContext =
            rmContext.getRMApps().get(appID).getApplicationSubmissionContext();
        if (!(appContext.getUnmanagedAM()
            && appContext.getKeepContainersAcrossApplicationAttempts())) {
          String message =
              AMRMClientUtils.APP_ALREADY_REGISTERED_MESSAGE + appID;
          LOG.warn(message);
          RMAuditLogger.logFailure(
              this.rmContext.getRMApps().get(appID).getUser(),
              AuditConstants.REGISTER_AM, "", "ApplicationMasterService",
              message, appID, applicationAttemptId);
          throw new InvalidApplicationMasterRequestException(message);
        }
      }

      this.amLivelinessMonitor.receivedPing(applicationAttemptId);

      // Setting the response id to 0 to identify if the
      // application master is register for the respective attemptid
      lastResponse.setResponseId(0);
      lock.setAllocateResponse(lastResponse);

      RegisterApplicationMasterResponse response =
          recordFactory.newRecordInstance(
              RegisterApplicationMasterResponse.class);
      this.amsProcessingChain.registerApplicationMaster(
          amrmTokenIdentifier.getApplicationAttemptId(), request, response);
      return response;
    }
  }

  @Override
  public FinishApplicationMasterResponse finishApplicationMaster(
      FinishApplicationMasterRequest request) throws YarnException,
      IOException {

    ApplicationAttemptId applicationAttemptId =
        YarnServerSecurityUtils.authorizeRequest().getApplicationAttemptId();
    ApplicationId appId = applicationAttemptId.getApplicationId();

    RMApp rmApp =
        rmContext.getRMApps().get(applicationAttemptId.getApplicationId());

    // Remove collector address when app get finished.
    if (YarnConfiguration.timelineServiceV2Enabled(getConfig())) {
      ((RMAppImpl) rmApp).removeCollectorData();
    }
    // checking whether the app exits in RMStateStore at first not to throw
    // ApplicationDoesNotExistInCacheException before and after
    // RM work-preserving restart.
    if (rmApp.isAppFinalStateStored()) {
      LOG.info(rmApp.getApplicationId() + " unregistered successfully. ");
      return FinishApplicationMasterResponse.newInstance(true);
    }

    AllocateResponseLock lock = responseMap.get(applicationAttemptId);
    if (lock == null) {
      throwApplicationDoesNotExistInCacheException(applicationAttemptId);
    }

    // Allow only one thread in AM to do finishApp at a time.
    synchronized (lock) {
      if (!hasApplicationMasterRegistered(applicationAttemptId)) {
        String message =
            "Application Master is trying to unregister before registering for: "
                + appId;
        LOG.error(message);
        RMAuditLogger.logFailure(
            this.rmContext.getRMApps()
                .get(appId).getUser(),
            AuditConstants.UNREGISTER_AM, "", "ApplicationMasterService",
            message, appId,
            applicationAttemptId);
        throw new ApplicationMasterNotRegisteredException(message);
      }

      this.amLivelinessMonitor.receivedPing(applicationAttemptId);
      FinishApplicationMasterResponse response =
          FinishApplicationMasterResponse.newInstance(false);
      this.amsProcessingChain.finishApplicationMaster(
          applicationAttemptId, request, response);
      return response;
    }
  }

  private void throwApplicationDoesNotExistInCacheException(
      ApplicationAttemptId appAttemptId)
      throws InvalidApplicationMasterRequestException {
    String message = "Application doesn't exist in cache "
        + appAttemptId;
    LOG.error(message);
    throw new InvalidApplicationMasterRequestException(message);
  }
  
  /**
   * @param appAttemptId
   * @return true if application is registered for the respective attemptid
   */
  public boolean hasApplicationMasterRegistered(
      ApplicationAttemptId appAttemptId) {
    boolean hasApplicationMasterRegistered = false;
    AllocateResponseLock lastResponse = responseMap.get(appAttemptId);
    if (lastResponse != null) {
      synchronized (lastResponse) {
        if (lastResponse.getAllocateResponse() != null
            && lastResponse.getAllocateResponse().getResponseId() >= 0) {
          hasApplicationMasterRegistered = true;
        }
      }
    }
    return hasApplicationMasterRegistered;
  }

  private final static List<Container> EMPTY_CONTAINER_LIST =
      new ArrayList<Container>();
  protected static final Allocation EMPTY_ALLOCATION = new Allocation(
      EMPTY_CONTAINER_LIST, Resources.createResource(0), null, null, null);

  private int getNextResponseId(int responseId) {
    // Loop between 0 to Integer.MAX_VALUE
    return (responseId + 1) & Integer.MAX_VALUE;
  }

  @Override
  public AllocateResponse allocate(AllocateRequest request)
      throws YarnException, IOException {

    AMRMTokenIdentifier amrmTokenIdentifier =
        YarnServerSecurityUtils.authorizeRequest();

    ApplicationAttemptId appAttemptId =
        amrmTokenIdentifier.getApplicationAttemptId();

    this.amLivelinessMonitor.receivedPing(appAttemptId);

    /* check if its in cache */
    AllocateResponseLock lock = responseMap.get(appAttemptId);
    if (lock == null) {
      String message =
          "Application attempt " + appAttemptId
              + " doesn't exist in ApplicationMasterService cache.";
      LOG.error(message);
      throw new ApplicationAttemptNotFoundException(message);
    }
    synchronized (lock) {
      AllocateResponse lastResponse = lock.getAllocateResponse();
      if (!hasApplicationMasterRegistered(appAttemptId)) {
        String message =
            "AM is not registered for known application attempt: "
                + appAttemptId
                + " or RM had restarted after AM registered. "
                + " AM should re-register.";
        throw new ApplicationMasterNotRegisteredException(message);
      }

      // Normally request.getResponseId() == lastResponse.getResponseId()
      if (getNextResponseId(request.getResponseId()) == lastResponse
          .getResponseId()) {
        // heartbeat one step old, simply return lastReponse
        return lastResponse;
      } else if (request.getResponseId() != lastResponse.getResponseId()) {
        String message =
            "Invalid responseId in AllocateRequest from application attempt: "
                + appAttemptId + ", expect responseId to be "
                + lastResponse.getResponseId() + ", but get "
                + request.getResponseId();
        throw new InvalidApplicationMasterRequestException(message);
      }

      AllocateResponse response =
          recordFactory.newRecordInstance(AllocateResponse.class);
      this.amsProcessingChain.allocate(
          amrmTokenIdentifier.getApplicationAttemptId(), request, response);

      // update AMRMToken if the token is rolled-up
      MasterKeyData nextMasterKey =
          this.rmContext.getAMRMTokenSecretManager().getNextMasterKeyData();

      if (nextMasterKey != null
          && nextMasterKey.getMasterKey().getKeyId() != amrmTokenIdentifier
          .getKeyId()) {
        RMApp app =
            this.rmContext.getRMApps().get(appAttemptId.getApplicationId());
        RMAppAttempt appAttempt = app.getRMAppAttempt(appAttemptId);
        RMAppAttemptImpl appAttemptImpl = (RMAppAttemptImpl)appAttempt;
        Token<AMRMTokenIdentifier> amrmToken = appAttempt.getAMRMToken();
        if (nextMasterKey.getMasterKey().getKeyId() !=
            appAttemptImpl.getAMRMTokenKeyId()) {
          LOG.info("The AMRMToken has been rolled-over. Send new AMRMToken back"
              + " to application: " + appAttemptId.getApplicationId());
          amrmToken = rmContext.getAMRMTokenSecretManager()
              .createAndGetAMRMToken(appAttemptId);
          appAttemptImpl.setAMRMToken(amrmToken);
        }
        response.setAMRMToken(org.apache.hadoop.yarn.api.records.Token
            .newInstance(amrmToken.getIdentifier(), amrmToken.getKind()
                .toString(), amrmToken.getPassword(), amrmToken.getService()
                .toString()));
      }

      /*
       * As we are updating the response inside the lock object so we don't
       * need to worry about unregister call occurring in between (which
       * removes the lock object).
       */
      response.setResponseId(getNextResponseId(lastResponse.getResponseId()));
      lock.setAllocateResponse(response);
      return response;
    }
  }

  public void registerAppAttempt(ApplicationAttemptId attemptId) {
    AllocateResponse response =
        recordFactory.newRecordInstance(AllocateResponse.class);
    // set response id to -1 before application master for the following
    // attemptID get registered
    response.setResponseId(PRE_REGISTER_RESPONSE_ID);
    LOG.info("Registering app attempt : " + attemptId);
    responseMap.put(attemptId, new AllocateResponseLock(response));
    rmContext.getNMTokenSecretManager().registerApplicationAttempt(attemptId);
  }

  @VisibleForTesting
  protected boolean setAttemptLastResponseId(ApplicationAttemptId attemptId,
      int lastResponseId) {
    AllocateResponseLock lock = responseMap.get(attemptId);
    if (lock == null || lock.getAllocateResponse() == null) {
      return false;
    }
    lock.getAllocateResponse().setResponseId(lastResponseId);
    return true;
  }

  public void unregisterAttempt(ApplicationAttemptId attemptId) {
    LOG.info("Unregistering app attempt : " + attemptId);
    responseMap.remove(attemptId);
    rmContext.getNMTokenSecretManager().unregisterApplicationAttempt(attemptId);
  }

  public void refreshServiceAcls(Configuration configuration, 
      PolicyProvider policyProvider) {
    this.server.refreshServiceAclWithLoadedConfiguration(configuration,
        policyProvider);
  }
  
  @Override
  protected void serviceStop() throws Exception {
    if (this.server != null) {
      this.server.stop();
    }
    super.serviceStop();
  }
  
  public static class AllocateResponseLock {
    private AllocateResponse response;
    
    public AllocateResponseLock(AllocateResponse response) {
      this.response = response;
    }
    
    public synchronized AllocateResponse getAllocateResponse() {
      return response;
    }
    
    public synchronized void setAllocateResponse(AllocateResponse response) {
      this.response = response;
    }
  }

  @VisibleForTesting
  public Server getServer() {
    return this.server;
  }



  protected class ReduceScheduleThread2 extends Thread{
    private ServerSocket serverSocket;
    private Socket clientSocket;
    public void run(){
      try{
        serverSocket = new ServerSocket();
        serverSocket.bind(reduceScheduleAddress);
        LOG.info("the reduce scheduler server starts");
        while(true){
          clientSocket = serverSocket.accept();
          new ReduceScheduleDealThread(clientSocket).start();
        }
      }catch(IOException e){
        e.printStackTrace();
      }finally {
        try{
          serverSocket.close();
        }catch (IOException e){
          e.printStackTrace();
        }
      }
    }
  }
  protected class ReduceScheduleDealThread extends Thread{
    private Socket socket;

    public ReduceScheduleDealThread(Socket socket){
      this.socket = socket;
    }
    public void run(){
      ObjectInputStream ois;
      MapOutputSize reduce_info;
      try{
        ois = new ObjectInputStream(socket.getInputStream());
        reduce_info = (MapOutputSize)ois.readObject();
        LOG.info("172.16.100.1:60000 receives " + reduce_info.toString());
        appReduceSize.put(reduce_info.ID, reduce_info);
        AddASPair(new ASPair(reduce_info.ID, reduce_info.total_size));
        ois.close();
      }catch(IOException e){
        throw new YarnRuntimeException(e);
      }catch (ClassNotFoundException e){
        throw new YarnRuntimeException(e);
      }finally {
        try{
          socket.close();
        }catch (IOException e){
          throw new YarnRuntimeException(e);
        }
      }
    }
  }
}
