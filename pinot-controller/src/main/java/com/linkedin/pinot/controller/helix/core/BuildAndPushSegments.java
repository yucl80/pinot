/*
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.controller.helix.core;

import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskResult;
import org.apache.helix.task.Workflow;


/**
 * This class is to experument with helix workflows
 */
public class BuildAndPushSegments implements  Task {
    private final HelixManager _helixManager;
    public BuildAndPushSegments(HelixManager helixManager) {
      _helixManager = helixManager;
    }

    public void createWorkFlow() {
      TaskDriver driver = new TaskDriver(_helixManager);
      final String workflowName = "PublishingAnalyticsBNP";

      Workflow.Builder workflowBuilder = new Workflow.Builder(workflowName);

      JobConfig.Builder segmentBuilderConfig = new JobConfig.Builder();
      segmentBuilderConfig.setMaxAttemptsPerTask(5);
      segmentBuilderConfig.setFailureThreshold(0);
      segmentBuilderConfig.setTaskRetryDelay(100);  // Is this milliseconds? seconds? days?
      segmentBuilderConfig.setExecutionDelay(0);    // what is this?
      segmentBuilderConfig.setWorkflow(workflowName);   // Should I set this, or will helix set it when i add the job to workflow?
      segmentBuilderConfig.setNumberOfTasks(1);     // What is this? I thought a job is scheduled as a task in the worker. If I don't set this to 1, it asks me to map to a resource.
      segmentBuilderConfig.setJobId("BuildJob-a");  // Should this name be the same as the one in addJob below?

      workflowBuilder.addJob("BuildJob-W", segmentBuilderConfig);

      JobConfig.Builder segmentPushConfig = new JobConfig.Builder();
      segmentPushConfig.setMaxAttemptsPerTask(5);
      segmentPushConfig.setFailureThreshold(0);
      segmentPushConfig.setTaskRetryDelay(100);  // Is this milliseconds? seconds? days?
      segmentPushConfig.setExecutionDelay(0);    // what is this?
      segmentPushConfig.setWorkflow(workflowName);   // Should I set this, or will helix set it when i add the job to workflow?
      segmentPushConfig.setNumberOfTasks(1);     // What is this?
      segmentPushConfig.setJobId("PushJob-a");

      workflowBuilder.addJob("PushJob-W", segmentPushConfig);

      Workflow workflow = workflowBuilder.build();

      driver.start(workflow);
    }

    @Override
    public TaskResult run() {
      // When this method returns the task should be either completed, or failed or aborted. Cannot
      // start task in another thread and return here.
      System.out.println("Task run method called");
      return new TaskResult(TaskResult.Status.COMPLETED, "Returning a task result");
    }

    @Override
    public void cancel() {
      System.out.println("Cancel called");
    }

  public static void main(String[] args) throws Exception {
    final String clusterName = "OfflineClusterIntegrationTest";
    final String zkAddr = "localhost:2191";
    final String instanceName = "Minion";
    HelixManager helixManager = new ZKHelixManager(clusterName, instanceName, InstanceType.PARTICIPANT, zkAddr);
    helixManager.connect();
    BuildAndPushSegments minion = new BuildAndPushSegments(helixManager);
    minion.createWorkFlow();
  }
}
