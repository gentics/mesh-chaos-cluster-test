package com.gentics.mesh.test;

import static com.gentics.mesh.test.ClientHelper.call;
import static com.gentics.mesh.test.util.TestUtils.sleep;
import static com.gentics.mesh.util.UUIDUtil.randomUUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import com.gentics.mesh.FieldUtil;
import com.gentics.mesh.core.rest.admin.consistency.ConsistencyCheckResponse;
import com.gentics.mesh.core.rest.node.NodeCreateRequest;
import com.gentics.mesh.core.rest.project.ProjectCreateRequest;
import com.gentics.mesh.core.rest.project.ProjectResponse;
import com.gentics.mesh.core.rest.schema.impl.SchemaCreateRequest;
import com.gentics.mesh.core.rest.schema.impl.SchemaResponse;
import com.gentics.mesh.core.rest.schema.impl.SchemaUpdateRequest;
import com.gentics.mesh.core.rest.user.UserCreateRequest;
import com.gentics.mesh.core.rest.user.UserResponse;
import com.gentics.mesh.test.docker.MeshContainer;
import com.gentics.mesh.util.UUIDUtil;
import com.google.common.collect.Lists;

public class ChaosClusterTest extends AbstractClusterTest {

	private static String clusterPostFix = randomUUID();

	private static Random random = new Random();

	static {
		// Set the seed to get repeatable random operations
		random.setSeed(42L);
	}

	private static final String SCHEMA_NAME = "TestSchema";

	private static final String PROJECT_NAME = "Dummy";

	private static final int STARTUP_TIMEOUT = 100;

	private static final int TOTAL_ACTIONS = 40;

	private static final int WRITE_QUORUM = 2;

	private static final String CLUSTERNAME = "dummy";

	private static final int SERVER_LIMIT = 8;

	/* Test flags */

	private static final List<MeshContainer> disconnectedServers = new ArrayList<>(SERVER_LIMIT);

	private static final List<MeshContainer> runningServers = new ArrayList<>(SERVER_LIMIT);

	private static final List<MeshContainer> stoppedServers = new ArrayList<>(SERVER_LIMIT);

	private static final List<String> userUuids = new ArrayList<>();

	private static final List<String> nodeUuids = new ArrayList<>();

	private static final AtomicReference<String> schemaUuid = new AtomicReference<>();

	private static final AtomicReference<ProjectResponse> project = new AtomicReference<>();

	private boolean hasSplitBrain = false;

	private static int nAction = 0;

	private enum Actions {
		ADD_INSTANCE, REMOVE_INSTANCE, CREATE_USER, CREATE_NODE, STOP_INSTANCE, START_INSTANCE, KILL_INSTANCE, BACKUP_INSTANCE, SPLIT_BRAIN, MERGE_BRAIN, DISCONNECT_INSTANCE, CONNECT_INSTANCE, SCHEMA_MIGRATION;

		public static Actions random() {
			return values()[random.nextInt(values().length)];
		}
	};

	@Test
	public void runTest() throws InterruptedException, IOException {
		startInitialServer();

		while (nAction < TOTAL_ACTIONS) {
			printTopology();
			// System.out.println("Press any key to continue");
			// System.in.read();
			System.out.println("\n\n\nApplying action...");
			applyAction();
			Thread.sleep(15_000);
			System.out.println("\n\n\nChecking cluster...");
			assertCluster();
			nAction++;
		}
	}

	private void printTopology() {
		System.err.println("-----------------------------------");
		System.err.println("- Action: " + nAction);
		System.err.println("- Uuids:  " + userUuids.size());
		System.err.println("- Nodes in the cluster:");
		System.err.println("-----------------------------------");
		System.err.println("- ID, Nodename, Running, IP");
		System.err.println("-----------------------------------");
		for (MeshContainer server : runningServers) {
			System.err.println(
				"- " + server.getContainerId() + "\t" + server.getNodeName() + "\t" + server.getContainerIpAddress());
		}

		System.err.println("Stopped servers:");
		System.err.println("-----------------------------------");
		for (MeshContainer server : stoppedServers) {
			System.err.println(
				"- " + server.getContainerId() + "\t" + server.getNodeName() + "\t" + server.getContainerIpAddress());
		}
		System.err.println("-----------------------------------");
	}

	private void startInitialServer() throws InterruptedException {
		MeshContainer server = new MeshContainer(meshImage())
			.withInitCluster()
			.withClusterName(CLUSTERNAME + clusterPostFix)
			.withNodeName("master")
			.withClearFolders()
			.withFilesystem()
			.withWriteQuorum(WRITE_QUORUM)
			.withDataPathPostfix("master")
			.waitForStartup();

		server.start();
		server.awaitStartup(STARTUP_TIMEOUT);
		server.login();

		runningServers.add(server);
		reachWriteQuorum();
		setupInitialServer(server);
	}

	private void setupInitialServer(MeshContainer server) {
		// 1. Create Schema
		SchemaCreateRequest schemaCreateRequest = new SchemaCreateRequest();
		schemaCreateRequest.setName(SCHEMA_NAME);
		schemaCreateRequest.setContainer(false);
		schemaCreateRequest.setDescription("Test schema");
		schemaCreateRequest.setDisplayField("name");
		schemaCreateRequest.addField(FieldUtil.createStringFieldSchema("name"));
		schemaCreateRequest.addField(FieldUtil.createStringFieldSchema("content"));
		SchemaResponse response = call(() -> server.client().createSchema(schemaCreateRequest));
		schemaUuid.set(response.getUuid());

		// 2. Create project
		ProjectCreateRequest projectRequest = new ProjectCreateRequest();
		projectRequest.setName(PROJECT_NAME);
		projectRequest.setSchemaRef("folder");
		ProjectResponse projectResponse = call(() -> server.client().createProject(projectRequest));
		project.set(projectResponse);

		// 3. Assign schema to project
		call(() -> server.client().assignSchemaToProject(PROJECT_NAME, schemaUuid.get()));

		// 4. Create node
		createNode(server);

	}

	private void createNode(MeshContainer server) {
		NodeCreateRequest nodeCreateRequest = new NodeCreateRequest();
		nodeCreateRequest.setLanguage("en");
		nodeCreateRequest.setParentNodeUuid(project.get().getRootNode().getUuid());
		nodeCreateRequest.setSchemaName(SCHEMA_NAME);
		nodeCreateRequest.getFields().put("name", FieldUtil.createStringField("Node Name"));
		nodeCreateRequest.getFields().put("content", FieldUtil.createStringField("Node Content"));
		nodeUuids.add(call(() -> server.client().createNode(PROJECT_NAME, nodeCreateRequest)).getUuid());
	}

	private void applyAction() throws InterruptedException {
		while (true) {
			switch (Actions.random()) {
			case ADD_INSTANCE:
				if (runningServers.size() < SERVER_LIMIT) {
					addServer();
					return;
				}
				break;
			case REMOVE_INSTANCE:
				if (allowStopOrRemoval()) {
					removeServer();
					return;
				}
				break;
			case CREATE_USER:
				if (meetsWriteQuorumRequirements()) {
					createUser();
					return;
				}
				break;
			case CREATE_NODE:
				if (meetsWriteQuorumRequirements()) {
					createNode(randomRunningServer());
					return;
				}
				break;
			case SCHEMA_MIGRATION:
				if (meetsWriteQuorumRequirements()) {
					invokeSchemaMigration();
					return;
				}
				break;
			case SPLIT_BRAIN:
				if (!hasSplitBrain) {
					invokeSplitBrain();
				}
				break;
			case MERGE_BRAIN:
				if (hasSplitBrain) {
					mergeSplitBrain();
				}
				break;
			case CONNECT_INSTANCE:
				if (!disconnectedServers.isEmpty()) {
					connectServer();
				}
				return;
			case DISCONNECT_INSTANCE:
				disconnectServer();
				return;
			case KILL_INSTANCE:
				if (allowStopOrRemoval()) {
					killServer();
				}
				return;
			case BACKUP_INSTANCE:
				if (!runningServers.isEmpty()) {
					backupServer();
					return;
				}
				break;
			case STOP_INSTANCE:
				if (allowStopOrRemoval()) {
					stopServer();
					return;
				}
				break;
			case START_INSTANCE:
				if (!stoppedServers.isEmpty() && runningServers.size() < SERVER_LIMIT) {
					startServer();
					return;
				}
				break;
			}
		}
	}

	private void invokeSchemaMigration() {
		MeshContainer s = randomRunningServer();
		System.err.println("Invoking schema migration " + s.getNodeName());
		SchemaUpdateRequest request = new SchemaUpdateRequest();
		request.setName(SCHEMA_NAME);
		request.setContainer(false);
		// Randomize the description to invoke a migration
		request.setDescription("Test schema" + UUIDUtil.randomUUID());
		request.setDisplayField("name");
		request.addField(FieldUtil.createStringFieldSchema("name"));
		request.addField(FieldUtil.createStringFieldSchema("content"));
		call(() -> s.client().updateSchema(schemaUuid.get(), request));
	}

	private void invokeSplitBrain() {
		System.err.println("Invoking split brain situation on cluster");
		if (runningServers.size() % 2 == 0) {
			List<List<MeshContainer>> lists = Lists.partition(runningServers, (runningServers.size() + 1) / 2);
			List<MeshContainer> halfA = lists.get(0);
			List<MeshContainer> halfB = lists.get(1);

			// Drop Traffic in halfA to halfB
			for (MeshContainer server : halfA) {
				try {
					server.dropTraffic(halfB.toArray(new MeshContainer[halfB.size()]));
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			// Drop Traffic in halfB to halfA
			for (MeshContainer server : halfB) {
				try {
					server.dropTraffic(halfA.toArray(new MeshContainer[halfA.size()]));
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			hasSplitBrain = true;
		}
	}

	private void mergeSplitBrain() {
		System.err.println("Merging split brain in cluster");
		for (MeshContainer server : runningServers) {
			try {
				server.resumeTraffic();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		hasSplitBrain = false;
	}

	private void connectServer() {
		MeshContainer s = disconnectedServers.get(random.nextInt(disconnectedServers.size()));
		try {
			System.err.println("Reconnecting server from cluster " + s.getNodeName());
			s.resumeTraffic();
		} catch (Exception e) {
			e.printStackTrace();
		}
		disconnectedServers.remove(s);
	}

	private void disconnectServer() {
		MeshContainer s = randomRunningServer();
		try {
			System.err.println("Disconnecting server from cluster " + s.getNodeName());
			s.dropTraffic();
		} catch (Exception e) {
			e.printStackTrace();
		}
		disconnectedServers.add(s);
	}

	private void startServer() throws InterruptedException {
		MeshContainer s = randomStoppedServer();
		System.err.println("Starting server: " + s.getNodeName());
		String name = s.getNodeName();
		String dataPrefix = s.getDataPathPostfix();
		stoppedServers.remove(s);

		MeshContainer server = addSlave(CLUSTERNAME + clusterPostFix, name, dataPrefix, false, WRITE_QUORUM);
		server.awaitStartup(STARTUP_TIMEOUT);
		server.login();
		runningServers.add(server);
	}

	private void addServer() throws InterruptedException {
		String name = randomName();
		System.err.println("Adding server: " + name);
		MeshContainer server = addSlave(CLUSTERNAME + clusterPostFix, name, name, false, WRITE_QUORUM);
		server.awaitStartup(STARTUP_TIMEOUT);
		server.login();
		runningServers.add(server);
	}

	private void killServer() {
		MeshContainer s = randomRunningServer();
		System.err.println("Killing server: " + s.getNodeName());
		s.killHardContainer();
		runningServers.remove(s);
		stoppedServers.add(s);
	}

	private void stopServer() {
		MeshContainer s = randomRunningServer();
		System.err.println("Stopping server: " + s.getNodeName());
		s.close();
		runningServers.remove(s);
		stoppedServers.add(s);
	}

	private void backupServer() {
		MeshContainer s = randomRunningServer();
		System.err.println("Invoking backup on server " + s.getNodeName());
		call(() -> s.client().invokeBackup());
		System.err.println("Invoked backup on server: " + s.getNodeName());
	}

	private void createUser() {
		MeshContainer s = randomRunningServer();
		System.err.println("Utilize server " + s.getNodeName());
		UserCreateRequest request = new UserCreateRequest();
		request.setPassword("somepass");
		request.setUsername(randomName());
		UserResponse response = call(() -> s.client().createUser(request));
		String uuid = response.getUuid();
		System.err.println("Using server: " + s.getNodeName() + " - Created user {" + uuid + "}");
		userUuids.add(uuid);
	}

	/**
	 * Allow removal and stopping if the server limit is reached or if the server is not alone and not in the first half of the actions.
	 * 
	 * @return
	 */
	private boolean allowStopOrRemoval() {
		boolean isAlone = runningServers.size() <= 1;
		boolean firstHalf = nAction < (TOTAL_ACTIONS / 2);
		boolean reachedLimit = runningServers.size() >= SERVER_LIMIT;
		return reachedLimit || (!isAlone && !firstHalf);
	}

	private void removeServer() {
		MeshContainer s = randomRunningServer();
		System.err.println("Removing server: " + s.getNodeName());
		s.stop();
		runningServers.remove(s);
	}

	public MeshContainer randomRunningServer() {
		int n = random.nextInt(runningServers.size());
		return runningServers.get(n);
	}

	public MeshContainer randomStoppedServer() {
		int n = random.nextInt(stoppedServers.size());
		return stoppedServers.get(n);
	}

	private void assertCluster() {
		for (MeshContainer server : runningServers) {
			System.out.println("Asserting server " + server.getNodeName());

			// Verify consistency
			ConsistencyCheckResponse report = call(() -> server.client().checkConsistency());
			assertEquals("The database in server {" + server.getNodeName() + "} is not consistent.", 0, report.getInconsistencies().size());

			// Verify that all created users can be found on the server
			for (String uuid : userUuids) {
				try {
					call(() -> server.client().findUserByUuid(uuid));
				} catch (AssertionError e) {
					e.printStackTrace();
					fail("Error while checking server {" + server.getNodeName() + "} and user {" + uuid + "}");
				}
			}

			// Only assert write when we are not in a split brain
			if (!hasSplitBrain) {
				// Increase the quorum until we reached the write quorum
				reachWriteQuorum();
				sleep(15_000);

				// Verify that node can still be created
				createNode(server);
			}
		}
	}

	private void reachWriteQuorum() {
		int missingNodes = missingNodesForWriteQuorum();
		if (missingNodes > 0) {
			for (int i = 0; i < missingNodes; i++) {
				try {
					System.out.println("Adding node to meet the write quorum requirements.");
					addServer();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private boolean meetsWriteQuorumRequirements() {
		return missingNodesForWriteQuorum() <= 0;
	}

	private int missingNodesForWriteQuorum() {
		return WRITE_QUORUM - runningServers.size();
	}

}