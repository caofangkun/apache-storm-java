package backtype.storm;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.yaml.snakeyaml.Yaml;

import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StormTopology;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.TupleImpl;
import backtype.storm.utils.Time;
import backtype.storm.utils.Utils;

public class CommonTesting {

	public static String localTempPath() {
		return System.getProperty("java.io.tmpdir") + "/"
				+ UUID.randomUUID().toString();
	}

	public static void mkdirs(String path) throws IOException {
		new File(path).mkdirs();
	}

	public static String getTestFilePath(String fileName) {
		URL url = CommonTesting.class.getClass().getResource("/" + fileName);
		return url.getPath();
	}

	public static Map loadTestDefaultConfig() throws Exception {
		InputStream in = CommonTesting.class.getClass().getResourceAsStream(
				"/defaults.yaml");
		if (in == null)
			throw new Exception("Can not find defaults.yaml");
		else {
			Yaml yaml = new Yaml();
			Map ret = (Map) yaml.load(new InputStreamReader(in));
			if (ret == null)
				ret = new HashMap();
			return new HashMap(ret);
		}
	}

	public static void deleteAll(List<String> paths) throws IOException {
		for (String path : paths) {
			File file = new File(path);
			if (file.exists()) {
				FileUtils.forceDelete(file);
			}
		}
	}

	public void startSimulatingTime() {
		Time.startSimulating();
	}

	public void stopSimulatingTime() {
		Time.stopSimulating();
	}

	// for test
	public static int byteToInt2(byte[] b) {

		int iOutcome = 0;
		byte bLoop;
		for (int i = 0; i < 4; i++) {
			bLoop = b[i];
			int off = (b.length - 1 - i) * 8;
			iOutcome += (bLoop & 0xFF) << off;
		}
		return iOutcome;
	}

	@ClojureClass(className = "backtype.storm.testing#test-tuple")
	public static TupleImpl testTuple(List<Object> values) throws Exception {
		String stream = Utils.DEFAULT_STREAM_ID;
		String component = "component";
		List<String> fs = new ArrayList<String>();
		for (int i = 0; i < values.size(); i++) {
			fs.add(String.valueOf(i));
		}
		Fields fields = new Fields(fs);
		Map<String, SpoutSpec> spouts = new HashMap<String, SpoutSpec>();
		SpoutSpec spoutSpec = new SpoutSpec();
		spouts.put(component, spoutSpec);
		// TODO
		StormTopology topology = new StormTopology(spouts, null, null);
		Map conf = loadTestDefaultConfig();
		Map<Integer, String> taskToComponent = new HashMap<Integer, String>();
		taskToComponent.put(1, component);
		Map<String, List<Integer>> componentToSortedTasks = new HashMap<String, List<Integer>>();
		componentToSortedTasks.put(component, Arrays.asList(1));
		Map<String, Map<String, Fields>> componentToStreamToFields = new HashMap<String, Map<String, Fields>>();
		Map<String, Fields> streamToFields = new HashMap<String, Fields>();
		streamToFields.put(stream, fields);
		componentToStreamToFields.put(component, streamToFields);
		TopologyContext context = new TopologyContext(topology, conf,
				taskToComponent, componentToSortedTasks,
				componentToStreamToFields, "test-storm-id", null, null, 1,
				null, Arrays.asList(1), null, null, new HashMap(),
				new HashMap(), new clojure.lang.Atom(false));

		return new TupleImpl(context, values, 1, stream);
	}

	// @ClojureClass(className = "backtype.storm.testing#mk-shared-context")
	// public static IContext mkSharedContext(Map conf) {
	// Boolean isLocalModeZMQ =
	// Utils.parseBoolean(conf.get(Config.STORM_LOCAL_MODE_ZMQ), false);
	// LocalContext context = null;
	// if (!isLocalModeZMQ) {
	// context = new LocalContext(null, null);
	// context.prepare(conf);
	// return context;
	// }
	// return null;
	//
	// }
}
