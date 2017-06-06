package com.mahout;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.canopy.CanopyDriver;
import org.apache.mahout.clustering.conversion.InputDriver;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.utils.clustering.ClusterDumper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UserCluster{

	private static final String DIRECTORY_CONTAINING_CONVERTED_INPUT = "data";
	
	public static void main(String[] args) throws Exception {
			
		//mahout的输出至HDFS的目录
		String outputPath = args[0];
		//mahout的输入目录，为clustr_input表的HDFS目录
		String inputPath = args[1];
		//Canopy算法的t1
		double t1 = Double.parseDouble(args[2]);
		//Canopy算法的t2
		double t2 = Double.parseDouble(args[3]);
		//收敛阀值
		double convergenceDelta = Double.parseDouble(args[4]);
		//最大迭代次数
		int maxIterations = Integer.parseInt(args[5]);
		
		Path output = new Path(outputPath);
		Path input = new Path(inputPath);
	    Configuration conf = new Configuration();
	    HadoopUtil.delete(conf, output);
	    run(conf, input, output,new EuclideanDistanceMeasure(),t1,t2,convergenceDelta,maxIterations);
		
	}
	
	public static void run(Configuration conf, Path input, Path output,
			DistanceMeasure measure, double t1, double t2,
			double convergenceDelta, int maxIterations) throws Exception {
		//,float fuzziness
		
		Path directoryContainingConvertedInput = new Path(output,
				DIRECTORY_CONTAINING_CONVERTED_INPUT);
		log.info("Preparing Input");
		InputDriver.runJob(input, directoryContainingConvertedInput,
				"org.apache.mahout.math.RandomAccessSparseVector");
		log.info("Running Canopy to get initial clusters");
		Path canopyOutput = new Path(output, "canopies");
		CanopyDriver.run(new Configuration(),
				directoryContainingConvertedInput, canopyOutput, measure, t1,
				t2, false, 0.0, false);
		log.info("Running KMeans");
		KMeansDriver.run(conf, directoryContainingConvertedInput, new Path(canopyOutput, Cluster.INITIAL_CLUSTERS_DIR + "-final"), output,
				measure, convergenceDelta, maxIterations, true, 0.0, false);
//		FuzzyKMeansDriver.run(directoryContainingConvertedInput, new Path(canopyOutput, "clusters-0-final"), output,
//			        measure, convergenceDelta, maxIterations, fuzziness, true, true, 0.0, false);
		log.info("run clusterdumper");

		ClusterDumper clusterDumper = new ClusterDumper(new Path(output,
				"clusters-*-final"), new Path(output, "clusteredPoints"));
		clusterDumper.printClusters(null);
	}

	private static final Logger log = LoggerFactory
			.getLogger(UserCluster.class);

}
