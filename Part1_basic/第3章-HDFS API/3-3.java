import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class FileCopyFromLocal {
	 public static void main(String[] args) throws IOException {
		//本地文件路径
		String source = "/home/hadoop/test";
		String destination = "hdfs://master:9000/user/hadoop/test2";
		InputStream in = new BufferedInputStream(new FileInputStream(source));
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(destination),conf);
		OutputStream out = fs.create(new Path(destination));
		IOUtils.copyBytes(in, out, 4096,true);
	 }
}
