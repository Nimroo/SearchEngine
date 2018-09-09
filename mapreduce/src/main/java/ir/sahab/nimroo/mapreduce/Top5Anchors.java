package ir.sahab.nimroo.mapreduce;

import ir.sahab.nimroo.Config;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

public class Top5Anchors {

  private static Top5Anchors ourInstance = new Top5Anchors();
  public static Top5Anchors getInstance() {
    return ourInstance;
  }
  private static HashSet<String> uselessAnchors;

  private Top5Anchors() {
    uselessAnchors = new HashSet<>();
    uselessAnchors.add("link");
    uselessAnchors.add("this");
    uselessAnchors.add("site");
    uselessAnchors.add("click");
    uselessAnchors.add("ref");
    uselessAnchors.add("here");
    uselessAnchors.add(".");

    uselessAnchors.add("january");
    uselessAnchors.add("february");
    uselessAnchors.add("march");
    uselessAnchors.add("april");
    uselessAnchors.add("may");
    uselessAnchors.add("june");
    uselessAnchors.add("july");
    uselessAnchors.add("august");
    uselessAnchors.add("september");
    uselessAnchors.add("october");
    uselessAnchors.add("november");
    uselessAnchors.add("december");

    uselessAnchors.add("monday");
    uselessAnchors.add("tuesday");
    uselessAnchors.add("wednesday");
    uselessAnchors.add("thursday");
    uselessAnchors.add("friday");
    uselessAnchors.add("saturday");
    uselessAnchors.add("sunday");

    uselessAnchors.add("older posts");
    uselessAnchors.add("privacy policy");
    uselessAnchors.add("links to this post");
    uselessAnchors.add("no comments:");
    uselessAnchors.add("comment");
    uselessAnchors.add("comments");
    uselessAnchors.add("skip to sidebar");
    uselessAnchors.add("skip to main");
    uselessAnchors.add("(3)");
    uselessAnchors.add("(2)");
    uselessAnchors.add("(1)");
    uselessAnchors.add("(4)");
    uselessAnchors.add("(5)");
    uselessAnchors.add("(6)");
    uselessAnchors.add("(7)");
    uselessAnchors.add("(8)");
    uselessAnchors.add("(9)");
    uselessAnchors.add("(0)");

    uselessAnchors.add("reply");
    uselessAnchors.add("posts");
    uselessAnchors.add("post");
    uselessAnchors.add("click here");
    uselessAnchors.add("?");
    uselessAnchors.add("??");
    uselessAnchors.add("???");
    uselessAnchors.add("????");
    uselessAnchors.add("leave a comment");
    uselessAnchors.add("about");
    uselessAnchors.add("contact");
    uselessAnchors.add("contact us");
    uselessAnchors.add("home");
    uselessAnchors.add("edit");
    uselessAnchors.add("skip to content");
    uselessAnchors.add("blog");
    uselessAnchors.add("login");
    uselessAnchors.add("log in");
    uselessAnchors.add("log out");
    uselessAnchors.add("logout");
    uselessAnchors.add("about us");
    uselessAnchors.add("google");
    uselessAnchors.add("read more");
    uselessAnchors.add("events");
    uselessAnchors.add("visit");
    uselessAnchors.add("share");
    uselessAnchors.add("report");
    uselessAnchors.add("help");
    uselessAnchors.add("modify");
    uselessAnchors.add("email");
    uselessAnchors.add("terms");
    uselessAnchors.add("download");
    uselessAnchors.add("menu");
    uselessAnchors.add("faq");
    uselessAnchors.add("faqs");
  }

  public static class TopAnchorMapper extends TableMapper<BytesWritable, BytesWritable> {

    private int numRecords = 0;
    @Override
    public void map(ImmutableBytesWritable row, Result values, Context context) {
      BytesWritable link;
      BytesWritable anchor;
      for(Cell cell : values.listCells()){
        if(CellUtil.cloneQualifier(cell).equals(Bytes.toBytes("url")))
          continue;
        if(uselessAnchors.contains(Bytes.toString(CellUtil.cloneValue(cell))))
          continue;
        link = new BytesWritable(Bytes.toBytes(DigestUtils.md5Hex(Bytes.toString(CellUtil.cloneQualifier(cell)))));
        anchor = new BytesWritable(Bytes.toBytes(Bytes.toString(CellUtil.cloneValue(cell)).toLowerCase()));
        try {
          context.write(link,anchor);
        } catch (IOException | InterruptedException ignored) {
        }
      }
      numRecords++;
      if ((numRecords % 10000) == 0) {
        context.setStatus("mapper processed " + numRecords + " records so far");
      }
    }
  }

  public static class TopAnchorCombiner extends Reducer<BytesWritable, BytesWritable, BytesWritable, Pair<BytesWritable, LongWritable>> {
    HashMap<BytesWritable, Long> hashMap = new HashMap<>();
    public void reduce(BytesWritable key, Iterable<BytesWritable> values, Context context) {
      for(BytesWritable tmp : values){
        if(!hashMap.containsKey(tmp))
          hashMap.put(tmp, 1L);
        else
          hashMap.replace(tmp, hashMap.get(tmp) + 1);
      }
      hashMap.forEach((k, v) -> {
        LongWritable total = new LongWritable(v);
        Pair<BytesWritable, LongWritable> pair = new Pair<>(k,total);
        try {
          context.write(key, pair);
        } catch (IOException | InterruptedException ignored) {
        }
      });
    }
  }

  public static class TopAnchorReducer extends TableReducer<BytesWritable, Pair<BytesWritable, LongWritable>, BytesWritable> {
    HashMap<BytesWritable, Long> hashMap = new HashMap<>();
    @Override
    public void reduce(BytesWritable key, Iterable<Pair<BytesWritable, LongWritable>> values, Context context) {
      for(Pair<BytesWritable, LongWritable> tmp : values){
        if(!hashMap.containsKey(tmp.getFirst()))
          hashMap.put(tmp.getFirst(), tmp.getSecond().get());
        else
          hashMap.replace(tmp.getFirst(), hashMap.get(tmp.getFirst()) + tmp.getSecond().get());
      }
      Map.Entry<BytesWritable, Long> maxEntry;
      for (int i = 0; i < Math.min(5, hashMap.size()); i++) {
        maxEntry = null;
        for (Map.Entry<BytesWritable, Long> entry : hashMap.entrySet()) {
          if (maxEntry == null || entry.getValue().compareTo(maxEntry.getValue()) > 0) {
            maxEntry = entry;
          }
        }
        if (maxEntry == null) continue;
        hashMap.remove(maxEntry.getKey());
        Put put = new Put(key.getBytes());
        put.addColumn(Bytes.toBytes("anchor"), Bytes.toBytes(i), maxEntry.getKey().getBytes());
        try {
          context.write(key, put);
        } catch (IOException | InterruptedException ignored) {
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration config = HBaseConfiguration.create();
    config.addResource(new Path(Config.hBaseSite));
    config.addResource(new Path(Config.hadoopCoreSite));
    Job job = Job.getInstance(config, "Top5Anchors");
    job.setJarByClass(Top5Anchors.class);
    job.setCombinerClass(Top5Anchors.TopAnchorCombiner.class);

    Scan scan = new Scan();
    scan.setCaching(500);
    scan.setCacheBlocks(false);
    scan.addFamily(Bytes.toBytes("outLink"));
    scan.setStopRow(Bytes.toBytes("000001b267c1829f8e737d63d726dc00"));

    TableMapReduceUtil.initTableMapperJob(
        "crawler", scan, Top5Anchors.TopAnchorMapper.class, BytesWritable.class, BytesWritable.class, job);
    TableMapReduceUtil.initTableReducerJob("testAnchor", Top5Anchors.TopAnchorReducer.class, job);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}
