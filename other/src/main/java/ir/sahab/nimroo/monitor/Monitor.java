package ir.sahab.nimroo.monitor;


import ir.sahab.nimroo.Config;
import ir.sahab.nimroo.crawler.CrawlerLauncher;
import ir.sahab.nimroo.crawler.util.Language;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import org.apache.commons.io.input.Tailer;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class Monitor {

    NimrooTailer tailer;

    public static void main(String [] args) throws IOException, InterruptedException {
        Config.load();
        PropertyConfigurator.configure(CrawlerLauncher.class.getClassLoader().getResource("log4j.properties"));
        Logger logger = Logger.getLogger(Monitor.class);
        Monitor monitor = new Monitor();
        HashMap<Pattern, Consumer> handles = new HashMap<>();

        handles.put(Pattern.compile("\\[MONITOR\\] value:"), metric->{
            logger.error("Fucking works: " + metric);
        });

        monitor.tailer = new NimrooTailer(handles);
        Tailer tailer = Tailer.create(new File("/home/ali/tmp/log"), monitor.tailer, 50);
        while (true) {
            Thread.sleep(1000);
        }
    }

    public Monitor() throws IOException {

    }

    public void start() throws IOException {

    }

}

// server:filePath:regex