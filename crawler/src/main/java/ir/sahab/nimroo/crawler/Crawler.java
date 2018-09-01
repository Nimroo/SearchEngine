package ir.sahab.nimroo.crawler;

import com.codahale.metrics.*;
import com.codahale.metrics.Timer;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import ir.sahab.nimroo.Config;
import ir.sahab.nimroo.connection.HttpRequest;
import ir.sahab.nimroo.crawler.cache.DummyDomainCache;
import ir.sahab.nimroo.crawler.cache.DummyUrlCache;
import ir.sahab.nimroo.crawler.parser.HtmlParser;
import ir.sahab.nimroo.crawler.util.Language;
import ir.sahab.nimroo.hbase.CrawlerRepository;
import ir.sahab.nimroo.kafka.KafkaHtmlProducer;
import ir.sahab.nimroo.kafka.KafkaLinkConsumer;
import ir.sahab.nimroo.model.PageData;
import ir.sahab.nimroo.serialization.PageDataSerializer;
import ir.sahab.nimroo.util.LinkNormalizer;
import javafx.util.Pair;
import org.apache.log4j.Logger;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class Crawler {



    private final MetricRegistry crawlerMetrics = new MetricRegistry();
    private final Meter dlMeter, parseMeter, crawlMeter, lruPassMeter, lruCheckMeter, enterCrawlMeter,
            langEnMeter, dlFailMeter, hbaseCheckMeter, urlCacheHitMeter, notHtml;
    private final Timer crawlTime, fetchTime, hbaseTime, domainCacheTime, urlCacheTime;
    private final Counter lruRejectCounter;
    private final Histogram refCount = crawlerMetrics.histogram(MetricRegistry.name(Crawler.class, "refCount"));
    private HtmlParser htmlParser;
    private KafkaLinkConsumer kafkaLinkConsumer;
    private KafkaHtmlProducer kafkaHtmlProducer = new KafkaHtmlProducer();
    private final DummyDomainCache dummyDomainCache = new DummyDomainCache(30000);
    private LinkShuffler linkShuffler = new LinkShuffler();
    private final BlockingQueue<String> links = new ArrayBlockingQueue<>(2000);

    private Logger logger = Logger.getLogger(Crawler.class);
    private ExecutorService executorService;
    private ScheduledExecutorService scraperService;
    private final DummyUrlCache dummyUrlCache = new DummyUrlCache();
    private AtomicLong count, prevCount = new AtomicLong(0);

    public Crawler() {
        executorService = new ThreadPoolExecutor(250, 250, 0L,
                TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(300));
        scraperService = new ScheduledThreadPoolExecutor(2);

        crawlMeter = crawlerMetrics.meter(MetricRegistry.name(Crawler.class, "page", "crawl"));
        parseMeter = crawlerMetrics.meter(MetricRegistry.name(Crawler.class, "page", "parse"));
        dlMeter = crawlerMetrics.meter(MetricRegistry.name(Crawler.class, "page", "fetch", "success"));
        dlFailMeter = crawlerMetrics.meter(MetricRegistry.name(Crawler.class, "page", "fetch", "fail"));
        notHtml = crawlerMetrics.meter(MetricRegistry.name(Crawler.class, "page", "fetch", "notHtml"));
        lruPassMeter = crawlerMetrics.meter(MetricRegistry.name(Crawler.class, "lru", "pass"));
        lruCheckMeter = crawlerMetrics.meter(MetricRegistry.name(Crawler.class, "lru", "check"));
        enterCrawlMeter = crawlerMetrics.meter(MetricRegistry.name(Crawler.class, "crawl", "enter"));
        langEnMeter = crawlerMetrics.meter(MetricRegistry.name(Crawler.class, "language", "en"));
        hbaseCheckMeter = crawlerMetrics.meter(MetricRegistry.name(Crawler.class, "hbase", "markCheck"));
        urlCacheHitMeter = crawlerMetrics.meter(MetricRegistry.name(Crawler.class, "cache", "url", "hit"));
        crawlerMetrics.register(MetricRegistry.name(Crawler.class, "kafka", "queue", "links",
                "size"), (Gauge<Integer>) links::size);

        lruRejectCounter = crawlerMetrics.counter(MetricRegistry.name(Crawler.class, "page", "lru", "reject"));
        crawlTime = crawlerMetrics.timer(MetricRegistry.name(Crawler.class, "time", "crawl"));
        fetchTime = crawlerMetrics.timer(MetricRegistry.name(Crawler.class, "time", "fetch"));
        hbaseTime = crawlerMetrics.timer(MetricRegistry.name(Crawler.class, "time", "hbaseCheck"));
        domainCacheTime = crawlerMetrics.timer(MetricRegistry.name(Crawler.class, "time", "domainCache"));
        urlCacheTime = crawlerMetrics.timer(MetricRegistry.name(Crawler.class, "time", "urlCache"));


//        ConsoleReporter reporter = ConsoleReporter.forRegistry(crawlerMetrics)
//                .convertRatesTo(TimeUnit.SECONDS)
//                .convertDurationsTo(TimeUnit.MILLISECONDS)
//                .build();
//        reporter.start(30, TimeUnit.SECONDS);

        Graphite graphite = new Graphite(new InetSocketAddress(Config.server1Address, 2003));
        GraphiteReporter graphiteReporter = GraphiteReporter.forRegistry(crawlerMetrics)
                .prefixedWith("test")
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .filter(MetricFilter.ALL)
                .build(graphite);
        graphiteReporter.start(10, TimeUnit.SECONDS);

    }

    public void start() throws InterruptedException {
        HttpRequest.init();
        kafkaLinkConsumer = new KafkaLinkConsumer(Config.kafkaLinkTopicName);
        executorService.submit(linkShuffler);

        scraperService.scheduleAtFixedRate(()-> {
            logger.info("Start scrapping");
            // TODO Fix this !!!
//            synchronized (dummyDomainCache) {
//                dummyDomainCache.scrap();
//            }

            synchronized (dummyUrlCache) {
                dummyUrlCache.scrap();
            }
            logger.info("Done scrapping");
        }, 1, 15, TimeUnit.MINUTES);

//        scraperService.scheduleAtFixedRate(()-> {
//            logger.info("rate in 1 minute: " + (count.get() - prevCount.get() / 60));
//            prevCount.set(count.get());
//        }, 1, 1, TimeUnit.MINUTES); // TODO Check this out !!! wasn't working why ?!


        while (true) {
            ArrayList<String> list = kafkaLinkConsumer.get();

            for (int i = 0; i < list.size(); i++) {
                String link = list.get(i);
                lruCheckMeter.mark();
                Timer.Context domainCacheCtx = domainCacheTime.time();
                if (!dummyDomainCache.add(link, System.currentTimeMillis())) {
                    lruRejectCounter.inc();
                    linkShuffler.submitLink(link);
                    domainCacheCtx.stop();
                    continue;
                }
                domainCacheCtx.stop();
                lruPassMeter.mark();

                while (true) {
                    try {
                        executorService.submit(() -> {
                            crawl(link);
                        });
                        break;
                    } catch (RejectedExecutionException e) {
                        Thread.sleep(2000);
                    }
                }
            }

        }
    }

    private void crawl(String link) {
        enterCrawlMeter.mark();
        final Timer.Context crawlTimeCtx = crawlTime.time();
        logger.info("Link: " + link);

        String response = null;
        final Timer.Context timeContext = fetchTime.time();

        HttpRequest httpRequest1 = new HttpRequest(link);
        httpRequest1.setMethod(HttpRequest.HTTP_REQUEST.GET);
        List<Pair<String, String>> headers = new ArrayList<>();
        headers.add(new Pair<>("accept", "text/html,application/xhtml+xml,application/xml"));
        headers.add(new Pair<>("user-agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Safari/537.36"));
        httpRequest1.setHeaders(headers);
        httpRequest1.setRequestTimeout(15000);

        try {
            response = httpRequest1.send().get().getResponseBody();
        } catch (InterruptedException | ExecutionException e) {
            timeContext.close();
            crawlTimeCtx.close();
            dlFailMeter.mark();
            return;
//            logger.error(e); // TODO separate logfile.
        }

        if (response == null || response.length() == 0) {
            notHtml.mark();
            crawlTimeCtx.close();
            return;
        }
        dlMeter.mark();
        timeContext.stop();

        PageData pageData = null;
        htmlParser = new HtmlParser();
        pageData = htmlParser.parse(link, response);
        parseMeter.mark();

        if (!Language.getInstance().detector(pageData.getText().substring(0, java.lang.Math.min(pageData.getText().length(), 1000)))) {
            return;
        }
        langEnMeter.mark();

        byte[] bytes = PageDataSerializer.getInstance().serialize(pageData);
        kafkaHtmlProducer.send(Config.kafkaHtmlTopicName, pageData.getUrl(), bytes); //todo topic

        refCount.update(pageData.getLinks().size());
        for (int i = 0; i < pageData.getLinks().size(); i+=10) { // Attention only 10% of links are passing !!!
            String pageLink = pageData.getLinks().get(i).getLink();
            pageLink = LinkNormalizer.getSimpleUrl(pageLink);
            Timer.Context urlCacheTimerCtx = urlCacheTime.time();
            if (!dummyUrlCache.add(pageLink)) {
                urlCacheHitMeter.mark();
                urlCacheTimerCtx.stop();
                continue;
            }
            urlCacheTimerCtx.stop();

            hbaseCheckMeter.mark();
            final Timer.Context hbaseTimeCtx = hbaseTime.time();
            if (!CrawlerRepository.getInstance().isDuplicateUrl(pageLink)) {
                logger.info("checked with hBase->  link: " + pageLink);
                linkShuffler.submitLink(pageLink);
            }
            hbaseTimeCtx.stop();
        }

        crawlMeter.mark();
        crawlTimeCtx.stop();
        count.addAndGet(1);
    }
}
