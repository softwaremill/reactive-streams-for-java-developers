package com.softwaremill.reactive;

import akka.NotUsed;
import akka.http.javadsl.model.ws.Message;
import akka.http.javadsl.model.ws.TextMessage;
import akka.http.javadsl.server.HttpApp;
import akka.http.javadsl.server.Route;
import akka.japi.Pair;
import akka.stream.FlowShape;
import akka.stream.Graph;
import akka.stream.SinkShape;
import akka.stream.ThrottleMode;
import akka.stream.UniformFanOutShape;
import akka.stream.alpakka.csv.javadsl.CsvParsing;
import akka.stream.alpakka.file.DirectoryChange;
import akka.stream.alpakka.file.javadsl.DirectoryChangesSource;
import akka.stream.javadsl.Broadcast;
import akka.stream.javadsl.FileIO;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.util.ByteString;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

import static akka.event.Logging.InfoLevel;
import static akka.stream.Attributes.createLogLevels;
import static java.util.stream.Collectors.averagingDouble;
import static java.util.stream.Collectors.toList;

public class CsvProcessor {

  private static final Config config = ConfigFactory.load();
  private static final Path DATA_DIR = Paths.get(config.getString("csv-processor.data-dir"));
  private static final FiniteDuration DATA_DIR_POLL_INTERVAL =
      FiniteDuration.fromNanos(config.getDuration("csv-processor.data-dir-poll-interval").toNanos());
  private static final double AVERAGE_THRESHOLD = config.getDouble("csv-processor.average-threshold");
  private static final int EMAIL_THRESHOLD = config.getInt("csv-processor.email-threshold");

  private static final Logger logger = Logger.getLogger("CsvProcessor");

  private final Source<Pair<Path, DirectoryChange>, NotUsed> newFiles =
      DirectoryChangesSource.create(DATA_DIR, DATA_DIR_POLL_INTERVAL, 128);

  private final Flow<Pair<Path, DirectoryChange>, Path, NotUsed> csvPaths =
      Flow.<Pair<Path, DirectoryChange>>create()
          .filter(this::isCsvFileCreationEvent)
          .log("new file", p -> p.first().getFileName())
          .map(Pair::first);

  private boolean isCsvFileCreationEvent(Pair<Path, DirectoryChange> p) {
    return p.first().toString().endsWith(".csv") && p.second().equals(DirectoryChange.Creation);
  }

  private final Flow<Path, ByteString, NotUsed> fileBytes =
      Flow.of(Path.class).flatMapConcat(FileIO::fromPath);


  private final Flow<ByteString, Collection<ByteString>, NotUsed> csvFields =
      Flow.of(ByteString.class).via(CsvParsing.lineScanner())
          .throttle(100, Duration.create(1, TimeUnit.SECONDS), 10, ThrottleMode.shaping());

  private final Flow<Collection<ByteString>, Reading, NotUsed> readings =
      Flow.<Collection<ByteString>>create().map(Reading::create);

  private final Flow<Reading, Double, NotUsed> averageReadings =
      Flow.of(Reading.class)
          .grouped(2)
          .mapAsyncUnordered(10, readings ->
              CompletableFuture.supplyAsync(() ->
                  readings.stream()
                      .map(Reading::getValue)
                      .collect(averagingDouble(v -> v)))
          )
          .filter(v -> v > AVERAGE_THRESHOLD)
          .log("greater than " + AVERAGE_THRESHOLD);

  private final Graph<FlowShape<Double, Double>, NotUsed> notifier = GraphDSL.create(builder -> {
    Sink<Double, NotUsed> mailerSink = Flow.of(Double.class)
        .grouped(EMAIL_THRESHOLD)
        .log("e-mail trigger")
        .to(Sink.foreach(ds ->
            logger.info("Sending e-mail")
        ));

    UniformFanOutShape<Double, Double> broadcast = builder.add(Broadcast.create(2));
    SinkShape<Double> mailer = builder.add(mailerSink);

    builder.from(broadcast.out(1)).toInlet(mailer.in());

    return FlowShape.of(broadcast.in(), broadcast.out(0));
  });

  private final Source<Double, NotUsed> liveReadings =
      newFiles
          .via(csvPaths)
          .via(fileBytes)
          .via(csvFields)
          .via(readings)
          .via(averageReadings)
          .via(notifier)
          .withAttributes(createLogLevels(InfoLevel(), InfoLevel(), InfoLevel()));

  public static void main(String[] args) throws IOException, InterruptedException, ExecutionException, TimeoutException {
    CsvProcessor csvProcessor = new CsvProcessor();
    Server server = new Server(csvProcessor.liveReadings);
    server.startServer(config.getString("server.host"), config.getInt("server.port"));
  }
}

class Server extends HttpApp {

  private final Source<Double, NotUsed> readings;

  Server(Source<Double, NotUsed> readings) {
    this.readings = readings;
  }

  @Override
  protected Route routes() {
    return route(
        path("data", () -> {
              Source<Message, NotUsed> messages = readings.map(String::valueOf).map(TextMessage::create);
              return handleWebSocketMessages(Flow.fromSinkAndSourceCoupled(Sink.ignore(), messages));
            }
        ),
        get(() ->
            pathSingleSlash(() ->
                getFromResource("index.html")
            )
        )
    );
  }
}

class Reading {

  private final int id;

  private final double value;

  private Reading(int id, double value) {
    this.id = id;
    this.value = value;
  }

  double getValue() {
    return value;
  }

  @Override
  public String toString() {
    return String.format("Reading(%d, %f)", id, value);
  }

  static Reading create(Collection<ByteString> fields) {
    List<String> fieldList = fields.stream().map(ByteString::utf8String).collect(toList());
    int id = Integer.parseInt(fieldList.get(0));
    double value = Double.parseDouble(fieldList.get(1));
    return new Reading(id, value);
  }
}