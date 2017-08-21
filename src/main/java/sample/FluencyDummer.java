package sample;

import org.komamitsu.fluency.Fluency;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchService;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class FluencyDummer {
    private static final Logger log = LoggerFactory.getLogger(FluencyDummer.class);
    private ExecutorService executor;
    private final Fluency fluency;
    private final Path path;

    public FluencyDummer(String host, Integer port, Path path) throws IOException {
        log.info("{}:{} {}", host, port, path);
        Fluency.Config conf = new Fluency.Config()
                .setMaxBufferSize(Long.valueOf(1024 * 1024 * 1024L)); // 1GB
        this.fluency = Fluency.defaultFluency(host, port, conf);
        this.path = path;
    }

    public static void main(String... args) throws IOException, InterruptedException {
        final FluencyDummer dummer = new FluencyDummer(
                args[0],
                Integer.parseInt(args[1]),
                Paths.get(args[2])
        );
        dummer.run();
    }

    public void run() {
        try (BufferedReader reader = Files.newBufferedReader(path, Charset.forName("UTF-8"))) {
            reader.lines().forEach(line -> emitEvent(line));
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    private void emitEvent(String line) {
        String tag = "dummy";
        Map<String, Object> event = new HashMap<>();
        event.put("message", line);
        try {
            fluency.emit(tag, event);
        } catch (IOException e) {
            System.out.println(e.getMessage());
            log.error("Failed to emit event", e);
        }
        log.debug("buffered data size: {}", fluency.getBufferedDataSize());
    }
}
