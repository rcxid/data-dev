package org.example.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * @author vision
 * @version 1.0
 * @date 2021/4/3 22:23
 */
public class CustomSource implements SourceFunction<User> {

    private final String host;
    private final int port;
    private BufferedReader reader;
    private Socket socket;
    private boolean cancel = false;
    private boolean stop = false;

    public CustomSource(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    public void run(SourceContext<User> sourceContext) throws Exception {
        socket = new Socket(host, port);
        reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
        String line;
        while (!cancel && (line = reader.readLine()) != null) {
            String[] split = line.split(",");
            if (split.length == 3) {
                try {
                    sourceContext.collect(new User(Long.valueOf(split[0]), split[1] ,Integer.valueOf(split[2])));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        stop = true;
    }

    @Override
    public void cancel() {
        cancel = true;
        while (!stop) {
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        try {
            if (reader != null) {
                reader.close();
            }
            if (socket != null) {
                socket.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
