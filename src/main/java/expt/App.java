package expt;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;

/**
 * Created by tusharghosh on 13/03/17.
 */
public class App {

    final private static String APP_NAME = "kinesis_expt_app";

    final private static String APP_STREAM_NAME = "";

    final private static String AWS_REGION_NAME = "us-east-1";

    final private static InitialPositionInStream APP_INITIAL_POSITION = InitialPositionInStream.LATEST;

    private static AWSCredentialsProvider credentialsProvider;

    public static void main(String[] args) throws Exception {
        java.security.Security.setProperty("networkaddress.cache.ttl", "60");
        load();

        KinesisClientLibConfiguration config = new KinesisClientLibConfiguration(APP_NAME, APP_STREAM_NAME, credentialsProvider, getWorkId());
        config.withRegionName(AWS_REGION_NAME);
        config.withInitialPositionInStream(APP_INITIAL_POSITION);

        CustomRecordProcessorFactory customRecordProcessorFactory = new CustomRecordProcessorFactory();
        Worker worker = new Worker.Builder()
                .recordProcessorFactory(customRecordProcessorFactory)
                .config(config)
                .build();

        System.out.println("Attempting to run " + APP_NAME + " for processing " + APP_STREAM_NAME);

        try {
            worker.run();
        } catch (Throwable t) {
            t.printStackTrace();
        }

    }

    private static void load() {
        credentialsProvider = new DefaultAWSCredentialsProviderChain();
        try {
            credentialsProvider.getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException("Failed to load credentials");
        }
    }

    private static String getWorkId() throws UnknownHostException {
        return InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
    }

}
