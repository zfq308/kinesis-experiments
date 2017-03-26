package expt;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;

/**
 * Created by tusharghosh on 27/02/17.
 */
public class CustomRecordProcessor implements IRecordProcessor {

    private static final CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();

    private static final long CHECKPOINT_INTERVAL_MS = 60000L;

    private InitializationInput initializationInput;

    private long checkPointInMs = 0L;

    @Override
    public void initialize(InitializationInput initializationInput) {
        this.initializationInput = initializationInput;
        System.out.println("Processing shard ID: " + this.initializationInput.getShardId());
    }

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
        int counter = 0;
        for (Record record : processRecordsInput.getRecords()) {
            try {
                System.out.println("[" + (++counter) + "] " + decoder.decode(record.getData()));
            }
            catch (CharacterCodingException e) {
                e.printStackTrace();
            }
        }

        Long timeInMs = System.currentTimeMillis();

        if (timeInMs > checkPointInMs) {
            checkpoint(processRecordsInput.getCheckpointer());
            checkPointInMs = timeInMs + CHECKPOINT_INTERVAL_MS;
        }

    }

    @Override
    public void shutdown(ShutdownInput shutdownInput) {
        System.out.println("Attempting shut down for shard ID: " + this.initializationInput.getShardId());
        if (shutdownInput.getShutdownReason() == ShutdownReason.TERMINATE) {
            checkpoint(shutdownInput.getCheckpointer());
        }
    }

    private void checkpoint(IRecordProcessorCheckpointer checkPointer) {
        try {
            checkPointer.checkpoint();
        }
        catch (InvalidStateException e) {
            e.printStackTrace();
        }
        catch (ShutdownException e) {
            e.printStackTrace();
        }
    }

}
