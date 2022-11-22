package org.apache.hudi.sink.transform;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hudi.adapter.RateLimiterAdapter;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.configuration.FlinkOptions;

/**
 * @author gaosh
 * @version 1.0
 * @since 11/22/22
 */
public class MyRowDataToHoodieFunctionWithRateLimit<I extends RowData, O extends HoodieRecord>
        extends MyRowDataToHoodieFunction<I, O> {

    /**
     * Total rate limit per second for this job.
     */
    private final double totalLimit;

    /**
     * Rate limit per second for per task.
     */
    private transient RateLimiterAdapter rateLimiter;

    public MyRowDataToHoodieFunctionWithRateLimit(RowType rowType, Configuration config) {
        super(rowType, config);
        this.totalLimit = config.getLong(FlinkOptions.WRITE_RATE_LIMIT);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.rateLimiter =
                RateLimiterAdapter.create(totalLimit / getRuntimeContext().getNumberOfParallelSubtasks());
    }

    @Override
    public O map(I i) throws Exception {
        rateLimiter.acquire();
        return super.map(i);
    }

}
