package com.creditscore.service;

import com.creditscore.model.ScoreResponse;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Optional;

@Service
public class ScoreService {

    private static final TableName TABLE = TableName.valueOf("realtime_scores");
    private static final byte[] CF_SCORE = Bytes.toBytes("score");
    private static final byte[] CF_META  = Bytes.toBytes("meta");
    private static final byte[] QUAL_PD1 = Bytes.toBytes("pd_1");
    private static final byte[] QUAL_TS  = Bytes.toBytes("ts");

    private final Connection connection;

    public ScoreService(Connection connection) {
        this.connection = connection;
    }

    public Optional<ScoreResponse> getScore(long skIdCurr) {
        String rowKey = String.valueOf(skIdCurr);

        try (Table table = connection.getTable(TABLE)) {
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addColumn(CF_SCORE, QUAL_PD1);
            get.addColumn(CF_META, QUAL_TS);

            Result result = table.get(get);
            if (result == null || result.isEmpty()) {
                return Optional.empty();
            }

            byte[] pd1Bytes = result.getValue(CF_SCORE, QUAL_PD1);
            byte[] tsBytes  = result.getValue(CF_META, QUAL_TS);

            if (pd1Bytes == null) {
                return Optional.empty();
            }

            String pd1Str = Bytes.toString(pd1Bytes);
            String tsStr  = tsBytes != null ? Bytes.toString(tsBytes) : null;

            double pd1 = Double.parseDouble(pd1Str);

            return Optional.of(new ScoreResponse(skIdCurr, pd1, tsStr));
        } catch (IOException | NumberFormatException e) {
            // TODO: có thể log lỗi chi tiết
            return Optional.empty();
        }
    }
}
