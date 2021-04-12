package org.example.state.operate;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.example.pojo.WaterSensor;
import org.example.source.WaterSensorSourceInOrder;

import java.util.*;

/**
 * @author vision
 * @version 1.0
 * @date 2021/4/8 16:17
 *
 * 通过列表状态记录前三最高水位
 */
public class FlinkListState {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new WaterSensorSourceInOrder(1))
                .process(new CustomProcessFunction())
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class CustomProcessFunction
        extends ProcessFunction<WaterSensor, String>
        implements CheckpointedFunction {
    private ListState<Integer> vcs;

    @Override
    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
        Integer vc = value.getVc();
        vcs.add(vc);
        List<Integer> vcList = new ArrayList<>();
        for (Integer integer : vcs.get()) {
            vcList.add(integer);
        }
        vcList.sort(Comparator.reverseOrder());
        if (vcList.size() == 4) {
            vcList.remove(3);
        }
        vcs.update(vcList);
        out.collect(vcList.toString());
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        vcs = context.getOperatorStateStore().getListState(new ListStateDescriptor<Integer>("vc", Integer.class));
    }
}
