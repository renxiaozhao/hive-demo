package com.renxiaozhao.collect.hive;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext.HookType;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.renxiaozhao.collect.util.HiveKafkaUtil;


public class HiveHook implements ExecuteWithHookContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(HiveHook.class);

    // 存储Hive的SQL操作类型

    private static final HashSet<String> OPERATION_NAMES = new HashSet<>();

    // HiveOperation是一个枚举类，封装了Hive的SQL操作类型

    // 监控SQL操作类型

    static {

        // 建表

        OPERATION_NAMES.add(HiveOperation.CREATETABLE.getOperationName());

        // 修改数据库属性

        OPERATION_NAMES.add(HiveOperation.ALTERDATABASE.getOperationName());

        // 修改数据库属主

        OPERATION_NAMES.add(HiveOperation.ALTERDATABASE_OWNER.getOperationName());

        // 修改表属性,添加列

        OPERATION_NAMES.add(HiveOperation.ALTERTABLE_ADDCOLS.getOperationName());

        // 修改表属性,表存储路径

        OPERATION_NAMES.add(HiveOperation.ALTERTABLE_LOCATION.getOperationName());

        // 修改表属性

        OPERATION_NAMES.add(HiveOperation.ALTERTABLE_PROPERTIES.getOperationName());

        // 表重命名

        OPERATION_NAMES.add(HiveOperation.ALTERTABLE_RENAME.getOperationName());

        // 列重命名

        OPERATION_NAMES.add(HiveOperation.ALTERTABLE_RENAMECOL.getOperationName());

        // 更新列,先删除当前的列,然后加入新的列

        OPERATION_NAMES.add(HiveOperation.ALTERTABLE_REPLACECOLS.getOperationName());

        // 创建数据库

        OPERATION_NAMES.add(HiveOperation.CREATEDATABASE.getOperationName());

        // 删除数据库

        OPERATION_NAMES.add(HiveOperation.DROPDATABASE.getOperationName());

        // 删除表

        OPERATION_NAMES.add(HiveOperation.DROPTABLE.getOperationName());

    }

    @Override

    public void run(HookContext hookContext) throws Exception {

        assert (hookContext.getHookType() == HookType.POST_EXEC_HOOK);

        // 执行计划

        QueryPlan plan = hookContext.getQueryPlan();

        // 操作名称

        String operationName = plan.getOperationName();

        LOGGER.info("执行的SQL语句: " + plan.getQueryString());

        LOGGER.info("操作名称: " + operationName);

        if (OPERATION_NAMES.contains(operationName) && !plan.isExplain()) {

            LOGGER.info("监控SQL操作");

            Set<ReadEntity> inputs = hookContext.getInputs();

            Set<WriteEntity> outputs = hookContext.getOutputs();

            for (Entity entity : inputs) {

                LOGGER.info("Hook metadata输入值: " + toJson(entity));

            }
            for (Entity entity : outputs) {
                LOGGER.info("发送Hook metadata到Kafka，输出值: " + toJson(entity));
                HiveKafkaUtil.kafkaProducer("HIVE_HOOK", toJson(entity));

            }

        } else {
            LOGGER.info("不在监控范围，忽略该hook!");

        }

    }

    private static String toJson(Entity entity) throws Exception {

        ObjectMapper mapper = new ObjectMapper();

        //  entity的类型

        // 主要包括：

        // DATABASE, TABLE, PARTITION, DUMMYPARTITION, DFS_DIR, LOCAL_DIR, FUNCTION

        switch (entity.getType()) {

            case DATABASE:

                Database db = entity.getDatabase();

                return mapper.writeValueAsString(db);

            case TABLE:

                return mapper.writeValueAsString(entity.getTable().getTTable());

        }

        return null;

    }

}