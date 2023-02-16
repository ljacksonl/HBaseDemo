package com.lagou.hbase.processor;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

//重写prePut方法,监听到向t1表插入数据时，执行向t2表插入数据的代码
public class MyProcessor extends BaseRegionObserver {
    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        //把自己需要执行的逻辑定义在此处,向t2表插入数据，数据具体时什么内容与Put一样
        //获取t2表table对象
        HTable t2 = (HTable) e.getEnvironment().getTable(TableName.valueOf("t2"));

        //解析t1表的插入对象put
        Cell cell = put.get(Bytes.toBytes("info"), Bytes.toBytes("name")).get(0);

        //table对象.put
        Put put1 = new Put(put.getRow());
        put1.add(cell);
        t2.put(put1);//执行向t2表插入数据
        t2.close();
    }
}
