package org.lmdbjava;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static java.nio.ByteBuffer.allocateDirect;
import static org.lmdbjava.ByteBufferProxy.PROXY_OPTIMAL;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.Env.create;

public class MyTest {

    public static final String lmdbPath = "E:\\SVN\\lm-db";

    File path = new File(lmdbPath);

    Env<ByteBuffer> env = create(PROXY_OPTIMAL)
            .setMapSize(256 * 1024 * 1024)
            .setMaxDbs(1)
            .setMaxReaders(2)
            .open(path);

//    final ByteBuffer bb_key = allocateDirect(env.getMaxKeySize());
//    final ByteBuffer bb_val = allocateDirect(700);

    /**
     * 向库中插入数据
     *
     * @param dbName
     * @param key
     * @param value
     */
    public void putValueToLmDb(String dbName, String key, byte[] value) {
        final Dbi<ByteBuffer> db = env.openDbi(dbName, MDB_CREATE);
        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            ByteBuffer bb_key = bb(key);
            ByteBuffer bb_val = bb(value);
            // 第一种
//            db.put(txn, bb_key, bb_val);
            // 第二种
            Cursor<ByteBuffer> cursor = db.openCursor(txn);
            cursor.put(bb_key, bb_val);
            // 记得关闭缓冲区
            cursor.close();
            // 最后都必须提交
            txn.commit();
        }
    }

    /**
     * 根据指定的key获取数据
     *
     * @param dbName
     * @param key
     * @return
     */
    public byte[] getValueByKey(String dbName, String key) {
        final Dbi<ByteBuffer> db = env.openDbi(dbName, MDB_CREATE);
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            ByteBuffer bb_key = bb(key);
            // 第一种
//            ByteBuffer value = db.get(txn, bb_key);
            // 第二种
            Cursor<ByteBuffer> cursor = db.openCursor(txn);
            cursor.get(bb_key, GetOp.MDB_SET_KEY);
            ByteBuffer value = cursor.val();
            // 记得关闭缓冲区
            cursor.close();
            // 转换
            byte[] v = new byte[value.capacity()];
            value.get(v);
            return v;
        }
    }

    /**
     * 获取库下所有的数据
     *
     * @param dbName
     * @return
     */
    public Map<String, byte[]> getAllValueByDbName(String dbName) {
        final Dbi<ByteBuffer> db = env.openDbi(dbName, MDB_CREATE);
        Map<String, byte[]> map = new HashMap<>();
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            Cursor<ByteBuffer> cursor = db.openCursor(txn);
            while (cursor.next()) {
                ByteBuffer key = cursor.key();
                ByteBuffer val = cursor.val();
                byte[] k = new byte[key.capacity()];
                byte[] v = new byte[val.capacity()];
                key.get(k);
                val.get(v);
                map.put(new String(k), v);
            }
            cursor.close();
        }
        return map;
    }

    /**
     * 格式化 ByteBuffer
     *
     * @param value
     * @return
     */
    static ByteBuffer bb(final String value) {
        byte[] val = value.getBytes();
        final ByteBuffer bb = allocateDirect(val.length);
        bb.put(val).flip();
        return bb;
    }

    /**
     * 格式化 ByteBuffer
     *
     * @param value
     * @return
     */
    static ByteBuffer bb(final byte[] value) {
        final ByteBuffer bb = allocateDirect(value.length);
        bb.put(value).flip();
        return bb;
    }

    public static void main(String[] args) {
        MyTest myTest = new MyTest();
        String dbName = "woceshi1";
        String key1 = "wodeceshiyaoshi1";
        String key2 = "wodeceshiyaoshi2";
        String key3 = "wodeceshiyaoshi3";
        myTest.putValueToLmDb(dbName, key1, "wodeceshishuju1".getBytes());
        myTest.putValueToLmDb(dbName, key2, "wodeceshishuju2".getBytes());
        myTest.putValueToLmDb(dbName, key3, "wodeceshishuju3".getBytes());
        Map<String, byte[]> map = myTest.getAllValueByDbName(dbName);
        for (Map.Entry<String, byte[]> entry : map.entrySet()) {
            String key = entry.getKey();
            byte[] value = entry.getValue();
            String s = new String(value);
            System.out.println("key = " + key);
            System.out.println("val = " + s);
        }
        byte[] valueByKey = myTest.getValueByKey(dbName, key2);
        String s = new String(valueByKey);
        System.out.println("getvalue = " + s);
    }

}
