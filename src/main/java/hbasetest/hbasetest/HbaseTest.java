package hbasetest.hbasetest;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseTest {

	public static void main(String[] args) throws IOException {

		// Instantiating Configuration class
		Configuration config = HBaseConfiguration.create();
		//config.set("hbase.zookeeper.quorum", "Slave1");
		//config.set("hbase.zookeeper.property.clientPort", "2181");

		// Instantiating HTable class
		HTable hTable = new HTable(config, "ych0");

		// Instantiating Put class
		// accepts a row name.
		//Put p = new Put(Bytes.toBytes("row1"));

		// adding values using add() method
		// accepts column family name, qualifier/row name ,value
		//p.add(Bytes.toBytes("personal"), Bytes.toBytes("name"),
		//		Bytes.toBytes("raju"));

	//	p.add(Bytes.toBytes("personal"), Bytes.toBytes("city"),
				//Bytes.toBytes("hyderabad"));

		// p.add(Bytes.toBytes("professional"),Bytes.toBytes("designation"),
		// Bytes.toBytes("manager"));

		// p.add(Bytes.toBytes("professional"),Bytes.toBytes("salary"),
		// Bytes.toBytes("50000"));

		// Saving the put Instance to the HTable.
		//hTable.put(p);
		//System.out.println("data inserted");
		
		scanData(config,"ych0",hTable);

		// closing HTable
		hTable.close();
	}

	public static void createTable(HBaseConfiguration config, String tableName) {
		// Instantiating HbaseAdmin class
		// Instantiating table descriptor class
		try {
			System.out.println("start create table ......");
			HBaseAdmin hBaseAdmin = new HBaseAdmin(config);
			if (hBaseAdmin.tableExists(tableName)) {// ������Ҫ�����ı?��ô��ɾ���ٴ���
				hBaseAdmin.disableTable(tableName);
				hBaseAdmin.deleteTable(tableName);
				System.out.println(tableName + " is exist,detele....");
			}
			HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
			tableDescriptor.addFamily(new HColumnDescriptor("personal"));
			// tableDescriptor.addFamily(new HColumnDescriptor("column2"));
			// tableDescriptor.addFamily(new HColumnDescriptor("column3"));
			hBaseAdmin.createTable(tableDescriptor);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("end create table ......");
	}
	
	
	public static void insertData(Configuration config, String tableName) throws IOException {
		HTable hTable = new HTable(config, "tableName");

		// Instantiating Put class
		// accepts a row name.
		Put p = new Put(Bytes.toBytes("row1"));

		// adding values using add() method
		// accepts column family name, qualifier/row name ,value
		p.add(Bytes.toBytes("personal"), Bytes.toBytes("name"),
				Bytes.toBytes("raju"));

		p.add(Bytes.toBytes("personal"), Bytes.toBytes("city"),
				Bytes.toBytes("hyderabad"));

		// p.add(Bytes.toBytes("professional"),Bytes.toBytes("designation"),
		// Bytes.toBytes("manager"));

		// p.add(Bytes.toBytes("professional"),Bytes.toBytes("salary"),
		// Bytes.toBytes("50000"));

		// Saving the put Instance to the HTable.
		hTable.put(p);
		System.out.println("data inserted");

		// closing HTable
		hTable.close();
	}
	
	public static void scanData(Configuration config, String tableName,HTable table) throws IOException {
		//HTable table = new HTable(config, "emp");

	      // Instantiating the Scan class
	      Scan scan = new Scan();

	      // Scanning the required columns
	      scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("name"));
	      scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("city"));

	      // Getting the scan result
	      ResultScanner scanner = table.getScanner(scan);

	      // Reading values from scan result
	      for (Result result = scanner.next(); result != null; result = scanner.next())

	      System.out.println("Found row : " + result);
	      //closing the scanner
	      scanner.close();
	   }

}
