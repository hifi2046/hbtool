package hifi;

import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import java.util.*;
import com.google.gson.*;
import org.apache.commons.cli.*;
import org.apache.commons.cli.DefaultParser;
import java.sql.*;

/**
 * Hello world!
 *
 */

class Line {
	public String key;
	public ArrayList<Item> set;
	public Line(String k, ArrayList s) {
		key=k;
		set=new ArrayList(s);
	}
}

class Item {
	public String col;
	public String value;
	public Item(String c, String v) {
		col=c;
		value=v;
	}
}

public class App 
{
    public static String hbHost;
    public static String sqlHost;
    public static String sqlUser;
    public static String sqlPass;

    public static void loadConfig()
    {
        try {
            InputStream in=new BufferedInputStream(new FileInputStream("/home/hifi/tool/hbtool.properties"));
            Properties p = new Properties();
            p.load(in);
            hbHost=p.getProperty("hbase.host");
            sqlHost=p.getProperty("mysql.host");
            sqlUser=p.getProperty("mysql.user");
            sqlPass=p.getProperty("mysql.pass");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void queryName()
    {
    	try {
            Class.forName("com.mysql.jdbc.Driver");
            java.sql.Connection con = DriverManager.getConnection("jdbc:mysql://"+sqlHost+":3306", sqlUser, sqlPass);
    	    Statement stt = con.createStatement();
    	    ResultSet ret = stt.executeQuery("select CUSTOMER_NO from crm_customer where CUSTOMER_NAME like '%长电%'");
    	    while( ret.next()) {
    		System.out.println(ret.getString(1));
    	    }
    	    ret.close();
    	    con.close();
    	} catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void retrieveFulldayData()
    {
    	try {
            System.setProperty("zookeeper.sasl.client", "false");
            Configuration conf=HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", hbHost);
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            org.apache.hadoop.hbase.client.Connection conn=ConnectionFactory.createConnection(conf);
        	long start=System.currentTimeMillis();
            Table table=conn.getTable(TableName.valueOf("hy_data"));
            Admin admin=conn.getAdmin();
            String key="662067680601001_COS20210101";
            String cf="c";
            Get rk=new Get(key.getBytes());
            Result result=table.get(rk);
            List<Cell> list=result.listCells();
            System.out.println("Key: " + key);
            ArrayList<Item> olist=new ArrayList();
            for( Cell c : list) {
            	String col=new String(CellUtil.cloneQualifier(c));
            	String value=new String(CellUtil.cloneValue(c));
            	System.out.println("Col: " + col + ", Value: " + value);
            	olist.add(new Item(col, value));
            }
        
        	Gson gson=new GsonBuilder().setPrettyPrinting().create();
        	Line b=new Line(key, olist);
        	long now=System.currentTimeMillis();
        	System.out.println("query time ellapse: " + (now-start) + "ms");
        	BufferedWriter out=new BufferedWriter(new FileWriter("/home/hifi/pwork/fullday.json"));
        	out.write(gson.toJson(b));
        	out.close();
    	} catch (IOException e) {
    	    e.printStackTrace();
    	}
    }
    
    public static void main( String[] args )
    {
        for( String arg : args) {
            //System.out.println(arg);
        }
		Option opt1=new Option("c","cmd",true,"命令，包括：query/q, retrieve/r");
		opt1.setRequired(true);
		Option opt2=new Option("n","name",true,"企业名称，可以是部分名称");
		//opt2.setRequired(false);
		Option opt3=new Option("o","concode",true,"连接码");
		Option opt4=new Option("p","pointcode",true,"点码");
		Option opt5=new Option("m","metric",true,"指标");
		Option opt6=new Option("d","date",true,"日期，格式：YYYYMMDD 或 MMDD");

		Options options=new Options();
		options.addOption(opt1);
		options.addOption(opt2);
		options.addOption(opt3);
		options.addOption(opt4);
		options.addOption(opt5);
		options.addOption(opt6);

		CommandLine cli=null;
		CommandLineParser cliParser=new DefaultParser();
		HelpFormatter helpFormatter=new HelpFormatter();
    	try {
    		cli=cliParser.parse(options, args);
    		loadConfig();
    		long start=System.currentTimeMillis();
    		
    		String command=cli.getOptionValue("c","q");
    		if( command.equals("q") || command.equals("query") ) {
			    System.out.println("query");
    			queryName();
    		}
    		else if( command.equals("r") || command.equals("retrieve") ) {
			    System.out.println("retrieve");
        		retrieveFulldayData();
    		}
    		
    		long now=System.currentTimeMillis();
    		System.out.println("total time ellapse: " + (now-start) + "ms");
    	} catch (ParseException e) {
    		helpFormatter.printHelp(">>> hbtool options", options);
    		e.printStackTrace();
    	}

    }
}
