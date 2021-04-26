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

    public static void queryName(String name)
    {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            java.sql.Connection con = DriverManager.getConnection("jdbc:mysql://"+sqlHost+":3306/iecp", sqlUser, sqlPass);
            Statement stt = con.createStatement();
            ResultSet ret = stt.executeQuery("select CUSTOMER_NO from crm_customer where CUSTOMER_NAME like '%"+name+"%'");
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

//    public static void queryLine(java.sql.Connection con, String prefix, String fatherid)
    public static void queryLine(java.sql.Connection con, String prefix, String fathercode)
    {
        try {
            Statement stt = con.createStatement();
            ResultSet ret = stt.executeQuery("select F1001_SID, F1001_DESC, F1001_CONCODE from tb1001_container where F1001_FATHERCODE='" + fathercode + "'");
            while( ret.next()) {
                System.out.println(prefix + ret.getString(1) + " | " + ret.getString(2) + " | " + ret.getString(3));
                queryLine(con, prefix+"  ", ret.getString(3));
            }
            ret.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    
    public static void queryTree(String concode)
    {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            java.sql.Connection con = DriverManager.getConnection("jdbc:mysql://"+sqlHost+":3306/iecp", sqlUser, sqlPass);
            Statement stt = con.createStatement();
//            ResultSet ret = stt.executeQuery("select F1001_SID, F1001_DESC, F1001_CONCODE from tb1001_container where F1001_SID='" + concode + "'");
            ResultSet ret = stt.executeQuery("select F1001_SID, F1001_DESC, F1001_CONCODE from tb1001_container where F1001_CONID=" + concode);
            while( ret.next()) {
                System.out.println(ret.getString(1) + " | " + ret.getString(2) + " | " + ret.getString(3));
                queryLine(con, "  ", ret.getString(3));
            }
            ret.close();
            con.close();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    
    public static void queryPointCode(String pointhead)
    {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            java.sql.Connection con = DriverManager.getConnection("jdbc:mysql://"+sqlHost+":3306/iecp", sqlUser, sqlPass);
            Statement stt = con.createStatement();
            System.out.println("anapoint:");
            ResultSet ret = stt.executeQuery("select F4007_POINTCODE, F4007_POINTDESC from tb4007_anapoint where F1001_CONCODE='" + pointhead + "'");
            while( ret.next()) {
                System.out.println(ret.getString(1) + " | " + ret.getString(2));
            }
            ret.close();
            System.out.println("digpoint:");
            ret = stt.executeQuery("select F4009_POINTCODE, F4009_POINTDESC from tb4009_digpoint where F1001_CONCODE='" + pointhead + "'");
            while( ret.next()) {
                System.out.println(ret.getString(1) + " | " + ret.getString(2));
            }
            ret.close();
            System.out.println("accpoint:");
            ret = stt.executeQuery("select F4014_POINTCODE, F4014_POINTDESC from tb4014_accpoint where F1001_CONCODE='" + pointhead + "'");
            while( ret.next()) {
                System.out.println(ret.getString(1) + " | " + ret.getString(2));
            }
            ret.close();
            con.close();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void retrieveSingle(String pointcode, String date)
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
//            String key="662067680601001_COS20210101";
            String key=pointcode+date;
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
    
    public static void retrieveMultiple()
    {
    }

    public static void retrieveTotal()
    {
    }

    public static void summaryDay()
    {
    }

    public static void summaryArea()
    {
    }

    public static void summaryMonth()
    {
    }

    public static void summaryQuarter()
    {
    }

    public static void summaryYear()
    {
    }

    public static void main( String[] args )
    {
        for( String arg : args) {
            //System.out.println(arg);
        }
        Option opt1=new Option("c","cmd",true,"命令，包括：queryname/qn, querytree/qt, querypointcode/qp, retrievesingle/rs, retrievemultiple/rm, retrievetotal/rt, summaryday/sd, summaryarea/sa, summarymonth/sm, summaryquarter/sq, summaryyear/sy");
        opt1.setRequired(true);
        Option opt2=new Option("n","name",true,"企业名称，可以是部分名称");
        //opt2.setRequired(false);
        Option opt3=new Option("o","concode",true,"连接码");
        Option opt4=new Option("p","pointcode",true,"点码");
        Option opt5=new Option("h","pointhead",true,"点码头");
        Option opt6=new Option("m","metric",true,"指标");
        Option opt7=new Option("d","date",true,"日期，格式：YYYYMMDD 或 MMDD");

        Options options=new Options();
        options.addOption(opt1);
        options.addOption(opt2);
        options.addOption(opt3);
        options.addOption(opt4);
        options.addOption(opt5);
        options.addOption(opt6);
        options.addOption(opt7);

        CommandLine cli=null;
        CommandLineParser cliParser=new DefaultParser();
        HelpFormatter helpFormatter=new HelpFormatter();
        try {
            cli=cliParser.parse(options, args);
            loadConfig();
            long start=System.currentTimeMillis();
            
            String command=cli.getOptionValue("c","qn");
            if( command.equals("qn") || command.equals("queryname") ) {
                System.out.println("query name");
                String name=cli.getOptionValue("n","长电");
                queryName(name);
            }
            else if( command.equals("qt") || command.equals("querytree") ) {
                System.out.println("query tree");
                String concode=cli.getOptionValue("o","1600019916");
                queryTree(concode);
            }
            else if( command.equals("qp") || command.equals("querypointcode") ) {
                System.out.println("query point code");
                String pointhead=cli.getOptionValue("h","680006200301001");
                queryPointCode(pointhead);
            }
            else if( command.equals("rs") || command.equals("retrievesingle") ) {
                System.out.println("retrieve single");
                String pointcode=cli.getOptionValue("p","680006200301001_COS");
                String date=cli.getOptionValue("d","20210101");
                if (date.length()==4) {
                    date="2021"+date;
                }
                retrieveSingle(pointcode, date);
            }
            else if( command.equals("rm") || command.equals("retrievemultiple") ) {
                System.out.println("retrieve multiple");
                retrieveMultiple();
            }
            else if( command.equals("rt") || command.equals("retrievetotal") ) {
                System.out.println("retrieve total");
                retrieveTotal();
            }
            else if( command.equals("sd") || command.equals("summaryday") ) {
                System.out.println("summary day");
                summaryDay();
            }
            else if( command.equals("sa") || command.equals("summaryarea") ) {
                System.out.println("summary area");
                summaryArea();
            }
            else if( command.equals("sm") || command.equals("summarymonth") ) {
                System.out.println("summary month");
                summaryMonth();
            }
            else if( command.equals("sq") || command.equals("summaryquarter") ) {
                System.out.println("summary quarter");
                summaryQuarter();
            }
            else if( command.equals("sy") || command.equals("summaryyear") ) {
                System.out.println("summary year");
                summaryYear();
            }
            
            long now=System.currentTimeMillis();
            System.out.println("total time ellapse: " + (now-start) + "ms");
        } catch (ParseException e) {
            helpFormatter.printHelp(">>> hbtool options", options);
            e.printStackTrace();
        }

    }
}
