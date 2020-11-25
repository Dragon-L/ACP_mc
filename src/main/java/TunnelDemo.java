import com.aliyun.odps.Odps;
import com.aliyun.odps.Table;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TableTunnel.*;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.TunnelRecordReader;

import java.io.IOException;

public class TunnelDemo {
    private static String project = "ACP_mc";
    private static String table = "person";
    private static String endpoint = "http://service.cn-shenzhen.maxcompute.aliyun.com/api";
    private static String accessId = "";
    private static String accessKey = "";

    public static void main(String[] args) throws TunnelException, IOException {

        Odps odps = initOdps(project, endpoint, accessId, accessKey);

        showTable(odps);

        TableTunnel tunnel = new TableTunnel(odps);

        tunnelUpload(project, table, tunnel);
        tunnelDownload(project, table, tunnel);
    }

    private static void tunnelUpload(String project, String table, TableTunnel tunnel) throws TunnelException, IOException {
        UploadSession session = tunnel.createUploadSession(project, table);
        RecordWriter writer = session.openRecordWriter(0);
        Record record = session.newRecord();
        record.setString(0, "小黑");
        record.setBigint(1, 20L);
        writer.write(record);
        writer.close();
        session.commit(new Long[]{0L});
    }

    private static void tunnelDownload(String project, String table, TableTunnel tunnel) {
        try {
            DownloadSession session = tunnel.createDownloadSession(project, table);
            System.out.println("Session status is: " + session.getStatus().toString());

            long count = session.getRecordCount();
            System.out.println("RecordCount is: "+ count);

            TunnelRecordReader reader = session.openRecordReader(0, count);
            Record record;
            while ((record = reader.read())!= null) {
                String job = record.getString("name");
                Long age = record.getBigint("age");
                System.out.println("job: " + job + "| age:" + age);
            }
        } catch (TunnelException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void showTable(Odps odps) {
        for (Table t: odps.tables()){
            String owner = t.getName();
            System.out.println(owner);
        }
    }

    private static Odps initOdps(String project, String endpoint, String accessId, String accessKey) {
        Account account = new AliyunAccount(accessId, accessKey);
        Odps odps = new Odps(account);
        odps.setEndpoint(endpoint);
        odps.setDefaultProject(project);
        return odps;
    }
}
