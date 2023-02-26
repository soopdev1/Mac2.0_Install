/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.so.db;

import rc.so.engine.Oper;
import rc.so.util.Aggiornamenti_mod;
import rc.so.util.Rate_history;
import rc.so.util.Util;
import static rc.so.util.Util.generaId;
import static rc.so.util.Util.parseStringDate;
import static rc.so.util.Util.patternsqldate;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;
import org.joda.time.DateTime;
import static rc.so.util.Util.rb;

/**
 *
 * @author srotella
 */
public class DBHost {

    private Connection conn = null;

    private final String user = "maccorp";
    private final String pwd = "M4cc0Rp";

    public DBHost(String host) {
        String drivername = rb.getString("db.driver");
        String typedb = rb.getString("db.tipo");
        try {
            Class.forName(drivername).newInstance();
            Properties p = new Properties();
            p.put("user", user);
            p.put("password", pwd);
            p.put("useUnicode", "true");
            p.put("characterEncoding", "UTF-8");
            p.put("useSSL", "false");
            p.put("connectTimeout", "1000");
            p.put("useUnicode", "true");
            p.put("useJDBCCompliantTimezoneShift", "true");
            p.put("useLegacyDatetimeCode", "false");
            p.put("serverTimezone", "Europe/Rome");
            this.conn = DriverManager.getConnection("jdbc:" + typedb + ":" + host, p);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public DBHost(Connection conn) {
        try {
            this.conn = conn;
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public Connection getConnectionDB() {
        return conn;
    }

    public void closeDB() {
        try {
            if (conn != null) {
                this.conn.close();
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public ResultSet getValuteForMonitor() {
        try {
            Statement stmt = conn.createStatement();
            return stmt.executeQuery("SELECT valuta, de_valuta, cambio_bce, buy_std_type, buy_std_value, buy_std, sell_std_value, sell_std_type, sell_std "
                    + "FROM valute where fg_valuta_corrente='0'");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public String getPathImgMonitor() {
        String path = "";
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT valore FROM configmonitor where id='pathftp'");
            if (rs.next()) {
                path = rs.getString("valore");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return path;
    }

    public String getDataITA() {
        String data = "";
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select DATE_FORMAT(NOW(),'%d/%m/%Y %H:%i:%S') as data");
            if (rs.next()) {
                data = rs.getString("data");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return data;
    }

    public int get_national_office_minutes() {
        try {
            String sql = "SELECT minutes FROM office";
            PreparedStatement ps = this.conn.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                int v = Integer.parseInt(rs.getString(1));
                return v;
            }
        } catch (NumberFormatException | SQLException ex) {
            ex.printStackTrace();
        }
        return 0;
    }

    public DateTime now() {
        try {
            String sql = "SELECT now()";
            PreparedStatement ps = this.conn.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return parseStringDate(rs.getString(1), patternsqldate);
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public DateTime get_last_date_blocked() {
        try {
            String sql = "SELECT timestamp FROM block_it_et WHERE data = curdate() AND status = ?";
            PreparedStatement ps = this.conn.prepareStatement(sql);
            ps.setString(1, "1");
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return parseStringDate(rs.getString(1), patternsqldate);
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public boolean updateBlockedOperation(String user, String status) {
        try {
            String upd = "UPDATE block_it_et SET user = ?, status = ? WHERE data = curdate()";
            PreparedStatement ps = this.conn.prepareStatement(upd);
            ps.setString(1, user);
            ps.setString(2, status);
            return ps.executeUpdate() > 0;
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return false;
    }

    public String[] getCodLocal(boolean onlycod) {
        try {
            String sql = "SELECT cod FROM local LIMIT 1";
            PreparedStatement ps = this.conn.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                String cod = rs.getString(1);
                if (onlycod) {
                    String[] out = {cod, cod};
                    return out;
                }
                String sql2 = "SELECT de_branch FROM maccorp.branch WHERE cod = ?";
                PreparedStatement ps2 = this.conn.prepareStatement(sql2);
                ps2.setString(1, cod);
                ResultSet rs2 = ps2.executeQuery();
                if (rs2.next()) {
                    String[] out = {cod, rs2.getString(1)};
                    return out;
                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public ArrayList<String[]> list_aggiornamenti_mod(String filiale, String stato) {
        ArrayList<String[]> li = new ArrayList<>();
        try {
            String sql = "SELECT cod,dt_start,tipost,action FROM aggiornamenti_mod where filiale = '" + filiale + "' AND fg_stato='" + stato + "' ORDER BY timestamp,cod";
            ResultSet rs = this.conn.createStatement().executeQuery(sql);
            while (rs.next()) {
                String[] output = {rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4)};
                li.add(output);
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return li;
    }

    public boolean execute_agg_copia(String cod, String filiale, String dt_start, String type, String action, String user) {
        try {
            String sqlins = "INSERT INTO aggiornamenti_mod VALUES (?,?,?,?,?,?,?,?)";
            PreparedStatement ps = this.conn.prepareStatement(sqlins);
            ps.setString(1, cod);
            ps.setString(2, filiale);
            ps.setString(3, dt_start);
            ps.setString(4, "0");
            ps.setString(5, type);
            ps.setString(6, action);
            ps.setString(7, user);
            ps.setString(8, now().toString("yyyy-MM-dd HH:mm:ss"));

            int x = ps.executeUpdate();
            return x > 0;

        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return false;

    }

    public boolean execute_agg(String type, Oper oper) {
        try {
            if (type.equalsIgnoreCase("PS")) {
                PreparedStatement ps = this.conn.prepareStatement(oper.getSql());
                for (int i = 0; i < oper.getParam().size(); i++) {
                    ps.setString(i + 1, oper.getParam().get(i));
                }
                if (oper.getType().equals("UPD")) {
                    return ps.executeUpdate() > 0;
                } else if (oper.getType().equals("DEL") || oper.getType().equals("INS")) {
                    ps.execute();
                    return true;
                }
            } else if (type.equalsIgnoreCase("ST")) {
                this.conn.createStatement().executeUpdate(oper.getSql());
                return true;
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return false;
    }

    public void setStatus_agg(String cod, String st) {
        try {
            String upd = "UPDATE aggiornamenti_mod SET fg_stato = ? WHERE cod = ?";
            PreparedStatement ps = this.conn.prepareStatement(upd);
            ps.setString(1, st);
            ps.setString(2, cod);
            ps.executeUpdate();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public String[] get_national_office() {
        try {
            String sql = "SELECT * FROM office";
            PreparedStatement ps = this.conn.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                String[] valori = {rs.getString("risk_days"), rs.getString("risk_ntr"), rs.getString("risk_soglia")};
                return valori;
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public ArrayList<String> list_cod_branch_enabled() {
        ArrayList<String> out = new ArrayList<>();
        try {
            String sql = "SELECT cod FROM branch WHERE fg_annullato = ? AND filiale = ? ORDER BY cod";
            PreparedStatement ps = this.conn.prepareStatement(sql);
            ps.setString(1, "0");
            ps.setString(2, getCodLocal(true)[0]);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                out.add(rs.getString("cod"));
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public void insert_indicerischio(String msg, String dt) {
        try {
            String ins = "INSERT INTO indice_rischio VALUES (?,?,?,?)";
            PreparedStatement ps = this.conn.prepareStatement(ins);
            ps.setString(1, generaId(50));
            ps.setString(2, msg);
            ps.setString(3, "0");
            ps.setString(4, dt);
            ps.execute();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public boolean setUsersResetPassword() {
        try {
            String query = "update users set fg_stato='2' where validita<>'0' and now()>date_add(dt_mod_pwd, interval cast(validita as decimal(10,0)) day) and fg_stato<>'2';";
            PreparedStatement ps = this.conn.prepareStatement(query);
            int x = ps.executeUpdate();
            return x > 0;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    public ResultSet getTables() {
        try {
            String query = "show tables";
            PreparedStatement ps = this.conn.prepareStatement(query);
            ResultSet rs = ps.executeQuery();
            return rs;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public ResultSet getData(String sql) {
        try {
            PreparedStatement ps = this.conn.prepareStatement(sql);
            //      System.out.println(ps);

            ResultSet rs = ps.executeQuery();
            return rs;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void setDim() {
        try {
            String sql = "SET GLOBAL max_allowed_packet = 1024*1024*14;";
            PreparedStatement ps = this.conn.prepareStatement(sql);
            //      System.out.println(ps);

            ps.executeQuery();

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public String getIpFiliale(String filiale) {
        try {
            String sql = "SELECT ip FROM dbfiliali WHERE filiale = ?";
            PreparedStatement ps = this.conn.prepareStatement(sql);
            ps.setString(1, filiale);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getString(1);
            }
            return "";
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public String[] getExcelValuteFile() {
        try {
            String sql = "SELECT cod,fileout,dt_start,user FROM excel_upload WHERE stato='0' order by data";
            PreparedStatement ps = this.conn.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                String[] out = {rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4)};
                return out;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public ArrayList<String> list_branchcode() {
        ArrayList<String> li = new ArrayList<>();
        try {
            ResultSet rs = this.conn.createStatement().executeQuery("SELECT distinct(cod) FROM branch where fg_annullato='0' ORDER BY cod");
            while (rs.next()) {
                li.add(rs.getString(1));
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return li;
    }

    public boolean getPresenzaValuta(String valuta, String bce, String dt_start, String user, ArrayList<String> al) {
        try {
            ResultSet rs = conn.createStatement().executeQuery("select valuta from valute where valuta='" + valuta + "'");

            return rs.next();

//            if (!rs.next()) {
////                for(int i = 0;i<al.size();i++){
////                    
////                    String ins = "insert into valute (filiale,valuta,codice_uic_divisa,de_valuta,cambio_acquisto,cambio_vendita,cambio_bce,de_messaggio) values ('" + al.get(i) + "','" + valuta + "','','-','','','" + bce + "','-')";
////                    
////                    insert_aggiornamenti_mod(new Aggiornamenti_mod(
////                            
////                        Util.generaId(50), value, dt_val, "0",
////                        ty, psstring, username, dtoper));
////                }
////                
////                insertValue_agg(null, ins, null, dt_start, user,al);
//            }
//            return true;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    public void insert_aggiornamenti_mod(Aggiornamenti_mod am) {
        try {
            String ins = "INSERT INTO aggiornamenti_mod VALUES (?,?,?,?,?,?,?,?)";
            PreparedStatement ps = this.conn.prepareStatement(ins);
            ps.setString(1, am.getCod());
            ps.setString(2, am.getFiliale());
            ps.setString(3, am.getDt_start());
            ps.setString(4, am.getFg_stato());
            ps.setString(5, am.getTipost());
            ps.setString(6, am.getAction());
            ps.setString(7, am.getUser());
            ps.setString(8, am.getTimestamp());
            ps.execute();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public boolean update_change_BCE(String filiale, String valuta, String valore, String dtstart, String username, ArrayList<String> al, String dtoper) {
        String upd = "update valute set cambio_bce = '" + valore + "' where valuta = '" + valuta + "' AND filiale = '" + filiale + "'";
        insert_aggiornamenti_mod(new Aggiornamenti_mod(
                Util.generaId(50), filiale, dtstart, "0",
                "ST", upd, username, dtoper));
        //insertValue_agg(null, upd, filiale, dtstart, username, al);
        return true;
    }

    public boolean insert_ratehistory(Rate_history rh, String dtoper, String dtstart) {
        try {
            String ins = "INSERT INTO rate_history VALUES (?,?,?,?,?,?,?)";
            PreparedStatement ps = this.conn.prepareStatement(ins);
            ps.setString(1, rh.getCodic());
            ps.setString(2, rh.getFiliale());
            ps.setString(3, rh.getValuta());
            ps.setString(4, rh.getTipomod());
            ps.setString(5, rh.getModify());
            ps.setString(6, rh.getUser());
            ps.setString(7, rh.getDt_mod());
//            ps.execute();
            insert_aggiornamenti_mod(new Aggiornamenti_mod(
                    Util.generaId(50), rh.getFiliale(), dtstart, "0",
                    "PS", ps.toString(), "service", dtoper));
            return true;
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return false;
    }

    public String getNow() {
        try {
            String sql = "SELECT now()";
            PreparedStatement ps = this.conn.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getString(1);
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return new DateTime().toString("yyyy-MM-dd HH:mm:ss");
    }

    public ResultSet getDatiPerFiliale(String tabella) {
        try {
            String sql = "SELECT * FROM " + tabella + " WHERE filiale = '000'";
            return this.conn.prepareStatement(sql).executeQuery();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public ResultSet getDatiPerFiliale(String tabella, String filiale) {
        try {
            String sql = "SELECT * FROM " + tabella + " WHERE filiale = '" + filiale + "'";
            return this.conn.prepareStatement(sql).executeQuery();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return null;
    }

}
