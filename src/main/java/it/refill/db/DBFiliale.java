/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.refill.db;

import it.refill.engine.Oper;
import static it.refill.util.Util.generaId;
import static it.refill.util.Util.parseStringDate;
import static it.refill.util.Util.patternsqldate;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;
import org.joda.time.DateTime;

/**
 *
 * @author srotella
 */
public class DBFiliale {

    
    private String host = "";
    private static final String pwd = "root";
    private static final String user = "root";
    
    private Connection conn = null;

    public DBFiliale() {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
            Properties p = new Properties();
            p.put("user", user);
            p.put("password", pwd);
            p.put("useUnicode", "true");
            p.put("characterEncoding", "UTF-8");
            p.put("useSSL", "false");
            p.put("rewriteBatchedStatements", "true");
            p.put("relaxAutoCommit", "true");
            this.conn = DriverManager.getConnection("jdbc:mysql://localhost:3306", p);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | SQLException ex) {
            ex.printStackTrace();
            System.err.println("ERRORE CONNESSIONE DB FILIALE");
        }
    }

    public DBFiliale(String ip, String filiale) {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
            host = "//" + ip + ":3306/maccorp";
            Properties p = new Properties();
            p.put("user", user);
            p.put("password", pwd);
            p.put("useUnicode", "true");
            p.put("characterEncoding", "UTF-8");
            p.put("useSSL", "false");
            this.conn = DriverManager.getConnection("jdbc:mysql:" + host, p);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | SQLException ex) {
            ex.printStackTrace();
            System.err.println("ERRORE CONNESSIONE DB FILIALE");
        }
    }

    public DBFiliale(Connection conn) {
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
            System.out.println(sql);
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

    public ArrayList<String[]> list_aggiornamenti_mod_div(String filiale, String stato) {
        ArrayList<String[]> li = new ArrayList<>();
        try {
            String sql = "SELECT cod,filiale,dt_start,fg_stato,tipost,action,user FROM aggiornamenti_mod where filiale <> '" + filiale + "' AND fg_stato='" + stato + "' ORDER BY timestamp,cod";
            ResultSet rs = this.conn.createStatement().executeQuery(sql);
            while (rs.next()) {
                String[] output = {rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4), rs.getString(5), rs.getString(6), rs.getString(7)};
                li.add(output);
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return li;
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
                    System.out.println(ps);
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

    public String[] addInfo(ResultSet rs1, String sql1, int colsize) {
        String[] out = new String[3];
        try {
            PreparedStatement ps = this.conn.prepareStatement(sql1);
            for (int i = 0; i < colsize; i++) {
                ps.setString(i + 1, rs1.getString(i + 1));
            }
            // System.out.println(ps);
            out[0] = "false";
            out[1] = ps.toString();
            // System.out.println(ps);
            if (ps.executeUpdate() > 0) {
                out[0] = "true";
            }
            out[2] = "";
        } catch (SQLException e) {
            e.printStackTrace();
            out[2] = e.getMessage();
            out[0] = "false";
        }

        return out;

    }

    public String[] addInfo(String sql1) {
        String[] out = new String[3];
        try {
            PreparedStatement ps = this.conn.prepareStatement(sql1);

            // System.out.println(ps);
            out[0] = "false";
            out[1] = ps.toString();
            //System.out.println(ps);            
            if (ps.executeUpdate() > 0) {
                out[0] = "true";
            }
            out[2] = "";
        } catch (SQLException e) {
            e.printStackTrace();
            out[2] = e.getMessage();
            out[0] = "false";
        }

        return out;

    }

    public boolean eseuitruncate(String tablename) {

        try {
            String sql = "truncate table " + tablename;
            PreparedStatement ps = this.conn.prepareStatement(sql);

            return ps.execute();

        } catch (SQLException ex) {
            ex.printStackTrace();
            return false;
        }

    }

}
