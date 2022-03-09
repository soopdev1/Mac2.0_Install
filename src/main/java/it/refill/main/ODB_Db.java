/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.refill.main;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;
import static it.refill.engine.Utility.patternsqldate;
import org.joda.time.DateTime;

/**
 *
 * @author rcosco
 */
public class ODB_Db {

    public static final String comma = ",";
    public Connection c = null;
    public String h = null;

    public Connection getC() {
        return c;
    }

    public void setC(Connection c) {
        this.c = c;
    }

    public ODB_Db(String host, boolean filiale) {
        String drivername = "org.mariadb.jdbc.Driver";
        String typedb = "mariadb";
        String user = "maccorp";
        String pwd = "M4cc0Rp";
        if (filiale) {
            drivername = "com.mysql.cj.jdbc.Driver";
            typedb = "mysql";
            user = "root";
            pwd = "root";
            host = "//" + host + ":3306/maccorp";
        }
        try {
            Class.forName(drivername).newInstance();
            Properties p = new Properties();
            p.put("user", user);
            p.put("password", pwd);
            p.put("useUnicode", "true");
            p.put("characterEncoding", "UTF-8");
            p.put("useSSL", "false");
            p.put("connectTimeout", "1000");
            System.out.println("jdbc:" + typedb + ":" + host);
            this.c = DriverManager.getConnection("jdbc:" + typedb + ":" + host, p);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | SQLException ex) {
            ex.printStackTrace();
        }

    }

    public void closeDB() {
        try {
            if (this.c != null) {
                this.c.close();
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public boolean updateBranch(String[] val) {
        try {
            String upd = "UPDATE branch SET add_via = ?, add_city = ?, add_cap = ? WHERE cod = ?";
            PreparedStatement ps = this.c.prepareStatement(upd);
            ps.setString(1, val[1]);
            ps.setString(2, val[2]);
            ps.setString(3, val[3]);
            ps.setString(4, val[0]);

            return ps.executeUpdate() > 0;
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return false;
    }

    public ArrayList<String> listToUpdate() {
        ArrayList<String> li = new ArrayList<>();
        try {
            String sql = "SELECT distinct(filiale) FROM valute WHERE filiale <>'000' AND buy_std = '0.00' and valuta<>'EUR'";
            ResultSet rs = this.c.createStatement().executeQuery(sql);
            while (rs.next()) {
                li.add(rs.getString(1));
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return li;
    }

    public String getNow() {
        try {
            String sql = "SELECT now()";
            PreparedStatement ps = this.c.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getString(1);
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return new DateTime().toString(patternsqldate);
    }

    public ArrayList<String> list_cod_branch_enabled() {
        ArrayList<String> out = new ArrayList<>();
        try {
            String sql = "SELECT cod FROM branch WHERE fg_annullato = ? AND filiale = ? ORDER BY cod";
            PreparedStatement ps = this.c.prepareStatement(sql);
            ps.setString(1, "0");
            ps.setString(2, "000");
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                out.add(rs.getString("cod"));
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public ArrayList<String[]> getIpFiliale() {
        ArrayList<String[]> out = new ArrayList<>();
        try {
            String sql = "SELECT filiale,ip FROM dbfiliali";
            PreparedStatement ps = this.c.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String[] ip = {rs.getString(1), rs.getString(2)};
                out.add(ip);
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public ArrayList<String[]> aggfiliali() {
        ArrayList<String[]> out = new ArrayList<>();
        try {
//            String sql = "SELECT filiale,count(*) FROM aggiornamenti_mod WHERE fg_stato='0' group by filiale";
            String sql = "SELECT filiale,count(*) FROM aggiornamenti_mod WHERE fg_stato='0' AND now()>STR_TO_DATE(dt_start, '%d/%m/%Y %H:%i:%s') group by filiale";
            PreparedStatement ps = this.c.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String[] ip = {rs.getString(1), rs.getString(2)};
                out.add(ip);
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public String getConf(String id) {
        try {
            String sql = "SELECT des FROM conf WHERE id = ? ";
            PreparedStatement ps = this.c.prepareStatement(sql);
            ps.setString(1, id);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getString(1).trim();
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return "-";
    }

    public String getH() {
        return h;
    }

    public void setH(String h) {
        this.h = h;
    }

    public ArrayList<String[]> listagg(String filiale) {
        ArrayList<String[]> li = new ArrayList<>();
        try {
            String sql = "SELECT filiale,count(*) FROM aggiornamenti_mod Where fg_stato='0' group by filiale";
            ResultSet rs = this.c.createStatement().executeQuery(sql);

            while (rs.next()) {
                String[] ou = {rs.getString(1), rs.getString(2)};
                li.add(ou);
                System.out.println("FILIALE ORIGINE: " + filiale + " - RISULTATI: " + rs.getString(1) + " : " + rs.getString(2));
            }

        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return li;
    }

    public int countA() {
        int agg = 0;
        try {
            //String sql = "SELECT count(*) FROM aggiornamenti_mod Where fg_stato='0' ";
            String sql = "SELECT count(*) FROM aggiornamenti_mod Where fg_stato='0' AND now()>STR_TO_DATE(dt_start, '%d/%m/%Y %H:%i:%s'); ";
            ResultSet rs = this.c.createStatement().executeQuery(sql);
            while (rs.next()) {
                agg = agg + rs.getInt(1);
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
            agg = -1;
        }
        return agg;
    }

    public void listagg_in_locale(String filiale) {
        try {
            String sql = "SELECT filiale,count(*) FROM aggiornamenti_mod Where fg_stato='0' AND filiale = '" + filiale + "' group by filiale";

            ResultSet rs = this.c.createStatement().executeQuery(sql);
            while (rs.next()) {
                System.out.println("FILIALE ORIGINE: " + filiale + " - RISULTATI: " + rs.getString(1) + " : " + rs.getString(2));
            }

        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public void setDim() {
        try {
            String sql = "SET GLOBAL max_allowed_packet = 1024*1024*14;";
            PreparedStatement ps = this.c.prepareStatement(sql);
            ps.executeQuery();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public ResultSet getData(String sql) {
        try {
            PreparedStatement ps = this.c.prepareStatement(sql);
            return ps.executeQuery();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public String[] addInfo(ResultSet rs1, String sql1, int colsize) {
        String[] out = new String[3];
        try {
            PreparedStatement ps = this.c.prepareStatement(sql1);
            for (int i = 0; i < colsize; i++) {
                ps.setString(i + 1, rs1.getString(i + 1));
            }
            out[0] = "false";
            out[1] = ps.toString();
            if (ps.executeUpdate() > 0) {
                out[0] = "true";
            }
            out[2] = "";
        } catch (SQLException e) {
            if (e.getMessage().toLowerCase().contains("duplicate")) {
                out[0] = "true";
                out[2] = "";
            } else {
                e.printStackTrace();
                out[2] = e.getMessage();
                out[0] = "false";
            }
        }
        return out;
    }

    public String[] addInfo(String sql1) {
        String[] out = new String[3];
        try {
            PreparedStatement ps = this.c.prepareStatement(sql1);
            out[0] = "false";
            out[1] = ps.toString();
            if (ps.executeUpdate() > 0) {
                out[0] = "true";
            }
            out[2] = "";
        } catch (SQLException e) {
            if (e.getMessage().toLowerCase().contains("duplicate")) {
                out[0] = "true";
                out[2] = "";
            } else {
                e.printStackTrace();
                out[2] = e.getMessage();
                out[0] = "false";
            }
        }
        return out;
    }

    public boolean eseuitruncate(String tablename) {
        try {
            String sql = "truncate table " + tablename;
            PreparedStatement ps = this.c.prepareStatement(sql);
            return ps.execute();
        } catch (SQLException ex) {
            ex.printStackTrace();
            return false;
        }

    }

}
