/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.so.engine;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterators;
import rc.so.db.DB;
import rc.so.db.DBFiliale;
import rc.so.db.DBHost;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import org.apache.commons.net.ntp.NTPUDPClient;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 *
 * @author rcosco
 */
public class Update {

    private static final String pattern = "dd/MM/yyyy HH:mm:ss";
    ArrayList<String> elencotabelle = new ArrayList();
    //CONFIGURAZIONE
    private String filiale = "";
    private String myip = "";
    File log = null;
    String host = "";

    BufferedWriter writer = null;

    public Update(String filiale, String host) {
        this.filiale = filiale;
        this.host = host;
        DateFormat df = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss");
        Date today = Calendar.getInstance().getTime();
        String reportDate = df.format(today);
        log = new File(reportDate + "_log.txt");
    }

    public ArrayList<String> getElencotabelle() {
        return elencotabelle;
    }

    public void setElencotabelle(ArrayList<String> elencotabelle) {
        this.elencotabelle = elencotabelle;
    }

    public String getFiliale() {
        return filiale;
    }

    public void setFiliale(String filiale) {
        this.filiale = filiale;
    }

    public String getMyip() {
        return myip;
    }

    public void setMyip(String myip) {
        this.myip = myip;
    }

    public File getLog() {
        return log;
    }

    public void setLog(File log) {
        this.log = log;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public BufferedWriter getWriter() {
        return writer;
    }

    public void setWriter(BufferedWriter writer) {
        this.writer = writer;
    }

    private static DateTime getTime() {
        try {
            return new DateTime(new NTPUDPClient().getTime(InetAddress.getByName("193.204.114.232")).getMessage().getTransmitTimeStamp().getTime());
        } catch (UnknownHostException ex) {
        } catch (IOException ex) {
        }
        DB db = new DB();
        DateTime time = db.now();
        db.closeDB();
        if (time != null) {
            return time;
        } else {
            return new DateTime();
        }
    }

    private static String formatType(String sql) {
        if (sql.toUpperCase().startsWith("INSERT")) {
            return "INS";
        } else if (sql.toUpperCase().startsWith("DELETE")) {
            return "DEL";
        } else if (sql.toUpperCase().startsWith("UPDATE")) {
            return "UPD";
        }
        return "";
    }

    public boolean updateFromBranch() {
        boolean errore = false;
        try {
            DBHost db = new DBHost(host);
            myip = db.getIpFiliale(filiale);
//            System.out.println(myip);
            db.closeDB();

            DBFiliale dbfiliale = new DBFiliale(myip, filiale);
            //mi prendo tutti gli aggiornamenti con filiale diversa da quella che sto aggiornando
            ArrayList<String[]> li = dbfiliale.list_aggiornamenti_mod_div(filiale, "0");
            dbfiliale.closeDB();
            System.out.println("NUMERO AGGIORNAMENTI DALLA FILIALE " + li.size());
            for (int i = 0; i < li.size(); i++) {
                dbfiliale = new DBFiliale(myip, filiale);
                System.out.println("avanzamento.....  " + i);
                String[] st = li.get(i);
                String cod = st[0];
                String filiale1 = st[1];
                String dt_start = st[2];
                String type = st[4];
                String action = st[5];
                String user = st[6];

                DBHost db2 = new DBHost(host);
                boolean es = db2.execute_agg_copia(cod, filiale1, dt_start, type, action, user);
                if (es) {
                    dbfiliale.setStatus_agg(cod, "1");
                } else {
                    errore = true;
                }
                dbfiliale.closeDB();
                db2.closeDB();

//                }
            }

        } catch (Exception e) {
            errore = true;
            e.printStackTrace();
        }
        return errore;
    }

    public boolean updateCentral() {
        boolean errore = false;
        try {
            DBHost db = new DBHost(host);
            myip = db.getIpFiliale(filiale);
//            System.out.println(myip);

            //mi prendo tutti gli aggiornamenti con filiale diversa da quella che sto aggiornando
            ArrayList<String[]> li = db.list_aggiornamenti_mod("000", "0");
            db.closeDB();
            System.out.println("NUMERO AGGIORNAMENTI CENTRALE " + li.size());
            DateTime adesso = getTime();
            for (int i = 0; i < li.size(); i++) {
                System.out.println("avanzamento.....  " + i);
                String[] st = li.get(i);
                String cod = st[0];
                String dt_start = st[1];
                String type = st[2];
                String action = st[3];
                Oper oper = new Oper();

                LinkedList<String> paramlist = new LinkedList<>();
                if (type.equalsIgnoreCase("ps")) {

                    if (action.contains("com.mysql.jdbc")) {
                        if (action.contains(":")) {
                            type = "ST";
                            oper.setSql(action.substring(action.indexOf(":") + 1).trim());
                        }
                    } else {
                        action = action.replace("sql : '", "").trim();
                        if (action.contains("', parameters : ")) {
                            Iterable<String> parameters = Splitter.on("', parameters : ").split(action);
                            Iterator<String> it = parameters.iterator();
                            if (Iterators.size(it) == 2) {
                                it = parameters.iterator();
                                String sql = it.next();
                                String param = it.next();
                                oper.setSql(sql);
                                Iterable<String> parameters2 = Splitter.on("','").split(param);
                                Iterator<String> it2 = parameters2.iterator();
                                int length = Iterators.size(it2);
                                it2 = parameters2.iterator();
                                for (int j = 0; j < length; j++) {
                                    String val = it2.next();
                                    if (j == 0) {
                                        val = val.substring(2);
                                    } else if (j == length - 1) {
                                        val = val.substring(0, val.length() - 2);
                                    }
                                    paramlist.add(val.trim());
                                }
                                oper.setParam(paramlist);
                            }
                        }
                    }
                } else if (type.equalsIgnoreCase("st")) {
                    oper.setSql(st[3]);
                }
                oper.setType(formatType(oper.getSql()));
                DateTimeFormatter formatter = DateTimeFormat.forPattern(pattern);
                DateTime dt_start_value = formatter.parseDateTime(dt_start);
                if (dt_start_value.isBefore(adesso)) {
                    DBHost db2 = new DBHost(host);
                    boolean es = db2.execute_agg(type, oper);
                    if (es) {
                        db2.setStatus_agg(cod, "1");
                    } else {
                        errore = true;
                    }

                    db2.closeDB();
                }
            }

        } catch (Exception e) {
            errore = true;
            e.printStackTrace();
        }
        return errore;
    }

    public boolean updateToBranch() {
        boolean errore = false;
        try {
            DBHost db = new DBHost(host);
            myip = db.getIpFiliale(filiale);
//            System.out.println(myip);

//            DBFiliale dbfiliale = new DBFiliale(myip, filiale);
            ArrayList<String[]> li = db.list_aggiornamenti_mod(filiale, "0");
            System.out.println("NUMERO AGGIORNAMENTI " + li.size() + " PER LA FILIALE " + filiale);

            DateTime adesso = getTime();
            for (int i = 0; i < li.size(); i++) {
                System.out.println("avanzamento.....  " + i);
                String[] st = li.get(i);
                String cod = st[0];
                String dt_start = st[1];
                String type = st[2];
                String action = st[3];
                Oper oper = new Oper();
                LinkedList<String> paramlist = new LinkedList<>();
                if (type.equalsIgnoreCase("ps")) {
                    if (action.contains("com.mysql.jdbc")) {
                        if (action.contains(":")) {
                            type = "ST";
                            oper.setSql(action.substring(action.indexOf(":") + 1).trim());
                        }
                    } else {
                        action = action.replace("sql : '", "").trim();
                        if (action.contains("', parameters : ")) {
                            Iterable<String> parameters = Splitter.on("', parameters : ").split(action);
                            Iterator<String> it = parameters.iterator();
                            if (Iterators.size(it) == 2) {
                                it = parameters.iterator();
                                String sql = it.next();
                                String param = it.next();
                                oper.setSql(sql);
                                Iterable<String> parameters2 = Splitter.on("','").split(param);
                                Iterator<String> it2 = parameters2.iterator();
                                int length = Iterators.size(it2);
                                it2 = parameters2.iterator();
                                for (int j = 0; j < length; j++) {
                                    String val = it2.next();
                                    if (j == 0) {
                                        val = val.substring(2);
                                    } else if (j == length - 1) {
                                        val = val.substring(0, val.length() - 2);
                                    }
                                    paramlist.add(val.trim());
                                }
                                oper.setParam(paramlist);
                            }
                        }
                    }
                } else if (type.equalsIgnoreCase("st")) {
                    oper.setSql(st[3]);
                }
                oper.setType(formatType(oper.getSql()));
                DateTimeFormatter formatter = DateTimeFormat.forPattern(pattern);
                DateTime dt_start_value = formatter.parseDateTime(dt_start);
                if (dt_start_value.isBefore(adesso)) {
                    DBFiliale dbfiliale = new DBFiliale(myip, filiale);
                    boolean es = dbfiliale.execute_agg(type, oper);
                    if (es) {
                        db.setStatus_agg(cod, "1");
                    } else {
                        errore = true;
                    }
                    dbfiliale.closeDB();

                }
            }
            db.closeDB();

        } catch (Exception e) {
            errore = true;
            e.printStackTrace();
        }
        return errore;
    }

}