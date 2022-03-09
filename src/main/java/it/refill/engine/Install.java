/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.refill.engine;

import it.refill.db.DBFiliale;
import it.refill.db.DBHost;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;

/**
 *
 * @author rcosco
 */
public class Install {

    ArrayList<String> elencotabelle = new ArrayList();
    //CONFIGURAZIONE
    private String filiale = "";
    private String myip = "";
    private String tipooperazione = "";
    File log = null;
    String host = "";

    BufferedWriter writer = null;

    public Install(String filiale, String tipooperazione, String host) {
        this.filiale = filiale;
        this.tipooperazione = tipooperazione;
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

    public String getTipooperazione() {
        return tipooperazione;
    }

    public void setTipooperazione(String tipooperazione) {
        this.tipooperazione = tipooperazione;
    }

    public File getLog() {
        return log;
    }

    public void setLog(File log) {
        this.log = log;
    }

    public BufferedWriter getWriter() {
        return writer;
    }

    public void setWriter(BufferedWriter writer) {
        this.writer = writer;
    }

    public boolean firstInstallation() {
        DBHost db = new DBHost(host);
        DBFiliale dbfiliale = null;
        boolean errore = false;
        String erroredescr = "";

        try {
            writer = new BufferedWriter(new FileWriter(log));
            writer.write("PRIMA INSTALLAZIONE");
            writer.write("");

            db.setDim();

            ResultSet rs = db.getTables();
            if (rs != null) {
                while (rs.next()) {
                    elencotabelle.add(rs.getString(1));
//                System.out.println(rs.getString(1));
                }

//                System.out.println("NUMERO DI TABELLE DEL DB: " + elencotabelle.size());
                myip = db.getIpFiliale(filiale);

//            System.out.println(myip);
                db.closeDB();

                int ctntabupdate = 0;

                for (int i = 0; i < elencotabelle.size(); i++) {

                    System.out.println("avanzamento.....  " + i);

                    errore = false;
                    erroredescr = "";

                    String sql;
                    String sql1;
                    ResultSet rs1;
                    ArrayList<String> elencocolonne = new ArrayList();
                    String tablename = elencotabelle.get(i);

                    if (tablename.equals("agenzie")
                            || tablename.equals("bank")
                            || tablename.equals("branchbudget")
                            || tablename.equals("branchgroup")
                            || tablename.equals("anagrafica_ru")
                            || tablename.equals("anagrafica_ru_attach")
                            || tablename.equals("province")
                            || tablename.equals("bce_year")
                            || tablename.equals("blacklist")
                            || tablename.equals("branch")
                            || tablename.equals("carte_credito")
                            || tablename.equals("cash_perm")
                            || tablename.equals("codici_fiscali_esteri")
                            || tablename.equals("codici_fiscali_italia")
                            || tablename.equals("codici_fiscali_mese")
                            || tablename.equals("commissione_fissa")
                            || tablename.equals("compro")
                            || tablename.equals("comuni_apm")
                            || tablename.equals("conf")
                            || tablename.equals("configmonitor")
                            || tablename.equals("contabilita")
                            || tablename.equals("internetbooking")
                            || tablename.equals("kind_commissione_fissa")
                            || tablename.equals("logical")
                            || tablename.equals("nazioni")
                            || tablename.equals("nc_kind")
                            || tablename.equals("office")
                            || tablename.equals("pages")
                            || tablename.equals("path")
                            || tablename.equals("paymat")
                            || tablename.equals("select_g01")
                            || tablename.equals("select_g02")
                            || tablename.equals("select_g03")
                            || tablename.equals("select_rv")
                            || tablename.equals("selectareanaz")
                            || tablename.equals("selectdoctrans")
                            || tablename.equals("selectgroupbranch")
                            || tablename.equals("selectgrouptype")
                            || tablename.equals("selectinout")
                            || tablename.equals("selectkind")
                            || tablename.equals("selectlevelrate")
                            || tablename.equals("selectncde")
                            || tablename.equals("selectresident")
                            || tablename.equals("selecttipocliente")
                            || tablename.equals("selecttipov")
                            || tablename.equals("temppaymat")
                            || tablename.equals("tipologiaclienti")
                            || tablename.equals("tipologiadocumento")
                            || tablename.equals("under_min_comm_justify")
                            || tablename.equals("user_sito")
                            || tablename.equals("users")
                            || tablename.equals("vatcode")) {

                        db = new DBHost(host);

                        dbfiliale = new DBFiliale(myip, filiale);
                        dbfiliale.setDim();

                        if (dbfiliale.getConnectionDB() != null) {

                            ctntabupdate++;

                            sql = "show columns from " + tablename;
                            writer.write(sql + "\n");
                            System.out.println(sql);
                            rs1 = db.getData(sql);
                            if (rs1 != null) {
                                while (rs1.next()) {
                                    elencocolonne.add(rs1.getString(1));
                                }

                                sql = "SELECT ";
                                for (int g = 0; g < elencocolonne.size(); g++) {
                                    if (g == elencocolonne.size() - 1) {
                                        sql += elencocolonne.get(g) + " ";
                                    } else {
                                        sql += elencocolonne.get(g) + " , ";
                                    }
                                }
                                sql += " FROM " + tablename;
                                rs1 = db.getData(sql);
                                if (rs1 != null) {
                                    sql1 = "INSERT INTO " + tablename + " (";
                                    for (int v = 0; v < elencocolonne.size(); v++) {
                                        if (v == elencocolonne.size() - 1) {
                                            sql1 += elencocolonne.get(v) + ") VALUES (";
                                        } else {
                                            sql1 += elencocolonne.get(v) + ",";
                                        }
                                    }
                                    for (int v = 0; v < elencocolonne.size(); v++) {
                                        if (v == elencocolonne.size() - 1) {
                                            sql1 += "?)";
                                        } else {
                                            sql1 += "?,";
                                        }
                                    }
                                    System.out.println("");
                                    writer.write("doing insert... \n");

                                    String[] ris = new String[3];
                                    ris[0] = "true";
                                    ris[1] = "";
                                    ris[2] = "";
                                    while (rs1.next() && ris[0].equals("true")) {
                                        ris = dbfiliale.addInfo(rs1, sql1, elencocolonne.size());
//                        System.out.println("doing.....");
                                    }
                                    if (!ris[0].equals("true")) {
                                        errore = true;
                                        erroredescr = "5.Errore in query " + sql1;
                                        writer.write(erroredescr + "\n");
                                        writer.write(ris[1] + "\n");
                                        writer.write(ris[2] + "\n");
                                        break;
                                    }

                                    if (tablename.equals("path")) {
                                        ris = new String[3];
                                        ris[0] = "true";
                                        ris[1] = "";
                                        ris[2] = "";
                                        sql1 = "UPDATE path SET descr = 'C:\\\\Maccorp\\\\temp\\\\' where cod = 'temp' ";
                                        dbfiliale.addInfo(sql1);
                                        if (!ris[0].equals("true")) {
                                            errore = true;
                                            erroredescr = "7.Errore in query " + sql1;
                                            writer.write(erroredescr + "\n");
                                            writer.write(ris[1] + "\n");
                                            writer.write(ris[2] + "\n");
                                            break;
                                        }

                                    }

                                    dbfiliale.closeDB();
                                    System.out.println("");
                                    System.out.println();
                                    writer.write("END table " + tablename + "\n");
                                    System.out.println("");
//                 System.in.read();

                                    db.closeDB();

                                } else {
                                    errore = true;
                                    erroredescr = "4.Errore in query " + sql;
                                    writer.write(erroredescr + "\n");
                                    break;
                                }

                            } else {
                                errore = true;
                                erroredescr = "3.Errore in query " + sql;
                                writer.write(erroredescr + "\n");
                                break;
                            }

                        }//if dbfiliale !=null
                        else {
                            erroredescr = "2.Errore Connessione a DB filiale " + filiale;
                            errore = true;
                            writer.write(erroredescr + "\n");
                            break;
                        }
                    } else {
                        //qui vanno le altre tabelle distinte per filiale

                    }

                }//for elencotabelle

                System.out.println("NUMERO DI TABELLE AGGIORNATE: " + ctntabupdate);

            } else {
                errore = true;
                erroredescr = "1.Errore connessione al DB centrale";
                writer.write(erroredescr + "\n");
            }

            if (errore) {
                boolean esitoreset = false;
                if (dbfiliale != null) {
                    dbfiliale.closeDB();
                }
                db.closeDB();
                if (!erroredescr.startsWith("1.") && tipooperazione.equals("1")) { //se almeno mi sono collegato al DB centrale e se sto installando la prima volta
                    esitoreset = eseguiresettabelle(myip, filiale);
                }

                writer.write(erroredescr + "\n");

                System.err.println(erroredescr);

                System.err.println("Si sono verificati errori durante l'operazione di inizializzazione dei dati sul DB.");
                writer.write("Si sono verificati errori durante l'operazione di inizializzazione dei dati sul DB. \n");

                if (!tipooperazione.equals("1")) {
                    System.err.println("Il DB è stato resettato");
                    writer.write("Il DB è stato resettato");
                }

            }
            writer.write("-----------------   END   ------------------------------");
            writer.close();

        } catch (IOException | SQLException e) {
            e.printStackTrace();
        }

        System.out.println("-----------------   END   ------------------------------");

        db.closeDB();

        return errore;

    }

    public boolean updateFull() {
        DBHost db = new DBHost(host);
        DBFiliale dbfiliale = null;
        boolean errore = false;
        String erroredescr = "";
        try {
            writer = new BufferedWriter(new FileWriter(log));
            writer.write("UPDATE TABELLE BASE");
            writer.write("");

            ResultSet rs = db.getTables();
            if (rs != null) {
                while (rs.next()) {
                    elencotabelle.add(rs.getString(1));
//                System.out.println(rs.getString(1));
                }

                myip = db.getIpFiliale(filiale);

//            System.out.println(myip);
                db.closeDB();
                int cnttableupdate = 0;
                for (int i = 0; i < elencotabelle.size(); i++) {

                    System.out.println("avanzamento.....  " + i);

                    errore = false;
                    erroredescr = "";

                    String sql;
                    String sql1;
                    ResultSet rs1;
                    ArrayList<String> elencocolonne = new ArrayList();
                    String tablename = elencotabelle.get(i);

                    if (tablename.equals("codici_sblocco") || tablename.equals("codici_sblocco_file") || tablename.equals("transaction_reprint")) {

                        db = new DBHost(host);

                        dbfiliale = new DBFiliale(myip, filiale);
                        dbfiliale.setDim();

                        if (dbfiliale.getConnectionDB() != null) {

                            cnttableupdate++;
                            sql = "show columns from " + tablename;
                            System.out.println(sql);
                            writer.write(sql + "\n");
                            rs1 = db.getData(sql);
                            if (rs1 != null) {
                                while (rs1.next()) {
                                    elencocolonne.add(rs1.getString(1));
                                }

                                sql = "SELECT ";
                                for (int g = 0; g < elencocolonne.size(); g++) {
                                    if (g == elencocolonne.size() - 1) {
                                        sql += elencocolonne.get(g) + " ";
                                    } else {
                                        sql += elencocolonne.get(g) + " , ";
                                    }
                                }
                                sql += " FROM " + tablename;
                                if (tablename.equals("codici_sblocco")) {
                                    sql += " WHERE dt_gen >= DATE_SUB(NOW(),INTERVAL 1 YEAR);";
                                }
                                if (tablename.equals("codici_sblocco_file")) {
                                    sql += " WHERE dt_oper >= DATE_SUB(NOW(),INTERVAL 1 YEAR);";
                                }
                                if (tablename.equals("transaction_reprint")) {
                                    sql += " WHERE date >= DATE_SUB(NOW(),INTERVAL 1 YEAR);";
                                }

                                rs1 = db.getData(sql);
                                if (rs1 != null) {
                                    sql1 = "INSERT INTO " + tablename + " (";
                                    for (int v = 0; v < elencocolonne.size(); v++) {
                                        if (v == elencocolonne.size() - 1) {
                                            sql1 += elencocolonne.get(v) + ") VALUES (";
                                        } else {
                                            sql1 += elencocolonne.get(v) + ",";
                                        }
                                    }
                                    for (int v = 0; v < elencocolonne.size(); v++) {
                                        if (v == elencocolonne.size() - 1) {
                                            sql1 += "?)";
                                        } else {
                                            sql1 += "?,";
                                        }
                                    }
                                    System.out.println("doing insert...");
                                    writer.write("doing insert..." + "\n");
                                    String[] ris = new String[3];
                                    ris[0] = "true";
                                    ris[1] = "";
                                    ris[2] = "";
                                    while (rs1.next() && ris[0].equals("true")) {
                                        ris = dbfiliale.addInfo(rs1, sql1, elencocolonne.size());
//                        System.out.println("doing.....");
                                    }
                                    if (!ris[0].equals("true")) {
                                        errore = true;
                                        erroredescr = "5.Errore in query " + sql1;
                                        writer.write(erroredescr + "\n");
                                        writer.write(ris[1] + "\n");
                                        writer.write(ris[2] + "\n");
                                        break;
                                    }

                                    dbfiliale.closeDB();
                                    System.out.println("");
                                    System.out.println("END table " + tablename);
                                    writer.write("END table " + tablename + "\n");
                                    System.out.println("");
//                 System.in.read();

                                    db.closeDB();

                                } else {
                                    errore = true;
                                    erroredescr = "4.Errore in query " + sql;
                                    System.err.println(erroredescr);
                                    writer.write(erroredescr + "\n");
                                    break;
                                }

                            } else {
                                errore = true;
                                erroredescr = "3.Errore in query " + sql;
                                System.err.println(erroredescr);
                                writer.write(erroredescr + "\n");
                                break;
                            }

                        }//if dbfiliale !=null
                        else {
                            erroredescr = "2.Errore Connessione a DB filiale " + filiale;
                            System.err.println(erroredescr);
                            writer.write(erroredescr + "\n");
                            errore = true;
                            break;
                        }

                    } else {

                        db = new DBHost(host);

                        dbfiliale = new DBFiliale(myip, filiale);
                        dbfiliale.setDim();

                        //qui vanno le altre tabelle 
                        if (tablename.equals("newsletter_status")) {

                            cnttableupdate++;
                            sql = "show columns from " + tablename;
                            System.out.println(sql);
                            writer.write(sql + "\n");
                            rs1 = db.getData(sql);
                            if (rs1 != null) {
                                while (rs1.next()) {
                                    elencocolonne.add(rs1.getString(1));
                                }

                                sql = "SELECT ";
                                for (int g = 0; g < elencocolonne.size(); g++) {
                                    if (g == elencocolonne.size() - 1) {
                                        sql += elencocolonne.get(g) + " ";
                                    } else {
                                        sql += elencocolonne.get(g) + " , ";
                                    }
                                }
                                sql += " FROM " + tablename;
                                sql += " WHERE cod IN ( SELECT cod FROM newsletter WHERE dt_upload >= DATE_SUB(NOW(),INTERVAL 1 YEAR)); ";
                                System.out.println(sql);
                                rs1 = db.getData(sql);
                                if (rs1 != null) {
                                    sql1 = "INSERT INTO " + tablename + " (";
                                    for (int v = 0; v < elencocolonne.size(); v++) {
                                        if (v == elencocolonne.size() - 1) {
                                            sql1 += elencocolonne.get(v) + ") VALUES (";
                                        } else {
                                            sql1 += elencocolonne.get(v) + ",";
                                        }
                                    }
                                    for (int v = 0; v < elencocolonne.size(); v++) {
                                        if (v == elencocolonne.size() - 1) {
                                            sql1 += "?)";
                                        } else {
                                            sql1 += "?,";
                                        }
                                    }
                                    System.out.println("doing insert...");
                                    writer.write("doing insert..." + "\n");
                                    String[] ris = new String[3];
                                    ris[0] = "true";
                                    ris[1] = "";
                                    ris[2] = "";
                                    while (rs1.next() && ris[0].equals("true")) {
                                        ris = dbfiliale.addInfo(rs1, sql1, elencocolonne.size());
//                        System.out.println("doing.....");
                                    }
                                    if (!ris[0].equals("true")) {
                                        errore = true;
                                        erroredescr = "5.Errore in query " + sql1;
                                        writer.write(erroredescr + "\n");
                                        writer.write(ris[1] + "\n");
                                        writer.write(ris[2] + "\n");
                                        break;
                                    }

                                    dbfiliale.closeDB();
                                    System.out.println("");
                                    System.out.println("END table " + tablename);
                                    writer.write("END table " + tablename + "\n");
                                    System.out.println("");
//                 System.in.read();

                                    db.closeDB();

                                } else {
                                    errore = true;
                                    erroredescr = "4.Errore in query " + sql;
                                    System.err.println(erroredescr);
                                    writer.write(erroredescr + "\n");
                                    break;
                                }

                            } else {
                                errore = true;
                                erroredescr = "3.Errore in query " + sql;
                                System.err.println(erroredescr);
                                writer.write(erroredescr + "\n");
                                break;
                            }

                        }
                        if (tablename.equals("newsletter")) {

                            cnttableupdate++;
                            sql = "show columns from " + tablename;
                            System.out.println(sql);
                            writer.write(sql + "\n");
                            rs1 = db.getData(sql);
                            if (rs1 != null) {
                                while (rs1.next()) {
                                    elencocolonne.add(rs1.getString(1));
                                }

                                sql = "SELECT ";
                                for (int g = 0; g < elencocolonne.size(); g++) {
                                    if (g == elencocolonne.size() - 1) {
                                        sql += elencocolonne.get(g) + " ";
                                    } else {
                                        sql += elencocolonne.get(g) + " , ";
                                    }
                                }
                                sql += " FROM " + tablename;
                                sql += " WHERE dt_upload >= DATE_SUB(NOW(),INTERVAL 1 YEAR);";
                                System.out.println(sql);
                                rs1 = db.getData(sql);
                                if (rs1 != null) {
                                    sql1 = "INSERT INTO " + tablename + " (";
                                    for (int v = 0; v < elencocolonne.size(); v++) {
                                        if (v == elencocolonne.size() - 1) {
                                            sql1 += elencocolonne.get(v) + ") VALUES (";
                                        } else {
                                            sql1 += elencocolonne.get(v) + ",";
                                        }
                                    }
                                    for (int v = 0; v < elencocolonne.size(); v++) {
                                        if (v == elencocolonne.size() - 1) {
                                            sql1 += "?)";
                                        } else {
                                            sql1 += "?,";
                                        }
                                    }
                                    System.out.println("doing insert...");
                                    writer.write("doing insert..." + "\n");
                                    String[] ris = new String[3];
                                    ris[0] = "true";
                                    ris[1] = "";
                                    ris[2] = "";
                                    while (rs1.next() && ris[0].equals("true")) {
                                        ris = dbfiliale.addInfo(rs1, sql1, elencocolonne.size());
//                        System.out.println("doing.....");
                                    }
                                    if (!ris[0].equals("true")) {
                                        errore = true;
                                        erroredescr = "5.Errore in query " + sql1;
                                        writer.write(erroredescr + "\n");
                                        writer.write(ris[1] + "\n");
                                        writer.write(ris[2] + "\n");
                                        break;
                                    }

                                    dbfiliale.closeDB();
                                    System.out.println("");
                                    System.out.println("END table " + tablename);
                                    writer.write("END table " + tablename + "\n");
                                    System.out.println("");
//                 System.in.read();

                                    db.closeDB();

                                } else {
                                    errore = true;
                                    erroredescr = "4.Errore in query " + sql;
                                    System.err.println(erroredescr);
                                    writer.write(erroredescr + "\n");
                                    break;
                                }

                            } else {
                                errore = true;
                                erroredescr = "3.Errore in query " + sql;
                                System.err.println(erroredescr);
                                writer.write(erroredescr + "\n");
                                break;
                            }

                        }
                        if (tablename.equals("local")) {
                            cnttableupdate++;
                            String[] ris = new String[3];
                            ris[0] = "true";
                            ris[1] = "";
                            ris[2] = "";
                            sql1 = "INSERT INTO " + tablename + " (cod) VALUES ('" + filiale + "')";
                            dbfiliale.addInfo(sql1);
                            if (!ris[0].equals("true")) {
                                errore = true;
                                erroredescr = "6.Errore in query " + sql1;
                                writer.write(erroredescr + "\n");
                                writer.write(ris[1] + "\n");
                                writer.write(ris[2] + "\n");
                                break;
                            }

                        }

                        db.closeDB();
                        dbfiliale.closeDB();

                    }

                }//for elencotabelle

                System.out.println("NUMERO DI TABELLE AGGIORNATE: " + cnttableupdate);

            } else {
                errore = true;
                erroredescr = "1.Errore connessione al DB centrale";
                System.err.println(erroredescr);
                writer.write(erroredescr + "\n");
            }

            if (errore) {
                boolean esitoreset = false;
                if (dbfiliale != null) {
                    dbfiliale.closeDB();
                }
                db.closeDB();
                if (!erroredescr.startsWith("1.") && tipooperazione.equals("1")) { //se almeno mi sono collegato al DB centrale e se sto installando la prima volta
                    esitoreset = eseguiresettabelleFull(myip, filiale, elencotabelle);
                }

//                System.err.println(erroredescr);
                System.err.println("Si sono verificati errori durante l'operazione di inizializzazione dei dati sul DB.");
                writer.write("Si sono verificati errori durante l'operazione di inizializzazione dei dati sul DB." + "\n");

//                if (!tipooperazione.equals("1")) {
//                    System.err.println("Il DB è stato resettato");
//                }
            }

            writer.write("END " + "\n");

            writer.write("-----------------   END   ------------------------------");
            writer.close();

        } catch (IOException | SQLException e) {
            e.printStackTrace();
        }

        System.out.println("-----------------   END   ------------------------------");

        db.closeDB();

        return errore;

    }

    public boolean updateForFiliale() {
        DBHost db = new DBHost(host);
        DBFiliale dbfiliale = null;
        boolean errore = false;
        String erroredescr = "";
        int cnttableupdate = 0;
        try {
            writer = new BufferedWriter(new FileWriter(log));
            writer.write("UPDATE TABELLE PER FILIALE");
            writer.write("");

            ResultSet rs = db.getTables();
            if (rs != null) {
                while (rs.next()) {
                    elencotabelle.add(rs.getString(1));
//                System.out.println(rs.getString(1));
                }

//                int ctntabupdate = 0;

                myip = db.getIpFiliale(filiale);

//            System.out.println(myip);
                db.closeDB();

                for (int i = 0; i < elencotabelle.size(); i++) {

                    System.out.println("avanzamento.....  " + i);

                    errore = false;
                    erroredescr = "";

                    String sql;
                    String sql1;
                    ResultSet rs1;
                    ArrayList<String> elencocolonne = new ArrayList();
                    String tablename = elencotabelle.get(i);

                    if ( //ultimo anno con insert in altre tabelle
                            tablename.equals("ch_transaction")
                            || tablename.equals("et_change")
                            || tablename.equals("it_change")
                            || tablename.equals("oc_lista")
                            || tablename.equals("office_sp")) {
                        db = new DBHost(host);
                        dbfiliale = new DBFiliale(myip, filiale);
                        dbfiliale.setDim();
                        cnttableupdate++;
                        sql = "show columns from " + tablename;
                        System.out.println(sql);
                        writer.write(sql + "\n");
                        rs1 = db.getData(sql);
                        if (rs1 != null) {
                            while (rs1.next()) {
                                elencocolonne.add(rs1.getString(1));
                            }

                            sql = "SELECT ";
                            for (int g = 0; g < elencocolonne.size(); g++) {
                                if (g == elencocolonne.size() - 1) {
                                    sql += elencocolonne.get(g) + " ";
                                } else {
                                    sql += elencocolonne.get(g) + " , ";
                                }
                            }
                            sql += " FROM " + tablename;
                            sql += " WHERE filiale = '" + filiale + "'";
                            System.out.println(sql);
                            rs1 = db.getData(sql);
                            if (rs1 != null) {
                                sql1 = "INSERT INTO " + tablename + " (";
                                for (int v = 0; v < elencocolonne.size(); v++) {
                                    if (v == elencocolonne.size() - 1) {
                                        sql1 += elencocolonne.get(v) + ") VALUES (";
                                    } else {
                                        sql1 += elencocolonne.get(v) + ",";
                                    }
                                }
                                for (int v = 0; v < elencocolonne.size(); v++) {
                                    if (v == elencocolonne.size() - 1) {
                                        sql1 += "?)";
                                    } else {
                                        sql1 += "?,";
                                    }
                                }
                                System.out.println("doing insert...");
                                writer.write("doing insert..." + "\n");
                                String[] ris = new String[3];
                                ris[0] = "true";
                                ris[1] = "";
                                ris[2] = "";
                                while (rs1.next() && ris[0].equals("true")) {
                                    ris = dbfiliale.addInfo(rs1, sql1, elencocolonne.size());
//                        System.out.println("doing.....");
                                }
                                if (!ris[0].equals("true")) {
                                    errore = true;
                                    erroredescr = "5.Errore in query " + sql1;
                                    writer.write(erroredescr + "\n");
                                    writer.write(ris[1] + "\n");
                                    writer.write(ris[2] + "\n");
                                    break;
                                }

                                dbfiliale.closeDB();
                                System.out.println("");
                                System.out.println("END table " + tablename);
                                writer.write("END table " + tablename + "\n");
                                System.out.println("");
//                 System.in.read();

                                db.closeDB();

                            } else {
                                errore = true;
                                erroredescr = "4.Errore in query " + sql;
                                System.err.println(erroredescr);
                                writer.write(erroredescr + "\n");
                                break;
                            }

                        } else {
                            errore = true;
                            erroredescr = "3.Errore in query " + sql;
                            System.err.println(erroredescr);
                            writer.write(erroredescr + "\n");
                            break;
                        }

                    }

                    if ( //ultimo anno con insert in altre tabelle                           
                            tablename.equals("ch_transaction_temp")) {
                        db = new DBHost(host);
                        dbfiliale = new DBFiliale(myip, filiale);
                        dbfiliale.setDim();
                        cnttableupdate++;
                        sql = "show columns from " + tablename;
                        System.out.println(sql);
                        writer.write(sql + "\n");
                        rs1 = db.getData(sql);
                        if (rs1 != null) {
                            while (rs1.next()) {
                                elencocolonne.add(rs1.getString(1));
                            }

                            sql = "SELECT ";
                            for (int g = 0; g < elencocolonne.size(); g++) {
                                if (g == elencocolonne.size() - 1) {
                                    sql += elencocolonne.get(g) + " ";
                                } else {
                                    sql += elencocolonne.get(g) + " , ";
                                }
                            }
                            sql += " FROM " + tablename;
                            sql += " WHERE filiale = '" + filiale + "' AND cod IN ( SELECT cod FROM ch_transaction WHERE filiale = '" + filiale + "' AND data >= DATE_SUB(NOW(),INTERVAL 1 YEAR)); ";
                            System.out.println(sql);
                            rs1 = db.getData(sql);
                            if (rs1 != null) {
                                sql1 = "INSERT INTO " + tablename + " (";
                                for (int v = 0; v < elencocolonne.size(); v++) {
                                    if (v == elencocolonne.size() - 1) {
                                        sql1 += elencocolonne.get(v) + ") VALUES (";
                                    } else {
                                        sql1 += elencocolonne.get(v) + ",";
                                    }
                                }
                                for (int v = 0; v < elencocolonne.size(); v++) {
                                    if (v == elencocolonne.size() - 1) {
                                        sql1 += "?)";
                                    } else {
                                        sql1 += "?,";
                                    }
                                }
                                System.out.println("doing insert...");
                                writer.write("doing insert..." + "\n");
                                String[] ris = new String[3];
                                ris[0] = "true";
                                ris[1] = "";
                                ris[2] = "";
                                while (rs1.next() && ris[0].equals("true")) {
                                    ris = dbfiliale.addInfo(rs1, sql1, elencocolonne.size());
//                        System.out.println("doing.....");
                                }
                                if (!ris[0].equals("true")) {
                                    errore = true;
                                    erroredescr = "5.Errore in query " + sql1;
                                    writer.write(erroredescr + "\n");
                                    writer.write(ris[1] + "\n");
                                    writer.write(ris[2] + "\n");
                                    break;
                                }

                                dbfiliale.closeDB();
                                System.out.println("");
                                System.out.println("END table " + tablename);
                                writer.write("END table " + tablename + "\n");
                                System.out.println("");
//                 System.in.read();

                                db.closeDB();

                            } else {
                                errore = true;
                                erroredescr = "4.Errore in query " + sql;
                                System.err.println(erroredescr);
                                writer.write(erroredescr + "\n");
                                break;
                            }

                        } else {
                            errore = true;
                            erroredescr = "3.Errore in query " + sql;
                            System.err.println(erroredescr);
                            writer.write(erroredescr + "\n");
                            break;
                        }
                    }

                    if ( //ultimo anno con insert in altre tabelle
                            tablename.equals("ch_transaction_doc")) {
                        db = new DBHost(host);
                        dbfiliale = new DBFiliale(myip, filiale);
                        dbfiliale.setDim();
                        cnttableupdate++;
                        sql = "show columns from " + tablename;
                        System.out.println(sql);
                        writer.write(sql + "\n");
                        rs1 = db.getData(sql);
                        if (rs1 != null) {
                            while (rs1.next()) {
                                elencocolonne.add(rs1.getString(1));
                            }

                            sql = "SELECT ";
                            for (int g = 0; g < elencocolonne.size(); g++) {
                                if (g == elencocolonne.size() - 1) {
                                    sql += elencocolonne.get(g) + " ";
                                } else {
                                    sql += elencocolonne.get(g) + " , ";
                                }
                            }
                            sql += " FROM " + tablename;
                            sql += " WHERE codtr IN ( SELECT cod FROM ch_transaction WHERE filiale = '" + filiale + "' AND data >= DATE_SUB(NOW(),INTERVAL 1 YEAR)); ";
                            System.out.println(sql);
                            rs1 = db.getData(sql);
                            if (rs1 != null) {
                                sql1 = "INSERT INTO " + tablename + " (";
                                for (int v = 0; v < elencocolonne.size(); v++) {
                                    if (v == elencocolonne.size() - 1) {
                                        sql1 += elencocolonne.get(v) + ") VALUES (";
                                    } else {
                                        sql1 += elencocolonne.get(v) + ",";
                                    }
                                }
                                for (int v = 0; v < elencocolonne.size(); v++) {
                                    if (v == elencocolonne.size() - 1) {
                                        sql1 += "?)";
                                    } else {
                                        sql1 += "?,";
                                    }
                                }
                                System.out.println("doing insert...");
                                writer.write("doing insert..." + "\n");
                                String[] ris = new String[3];
                                ris[0] = "true";
                                ris[1] = "";
                                ris[2] = "";
                                while (rs1.next() && ris[0].equals("true")) {
                                    ris = dbfiliale.addInfo(rs1, sql1, elencocolonne.size());
//                        System.out.println("doing.....");
                                }
                                if (!ris[0].equals("true")) {
                                    errore = true;
                                    erroredescr = "5.Errore in query " + sql1;
                                    writer.write(erroredescr + "\n");
                                    writer.write(ris[1] + "\n");
                                    writer.write(ris[2] + "\n");
                                    break;
                                }

                                dbfiliale.closeDB();
                                System.out.println("");
                                System.out.println("END table " + tablename);
                                writer.write("END table " + tablename + "\n");
                                System.out.println("");
//                 System.in.read();

                                db.closeDB();

                            } else {
                                errore = true;
                                erroredescr = "4.Errore in query " + sql;
                                System.err.println(erroredescr);
                                writer.write(erroredescr + "\n");
                                break;
                            }

                        } else {
                            errore = true;
                            erroredescr = "3.Errore in query " + sql;
                            System.err.println(erroredescr);
                            writer.write(erroredescr + "\n");
                            break;
                        }
                    }

                    if ( //ultimo anno con insert in altre tabelle
                            tablename.equals("ch_transaction_refund")
                            || tablename.equals("ch_transaction_file")
                            || tablename.equals("ch_transaction_valori")) {
                        db = new DBHost(host);
                        dbfiliale = new DBFiliale(myip, filiale);
                        dbfiliale.setDim();
                        cnttableupdate++;
                        sql = "show columns from " + tablename;
                        System.out.println(sql);
                        writer.write(sql + "\n");
                        rs1 = db.getData(sql);
                        if (rs1 != null) {
                            while (rs1.next()) {
                                elencocolonne.add(rs1.getString(1));
                            }

                            sql = "SELECT ";
                            for (int g = 0; g < elencocolonne.size(); g++) {
                                if (g == elencocolonne.size() - 1) {
                                    sql += elencocolonne.get(g) + " ";
                                } else {
                                    sql += elencocolonne.get(g) + " , ";
                                }
                            }
                            sql += " FROM " + tablename;
                            sql += " WHERE  cod_tr IN ( SELECT cod FROM ch_transaction WHERE filiale = '" + filiale + "' AND data >= DATE_SUB(NOW(),INTERVAL 1 YEAR)); ";
                            System.out.println(sql);
                            rs1 = db.getData(sql);
                            if (rs1 != null) {
                                sql1 = "INSERT INTO " + tablename + " (";
                                for (int v = 0; v < elencocolonne.size(); v++) {
                                    if (v == elencocolonne.size() - 1) {
                                        sql1 += elencocolonne.get(v) + ") VALUES (";
                                    } else {
                                        sql1 += elencocolonne.get(v) + ",";
                                    }
                                }
                                for (int v = 0; v < elencocolonne.size(); v++) {
                                    if (v == elencocolonne.size() - 1) {
                                        sql1 += "?)";
                                    } else {
                                        sql1 += "?,";
                                    }
                                }
                                System.out.println("doing insert...");
                                writer.write("doing insert..." + "\n");
                                String[] ris = new String[3];
                                ris[0] = "true";
                                ris[1] = "";
                                ris[2] = "";
                                while (rs1.next() && ris[0].equals("true")) {
                                    ris = dbfiliale.addInfo(rs1, sql1, elencocolonne.size());
//                        System.out.println("doing.....");
                                }
                                if (!ris[0].equals("true")) {
                                    errore = true;
                                    erroredescr = "5.Errore in query " + sql1;
                                    writer.write(erroredescr + "\n");
                                    writer.write(ris[1] + "\n");
                                    writer.write(ris[2] + "\n");
                                    break;
                                }

                                dbfiliale.closeDB();
                                System.out.println("");
                                System.out.println("END table " + tablename);
                                writer.write("END table " + tablename + "\n");
                                System.out.println("");
//                 System.in.read();

                                db.closeDB();

                            } else {
                                errore = true;
                                erroredescr = "4.Errore in query " + sql;
                                System.err.println(erroredescr);
                                writer.write(erroredescr + "\n");
                                break;
                            }

                        } else {
                            errore = true;
                            erroredescr = "3.Errore in query " + sql;
                            System.err.println(erroredescr);
                            writer.write(erroredescr + "\n");
                            break;
                        }
                    }

                    if ( //ultimo anno con insert in altre tabelle
                            tablename.equals("et_change_tg")
                            || tablename.equals("et_change_valori")
                            || tablename.equals("et_frombranch")
                            || tablename.equals("et_nochange_valori")) {
                        db = new DBHost(host);
                        dbfiliale = new DBFiliale(myip, filiale);
                        dbfiliale.setDim();
                        cnttableupdate++;
                        sql = "show columns from " + tablename;
                        System.out.println(sql);
                        writer.write(sql + "\n");
                        rs1 = db.getData(sql);
                        if (rs1 != null) {
                            while (rs1.next()) {
                                elencocolonne.add(rs1.getString(1));
                            }

                            sql = "SELECT ";
                            for (int g = 0; g < elencocolonne.size(); g++) {
                                if (g == elencocolonne.size() - 1) {
                                    sql += elencocolonne.get(g) + " ";
                                } else {
                                    sql += elencocolonne.get(g) + " , ";
                                }
                            }
                            sql += " FROM " + tablename;
                            sql += " WHERE filiale = '" + filiale + "' AND cod IN ( SELECT cod FROM et_change WHERE filiale = '" + filiale + "' AND dt_it >= DATE_SUB(NOW(),INTERVAL 1 YEAR)); ";
                            System.out.println(sql);
                            rs1 = db.getData(sql);
                            if (rs1 != null) {
                                sql1 = "INSERT INTO " + tablename + " (";
                                for (int v = 0; v < elencocolonne.size(); v++) {
                                    if (v == elencocolonne.size() - 1) {
                                        sql1 += elencocolonne.get(v) + ") VALUES (";
                                    } else {
                                        sql1 += elencocolonne.get(v) + ",";
                                    }
                                }
                                for (int v = 0; v < elencocolonne.size(); v++) {
                                    if (v == elencocolonne.size() - 1) {
                                        sql1 += "?)";
                                    } else {
                                        sql1 += "?,";
                                    }
                                }
                                System.out.println("doing insert...");
                                writer.write("doing insert..." + "\n");
                                String[] ris = new String[3];
                                ris[0] = "true";
                                ris[1] = "";
                                ris[2] = "";
                                while (rs1.next() && ris[0].equals("true")) {
                                    ris = dbfiliale.addInfo(rs1, sql1, elencocolonne.size());
//                        System.out.println("doing.....");
                                }
                                if (!ris[0].equals("true")) {
                                    errore = true;
                                    erroredescr = "5.Errore in query " + sql1;
                                    writer.write(erroredescr + "\n");
                                    writer.write(ris[1] + "\n");
                                    writer.write(ris[2] + "\n");
                                    break;
                                }

                                dbfiliale.closeDB();
                                System.out.println("");
                                System.out.println("END table " + tablename);
                                writer.write("END table " + tablename + "\n");
                                System.out.println("");
//                 System.in.read();

                                db.closeDB();

                            } else {
                                errore = true;
                                erroredescr = "4.Errore in query " + sql;
                                System.err.println(erroredescr);
                                writer.write(erroredescr + "\n");
                                break;
                            }

                        } else {
                            errore = true;
                            erroredescr = "3.Errore in query " + sql;
                            System.err.println(erroredescr);
                            writer.write(erroredescr + "\n");
                            break;
                        }

                    }

                    if ( //ultimo anno con insert in altre tabelle
                            tablename.equals("it_change_tg")
                            || tablename.equals("it_change_valori")
                            || tablename.equals("it_nochange_valori")) {

                        db = new DBHost(host);
                        dbfiliale = new DBFiliale(myip, filiale);
                        dbfiliale.setDim();
                        cnttableupdate++;
                        sql = "show columns from " + tablename;
                        System.out.println(sql);
                        writer.write(sql + "\n");
                        rs1 = db.getData(sql);
                        if (rs1 != null) {
                            while (rs1.next()) {
                                elencocolonne.add(rs1.getString(1));
                            }

                            sql = "SELECT ";
                            for (int g = 0; g < elencocolonne.size(); g++) {
                                if (g == elencocolonne.size() - 1) {
                                    sql += elencocolonne.get(g) + " ";
                                } else {
                                    sql += elencocolonne.get(g) + " , ";
                                }
                            }
                            sql += " FROM " + tablename;
                            sql += " WHERE filiale = '" + filiale + "' AND cod IN ( SELECT cod FROM it_change WHERE filiale = '" + filiale + "' AND dt_it >= DATE_SUB(NOW(),INTERVAL 1 YEAR)); ";
                            System.out.println(sql);
                            rs1 = db.getData(sql);
                            if (rs1 != null) {
                                sql1 = "INSERT INTO " + tablename + " (";
                                for (int v = 0; v < elencocolonne.size(); v++) {
                                    if (v == elencocolonne.size() - 1) {
                                        sql1 += elencocolonne.get(v) + ") VALUES (";
                                    } else {
                                        sql1 += elencocolonne.get(v) + ",";
                                    }
                                }
                                for (int v = 0; v < elencocolonne.size(); v++) {
                                    if (v == elencocolonne.size() - 1) {
                                        sql1 += "?)";
                                    } else {
                                        sql1 += "?,";
                                    }
                                }
                                System.out.println("doing insert...");
                                writer.write("doing insert..." + "\n");
                                String[] ris = new String[3];
                                ris[0] = "true";
                                ris[1] = "";
                                ris[2] = "";
                                while (rs1.next() && ris[0].equals("true")) {
                                    ris = dbfiliale.addInfo(rs1, sql1, elencocolonne.size());
//                        System.out.println("doing.....");
                                }
                                if (!ris[0].equals("true")) {
                                    errore = true;
                                    erroredescr = "5.Errore in query " + sql1;
                                    writer.write(erroredescr + "\n");
                                    writer.write(ris[1] + "\n");
                                    writer.write(ris[2] + "\n");
                                    break;
                                }

                                dbfiliale.closeDB();
                                System.out.println("");
                                System.out.println("END table " + tablename);
                                writer.write("END table " + tablename + "\n");
                                System.out.println("");
//                 System.in.read();

                                db.closeDB();

                            } else {
                                errore = true;
                                erroredescr = "4.Errore in query " + sql;
                                System.err.println(erroredescr);
                                writer.write(erroredescr + "\n");
                                break;
                            }

                        } else {
                            errore = true;
                            erroredescr = "3.Errore in query " + sql;
                            System.err.println(erroredescr);
                            writer.write(erroredescr + "\n");
                            break;
                        }

                    }

                    if ( //ultimo anno con insert in altre tabelle
                            tablename.equals("oc_change")
                            || tablename.equals("oc_change_tg")
                            || tablename.equals("oc_errors")
                            || tablename.equals("oc_nochange")
                            || tablename.equals("oc_pos")
                            || tablename.equals("real_oc_change")
                            || tablename.equals("real_oc_change_tg")
                            || tablename.equals("real_oc_nochange")
                            || tablename.equals("real_oc_pos")) {
                        dbfiliale = new DBFiliale(myip, filiale);
                        dbfiliale.setDim();
                        db = new DBHost(host);
                        cnttableupdate++;
                        sql = "show columns from " + tablename;
                        System.out.println(sql);
                        writer.write(sql + "\n");
                        rs1 = db.getData(sql);
                        if (rs1 != null) {
                            while (rs1.next()) {
                                elencocolonne.add(rs1.getString(1));
                            }

                            sql = "SELECT ";
                            for (int g = 0; g < elencocolonne.size(); g++) {
                                if (g == elencocolonne.size() - 1) {
                                    sql += elencocolonne.get(g) + " ";
                                } else {
                                    sql += elencocolonne.get(g) + " , ";
                                }
                            }
                            sql += " FROM " + tablename;
                            if (tablename.equals("oc_errors")) {
                                sql += " WHERE filiale = '" + filiale + "' AND cod IN ( SELECT cod FROM oc_lista WHERE filiale = '" + filiale + "' AND data >= DATE_SUB(NOW(),INTERVAL 1 YEAR)); ";
                            } else {
                                sql += " WHERE filiale = '" + filiale + "' AND cod_oc IN ( SELECT cod FROM oc_lista WHERE filiale = '" + filiale + "' AND data >= DATE_SUB(NOW(),INTERVAL 1 YEAR)); ";
                            }
                            System.out.println(sql);
                            rs1 = db.getData(sql);
                            if (rs1 != null) {
                                sql1 = "INSERT INTO " + tablename + " (";
                                for (int v = 0; v < elencocolonne.size(); v++) {
                                    if (v == elencocolonne.size() - 1) {
                                        sql1 += elencocolonne.get(v) + ") VALUES (";
                                    } else {
                                        sql1 += elencocolonne.get(v) + ",";
                                    }
                                }
                                for (int v = 0; v < elencocolonne.size(); v++) {
                                    if (v == elencocolonne.size() - 1) {
                                        sql1 += "?)";
                                    } else {
                                        sql1 += "?,";
                                    }
                                }
                                System.out.println("doing insert...");
                                writer.write("doing insert..." + "\n");
                                String[] ris = new String[3];
                                ris[0] = "true";
                                ris[1] = "";
                                ris[2] = "";
                                while (rs1.next() && ris[0].equals("true")) {
                                    ris = dbfiliale.addInfo(rs1, sql1, elencocolonne.size());
//                        System.out.println("doing.....");
                                }
                                if (!ris[0].equals("true")) {
                                    errore = true;
                                    erroredescr = "5.Errore in query " + sql1;
                                    writer.write(erroredescr + "\n");
                                    writer.write(ris[1] + "\n");
                                    writer.write(ris[2] + "\n");
                                    break;
                                }

                                dbfiliale.closeDB();
                                System.out.println("");
                                System.out.println("END table " + tablename);
                                writer.write("END table " + tablename + "\n");
                                System.out.println("");
//                 System.in.read();

                                db.closeDB();

                            } else {
                                errore = true;
                                erroredescr = "4.Errore in query " + sql;
                                System.err.println(erroredescr);
                                writer.write(erroredescr + "\n");
                                break;
                            }

                        } else {
                            errore = true;
                            erroredescr = "3.Errore in query " + sql;
                            System.err.println(erroredescr);
                            writer.write(erroredescr + "\n");
                            break;
                        }

                    }
                    if ( //ultimo anno con insert in altre tabelle
                            tablename.equals("stock_quantity")) {
                        db = new DBHost(host);
                        dbfiliale = new DBFiliale(myip, filiale);
                        dbfiliale.setDim();
                        cnttableupdate++;
                        sql = "show columns from " + tablename;
                        System.out.println(sql);
                        writer.write(sql + "\n");
                        rs1 = db.getData(sql);
                        if (rs1 != null) {
                            while (rs1.next()) {
                                elencocolonne.add(rs1.getString(1));
                            }

                            sql = "SELECT ";
                            for (int g = 0; g < elencocolonne.size(); g++) {
                                if (g == elencocolonne.size() - 1) {
                                    sql += elencocolonne.get(g) + " ";
                                } else {
                                    sql += elencocolonne.get(g) + " , ";
                                }
                            }
                            sql += " FROM " + tablename;
                            sql += " WHERE codice_stock IN ( SELECT codice FROM stock_story WHERE filiale = '" + filiale + "' AND date >= DATE_SUB(NOW(),INTERVAL 1 YEAR)); ";
                            System.out.println(sql);
                            rs1 = db.getData(sql);
                            if (rs1 != null) {
                                sql1 = "INSERT INTO " + tablename + " (";
                                for (int v = 0; v < elencocolonne.size(); v++) {
                                    if (v == elencocolonne.size() - 1) {
                                        sql1 += elencocolonne.get(v) + ") VALUES (";
                                    } else {
                                        sql1 += elencocolonne.get(v) + ",";
                                    }
                                }
                                for (int v = 0; v < elencocolonne.size(); v++) {
                                    if (v == elencocolonne.size() - 1) {
                                        sql1 += "?)";
                                    } else {
                                        sql1 += "?,";
                                    }
                                }
                                System.out.println("doing insert...");
                                writer.write("doing insert..." + "\n");
                                String[] ris = new String[3];
                                ris[0] = "true";
                                ris[1] = "";
                                ris[2] = "";
                                while (rs1.next() && ris[0].equals("true")) {
                                    ris = dbfiliale.addInfo(rs1, sql1, elencocolonne.size());
//                        System.out.println("doing.....");
                                }
                                if (!ris[0].equals("true")) {
                                    errore = true;
                                    erroredescr = "5.Errore in query " + sql1;
                                    writer.write(erroredescr + "\n");
                                    writer.write(ris[1] + "\n");
                                    writer.write(ris[2] + "\n");
                                    break;
                                }

                                dbfiliale.closeDB();
                                System.out.println("");
                                System.out.println("END table " + tablename);
                                writer.write("END table " + tablename + "\n");
                                System.out.println("");
//                 System.in.read();

                                db.closeDB();

                            } else {
                                errore = true;
                                erroredescr = "4.Errore in query " + sql;
                                System.err.println(erroredescr);
                                writer.write(erroredescr + "\n");
                                break;
                            }

                        } else {
                            errore = true;
                            erroredescr = "3.Errore in query " + sql;
                            System.err.println(erroredescr);
                            writer.write(erroredescr + "\n");
                            break;
                        }

                    }

                    if ( //ultimo anno con insert in altre tabelle
                            tablename.equals("office_sp_valori")) {
                        db = new DBHost(host);
                        dbfiliale = new DBFiliale(myip, filiale);
                        dbfiliale.setDim();
                        cnttableupdate++;
                        sql = "show columns from " + tablename;
                        System.out.println(sql);
                        writer.write(sql + "\n");
                        rs1 = db.getData(sql);
                        if (rs1 != null) {
                            while (rs1.next()) {
                                elencocolonne.add(rs1.getString(1));
                            }

                            sql = "SELECT ";
                            for (int g = 0; g < elencocolonne.size(); g++) {
                                if (g == elencocolonne.size() - 1) {
                                    sql += elencocolonne.get(g) + " ";
                                } else {
                                    sql += elencocolonne.get(g) + " , ";
                                }
                            }
                            sql += " FROM " + tablename;
                            sql += " WHERE cod IN ( SELECT codice FROM office_sp WHERE filiale = '" + filiale + "' AND data >= DATE_SUB(NOW(),INTERVAL 1 YEAR)); ";
                            System.out.println(sql);
                            rs1 = db.getData(sql);
                            if (rs1 != null) {
                                sql1 = "INSERT INTO " + tablename + " (";
                                for (int v = 0; v < elencocolonne.size(); v++) {
                                    if (v == elencocolonne.size() - 1) {
                                        sql1 += elencocolonne.get(v) + ") VALUES (";
                                    } else {
                                        sql1 += elencocolonne.get(v) + ",";
                                    }
                                }
                                for (int v = 0; v < elencocolonne.size(); v++) {
                                    if (v == elencocolonne.size() - 1) {
                                        sql1 += "?)";
                                    } else {
                                        sql1 += "?,";
                                    }
                                }
                                System.out.println("doing insert...");
                                writer.write("doing insert..." + "\n");
                                String[] ris = new String[3];
                                ris[0] = "true";
                                ris[1] = "";
                                ris[2] = "";
                                while (rs1.next() && ris[0].equals("true")) {
                                    ris = dbfiliale.addInfo(rs1, sql1, elencocolonne.size());
//                        System.out.println("doing.....");
                                }
                                if (!ris[0].equals("true")) {
                                    errore = true;
                                    erroredescr = "5.Errore in query " + sql1;
                                    writer.write(erroredescr + "\n");
                                    writer.write(ris[1] + "\n");
                                    writer.write(ris[2] + "\n");
                                    break;
                                }

                                dbfiliale.closeDB();
                                System.out.println("");
                                System.out.println("END table " + tablename);
                                writer.write("END table " + tablename + "\n");
                                System.out.println("");
//                 System.in.read();

                                db.closeDB();

                            } else {
                                errore = true;
                                erroredescr = "4.Errore in query " + sql;
                                System.err.println(erroredescr);
                                writer.write(erroredescr + "\n");
                                break;
                            }

                        } else {
                            errore = true;
                            erroredescr = "3.Errore in query " + sql;
                            System.err.println(erroredescr);
                            writer.write(erroredescr + "\n");
                            break;
                        }

                    }

                    if (//FULL
                            tablename.equals("nc_causali")
                            //      || tablename.equals("bank")
                            //   || tablename.equals("province")
                            || tablename.equals("nc_causali_pay")
                            || tablename.equals("nc_tipologia")
                            || tablename.equals("rate_range")
                            || tablename.equals("supporti")
                            || tablename.equals("supporti_valuta")
                            || tablename.equals("till")
                            || tablename.equals("valute")
                            || tablename.equals("valute_tagli")) {

                        db = new DBHost(host);

                        dbfiliale = new DBFiliale(myip, filiale);
                        dbfiliale.setDim();

                        if (dbfiliale.getConnectionDB() != null) {

                            cnttableupdate++;

                            sql = "show columns from " + tablename;
                            System.out.println(sql);
                            writer.write(sql + "\n");
                            rs1 = db.getData(sql);
                            if (rs1 != null) {
                                while (rs1.next()) {
                                    elencocolonne.add(rs1.getString(1));
                                }

                                sql = "SELECT ";
                                for (int g = 0; g < elencocolonne.size(); g++) {
                                    if (g == elencocolonne.size() - 1) {
                                        sql += elencocolonne.get(g) + " ";
                                    } else {
                                        sql += elencocolonne.get(g) + " , ";
                                    }
                                }
                                sql += " FROM " + tablename + " WHERE filiale = '" + filiale + "' ";
                                rs1 = db.getData(sql);
                                if (rs1 != null) {
                                    sql1 = "INSERT INTO " + tablename + " (";
                                    for (int v = 0; v < elencocolonne.size(); v++) {
                                        if (v == elencocolonne.size() - 1) {
                                            sql1 += elencocolonne.get(v) + ") VALUES (";
                                        } else {
                                            sql1 += elencocolonne.get(v) + ",";
                                        }
                                    }
                                    for (int v = 0; v < elencocolonne.size(); v++) {
                                        if (v == elencocolonne.size() - 1) {
                                            sql1 += "?)";
                                        } else {
                                            sql1 += "?,";
                                        }
                                    }
                                    System.out.println("doing insert...");
                                    writer.write("doing insert..." + "\n");
                                    String[] ris = new String[3];
                                    ris[0] = "true";
                                    ris[1] = "";
                                    ris[2] = "";
                                    while (rs1.next() && ris[0].equals("true")) {
                                        ris = dbfiliale.addInfo(rs1, sql1, elencocolonne.size());
//                        System.out.println("doing.....");
                                    }
                                    if (!ris[0].equals("true")) {
                                        errore = true;
                                        erroredescr = "5.Errore in query " + sql1;
                                        writer.write(erroredescr + "\n");
                                        writer.write(ris[1] + "\n");
                                        writer.write(ris[2] + "\n");
                                        break;
                                    }

                                    dbfiliale.closeDB();
                                    System.out.println("");
                                    writer.write("END TABLE " + tablename + "\n");
                                    System.out.println("END table " + tablename);
                                    System.out.println("");
//                 System.in.read();

                                    db.closeDB();

                                } else {
                                    errore = true;
                                    erroredescr = "4.Errore in query " + sql;
                                    writer.write(erroredescr + "\n");
                                    break;
                                }

                            } else {
                                errore = true;
                                erroredescr = "3.Errore in query " + sql;
                                writer.write(erroredescr + "\n");
                                break;
                            }

                        }//if dbfiliale !=null
                        else {
                            erroredescr = "2.Errore Connessione a DB filiale " + filiale;
                            errore = true;
                            writer.write(erroredescr + "\n");
                            break;
                        }

                    }

                    if ( //ultimo anno
                            tablename.equals("forex_prenot")
                            || tablename.equals("inv_incremental")
                            || tablename.equals("inv_list")
                            || tablename.equals("nc_transaction")
                            || tablename.equals("rate_history")
                            || tablename.equals("rate_history_mod")
                            || tablename.equals("stock")
                            || tablename.equals("stock_report")) //|| tablename.equals("stock_story")) 
                    {

                        db = new DBHost(host);

                        dbfiliale = new DBFiliale(myip, filiale);
                        dbfiliale.setDim();

                        if (dbfiliale.getConnectionDB() != null) {

                            cnttableupdate++;

                            sql = "show columns from " + tablename;
                            System.out.println(sql);
                            writer.write(sql + "\n");
                            rs1 = db.getData(sql);
                            if (rs1 != null) {
                                while (rs1.next()) {
                                    elencocolonne.add(rs1.getString(1));
                                }

                                sql = "SELECT ";
                                for (int g = 0; g < elencocolonne.size(); g++) {
                                    if (g == elencocolonne.size() - 1) {
                                        sql += elencocolonne.get(g) + " ";
                                    } else {
                                        sql += elencocolonne.get(g) + " , ";
                                    }
                                }
                                sql += " FROM " + tablename + " WHERE filiale = '" + filiale + "'";
                                //dt_upload >= DATE_SUB(NOW(),INTERVAL 1 YEAR);
                                if (tablename.equals("nc_transaction") || tablename.equals("stock_report")) { //data
                                    sql += " AND data >= DATE_SUB(NOW(),INTERVAL 1 YEAR);";
                                }
                                if (tablename.equals("forex_prenot")) {
                                    sql += " AND timestamp >= DATE_SUB(NOW(),INTERVAL 1 YEAR);";
                                }
                                if (tablename.equals("inv_incremental") || tablename.equals("inv_list")
                                        || tablename.equals("rate_history_mod")) {
                                    sql += " AND dt >= DATE_SUB(NOW(),INTERVAL 1 YEAR);";
                                }
                                if (tablename.equals("rate_history")) {
                                    sql += " AND dt_mod >= DATE_SUB(NOW(),INTERVAL 1 YEAR);";
                                }
                                //if (tablename.equals("stock") || tablename.equals("stock_story")) {
                                if (tablename.equals("stock")) {
                                    sql += " AND date >= DATE_SUB(NOW(),INTERVAL 1 YEAR);";
                                }
                                rs1 = db.getData(sql);
                                if (rs1 != null) {
                                    sql1 = "INSERT INTO " + tablename + " (";
                                    for (int v = 0; v < elencocolonne.size(); v++) {
                                        if (v == elencocolonne.size() - 1) {
                                            sql1 += elencocolonne.get(v) + ") VALUES (";
                                        } else {
                                            sql1 += elencocolonne.get(v) + ",";
                                        }
                                    }
                                    for (int v = 0; v < elencocolonne.size(); v++) {
                                        if (v == elencocolonne.size() - 1) {
                                            sql1 += "?)";
                                        } else {
                                            sql1 += "?,";
                                        }
                                    }
                                    System.out.println("doing insert...");
                                    writer.write("doing insert..." + "\n");
                                    String[] ris = new String[3];
                                    ris[0] = "true";
                                    ris[1] = "";
                                    ris[2] = "";
                                    while (rs1.next() && ris[0].equals("true")) {
                                        ris = dbfiliale.addInfo(rs1, sql1, elencocolonne.size());
//                        System.out.println("doing.....");
                                    }
                                    if (!ris[0].equals("true")) {
                                        errore = true;
                                        erroredescr = "5.Errore in query " + sql1;
                                        writer.write(erroredescr + "\n");
                                        writer.write(ris[1] + "\n");
                                        writer.write(ris[2] + "\n");
                                        break;
                                    }

                                    dbfiliale.closeDB();
                                    System.out.println("");
                                    writer.write("END TABLE " + tablename + "\n");
                                    System.out.println("END table " + tablename);
                                    System.out.println("");
//                 System.in.read();

                                    db.closeDB();

                                } else {
                                    errore = true;
                                    erroredescr = "4.Errore in query " + sql;
                                    writer.write(erroredescr + "\n");
                                    break;
                                }

                            } else {
                                errore = true;
                                erroredescr = "3.Errore in query " + sql;
                                writer.write(erroredescr + "\n");
                                break;
                            }

                        }//if dbfiliale !=null
                        else {
                            erroredescr = "2.Errore Connessione a DB filiale " + filiale;
                            errore = true;
                            writer.write(erroredescr + "\n");
                            break;
                        }
                    }

                }//for elencotabelle

            } else {
                errore = true;
                erroredescr = "1.Errore connessione al DB centrale";
                writer.write(erroredescr + "\n");
            }

            System.out.println("NUMERO DI TABELLE AGGIORNATE: " + cnttableupdate);

            if (errore) {
                boolean esitoreset = false;
                if(dbfiliale!=null){
                    dbfiliale.closeDB();
                }
                db.closeDB();
                if (!erroredescr.startsWith("1.") && tipooperazione.equals("1")) { //se almeno mi sono collegato al DB centrale e se sto installando la prima volta
                    esitoreset = eseguiresettabelleFiliale(myip, filiale, elencotabelle);
                }

                System.err.println(erroredescr);

                System.err.println("Si sono verificati errori durante l'operazione di inizializzazione dei dati sul DB.");
                writer.write(erroredescr + "\n");
                writer.write("Si sono verificati errori durante l'operazione di inizializzazione dei dati sul DB" + "\n");

//                if (!tipooperazione.equals("1")) {
//                    System.err.println("Il DB è stato resettato");
//                }
            }

            writer.write("-----------------   END   ------------------------------");
            writer.close();

        } catch (IOException | SQLException e) {
            e.printStackTrace();
        }

        System.out.println("-----------------   END   ------------------------------");

        db.closeDB();
        return errore;
    }

    public boolean eseguiresettabelle(String myip, String filiale) throws IOException {

        DBFiliale dbf = new DBFiliale(myip, filiale);
        boolean esitotruncate = true;
        for (int g = 0; g < elencotabelle.size(); g++) {
            String tablename = elencotabelle.get(g);
            if (tablename.equals("agenzie")
                    || tablename.equals("bank")
                    || tablename.equals("branchbudget")
                    || tablename.equals("branchgroup")
                    || tablename.equals("anagrafica_ru")
                    || tablename.equals("anagrafica_ru_attach")
                    || tablename.equals("province")
                    || tablename.equals("bce_year")
                    || tablename.equals("blacklist")
                    || tablename.equals("branch")
                    || tablename.equals("carte_credito")
                    || tablename.equals("cash_perm")
                    || tablename.equals("codici_fiscali_esteri")
                    || tablename.equals("codici_fiscali_italia")
                    || tablename.equals("codici_fiscali_mese")
                    || tablename.equals("commissione_fissa")
                    || tablename.equals("compro")
                    || tablename.equals("comuni_apm")
                    || tablename.equals("conf")
                    || tablename.equals("configmonitor")
                    || tablename.equals("contabilita")
                    || tablename.equals("internetbooking")
                    || tablename.equals("kind_commissione_fissa")
                    || tablename.equals("logical")
                    || tablename.equals("nazioni")
                    || tablename.equals("nc_kind")
                    || tablename.equals("office")
                    || tablename.equals("pages")
                    || tablename.equals("path")
                    || tablename.equals("paymat")
                    || tablename.equals("select_g01")
                    || tablename.equals("select_g02")
                    || tablename.equals("select_g03")
                    || tablename.equals("select_rv")
                    || tablename.equals("selectareanaz")
                    || tablename.equals("selectdoctrans")
                    || tablename.equals("selectgroupbranch")
                    || tablename.equals("selectgrouptype")
                    || tablename.equals("selectinout")
                    || tablename.equals("selectkind")
                    || tablename.equals("selectlevelrate")
                    || tablename.equals("selectncde")
                    || tablename.equals("selectresident")
                    || tablename.equals("selecttipocliente")
                    || tablename.equals("selecttipov")
                    || tablename.equals("temppaymat")
                    || tablename.equals("tipologiaclienti")
                    || tablename.equals("tipologiadocumento")
                    || tablename.equals("under_min_comm_justify")
                    || tablename.equals("user_sito")
                    || tablename.equals("users")
                    || tablename.equals("vatcode")) {
                System.out.println("Eseguo TRUNCATE per la tabella " + tablename);
                writer.write("Eseguo TRUNCATE per la tabella " + tablename + "\n");
                if (!dbf.eseuitruncate(tablename)) {
                    esitotruncate = false;

                }
            }
        }
        return esitotruncate;
    }

    public boolean eseguiresettabelleFull(String myip, String filiale, ArrayList<String> mytables) throws IOException {

        DBFiliale dbf = new DBFiliale(myip, filiale);
        boolean esitotruncate = true;
        for (int g = 0; g < mytables.size(); g++) {
            String tablename = mytables.get(g);

            if (tablename.equals("codici_sblocco") || tablename.equals("codici_sblocco_file") || tablename.equals("transaction_reprint")
                    || tablename.equals("newsletter_status") || tablename.equals("newsletter") || tablename.equals("local")) {

                System.out.println("Eseguo TRUNCATE per la tabella " + tablename);
                writer.write("Eseguo TRUNCATE per la tabella " + tablename + "\n");
                if (!dbf.eseuitruncate(tablename)) {
                    esitotruncate = false;

                }
            }
        }
        return esitotruncate;
    }

    public boolean eseguiresettabelleFiliale(String myip, String filiale, ArrayList<String> mytables) throws IOException {

        DBFiliale dbf = new DBFiliale(myip, filiale);
        boolean esitotruncate = true;
        for (int g = 0; g < mytables.size(); g++) {
            String tablename = mytables.get(g);
            if (tablename.equals("ch_transaction") || tablename.equals("et_change") || tablename.equals("it_change")
                    || tablename.equals("oc_lista") || tablename.equals("office_sp") || tablename.equals("ch_transaction_temp")
                    || tablename.equals("ch_transaction_doc")
                    || tablename.equals("ch_transaction_refund") || tablename.equals("ch_transaction_file")
                    || tablename.equals("ch_transaction_valori")
                    || tablename.equals("et_change_tg")
                    || tablename.equals("et_change_valori") || tablename.equals("et_frombranch")
                    || tablename.equals("et_nochange_valori")
                    || tablename.equals("it_change_tg") || tablename.equals("it_change_valori")
                    || tablename.equals("it_nochange_valori") || tablename.equals("oc_change") || tablename.equals("oc_change_tg")
                    || tablename.equals("oc_errors")
                    || tablename.equals("oc_nochange") || tablename.equals("oc_pos") || tablename.equals("real_oc_change")
                    || tablename.equals("real_oc_change_tg") || tablename.equals("real_oc_nochange") || tablename.equals("real_oc_pos")
                    || tablename.equals("office_sp_valori") || tablename.equals("nc_causali") || tablename.equals("nc_causali_pay")
                    || tablename.equals("nc_tipologia") || tablename.equals("rate_range")
                    || tablename.equals("supporti") || tablename.equals("supporti_valuta")
                    || tablename.equals("till")
                    || tablename.equals("valute") || tablename.equals("valute_tagli") || tablename.equals("forex_prenot")
                    || tablename.equals("inv_incremental") || tablename.equals("inv_list") || tablename.equals("nc_transaction")
                    || tablename.equals("rate_history") || tablename.equals("rate_history_mod") || tablename.equals("stock")
                    || tablename.equals("stock_report") || tablename.equals("stock_story")
                    || tablename.equals("stock_quantity")
                    || tablename.equals("ch_transaction_doc_story") || tablename.equals("ch_transaction_file_story")
                    || tablename.equals("ch_transaction_refund_story") || tablename.equals("ch_transaction_story")
                    || tablename.equals("ch_transaction_valori_story")) {

                System.out.println("Eseguo TRUNCATE per la tabella " + tablename);
                writer.write("Eseguo TRUNCATE per la tabella " + tablename + "\n");
                if (!dbf.eseuitruncate(tablename)) {
                    esitotruncate = false;

                }
            }
        }
        return esitotruncate;
    }

}
