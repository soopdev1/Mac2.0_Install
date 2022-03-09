/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.refill.engine;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.apache.commons.lang3.RandomStringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 *
 * @author rcosco
 */
public class Utility {

    public static final String patternnormdate = "dd/MM/yyyy HH:mm:ss";
    public static final String patternsqldate = "yyyy-MM-dd HH:mm:ss";
    public static final String patternsql = "yyyy-MM-dd";
    public static final String hostPROD = "//172.18.17.41:3306/maccorpita";
//    public static final String hostPROD = "//172.18.17.41:3306/maccorp";
    public static final String hostTEST = "//172.18.17.41:3306/maccorp";

    public static String generaId(int length) {
        String random = RandomStringUtils.randomAlphanumeric(length - 15).trim();
        return new DateTime().toString("yyMMddHHmmssSSS") + random;
    }

    public static String formatStringtoStringDate(String dat, String pattern1, String pattern2) {
        try {

            if (dat.length() == 21) {
                dat = dat.substring(0, 19);
            }
            if (dat.length() == pattern1.length()) {
                DateTimeFormatter formatter = DateTimeFormat.forPattern(pattern1);
                DateTime dt = formatter.parseDateTime(dat);
                return dt.toString(pattern2, Locale.ITALY);
            }
        } catch (Exception ex) {

        }
        return dat;
    }

    public static String formatAL(String cod, ArrayList<String[]> array, int index) {
        for (int i = 0; i < array.size(); i++) {
            if (cod.equals(((String[]) array.get(i))[0])) {
                return ((String[]) array.get(i))[index];
            }
        }
        return "-";
    }

    public static String formatIndex(ArrayList<String> valori) {
        String out = "";
        String start = "(";
        String end = ")";
        for (int i = 0; i < valori.size(); i++) {
            out = out + "`" + valori.get(i) + "`, ";
        }
        out = out.trim();
        if (out.contains(",")) {
            out = out.substring(0, out.length() - 1);
        }
        return start + out + end;
    }

    public static String[] formatAL(String cod, ArrayList<String[]> array) {
        for (int i = 0; i < array.size(); i++) {
            if (cod.equals(array.get(i)[0])) {
                return array.get(i);
            }
        }
        return null;
    }

    public static List<List<String>> getListTableAndQuery(String filiale, String fase) {
        String anno = new DateTime().toString("YYYY");

        List<List<String>> out = new ArrayList<>(2);

        List<String> elencotabelle = new ArrayList<>();
        List<String> elencoquery = new ArrayList<>();

        if (fase.equals("1")) {//configurazioni necessarie
            elencotabelle.add("bank");
            elencoquery.add("SELECT * FROM bank");

            elencotabelle.add("branch");
            elencoquery.add("SELECT * FROM branch");

            elencotabelle.add("carte_credito");
            elencoquery.add("SELECT * FROM carte_credito WHERE filiale = '" + filiale + "'");

            elencotabelle.add("commissione_fissa");
            elencoquery.add("SELECT * FROM commissione_fissa where filiale ='" + filiale + "'");

            elencotabelle.add("conf");
            elencoquery.add("SELECT * FROM conf");

            elencotabelle.add("internetbooking");
            elencoquery.add("SELECT * FROM internetbooking");

            elencotabelle.add("logical");
            elencoquery.add("SELECT * FROM logical");

            elencotabelle.add("nc_causali");
            elencoquery.add("SELECT * FROM nc_causali where filiale ='" + filiale + "'");

            elencotabelle.add("nc_causali_pay");
            elencoquery.add("SELECT * FROM nc_causali_pay where filiale ='" + filiale + "'");

            elencotabelle.add("nc_kind");
            elencoquery.add("SELECT * FROM nc_kind");

            elencotabelle.add("nc_tipologia");
            elencoquery.add("SELECT * FROM nc_tipologia where filiale ='" + filiale + "'");

            elencotabelle.add("office");
            elencoquery.add("SELECT * FROM office");
            elencotabelle.add("pages");
            elencoquery.add("SELECT * FROM pages");

            elencotabelle.add("path");
            elencoquery.add("SELECT * FROM path");

            elencotabelle.add("rate_range");
            elencoquery.add("SELECT * FROM rate_range where filiale='" + filiale + "'");

            elencotabelle.add("selectareanaz");
            elencoquery.add("SELECT * FROM selectareanaz");

            elencotabelle.add("selectdoctrans");
            elencoquery.add("SELECT * FROM selectdoctrans");

            elencotabelle.add("selectgroupbranch");
            elencoquery.add("SELECT * FROM selectgroupbranch");

            elencotabelle.add("selectgrouptype");
            elencoquery.add("SELECT * FROM selectgrouptype");

            elencotabelle.add("selectinout");
            elencoquery.add("SELECT * FROM selectinout");

            elencotabelle.add("selectkind");
            elencoquery.add("SELECT * FROM selectkind");

            elencotabelle.add("selectlevelrate");
            elencoquery.add("SELECT * FROM selectlevelrate");

            elencotabelle.add("selectncde");
            elencoquery.add("SELECT * FROM selectncde");

            elencotabelle.add("selectresident");
            elencoquery.add("SELECT * FROM selectresident");

            elencotabelle.add("selecttipocliente");
            elencoquery.add("SELECT * FROM selecttipocliente");

            elencotabelle.add("selecttipov");
            elencoquery.add("SELECT * FROM selecttipov");
            elencotabelle.add("supporti");
            elencoquery.add("SELECT * FROM supporti where filiale='" + filiale + "'");

            elencotabelle.add("supporti_valuta");
            elencoquery.add("SELECT * FROM supporti_valuta where filiale='" + filiale + "'");

            elencotabelle.add("supporti_valuta");
            elencoquery.add("SELECT * FROM supporti_valuta where filiale='" + filiale + "'");

            elencotabelle.add("temppaymat");
            elencoquery.add("SELECT * FROM temppaymat");

            elencotabelle.add("till");
            elencoquery.add("SELECT * FROM till where filiale='" + filiale + "'");

            elencotabelle.add("tipologiaclienti");
            elencoquery.add("SELECT * FROM tipologiaclienti");

            elencotabelle.add("tipologiadocumento");
            elencoquery.add("SELECT * FROM tipologiadocumento");
            elencotabelle.add("under_min_comm_justify");
            elencoquery.add("SELECT * FROM under_min_comm_justify");

            elencotabelle.add("users");
            elencoquery.add("SELECT * FROM users");

            elencotabelle.add("valute");
            elencoquery.add("SELECT * FROM valute where filiale='" + filiale + "'");

            elencotabelle.add("valute_tagli");
            elencoquery.add("SELECT * FROM valute_tagli where filiale='" + filiale + "'");

            elencotabelle.add("vatcode");
            elencoquery.add("SELECT * FROM vatcode");

            elencotabelle.add("sito_agevolazioni_varie");
            elencoquery.add("SELECT * FROM sito_agevolazioni_varie");

            elencotabelle.add("sito_commissione_fissa");
            elencoquery.add("SELECT * FROM sito_commissione_fissa");

            elencotabelle.add("sito_prenotazioni");
            elencoquery.add("SELECT * FROM sito_prenotazioni");

            elencotabelle.add("sito_rate_range");
            elencoquery.add("SELECT * FROM sito_rate_range");

            elencotabelle.add("sito_servizi_agg");
            elencoquery.add("SELECT * FROM sito_servizi_agg");

            elencotabelle.add("sito_spread");
            elencoquery.add("SELECT * FROM sito_spread");

            elencotabelle.add("sito_stato");
            elencoquery.add("SELECT * FROM sito_stato");

            elencotabelle.add("sito_supporti");
            elencoquery.add("SELECT * FROM sito_supporti");

            elencotabelle.add("department");
            elencoquery.add("SELECT * FROM department");

            elencotabelle.add("department_nc");
            elencoquery.add("SELECT * FROM department_nc");

        }

        if (fase.equals("2")) {//dati necessari della filiale

            elencotabelle.add("ch_transaction");
            elencoquery.add("SELECT * FROM ch_transaction where filiale ='" + filiale + "' AND data LIKE '" + anno + "%'");

            elencotabelle.add("ch_transaction_client");
            elencoquery.add("SELECT * FROM ch_transaction_client where codtr like '" + filiale + "%' AND timestamp LIKE '" + anno + "%'");

            elencotabelle.add("ch_transaction_doc");
            elencoquery.add("SELECT * FROM ch_transaction_doc where codtr like '" + filiale + "%' AND data_load LIKE '" + anno + "%'");

            elencotabelle.add("ch_transaction_temp");
            elencoquery.add("SELECT * FROM ch_transaction_temp where filiale ='" + filiale + "' AND data LIKE '" + anno + "%'");

            elencotabelle.add("ch_transaction_valori");
            elencoquery.add("SELECT * FROM ch_transaction_valori where cod_tr like '" + filiale + "%' AND dt_tr LIKE '" + anno + "%'");

            elencotabelle.add("et_change");
            elencoquery.add("SELECT * FROM et_change where filiale ='" + filiale + "' AND dt_it LIKE '" + anno + "%'");

            elencotabelle.add("et_change_tg");
            elencoquery.add("SELECT * FROM et_change_tg where filiale ='" + filiale + "' AND date LIKE '" + anno + "%'");

            elencotabelle.add("et_change_valori");
            elencoquery.add("SELECT * FROM et_change_valori where filiale ='" + filiale + "' AND date LIKE '" + anno + "%'");

            elencotabelle.add("et_nochange_valori");
            elencoquery.add("SELECT * FROM et_nochange_valori where filiale ='" + filiale + "' AND date LIKE '" + anno + "%'");

            elencotabelle.add("et_frombranch");
            elencoquery.add("SELECT * FROM et_frombranch where filiale ='" + filiale + "' OR cod_dest='" + filiale + "' AND dt_it LIKE '" + anno + "%'");

            elencotabelle.add("it_change");
            elencoquery.add("SELECT * FROM it_change where filiale ='" + filiale + "' AND dt_it LIKE '" + anno + "%'");

            elencotabelle.add("it_change_tg");
            elencoquery.add("SELECT * FROM it_change_tg where filiale ='" + filiale + "' AND date LIKE '" + anno + "%'");

            elencotabelle.add("it_change_valori");
            elencoquery.add("SELECT * FROM it_change_valori where filiale ='" + filiale + "' AND date LIKE '" + anno + "%'");

            elencotabelle.add("it_nochange_valori");
            elencoquery.add("SELECT * FROM it_nochange_valori where filiale ='" + filiale + "' AND date LIKE '" + anno + "%'");

            elencotabelle.add("kind_commissione_fissa");
            elencoquery.add("SELECT * FROM kind_commissione_fissa where filiale ='" + filiale + "'");

            elencotabelle.add("inv_incremental");
            elencoquery.add("SELECT * FROM inv_incremental WHERE filiale = '" + filiale + "' AND year = '" + anno + "'");

            elencotabelle.add("inv_list");
            elencoquery.add("SELECT * FROM inv_list WHERE filiale = '" + filiale + "' AND numero LIKE CONCAT('%','" + anno + "','%')");

            elencotabelle.add("nc_transaction");
            elencoquery.add("SELECT * FROM nc_transaction where filiale ='" + filiale + "' AND data LIKE '" + anno + "%'");

            elencotabelle.add("oc_change");
            elencoquery.add("SELECT * FROM oc_change where filiale ='" + filiale + "' AND data LIKE '" + anno + "%'");

            elencotabelle.add("oc_change_tg");
            elencoquery.add("SELECT * FROM oc_change_tg where filiale ='" + filiale + "' AND data LIKE '" + anno + "%'");

            elencotabelle.add("oc_errors");
            elencoquery.add("SELECT * FROM oc_errors where filiale ='" + filiale + "' AND data LIKE '" + anno + "%'");

            elencotabelle.add("oc_lista");
            elencoquery.add("SELECT * FROM oc_lista where filiale ='" + filiale + "' AND data LIKE '" + anno + "%'");

            elencotabelle.add("oc_nochange");
            elencoquery.add("SELECT * FROM oc_nochange where filiale ='" + filiale + "' AND data LIKE '" + anno + "%'");

            elencotabelle.add("oc_pos");
            elencoquery.add("SELECT * FROM oc_pos where filiale ='" + filiale + "' AND data LIKE '" + anno + "%'");

            elencotabelle.add("stock");
            elencoquery.add("SELECT * FROM stock where filiale='" + filiale + "' AND total<>'0.00'");

        }

        if (fase.equals("3")) {//altri

            elencotabelle.add("agenzie");
            elencoquery.add("SELECT * FROM agenzie");

            elencotabelle.add("anagrafica_client");
            elencoquery.add("SELECT * FROM anagrafica_client where code like'" + filiale + "%'");

            elencotabelle.add("anagrafica_ru");
            elencoquery.add("SELECT * FROM anagrafica_ru");

            elencotabelle.add("anagrafica_ru_attach");
            elencoquery.add("SELECT * FROM anagrafica_ru_attach");

//            elencotabelle.add("bb_story");
//            elencoquery.add("SELECT * FROM bb_story");
            elencotabelle.add("bce_year");
            elencoquery.add("SELECT * FROM bce_year");

            elencotabelle.add("blacklist");
            elencoquery.add("SELECT * FROM blacklist");

//            elencotabelle.add("branch_atl");
//            elencoquery.add("SELECT * FROM branch_atl");
            elencotabelle.add("branchbudget");
            elencoquery.add("SELECT * FROM branchbudget");

            elencotabelle.add("branchgroup");
            elencoquery.add("SELECT * FROM branchgroup");

            elencotabelle.add("cash_perm");
            elencoquery.add("SELECT * FROM cash_perm");

            elencotabelle.add("codici_fiscali_esteri");
            elencoquery.add("SELECT * FROM codici_fiscali_esteri");

//            elencotabelle.add("codici_fiscali_italia");
//            elencoquery.add("SELECT * FROM codici_fiscali_italia");

            elencotabelle.add("codici_fiscali_mese");
            elencoquery.add("SELECT * FROM codici_fiscali_mese");

            elencotabelle.add("codici_sblocco");
            elencoquery.add("SELECT * FROM codici_sblocco where cod_tr like'" + filiale + "%' OR fg_stato='E'");

            elencotabelle.add("codici_sblocco_file");
            elencoquery.add("SELECT * FROM codici_sblocco_file");

//            elencotabelle.add("compro");
//            elencoquery.add("SELECT * FROM compro");
//            elencotabelle.add("comuni_apm");
//            elencoquery.add("SELECT * FROM comuni_apm");

            elencotabelle.add("configmonitor");
            elencoquery.add("SELECT * FROM configmonitor");

            elencotabelle.add("contabilita");
            elencoquery.add("SELECT * FROM contabilita");

            elencotabelle.add("internetbooking_ch");
            elencoquery.add("SELECT * FROM internetbooking_ch WHERE cod LIKE '" + filiale + "%'");

            elencotabelle.add("loyalty_ch");
            elencoquery.add("SELECT * FROM loyalty_ch where codtr like '" + filiale + "%'");

            elencotabelle.add("nazioni");
            elencoquery.add("SELECT * FROM nazioni");

            elencotabelle.add("newsletter");
            elencoquery.add("SELECT * FROM newsletter");

            elencotabelle.add("newsletter_status");
            elencoquery.add("SELECT * FROM newsletter_status");

            elencotabelle.add("office_sp");
            elencoquery.add("SELECT * FROM office_sp WHERE filiale='" + filiale + "' AND data LIKE '" + anno + "%'");

            elencotabelle.add("office_sp_valori");
            elencoquery.add("SELECT * FROM office_sp_valori WHERE cod in (SELECT codice FROM office_sp WHERE filiale='" + filiale + "' AND data LIKE '" + anno + "%')");

            elencotabelle.add("paymat");
            elencoquery.add("SELECT * FROM paymat");

            elencotabelle.add("province");
            elencoquery.add("SELECT * FROM province");

            elencotabelle.add("rate_history");
            elencoquery.add("SELECT * FROM rate_history where filiale='" + filiale + "' AND dt_mod LIKE '" + anno + "%'");

            elencotabelle.add("rate_history_mod");
            elencoquery.add("SELECT * FROM rate_history_mod where filiale='" + filiale + "' AND dt LIKE '" + anno + "%'");

            elencotabelle.add("stock_quantity");
            elencoquery.add("SELECT * FROM stock_quantity where cod in (SELECT codice FROM stock where filiale='" + filiale + "' AND total<>'0.00')");

            elencotabelle.add("stock_report");
            elencoquery.add("SELECT * FROM stock_report where filiale='" + filiale + "' AND data LIKE '" + anno + "%'");

            elencotabelle.add("transaction_reprint");
            elencoquery.add("SELECT * FROM transaction_reprint WHERE codtr like'" + filiale + "%' AND date LIKE '" + anno + "%'");

        }

        out.add(elencotabelle);
        out.add(elencoquery);

        return out;
    }

}
