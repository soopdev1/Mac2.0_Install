/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.so.util;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Locale;
import java.util.ResourceBundle;
import net.jimmc.jshortcut.JShellLink;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 *
 * @author srotella
 */
public class Util {
    
    public static final ResourceBundle rb = 
            ResourceBundle.getBundle("img.conf");
    
    public static boolean test = rb.getString("test").equals("SI");
    public static boolean italia = rb.getString("nation").equals("IT");
    public static boolean cz = rb.getString("nation").equals("CZ");
    public static boolean uk = rb.getString("nation").equals("UK");
    
    public static final String patternnormdate = "dd/MM/yyyy HH:mm:ss";
    public static final String patternsqldate = "yyyy-MM-dd HH:mm:ss";
    
    public static final String patternnormdate_start = "dd/MM/yyyy HH:mm";
    public static final String patternsqldate_start = "yyyy-MM-dd HH:mm";

    // SOLO SE buy_std_type è uguale a 0 (zero)
    public static String addBuyStdStandard(String buy_std, String cambio_bce) {
        double d_rifbce = Double.parseDouble(cambio_bce);
        double d_standard = Double.parseDouble(buy_std);
        double tot_st = d_rifbce * (100 + d_standard) / 100;
        tot_st = new BigDecimal(tot_st).setScale(3, RoundingMode.HALF_UP).doubleValue();
        return tot_st + "";
    }

    // SOLO SE sell_std_type è uguale a 0 (zero)
    public static String addSellStdStandard(String sell_std, String cambio_bce) {
        double d_rifbce = Double.parseDouble(cambio_bce);
        double d_standard = Double.parseDouble(sell_std);
        double tot_st = d_rifbce * (100 - d_standard) / 100;
        tot_st = new BigDecimal(tot_st).setScale(3, RoundingMode.HALF_UP).doubleValue();
        return tot_st + "";
    }

    public static String formatValue(String value) {
        double d1 = 0.000;
        try {
            d1 = Double.parseDouble(value);
        } catch (NumberFormatException e) {
            d1 = 0.000;
        }
        DecimalFormat formatter = (DecimalFormat) NumberFormat.getCurrencyInstance(Locale.ITALY);
        DecimalFormatSymbols symbols = formatter.getDecimalFormatSymbols();
        symbols.setCurrencySymbol(""); // Don't use null.
        formatter.setDecimalFormatSymbols(symbols);
        return value;
//        return formatter.format(d1).trim();
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
        } catch (IllegalArgumentException localException) {
        }
        return dat;
    }
    
    public static DateTime parseStringDate(String dat, String pattern1) {
        try {
            if (dat.length() == 21) {
                dat = dat.substring(0, 19);
            }
            if (dat.length() == pattern1.length()) {
                DateTimeFormatter formatter = DateTimeFormat.forPattern(pattern1);
                return formatter.parseDateTime(dat);
            }
        } catch (Exception localException) {
        }
        return null;
    }

    
    
    
    public static double fd(String si_t_old) {
        double d1;
        si_t_old = si_t_old.replace(",", "").trim();
        try {
            d1 = Double.parseDouble(si_t_old);
        } catch (NumberFormatException e) {
            d1 = 0.0D;
        }
        return d1;
    }
    
    public static boolean removeDuplicatesAL(ArrayList l) {
        int sizeInit = l.size();

        Iterator p = l.iterator();
        while (p.hasNext()) {
            Object op = p.next();
            Iterator q = l.iterator();
            Object oq = q.next();
            while (op != oq) {
                oq = q.next();
            }
            boolean b = q.hasNext();
            while (b) {
                oq = q.next();
                if (op.equals(oq)) {
                    p.remove();
                    b = false;
                } else {
                    b = q.hasNext();
                }
            }
        }

        Collections.sort(l);

        return sizeInit != l.size();
    }
    
    public static String formatMysqltoDisplay(String ing) {
        if (ing == null) {
            return "";
        }
        if (ing.trim().equals("") || ing.trim().equals("-")) {
            return "";
        }
        if (ing.length() == 0) {
            return "";
        }

        String start = ing.substring(0, 1);
        if (start.equals("-") || start.equals("+")) {
            ing = ing.replaceAll(start, "");
        } else {
            start = "";
        }

        String decimal = ",";
        String thousand = ".";
        String out = "";
        if (ing.contains(",")) {
            ing = ing.replaceAll(",", "");
        }
        if (ing.contains(".")) {
            String[] inter1 = splitStringEvery(ing.split("\\.")[0], 3);
            if (inter1.length > 1) {
                for (int i = 0; i < inter1.length; i++) {
                    out = out + inter1[i] + thousand;
                }
            } else {
                out = inter1[0];
            }
            if (out.lastIndexOf(thousand) + 1 == out.length()) {
                out = out.substring(0, out.lastIndexOf(thousand));
            }
            String dec = ing.split("\\.")[1];
            out = out + decimal + dec;
        } else {

            String[] inter1 = splitStringEvery(ing, 3);
            if (inter1.length > 1) {
                for (int i = 0; i < inter1.length; i++) {
                    out = out + inter1[i] + thousand;
                }
            } else {
                out = inter1[0];
            }
            if (out.lastIndexOf(thousand) + 1 == out.length()) {
                out = out.substring(0, out.lastIndexOf(thousand));
            }
        }
        return start + out;
    }
    public static String[] splitStringEvery(String s, int interval) {
        int arrayLength = (int) Math.ceil(((s.length() / (double) interval)));
        String[] result = new String[arrayLength];
        int j = s.length();
        int lastIndex = result.length - 1;
        for (int i = lastIndex; i >= 0 && j >= interval; i--) {
            result[i] = s.substring(j - interval, j);
            j -= interval;
        } //Add the last bit
        if (result[0] == null) {
            result[0] = s.substring(0, j);
        }
        return result;
    }
    
    
    public static String roundDoubleandFormat(double d, int scale) {
        return StringUtils.replace(String.format("%."+scale+"f", d), ",", ".");
    }
    
    public static String generaId(int length) {
        String random = RandomStringUtils.randomAlphanumeric(length - 15).trim();
        return new DateTime().toString("yyMMddHHmmssSSS") + random;
    }
    
    public static String formatStringtoStringDate_null(String dat, String pattern1, String pattern2) {
        try {
            if (dat.length() == 21) {
                dat = dat.substring(0, 19);
            }
            if (dat.length() == pattern1.length()) {
                DateTimeFormatter formatter = DateTimeFormat.forPattern(pattern1);
                DateTime dt = formatter.parseDateTime(dat);
                return dt.toString(pattern2, Locale.ITALY);
            }
        } catch (IllegalArgumentException localException) {
        }
        return null;
    }
    
    
    
    public static void createStartShortcut() {
        try {
            JShellLink link = new JShellLink();
            String filePath = JShellLink.getDirectory("")+ "C:\\Maccorp\\AutoStart.bat";
            link.setFolder(JShellLink.getDirectory("desktop"));
            link.setName("DataBaseMac2.0.lnk");
            link.setPath(filePath);
            link.setIconLocation("C:\\Maccorp\\dbs.ico");
            link.save();
        } catch (Exception ex) {
        }
    }
    
    public static void createServerShortcut() {
        try {
            JShellLink link = new JShellLink();
            String filePath = JShellLink.getDirectory("")+ "C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe";
            link.setFolder(JShellLink.getDirectory("desktop"));
            link.setName("Mac 2.0 (SERVER)");
            link.setPath(filePath);
            link.setArguments("http://127.0.0.1:8080/Mac2.0/");
            link.setIconLocation("C:\\Maccorp\\favicon.ico");
            link.save();
        } catch (Exception ex) {
        }

    }
    
    public static void createClientShortcut(String ip) {
        try {
            JShellLink link = new JShellLink();
            String filePath = JShellLink.getDirectory("")+ "C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe";
            link.setFolder(JShellLink.getDirectory("desktop"));
            link.setName("Mac 2.0 (CLIENT)");
            link.setPath(filePath);
            link.setArguments("http://"+ip+":8080/Mac2.0/");
            link.setIconLocation("C:\\Maccorp\\favicon.ico");
            link.save();
        } catch (Exception ex) {
        }

    }
    
    
    
}

