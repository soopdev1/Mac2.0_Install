/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.refill.main;

import javax.swing.UIManager;

/**
 *
 * @author raf
 */
public class MainSelector {

    public static void main(String[] args) {
        int scelta;

        try {
            scelta = Integer.parseInt(args[0]);
        } catch (Exception e) {
            scelta = 1;
        }

        switch (scelta) {

            case 1: //NORMAL
                try {
                UIManager.setLookAndFeel("com.jtattoo.plaf.mcwin.McWinLookAndFeel");
                /* Create and display the form */
                java.awt.EventQueue.invokeLater(() -> {
                    new BranchConfiguration().setVisible(true);
                });
            } catch (Exception e) {
                e.printStackTrace();
            }
            break;

            default:
                break;
        }
    }
}
