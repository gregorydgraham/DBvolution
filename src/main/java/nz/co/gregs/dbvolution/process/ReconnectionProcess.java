/*
 * Copyright 2018 Gregory Graham.
 *
 * Commercial licenses are available, please contact info@gregs.co.nz for details.
 * 
 * This work is licensed under the Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License. 
 * To view a copy of this license, visit http://creativecommons.org/licenses/by-nc-sa/4.0/ 
 * or send a letter to Creative Commons, PO Box 1866, Mountain View, CA 94042, USA.
 * 
 * You are free to:
 *     Share - copy and redistribute the material in any medium or format
 *     Adapt - remix, transform, and build upon the material
 * 
 *     The licensor cannot revoke these freedoms as long as you follow the license terms.               
 *     Under the following terms:
 *                 
 *         Attribution - 
 *             You must give appropriate credit, provide a link to the license, and indicate if changes were made. 
 *             You may do so in any reasonable manner, but not in any way that suggests the licensor endorses you or your use.
 *         NonCommercial - 
 *             You may not use the material for commercial purposes.
 *         ShareAlike - 
 *             If you remix, transform, or build upon the material, 
 *             you must distribute your contributions under the same license as the original.
 *         No additional restrictions - 
 *             You may not apply legal terms or technological measures that legally restrict others from doing anything the 
 *             license permits.
 * 
 * Check the Creative Commons website for any details, legalese, and updates.
 */
package nz.co.gregs.dbvolution.process;

import nz.co.gregs.dbvolution.process.ClusterProcess;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.DBDatabaseCluster; 
import nz.co.gregs.dbvolution.exceptions.UnableToRemoveLastDatabaseFromClusterException;
import nz.co.gregs.separatedstring.Encoder;
import nz.co.gregs.separatedstring.SeparatedString;

/**
 *
 * @author gregorygraham
 */
public class ReconnectionProcess extends ClusterProcess {

	public static final long serialVersionUID = 1l;
	
	public ReconnectionProcess() {
		super();
	}

	@Override
	public synchronized String process() {
    Encoder encoder = SeparatedString.builder().encoder();
		String str = "No Databases To Reconnect";
		final DBDatabase database = getDatabase();
		if (database instanceof DBDatabaseCluster) {
			DBDatabaseCluster cluster = (DBDatabaseCluster) database;
			if (cluster.getAutoReconnect()) {
				String msg = cluster.getLabel()+ ": PREPARING TO RECONNECT DATABASES...";
				LOGGER.fine(msg);
				encoder.addLine(msg);
				try {
					encoder.addLine(cluster.reconnectQuarantinedDatabases());
				} catch (UnableToRemoveLastDatabaseFromClusterException | SQLException ex) {
					Logger.getLogger(ReconnectionProcess.class.getName()).log(Level.SEVERE, null, ex);
				}
				msg = cluster.getLabel() + ": FINISHED RECONNECTING DATABASES...";
				LOGGER.fine(msg);
				encoder.addLine(msg);
			}
		}
		return encoder.isEmpty()?str:encoder.encode();
	}
	protected static final Logger LOGGER = Logger.getLogger(ReconnectionProcess.class.getName());

  
  @Override
  public void handleExceptionDuringProcessing(Exception ex) {
    getCluster().quarantineDatabase(getDatabase(), ex);
    super.handleExceptionDuringProcessing(ex);
  }
	
}
