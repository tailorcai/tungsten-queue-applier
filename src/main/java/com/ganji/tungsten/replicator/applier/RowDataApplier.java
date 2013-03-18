package com.ganji.tungsten.replicator.applier;

import java.sql.Types;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.sql.rowset.serial.SerialException;

import org.apache.log4j.Logger;
import org.json.simple.JSONObject;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.applier.ApplierException;
import com.continuent.tungsten.replicator.applier.RawApplier;
import com.continuent.tungsten.replicator.consistency.ConsistencyException;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.LoadDataFileFragment;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.dbms.RowIdData;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnVal;
import com.continuent.tungsten.replicator.dbms.RowChangeData.ActionType;
import com.continuent.tungsten.replicator.event.DBMSEmptyEvent;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.extractor.mysql.SerialBlob;
import com.continuent.tungsten.replicator.plugin.PluginContext;

public abstract class RowDataApplier implements RawApplier {

	private static Logger  logger        = Logger.getLogger(RowDataApplier.class);
	
	protected ReplDBMSHeader latestHeader;
	
	protected static int ACTION_INSERT = 0;
	protected static int ACTION_UPDATE = 1;
	protected static int ACTION_DELETE = 2;
	
    /**
     * Applies row updates. Statements are discarded. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.RawApplier#apply(com.continuent.tungsten.replicator.event.DBMSEvent,
     *      com.continuent.tungsten.replicator.event.ReplDBMSHeader, boolean)
     */
    @Override
    public void apply(DBMSEvent event, ReplDBMSHeader header, boolean doCommit, boolean doRollback)
            throws ReplicatorException, ConsistencyException, InterruptedException
    {
        ArrayList<DBMSData> dbmsDataValues = event.getData();

        // COPY from JdbcApplier
        if (latestHeader != null && latestHeader.getLastFrag()
                && latestHeader.getSeqno() >= header.getSeqno()
                && ! (event instanceof DBMSEmptyEvent))
        {
            logger.info("Skipping over previously applied event: seqno="
                    + header.getSeqno() + " fragno=" + header.getFragno());
            return;
        }
        if (logger.isDebugEnabled())
            logger.debug("Applying event: seqno=" + header.getSeqno()
                    + " fragno=" + header.getFragno() + " commit=" + doCommit);
        
        // Iterate through values inferring the database name.
        for (DBMSData dbmsData : dbmsDataValues)
        {
            if (dbmsData instanceof StatementData)
            {
                if (logger.isDebugEnabled())
                    logger.debug("Ignoring statement");
            }
            else if (dbmsData instanceof RowChangeData)
            {
                RowChangeData rd = (RowChangeData) dbmsData;
                for (OneRowChange orc : rd.getRowChanges())
                {
                    ActionType action = orc.getAction();
                    String schema = orc.getSchemaName();
                    String table = orc.getTableName();

                    if (logger.isDebugEnabled())
                    {
                        logger.debug("Processing row update: action=" + action
                                + " schema=" + schema + " table=" + table);
                    }

                    if (action.equals(ActionType.INSERT))
                    {
                        // Fetch column names.
                        List<ColumnSpec> colSpecs = orc.getColumnSpec();

                        // Make a document and insert for each row.
                        Iterator<ArrayList<ColumnVal>> colValues = orc
                                .getColumnValues().iterator();
                        while (colValues.hasNext())
                        {
                            JSONObject doc = new JSONObject();
                            ArrayList<ColumnVal> row = colValues.next();
                            for (int i = 0; i < row.size(); i++)
                            {
                                String name = colSpecs.get(i).getName();
                                String value = ColValue2String(colSpecs.get(i), row.get(i));
                                doc.put(name, value);
                            }
                            //insert2Queue( schema, table , "insert", doc );
                            onRowData(schema, table, ACTION_INSERT, doc, event.getSourceTstamp());
                        }
                           
                    }
                    else if (action.equals(ActionType.UPDATE))
                    {
                        // Fetch key and column names.
                        //List<ColumnSpec> keySpecs = orc.getKeySpec();
                        List<ColumnSpec> colSpecs = orc.getColumnSpec();
                        ArrayList<ArrayList<OneRowChange.ColumnVal>> keyValues = orc
                                .getKeyValues();
                        ArrayList<ArrayList<OneRowChange.ColumnVal>> columnValues = orc
                                .getColumnValues();

                        // Iterate across the rows.
                        for (int row = 0; row < columnValues.size()
                                || row < keyValues.size(); row++)
                        {
                            JSONObject doc = new JSONObject();
                            //List<ColumnVal> keyValuesOfRow = keyValues.get(row);
                            List<ColumnVal> colValuesOfRow = columnValues.get(row);
                            for (int i = 0; i < colValuesOfRow.size(); i++)
                            {
                                String name = colSpecs.get(i).getName();
                                String value = ColValue2String(colSpecs.get(i), colValuesOfRow.get(i));
                                doc.put(name, value);
                            }
                            onRowData(schema, table, ACTION_UPDATE, doc, event.getSourceTstamp());
                        }
                    }
                    else if (action.equals(ActionType.DELETE))
                    {
                        // Fetch key and column names.
                        List<ColumnSpec> keySpecs = orc.getKeySpec();
                        ArrayList<ArrayList<OneRowChange.ColumnVal>> keyValues = orc
                                .getKeyValues();
                        //List<ColumnSpec> colSpecs = orc.getColumnSpec();
                        ArrayList<ArrayList<OneRowChange.ColumnVal>> columnValues = orc
                                .getColumnValues();

                        // Iterate across the rows.
                        for (int row = 0; row < columnValues.size()
                                || row < keyValues.size(); row++)
                        {
                            List<ColumnVal> keyValuesOfRow = keyValues.get(row);
                            JSONObject doc = new JSONObject();
                            
                            // Prepare key values query to search for rows.
                            for (int i = 0; i < keyValuesOfRow.size(); i++)
                            {
                                String name = keySpecs.get(i).getName();
                                String value = ColValue2String(keySpecs.get(i), keyValuesOfRow.get(i) );
                                doc.put(name, value);
                            }
                            onRowData(schema, table, ACTION_DELETE, doc, event.getSourceTstamp());
                        }
                    }
                    else
                    {
                        logger.warn("Unrecognized action type: " + action);
                        return;
                    }
                    
                }
            }
            else if (dbmsData instanceof LoadDataFileFragment)
            {
                if (logger.isDebugEnabled())
                    logger.debug("Ignoring load data file fragment");
            }
            else if (dbmsData instanceof RowIdData)
            {
                if (logger.isDebugEnabled())
                    logger.debug("Ignoring row ID data");
            }
            else
            {
                logger.warn("Unsupported DbmsData class: "
                        + dbmsData.getClass().getName());
            }
        }

        // Mark the current header and commit position if requested.
        this.latestHeader = header;
        if (doCommit)
            commit();
    }

	abstract protected void onRowData(String schema, String table, int aCTIONINSERT,
			JSONObject doc, Timestamp tm)throws ApplierException;


	@Override
	public void release(PluginContext arg0) throws ReplicatorException,
			InterruptedException {
		// TODO Auto-generated method stub

	}
	
//	abstract protected boolean filterSchemaTable(String schema, String table);

    private String ColValue2String(ColumnSpec cspec, ColumnVal cvalue) {
        String value;
        // BUG: if input is null , return null
        if( cvalue.getValue() == null )
        	return null;
        
        if( cspec.getType() == Types.VARCHAR ) {
            value = new String( (byte[]) cvalue.getValue() );
        }
        else if( cspec.getType() == Types.BLOB ) {
            SerialBlob blob = (SerialBlob) cvalue.getValue();
            try
            {
                value = new String( blob.getBytes((long)1, (int) blob.length()));
            }
            catch (SerialException e)
            {
                value = "ERROR";
            }
        }
        else {
            if( cvalue.getValue() != null ) 
                value = cvalue.getValue().toString();
            else
                value = null;
        }
        return value;
    }
}
