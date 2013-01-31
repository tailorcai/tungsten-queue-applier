/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2010 Continuent Inc.
 * Contact: tungsten@continuent.org
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of version 2 of the GNU General Public License as
 * published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA
 *
 * Initial developer(s): Teemu Ollakka
 * Contributor(s): Robert Hodges
 */

package com.ganji.tungsten.replicator.applier;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.DefaultConnectionFactory;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.transcoders.SerializingTranscoder;
import net.spy.memcached.transcoders.Transcoder;

import org.apache.log4j.Logger;
import org.json.simple.JSONObject;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.applier.ApplierException;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntime;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.DatabaseFactory;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.plugin.PluginContext;
import com.continuent.tungsten.replicator.thl.CommitSeqnoTable;


/**
 * Implements an applier for MemcacheQueue. The GanjiMcQueueApplier applier This class defines a
 * GanjiMcQueueApplier
 * 
 * @author <a href="mailto:caifeng@ganji.com">CaiFeng
 *         </a>
 * @version 1.0
 */
public class GanjiMcQueueApplier extends RowDataApplier
{
    private static Logger  logger        = Logger.getLogger(GanjiMcQueueApplier.class);

    // Task management information.
    private int            taskId;

    // using MySQL table to store commit seqno table
    // CaiFeng
    protected CommitSeqnoTable        commitSeqnoTable     = null;
    protected Database                conn                 = null;
    protected Statement               statement            = null;
    protected ReplicatorRuntime       runtime              = null;
    
    MemcachedClient 					mc_conn				 = null;
    protected String					queue_addr			 = "";
    protected String					queue_name			 = "noname";
     
    protected String                   db                   = null;
    
    public void setQueueAddr(String mcQueueHost) {
		queue_addr = mcQueueHost;
	}

	public void setQueueName(String mcQueueName) {
		queue_name = mcQueueName;
	}
    public void setDb(String db) {
        this.db = db;
    }
	
    public Database getDatabase()
    {
        return conn;
    }

    private boolean insert2Queue(String schema, String table, String actionName, JSONObject obj, Timestamp tm ) throws ApplierException
    {
        obj.put( "__schema", schema );
        obj.put( "__table", table );
        obj.put( "__action", actionName );
        obj.put( "__ts", tm.getTime() );
        
        Future<Boolean> f = mc_conn.set( queue_name, 0, obj.toJSONString() );
        try {
			Boolean b = f.get();
			return b.booleanValue();
		} catch (InterruptedException e) {
			throw new ApplierException(e);
		} catch (ExecutionException e) {
			throw new ApplierException(e);
		}
        
		//return false;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.RawApplier#commit()
     */
    @Override
    public void commit() throws ReplicatorException, InterruptedException
    {
        // If there's nothing to commit, go back.
        if (this.latestHeader == null )
            return;

        try
        {
            // Add applied latency so that we can easily track how far back each
            // partition is. If we don't have data we just put in zero.
            long appliedLatency;
            if (latestHeader instanceof ReplDBMSEvent)
                appliedLatency = (System.currentTimeMillis() - ((ReplDBMSEvent) latestHeader)
                        .getExtractedTstamp().getTime()) / 1000;
            else
                appliedLatency = 0;

            updateCommitSeqno(latestHeader, appliedLatency);
        }
        catch (SQLException e)
        {
            throw new ApplierException("Unable to commit transaction: "
                    + e.getMessage(), e);
        }
    }

    private void updateCommitSeqno(ReplDBMSHeader header, long appliedLatency) throws SQLException
    {
        if (commitSeqnoTable == null)
            return;
        if (logger.isDebugEnabled())
            logger.debug("Updating commit seqno to " + header.getSeqno());
        commitSeqnoTable.updateLastCommitSeqno(taskId, header, appliedLatency);
    }    
    
    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.RawApplier#getLastEvent()
     */
    @Override
    public ReplDBMSHeader getLastEvent() throws ApplierException,
            InterruptedException
    {
        if (commitSeqnoTable == null)
            return null;

        try
        {
            return commitSeqnoTable.lastCommitSeqno(taskId);
        }
        catch (SQLException e)
        {
            throw new ApplierException(e);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.RawApplier#rollback()
     */
    @Override
    public void rollback() throws InterruptedException
    {
        // Does nothing for now.
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.RawApplier#setTaskId(int)
     */
    @Override
    public void setTaskId(int id)
    {
        this.taskId = id;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    @Override
    public void configure(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        //this.serviceSchema = "tungsten_" + context.getServiceName();
        runtime = (ReplicatorRuntime) context;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    @Override
    public void prepare(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        // Connect to MongoDB.
        if (logger.isDebugEnabled())
        {
//            logger.debug("Connecting to Q4M: url="
//                    + url + ",user=" + user + ",password" + password);
        }

        try
        {
            String schema = db;
            if( db == null ) db = context.getReplicatorSchemaName();            
            // Create the database.
            conn = DatabaseFactory.createDatabase(
            		runtime.getJdbcUrl(db),
            		runtime.getJdbcUser(),
            		runtime.getJdbcPassword()
            		);
            		//url, user, password);
            conn.connect(false);
            statement = conn.createStatement();
            
            commitSeqnoTable = new CommitSeqnoTable(conn,
                    db,  // 2.0.6
                    runtime.getTungstenTableType(), false );
            commitSeqnoTable.prepare(taskId);
            latestHeader = commitSeqnoTable.lastCommitSeqno(taskId);
            
        }
        catch (Exception e)
        {
            throw new ReplicatorException(
                    "Unable to connect to MySQL: url="
                    + runtime.getJdbcUrl(db) 
                    + ",user=" + runtime.getJdbcUser() 
                    + ",password" + runtime.getJdbcPassword(), e);
        }

        
        try {
        	mc_conn = new MemcachedClient(
        			new ConnectionFactoryBuilder(new FixConnectionFactory() ).
        				//setFailureMode(fm).
        				setOpTimeout(30*1000).
        				build(),
        		    AddrUtil.getAddresses( queue_addr ));
        	
        }
        catch( Exception e)
        {
            throw new ReplicatorException(
                    "Unable to connect to Memcached: url="
                    + runtime.getJdbcUrl(context.getReplicatorSchemaName()) 
                    + ",user=" + runtime.getJdbcUser() 
                    + ",password" + runtime.getJdbcPassword(), e);        	
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    @Override
    public void release(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        if (commitSeqnoTable != null)
        {
            commitSeqnoTable.release();
            commitSeqnoTable = null;
        }

        //currentOptions = null;

        statement = null;
        if (conn != null)
        {
            conn.close();
            conn = null;
        }
    }

	@Override
	protected void onRowData(String schema, String table, int aCTIONINSERT, JSONObject doc, Timestamp tm) throws ApplierException {
		if( aCTIONINSERT == ACTION_INSERT) {
			insert2Queue( schema, table, "insert", doc, tm);
		}
		else if(aCTIONINSERT == ACTION_UPDATE) {
			insert2Queue( schema, table, "update", doc, tm );
		}
		else if(aCTIONINSERT == ACTION_DELETE) {
			insert2Queue( schema, table, "delete", doc, tm );
		}
		
	}
    class FixConnectionFactory extends DefaultConnectionFactory {
		 public Transcoder<Object> getDefaultTranscoder() {
			 SerializingTranscoder obj = new SerializingTranscoder();
			 obj.setCompressionThreshold(10000000); // set to 1M ,which is impossible
			 return obj;
		 }
    }
}
