using System;
using System.Data;using System.Data.Common;
using System.Data.Common;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NHibernate.AdoNet.Util;
using NHibernate.Driver;
using NHibernate.Exceptions;
using NHibernate.Util;

namespace NHibernate.AdoNet
{
	public class SqlClientBatchingBatcher : AbstractBatcher
	{
		private int _batchSize;
		private int _totalExpectedRowsAffected;
		private SqlClientSqlCommandSet _currentBatch;
		private StringBuilder _currentBatchCommandsLog;
		private readonly int _defaultTimeout;

		public SqlClientBatchingBatcher(ConnectionManager connectionManager, IInterceptor interceptor)
			: base(connectionManager, interceptor)
		{
			_batchSize = Factory.Settings.AdoBatchSize;
			_defaultTimeout = PropertiesHelper.GetInt32(Cfg.Environment.CommandTimeout, Cfg.Environment.Properties, -1);

			_currentBatch = CreateConfiguredBatch();
			//we always create this, because we need to deal with a scenario in which
			//the user change the logging configuration at runtime. Trying to put this
			//behind an if(log.IsDebugEnabled) will cause a null reference exception 
			//at that point.
			_currentBatchCommandsLog = new StringBuilder().AppendLine("Batch commands:");
		}

		/// <summary>
		/// Asynchronously gets an <see cref="IDataReader"/> by calling ExecuteReaderAsync on the <see cref="IDbCommand"/>.
		/// </summary>
		/// <param name="dbCommand">The <see cref="IDbCommand"/> to execute to get the <see cref="IDataReader"/>. Should be of type <see cref="System.Data.SqlClient.SqlCommand"/></param>
		/// <param name="cancellationToken">The <see cref="CancellationToken"/> propagates notification that operations should be canceled.</param>
		/// <returns>The <see cref="IDataReader"/> from the <see cref="IDbCommand"/>.</returns>
		/// <remarks>
		/// The Batcher is responsible for ensuring that all of the Drivers rules for how many open
		/// <see cref="IDataReader"/>s it can have are followed.
		/// </remarks>
		public override Task<DbDataReader> ExecuteReaderAsync(DbCommand dbCommand, CancellationToken cancellationToken)
		{
			var sqlCommand = CheckIfSqlCommand(dbCommand);
			var duration = PrepareExecuteReader(dbCommand);

			return Task<DbDataReader>
				.Factory
				.FromAsync(sqlCommand.BeginExecuteReader, sqlCommand.EndExecuteReader, null)
				.ContinueWith(task =>
					EndExecuteReader(task, sqlCommand, duration), cancellationToken);
		}

		private static System.Data.SqlClient.SqlCommand CheckIfSqlCommand(DbCommand cmd)
		{
			var sqlCommand = cmd as System.Data.SqlClient.SqlCommand;

			if (cmd == null)
				throw new NotSupportedException("DbCommand Should have been a SqlCommand");

			return sqlCommand;
		}

		private Stopwatch PrepareExecuteReader(DbCommand cmd)
		{
			CheckReaders();
			LogCommand(cmd);
			Prepare(cmd);

			Stopwatch duration = null;
			if (Log.IsDebugEnabled)
				duration = Stopwatch.StartNew();

			return duration;
		}

		private DbDataReader EndExecuteReader(Task<DbDataReader> task, System.Data.SqlClient.SqlCommand sqlCommand, Stopwatch duration)
		{
			DbDataReader reader = null;
			try
			{
				reader = task.Result;
			}
			finally
			{
				if (Log.IsDebugEnabled && duration != null && reader != null)
				{
					Log.DebugFormat("ExecuteReader took {0} ms", duration.ElapsedMilliseconds);
					_readersDuration[reader] = duration;
				}
			}

			if (!_factory.ConnectionProvider.Driver.SupportsMultipleOpenReaders)
			{
				reader = new NHybridDataReader(reader);
			}

			_readersToClose.Add(reader);
			LogOpenReader();
			return reader;
		}

		public override int BatchSize
		{
			get { return _batchSize; }
			set { _batchSize = value; }
		}

		protected override int CountOfStatementsInCurrentBatch
		{
			get { return _currentBatch.CountOfCommands; }
		}

		public override void AddToBatch(IExpectation expectation)
		{
			_totalExpectedRowsAffected += expectation.ExpectedRowCount;
			DbCommand batchUpdate = CurrentCommand;
			Driver.AdjustCommand(batchUpdate);
			string lineWithParameters = null;
			var sqlStatementLogger = Factory.Settings.SqlStatementLogger;
			if (sqlStatementLogger.IsDebugEnabled || Log.IsDebugEnabled)
			{
				lineWithParameters = sqlStatementLogger.GetCommandLineWithParameters(batchUpdate);
				var formatStyle = sqlStatementLogger.DetermineActualStyle(FormatStyle.Basic);
				lineWithParameters = formatStyle.Formatter.Format(lineWithParameters);
				_currentBatchCommandsLog.Append("command ")
					.Append(_currentBatch.CountOfCommands)
					.Append(":")
					.AppendLine(lineWithParameters);
			}
			if (Log.IsDebugEnabled)
			{
				Log.Debug("Adding to batch:" + lineWithParameters);
			}
			_currentBatch.Append((System.Data.SqlClient.SqlCommand) batchUpdate);

			if (_currentBatch.CountOfCommands >= _batchSize)
			{
				ExecuteBatchWithTiming(batchUpdate);
			}
		}

		protected override void DoExecuteBatch(DbCommand ps)
		{
			Log.DebugFormat("Executing batch");
			CheckReaders();
			Prepare(_currentBatch.BatchCommand);
			if (Factory.Settings.SqlStatementLogger.IsDebugEnabled)
			{
				Factory.Settings.SqlStatementLogger.LogBatchCommand(_currentBatchCommandsLog.ToString());
				_currentBatchCommandsLog = new StringBuilder().AppendLine("Batch commands:");
			}

			int rowsAffected;
			try
			{
				rowsAffected = _currentBatch.ExecuteNonQuery();
			}
			catch (DbException e)
			{
				throw ADOExceptionHelper.Convert(Factory.SQLExceptionConverter, e, "could not execute batch command.");
			}

			Expectations.VerifyOutcomeBatched(_totalExpectedRowsAffected, rowsAffected);

			_currentBatch.Dispose();
			_totalExpectedRowsAffected = 0;
			_currentBatch = CreateConfiguredBatch();
		}

		private SqlClientSqlCommandSet CreateConfiguredBatch()
		{
			var result = new SqlClientSqlCommandSet();
			if (_defaultTimeout > 0)
			{
				try
				{
					result.CommandTimeout = _defaultTimeout;
				}
				catch (Exception e)
				{
					if (Log.IsWarnEnabled)
					{
						Log.Warn(e.ToString());
					}
				}
			}

			return result;
		}
	}
}