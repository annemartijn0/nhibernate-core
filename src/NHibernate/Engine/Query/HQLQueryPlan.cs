using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NHibernate.Event;
using NHibernate.Hql;
using NHibernate.Linq;
using NHibernate.Type;
using NHibernate.Util;

namespace NHibernate.Engine.Query
{
    public interface IQueryPlan
    {
        ParameterMetadata ParameterMetadata { get; }
        ISet<string> QuerySpaces { get; }
        IQueryTranslator[] Translators { get; }
        ReturnMetadata ReturnMetadata { get; }
        void PerformList(QueryParameters queryParameters, ISessionImplementor statelessSessionImpl, IList results);
		Task PerformListAsync(QueryParameters queryParameters, ISessionImplementor statelessSessionImpl, IList results, CancellationToken cancellationToken);
		int PerformExecuteUpdate(QueryParameters queryParameters, ISessionImplementor statelessSessionImpl);
        IEnumerable<T> PerformIterate<T>(QueryParameters queryParameters, IEventSource session);
        IEnumerable PerformIterate(QueryParameters queryParameters, IEventSource session);
    }

    public interface IQueryExpressionPlan : IQueryPlan
    {
        IQueryExpression QueryExpression { get; }
    }

	/// <summary> Defines a query execution plan for an HQL query (or filter). </summary>
	[Serializable]
	public class HQLQueryPlan : IQueryPlan
	{
		protected static readonly IInternalLogger Log = LoggerProvider.LoggerFor(typeof(HQLQueryPlan));

		private readonly string _sourceQuery;

        protected HQLQueryPlan(string sourceQuery, IQueryTranslator[] translators)
        {
            Translators = translators;
            _sourceQuery = sourceQuery;

            FinaliseQueryPlan();
        }

		internal HQLQueryPlan(HQLQueryPlan source)
		{
			Translators = source.Translators;
			_sourceQuery = source._sourceQuery;
			QuerySpaces = source.QuerySpaces;
			ParameterMetadata = source.ParameterMetadata;
			ReturnMetadata = source.ReturnMetadata;
			SqlStrings = source.SqlStrings;
		}

	    public ISet<string> QuerySpaces
		{
		    get;
		    private set;
		}

		public ParameterMetadata ParameterMetadata
		{
            get;
            private set;
        }

		public ReturnMetadata ReturnMetadata
		{
            get;
            private set;
        }

		public string[] SqlStrings
		{
            get;
            private set;
        }

		public IQueryTranslator[] Translators
		{
            get;
            private set;
        }

		public void PerformList(QueryParameters queryParameters, ISessionImplementor session, IList results)
		{
			LogPerformList(queryParameters, session);
			var needsLimit = NeedsLimit(queryParameters);
			QueryParameters queryParametersToUse;
			if (needsLimit)
			{
				Log.Warn("firstResult/maxResults specified on polymorphic query; applying in memory!");
				RowSelection selection = new RowSelection();
				selection.FetchSize = queryParameters.RowSelection.FetchSize;
				selection.Timeout = queryParameters.RowSelection.Timeout;
				queryParametersToUse = queryParameters.CreateCopyUsing(selection);
			}
			else
			{
				queryParametersToUse = queryParameters;
			}

			IList combinedResults = results ?? new List<object>();
			IdentitySet distinction = new IdentitySet();
			int includedCount = -1;
			for (int i = 0; i < Translators.Length; i++)
			{
				IList tmp = Translators[i].List(session, queryParametersToUse);
				if (needsLimit)
				{
					if (AddToLimitedList(queryParameters, tmp, distinction, includedCount, combinedResults)) return;
				}
				else
					ArrayHelper.AddAll(combinedResults, tmp);
			}
		}

		public Task PerformListAsync(QueryParameters queryParameters, ISessionImplementor session, IList results, CancellationToken cancellationToken)
		{
			LogPerformList(queryParameters, session);
			var needsLimit = NeedsLimit(queryParameters);
			QueryParameters queryParametersToUse;
			if (needsLimit)
			{
				Log.Warn("firstResult/maxResults specified on polymorphic query; applying in memory!");
				RowSelection selection = new RowSelection();
				selection.FetchSize = queryParameters.RowSelection.FetchSize;
				selection.Timeout = queryParameters.RowSelection.Timeout;
				queryParametersToUse = queryParameters.CreateCopyUsing(selection);
			}
			else
			{
				queryParametersToUse = queryParameters;
			}

			IList combinedResults = results ?? new List<object>();
			IdentitySet distinction = new IdentitySet();
			int includedCount = -1;

			var tasks = new Task<IList>[Translators.Length];
			for (int i = 0; i < Translators.Length; i++)
			{
				tasks[i] = Translators[i].ListAsync(session, queryParametersToUse, cancellationToken);
			}

			return Task.Factory.ContinueWhenAll(tasks, _ =>
			{
				foreach (IList tmp in tasks.Select(task => task.Result))
				{
					if (needsLimit)
					{
						if (AddToLimitedList(queryParameters, tmp, distinction, includedCount, combinedResults)) return;
					}
					else
						ArrayHelper.AddAll(combinedResults, tmp);
				}
			});
		}

		private void LogPerformList(QueryParameters queryParameters, ISessionImplementor session)
		{
			if (Log.IsDebugEnabled)
			{
				Log.Debug("find: " + _sourceQuery);
				queryParameters.LogParameters(session.Factory);
			}
		}

		private bool NeedsLimit(QueryParameters queryParameters)
		{
			bool hasLimit = queryParameters.RowSelection != null && queryParameters.RowSelection.DefinesLimits;
			bool needsLimit = hasLimit && Translators.Length > 1;
			return needsLimit;
		}

		private static bool AddToLimitedList(QueryParameters queryParameters, IList tmp, IdentitySet distinction,
			int includedCount, IList combinedResults)
		{
			// NOTE : firstRow is zero-based
			int first = queryParameters.RowSelection.FirstRow == RowSelection.NoValue
				? 0
				: queryParameters.RowSelection.FirstRow;

			int max = queryParameters.RowSelection.MaxRows == RowSelection.NoValue
				? RowSelection.NoValue
				: queryParameters.RowSelection.MaxRows;

			int size = tmp.Count;
			for (int x = 0; x < size; x++)
			{
				object result = tmp[x];
				if (distinction.Add(result))
				{
					continue;
				}
				includedCount++;
				if (includedCount < first)
				{
					continue;
				}
				combinedResults.Add(result);
				if (max >= 0 && includedCount > max)
				{
					// break the outer loop !!!
					return true;
				}
			}
			return false;
		}

		public IEnumerable PerformIterate(QueryParameters queryParameters, IEventSource session)
		{
			bool? many;
			IEnumerable[] results;
			IEnumerable result;

			DoIterate(queryParameters, session, out many, out results, out result);

			return (many.HasValue && many.Value) ? new JoinedEnumerable(results) : result;
		}

		public IEnumerable<T> PerformIterate<T>(QueryParameters queryParameters, IEventSource session)
		{
			return new SafetyEnumerable<T>(PerformIterate(queryParameters, session));
		}

        public int PerformExecuteUpdate(QueryParameters queryParameters, ISessionImplementor session)
        {
            if (Log.IsDebugEnabled)
            {
                Log.Debug("executeUpdate: " + _sourceQuery);
                queryParameters.LogParameters(session.Factory);
            }
            if (Translators.Length != 1)
            {
                Log.Warn("manipulation query [" + _sourceQuery + "] resulted in [" + Translators.Length + "] split queries");
            }
            int result = 0;
            for (int i = 0; i < Translators.Length; i++)
            {
                result += Translators[i].ExecuteUpdate(queryParameters, session);
            }
            return result;
        }

		void DoIterate(QueryParameters queryParameters, IEventSource session, out bool? isMany, out IEnumerable[] results, out IEnumerable result)
		{
			isMany = null;
			results = null;
			if (Log.IsDebugEnabled)
			{
				Log.Debug("enumerable: " + _sourceQuery);
				queryParameters.LogParameters(session.Factory);
			}
			if (Translators.Length == 0)
			{
				result = CollectionHelper.EmptyEnumerable;
			}
			else
			{
				results = null;
				bool many = Translators.Length > 1;
				if (many)
				{
					results = new IEnumerable[Translators.Length];
				}

				result = null;
				for (int i = 0; i < Translators.Length; i++)
				{
					result = Translators[i].GetEnumerable(queryParameters, session);
					if (many)
						results[i] = result;
				}
				isMany = many;
			}
		}

        void FinaliseQueryPlan()
        {
            BuildSqlStringsAndQuerySpaces();
            BuildMetaData();
        }

	    void BuildMetaData()
	    {
            if (Translators.Length == 0)
            {
                ParameterMetadata = new ParameterMetadata(null, null);
                ReturnMetadata = null;
            }
            else
            {
                ParameterMetadata = Translators[0].BuildParameterMetadata();

                if (Translators[0].IsManipulationStatement)
                {
                    ReturnMetadata = null;
                }
                else
                {
                    if (Translators.Length > 1)
                    {
                        int returns = Translators[0].ReturnTypes.Length;
                        ReturnMetadata = new ReturnMetadata(Translators[0].ReturnAliases, new IType[returns]);
                    }
                    else
                    {
                        ReturnMetadata = new ReturnMetadata(Translators[0].ReturnAliases, Translators[0].ReturnTypes);
                    }
                }
            }
        }

	    void BuildSqlStringsAndQuerySpaces()
        {
            var combinedQuerySpaces = new HashSet<string>();
            var sqlStringList = new List<string>();

            foreach (var translator in Translators)
            {
                foreach (var qs in translator.QuerySpaces)
                {
                    combinedQuerySpaces.Add(qs);
                }

                sqlStringList.AddRange(translator.CollectSqlStrings);
            }

            SqlStrings = sqlStringList.ToArray();
            QuerySpaces = combinedQuerySpaces;
        }
    }
}
