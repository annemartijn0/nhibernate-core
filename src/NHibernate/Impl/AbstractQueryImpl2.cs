using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NHibernate.Engine;
using NHibernate.Engine.Query;

namespace NHibernate.Impl
{
	public abstract class AbstractQueryImpl2 : AbstractQueryImpl
	{
		private readonly Dictionary<string, LockMode> _lockModes = new Dictionary<string, LockMode>(2);

		protected internal override IDictionary<string, LockMode> LockModes
		{
			get { return _lockModes; }
		}

		protected AbstractQueryImpl2(string queryString, FlushMode flushMode, ISessionImplementor session, ParameterMetadata parameterMetadata)
			: base(queryString, flushMode, session, parameterMetadata)
		{
		}

		public override IQuery SetLockMode(string alias, LockMode lockMode)
		{
			_lockModes[alias] = lockMode;
			return this;
		}

		public override int ExecuteUpdate()
		{
			var namedParams = BeforeList();
			try
			{
				return Session.ExecuteUpdate(ExpandParameters(namedParams), GetQueryParameters(namedParams));
			}
			finally
			{
				After();
			}
		}

		public override IEnumerable Enumerable()
		{
			var namedParams = BeforeList();
			try
			{
				return Session.Enumerable(ExpandParameters(namedParams), GetQueryParameters(namedParams));
			}
			finally
			{
				After();
			}
		}

		public override IEnumerable<T> Enumerable<T>()
		{
			var namedParams = BeforeList();
			try
			{
				return Session.Enumerable<T>(ExpandParameters(namedParams), GetQueryParameters(namedParams));
			}
			finally
			{
				After();
			}
		}

		public override IList List()
		{
			var namedParams = BeforeList();
			try
			{
				return Session.List(ExpandParameters(namedParams), GetQueryParameters(namedParams));
			}
			finally
			{
				After();
			}
		}

		public override Task<IList> ListAsync()
		{
			return ListAsync(CancellationToken.None);
		}

		public override Task<IList> ListAsync(CancellationToken cancellationToken)
		{
			var namedParams = BeforeList();
			if (cancellationToken.IsCancellationRequested)
				return CanceledTask<IList>();
			return Session.ListAsync(ExpandParameters(namedParams), GetQueryParameters(namedParams), cancellationToken)
				.ContinueWith(task =>
				{
					try
					{
						return task.Result;
					}
					finally
					{
						After();
					}
				});
		}

		public override void List(IList results)
		{
			var namedParams = BeforeList();
			try
			{
				Session.List(ExpandParameters(namedParams), GetQueryParameters(namedParams), results);
			}
			finally
			{
				After();
			}
		}

		public override IList<T> List<T>()
		{
			var namedParams = BeforeList();
			try
			{
				return Session.List<T>(ExpandParameters(namedParams), GetQueryParameters(namedParams));
			}
			finally
			{
				After();
			}
		}

		public override Task<IList<T>> ListAsync<T>()
		{
			return ListAsync<T>(CancellationToken.None);
		}

		public override Task<IList<T>> ListAsync<T>(CancellationToken cancellationToken)
		{
			if (cancellationToken.IsCancellationRequested)
			{
				return CanceledTask<IList<T>>();
			}
			var namedParams = BeforeList();
			return Session.ListAsync<T>(ExpandParameters(namedParams), GetQueryParameters(namedParams), cancellationToken)
				.ContinueWith(task =>
				{
					try
					{
						return task.Result;
					}
					finally
					{
						After();
					}
				});
		}

		private IDictionary<string, TypedValue> BeforeList()
		{
			VerifyParameters();
			Before();
			return NamedParams;
		}

		private static Task<T> CanceledTask<T>()
		{
			var taskCompletionSource = new TaskCompletionSource<T>();
			taskCompletionSource.SetCanceled();
			return taskCompletionSource.Task;
		}

		/// <summary> 
		/// Warning: adds new parameters to the argument by side-effect, as well as mutating the query expression tree!
		/// </summary>
		protected abstract IQueryExpression ExpandParameters(IDictionary<string, TypedValue> namedParamsCopy);

		protected internal override IEnumerable<ITranslator> GetTranslators(ISessionImplementor sessionImplementor, QueryParameters queryParameters)
		{
			// NOTE: updates queryParameters.NamedParameters as (desired) side effect
			var queryExpression = ExpandParameters(queryParameters.NamedParameters);

			return sessionImplementor.GetQueries(queryExpression, false)
									 .Select(queryTranslator => new HqlTranslatorWrapper(queryTranslator));
		}
	}
}