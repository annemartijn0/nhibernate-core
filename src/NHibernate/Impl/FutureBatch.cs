using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace NHibernate.Impl
{
	public abstract class FutureBatch<TQueryApproach, TMultiApproach>
	{
		private readonly List<TQueryApproach> queries = new List<TQueryApproach>();
		private readonly IList<System.Type> resultTypes = new List<System.Type>();
		private int index;
		private Task<IList> resultsAsync;
		private IList results;
		private bool isCacheable = true;
		private string cacheRegion;

		protected readonly SessionImpl session;

		protected FutureBatch(SessionImpl session)
		{
			this.session = session;
		}

		public IList Results
		{
			get
			{
				if (results == null)
				{
					GetResults();
				}
				return results;
			}
		}

		private static Task<IList> CanceledTask
		{
			get
			{
				var taskCompletionSource = new TaskCompletionSource<IList>();
				taskCompletionSource.SetCanceled();
				return taskCompletionSource.Task;
			}
		}

		private Task<IList> ResultsFromTaskCompletionSource
		{
			get
			{
				var taskCompletionSource = new TaskCompletionSource<IList>();
				taskCompletionSource.SetResult(results);
				return taskCompletionSource.Task;
			}
		}

		public Task<IList> ResultsAsync(CancellationToken cancellationToken)
		{
			if (results != null)
			{
				return cancellationToken.IsCancellationRequested
					? CanceledTask
					: ResultsFromTaskCompletionSource;
			}

			GetResultsAsync(cancellationToken);
			return resultsAsync;
		}

		public void Add<TResult>(TQueryApproach query)
		{
			if (queries.Count == 0)
			{
				cacheRegion = CacheRegion(query);
			}

			queries.Add(query);
			resultTypes.Add(typeof(TResult));
			index = queries.Count - 1;
			isCacheable = isCacheable && IsQueryCacheable(query);
			isCacheable = isCacheable && (cacheRegion == CacheRegion(query));
		}

		public void Add(TQueryApproach query)
		{
			Add<object>(query);
		}

		public IFutureValue<TResult> GetFutureValue<TResult>()
		{
			int currentIndex = index;
			return new FutureValue<TResult>(
				() => GetCurrentResult<TResult>(currentIndex),
				cancellationToken => GetCurrentResultAsync<TResult>(currentIndex, cancellationToken));
		}

		public IAwaitableEnumerable<TResult> GetEnumerator<TResult>()
		{
			int currentIndex = index;
			return new DelayedEnumerator<TResult>(
				() => GetCurrentResult<TResult>(currentIndex),
				cancellationToken => GetCurrentResultAsync<TResult>(currentIndex, cancellationToken));
		}

		private void GetResultsAsync(CancellationToken cancellationToken)
		{
			var multiApproach = CreateMultiApproach(isCacheable, cacheRegion);
			for (int i = 0; i < queries.Count; i++)
			{
				AddTo(multiApproach, queries[i], resultTypes[i]);
			}
			resultsAsync = GetResultsFromAsync(multiApproach, cancellationToken);

			resultsAsync.ContinueWith(task =>
			{
				try
				{
					results = task.Result;
					return results;
				}
				finally
				{
					ClearCurrentFutureBatch();
				}
			}, TaskContinuationOptions.NotOnCanceled);
			resultsAsync.ContinueWith(_ => ClearCurrentFutureBatch(), TaskContinuationOptions.OnlyOnCanceled);
		}

		private void GetResults()
		{
			var multiApproach = CreateMultiApproach(isCacheable, cacheRegion);
			for (int i = 0; i < queries.Count; i++)
			{
				AddTo(multiApproach, queries[i], resultTypes[i]);
			}
			results = GetResultsFrom(multiApproach);
			ClearCurrentFutureBatch();
		}

		private Task<IEnumerable<TResult>> GetCurrentResultAsync<TResult>(int currentIndex, CancellationToken cancellationToken)
		{
			return ResultsAsync(cancellationToken)
				.ContinueWith(task =>
					((IList)task.Result[currentIndex]).Cast<TResult>(), cancellationToken);
		}

		private IEnumerable<TResult> GetCurrentResult<TResult>(int currentIndex)
		{
			return ((IList)Results[currentIndex]).Cast<TResult>();
		}

		protected abstract TMultiApproach CreateMultiApproach(bool isCacheable, string cacheRegion);
		protected abstract void AddTo(TMultiApproach multiApproach, TQueryApproach query, System.Type resultType);
		protected abstract Task<IList> GetResultsFromAsync(TMultiApproach multiApproach, CancellationToken cancellationToken);
		protected abstract IList GetResultsFrom(TMultiApproach multiApproach);
		protected abstract void ClearCurrentFutureBatch();
		protected abstract bool IsQueryCacheable(TQueryApproach query);
		protected abstract string CacheRegion(TQueryApproach query);
	}
}