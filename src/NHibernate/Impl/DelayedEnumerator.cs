using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace NHibernate.Impl
{
    internal class DelayedEnumerator<T> : IAwaitableEnumerable<T>, IDelayedValue
    {
		public delegate IEnumerable<T> GetResult();
		public delegate Task<IEnumerable<T>> GetResultAsync(CancellationToken cancellationToken);

		private readonly GetResult result;
		private readonly GetResultAsync resultAsync;

        public Delegate ExecuteOnEval { get; set;}

        public DelayedEnumerator(GetResult result, GetResultAsync resultAsync)
        {
            this.result = result;
	        this.resultAsync = resultAsync;
        }

        public IEnumerable<T> Enumerable
        {
            get
            {
                var value = result();
	            return YieldItems(value);
            }
        }

		public Task<IEnumerable<T>> AsTask()
		{
			return AsTask(CancellationToken.None);
		}

		public Task<IEnumerable<T>> AsTask(CancellationToken cancellationToken)
		{
			return resultAsync(cancellationToken)
				.ContinueWith(task => YieldItems(task.Result), cancellationToken);
		}

		private IEnumerable<T> YieldItems(IEnumerable<T> value)
		{
			if (ExecuteOnEval != null)
				value = (IEnumerable<T>)ExecuteOnEval.DynamicInvoke(value);
			foreach (T item in value)
			{
				yield return item;
			}
		}

        #region IEnumerable<T> Members

        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable)Enumerable).GetEnumerator();
        }

        public IEnumerator<T> GetEnumerator()
        {
            return Enumerable.GetEnumerator();
        }

        #endregion
    }

    internal interface IDelayedValue
    {
        Delegate ExecuteOnEval { get; set; }
    }
}