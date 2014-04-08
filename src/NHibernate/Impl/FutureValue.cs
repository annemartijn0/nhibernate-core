using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace NHibernate.Impl
{
    internal class FutureValue<T> : IFutureValue<T>, IDelayedValue
    {
		public delegate IEnumerable<T> GetResult();
		public delegate Task<IEnumerable<T>> GetResultAsync(CancellationToken cancellationToken);

		private readonly GetResult getResult;
		private readonly GetResultAsync getResultAsync;

        public FutureValue(GetResult result, GetResultAsync resultAsync)
        {
            getResult = result;
	        getResultAsync = resultAsync;
        }

        public T Value
        {
            get
            {
	            return GetValueFromResult(getResult());
            }
        }

		public Task<T> ValueAsync()
		{
			return ValueAsync(CancellationToken.None);
		}

		public Task<T> ValueAsync(CancellationToken cancellationToken)
		{
			return getResultAsync(cancellationToken)
				.ContinueWith(task => GetValueFromResult(task.Result), cancellationToken);
		}

	    private T GetValueFromResult(IEnumerable<T> result)
	    {
		    var enumerator = result.GetEnumerator();

		    if (!enumerator.MoveNext())
		    {
			    var defVal = default(T);
			    if (ExecuteOnEval != null)
				    defVal = (T) ExecuteOnEval.DynamicInvoke(defVal);
			    return defVal;
		    }

		    var val = enumerator.Current;

		    if (ExecuteOnEval != null)
			    val = (T) ExecuteOnEval.DynamicInvoke(val);

		    return val;
	    }

	    public Delegate ExecuteOnEval
        {
            get; set;
        }
	}
}