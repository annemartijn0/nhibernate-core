using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NHibernate.AdoNet.AsyncExtensions.AsyncHandler
{
    public interface IHandler<in TSource, out TDestination>
    {
        TDestination Handle(TSource source);
    }
}
