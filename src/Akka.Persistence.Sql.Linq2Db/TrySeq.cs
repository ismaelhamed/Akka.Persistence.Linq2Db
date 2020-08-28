using System.Collections.Generic;
using System.Linq;
using Akka.Util;

namespace Akka.Persistence.Sql.Linq2Db
{
    public static class TrySeq
    {
        public static Try<IEnumerable<T>> Sequence<T>(IEnumerable<Try<T>> seq) => 
            Try<IEnumerable<T>>.From(() => seq.Select(r => r.Get()));
    }
}