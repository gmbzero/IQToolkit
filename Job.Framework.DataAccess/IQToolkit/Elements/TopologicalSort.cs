using System;
using System.Collections.Generic;

namespace IQToolkit
{
    internal static class TopologicalSorter
    {
        public static IEnumerable<T> Sort<T>(this IEnumerable<T> items, Func<T, IEnumerable<T>> fnItemsBeforeMe)
        {
            return Sort(items, fnItemsBeforeMe, null);
        }

        public static IEnumerable<T> Sort<T>(this IEnumerable<T> items, Func<T, IEnumerable<T>> fnItemsBeforeMe, IEqualityComparer<T> comparer)
        {
            var seen = comparer != null ? new HashSet<T>(comparer) : new HashSet<T>();
            var done = comparer != null ? new HashSet<T>(comparer) : new HashSet<T>();

            var result = new List<T>();

            foreach (var item in items)
            {
                SortItem(item, fnItemsBeforeMe, seen, done, result);
            }

            return result;
        }

        private static void SortItem<T>(T item, Func<T, IEnumerable<T>> fnItemsBeforeMe, HashSet<T> seen, HashSet<T> done, List<T> result)
        {
            if (done.Contains(item) == false)
            {
                if (seen.Contains(item))
                {
                    throw new InvalidOperationException("Cycle in topological sort");
                }

                seen.Add(item);

                var itemsBefore = fnItemsBeforeMe(item);

                if (itemsBefore != null)
                {
                    foreach (var itemBefore in itemsBefore)
                    {
                        SortItem(itemBefore, fnItemsBeforeMe, seen, done, result);
                    }
                }

                result.Add(item);

                done.Add(item);
            }
        }
    }
}