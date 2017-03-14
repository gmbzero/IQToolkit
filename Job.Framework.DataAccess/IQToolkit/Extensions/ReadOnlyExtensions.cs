using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Reflection;
using System.Text;

namespace IQToolkit
{
    internal static class ReadOnlyExtensions
    {
        public static ReadOnlyCollection<T> ToReadOnly<T>(this IEnumerable<T> collection)
        {
            var roc = collection as ReadOnlyCollection<T>;

            if (roc == null)
            {
                if (collection == null)
                {
                    roc = new List<T>().AsReadOnly();
                }
                else
                {
                    roc = new List<T>(collection).AsReadOnly();
                }
            }

            return roc;
        }
    }
}
