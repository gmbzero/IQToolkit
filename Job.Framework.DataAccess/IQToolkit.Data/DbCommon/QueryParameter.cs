using System;

namespace IQToolkit.Data.Common
{
    internal class QueryParameter
    {
        public string Name { get; }

        public Type Type { get; }

        public QueryType QueryType { get; }

        public QueryParameter(string name, Type type, QueryType queryType)
        {
            this.Name = name;
            this.Type = type;
            this.QueryType = queryType;
        }
    }
}