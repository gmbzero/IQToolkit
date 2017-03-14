using System.Collections.Generic;
using System.Collections.ObjectModel;

namespace IQToolkit.Data.Common
{
    internal class QueryCommand
    {
        public string CommandText { get; }

        public ReadOnlyCollection<QueryParameter> Parameters { get; }

        public QueryCommand(string commandText, IEnumerable<QueryParameter> parameters)
        {
            this.CommandText = commandText;
            this.Parameters = parameters.ToReadOnly();
        }
    }
}