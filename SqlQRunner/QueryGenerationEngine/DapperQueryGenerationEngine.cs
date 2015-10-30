using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;

namespace QueryGenerationEngine
{
    public class DapperQueryGenerationEngine<T>
    {
        private readonly SqlConnection _dataConnection;
        private readonly Dictionary<string, object> _queryParameters;
        private readonly ICollection<string> _whereStatements;

        public DapperQueryGenerationEngine(SqlConnection sqlConnection)
        {
            _dataConnection = sqlConnection;
            _queryParameters = new Dictionary<string, object>();
            _whereStatements = new List<string>();
        }

        public DapperQueryGenerationEngine()
            : this(new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConnection"].ToString()))
        {

        }

        public bool HasWhereParameter
        {
            get { return _queryParameters.Keys.Any(); }
        }

        public bool HasWhereStatement
        {
            get { return _whereStatements.Any(); }
        }

        public string CombinedWhereStatement
        {
            get { return string.Join(MatchAnyWhereStatement ? " OR " : " AND ", _whereStatements); }
        }

        public bool MatchAnyWhereStatement { get; set; }

        public string PreQuery { get; set; }
        public List<string> SelectQueryFields { get; set; }
        public int NumResults { get; set; }
        public string BaseQuery { get; set; }

        public void LoadQueryParameter(string key, object value)
        {
            _queryParameters.Add(key, value);
        }

        public void LoadWhereStatement(string statement)
        {
            _whereStatements.Add(statement);
        }

        public string BuildQuery()
        {
            var sb = new StringBuilder();

            if (!string.IsNullOrEmpty(PreQuery))
                sb.AppendLine(PreQuery);

            if (SelectQueryFields.Any())
            {
                sb.AppendLine(" SELECT ");
                if (NumResults > 0)
                    sb.AppendLine(" TOP " + NumResults);
                sb.AppendLine(string.Join(", ", SelectQueryFields));
                sb.AppendLine(" FROM ( ");
            }
            sb.AppendLine(BaseQuery);

            if (SelectQueryFields.Any())
            {
                sb.AppendLine(") baseQuery ");
            }

            if (!string.IsNullOrEmpty(CombinedWhereStatement))
            {
                sb.AppendLine(" WHERE ");
                sb.AppendLine(CombinedWhereStatement);
            }

            return sb.ToString();
        }

        public IEnumerable<T> RunQuery()
        {
            var builtQuery = BuildQuery();
            return HasWhereParameter && HasWhereStatement
                ? PerformRawQuery(builtQuery, _queryParameters)
                : PerformRawQuery(builtQuery);
        }

        public IEnumerable<T> PerformRawQuery(string query)
        {
            return _dataConnection.Query<T>(query);
        }

        public IEnumerable<T> PerformRawQuery(string query, Dictionary<string, object> parameters)
        {
            return _dataConnection.Query<T>(query, parameters);
        }
    }
}
