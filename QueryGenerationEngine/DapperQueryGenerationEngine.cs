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

        public DapperQueryGenerationEngine(SqlConnection sqlConnection)
        {
            _dataConnection = sqlConnection;
        }

        public DapperQueryGenerationEngine()
            : this(new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConnection"].ToString()))
        {
            
        }

        public IEnumerable<T> PerformQuery(string query)
        {
            return _dataConnection.Query<T>(query);
        }

        public IEnumerable<T> PerformQuery(string query, Dictionary<string, object> parameters)
        {
            return _dataConnection.Query<T>(query, parameters);
        }
    }
}
