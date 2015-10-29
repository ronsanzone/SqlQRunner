using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace QueryGenerationEngine.Test
{
    [TestClass]
    public class DapperQueryGenerationEngineTest
    {
        [TestMethod]
        public void TestMethod1()
        {
        }
    }

    public class MapDataSearchEngine
    {

        private List<GeneralQueryWhereFilter> _generalQueryWhereFilters;
        private DapperQueryGenerationEngine<GoogleMapPoint> _queryGenerationEngine;
        private const string PreDataQuery = @"
            DECLARE @TempProvRiskLevel AS TABLE (ProviderId INT NOT NULL PRIMARY KEY, RiskLevel INT, UNIQUE NONCLUSTERED (ProviderId))
			INSERT @TempProvRiskLevel
			SELECT ProviderId, MAX(RiskLevel)
			FROM Results 
			WHERE Discriminator = 'ProviderResult' AND ModelId in (113,114,115,116,117,118,119,120)
			GROUP BY ProviderId

			DECLARE @TempPharmRiskLevel AS TABLE (PharmacyId INT NOT NULL PRIMARY KEY, RiskLevel INT, UNIQUE NONCLUSTERED (PharmacyId))
			INSERT @TempPharmRiskLevel
			SELECT PharmacyId, MAX(RiskLevel)
			FROM Results 
			WHERE Discriminator = 'PharmacyResult' AND ModelId in (113,114,115,116,117,118,119,120)
			GROUP BY PharmacyId

			DECLARE @TempProvActionsTaken AS TABLE (ProviderId INT, ActionsTaken INT)
			INSERT @TempProvActionsTaken
			SELECT ProviderId, COUNT(*)
			FROM ActionTakens  
			WHERE Discriminator = 'ProviderActionTaken'
			GROUP BY ProviderId
			
			DECLARE @TempPharmActionsTaken AS TABLE (PharmacyId INT, ActionsTaken INT)
			INSERT @TempPharmActionsTaken 
			SELECT PharmacyId, COUNT(*)
			FROM ActionTakens  
			WHERE Discriminator = 'PharmacyActionTaken'			
			GROUP BY PharmacyId
        ";
        private const string BaseDataQuery = @"						        

					SELECT P.Id as id
						,null as PharmacyId
						,P.Id as ProviderId
						,ProviderName as name
						,'Providers' as entityTypeName
						,Lat as latitude
						,Lng as longitude
						,CASE RiskLevel WHEN 3 THEN 'red' WHEN 2 THEN 'yellow' ELSE 'green' END as color 
						,RiskLevel
						,P.Line1Addr as StreetAddress
						,NPI
						,City
						,State
						,Zipcode
						,ActionsTaken
					FROM Providers P
					LEFT JOIN @TempProvRiskLevel TR ON P.Id = TR.ProviderId
					LEFT JOIN @TempProvActionsTaken TA ON P.Id = TA.ProviderId					
					WHERE Lat is not null and Lng is not null
					UNION ALL
					SELECT P.Id as id
						,P.ID as PharmacyId
						,null as ProviderId
						,PharmacyName as name
						,'Pharmacies' as entityTypeName
						,Lat as latitude
						,Lng as longitude
						,CASE RiskLevel WHEN 3 THEN 'red' WHEN 2 THEN 'yellow' ELSE 'green' END as color 						
						,RiskLevel
						,P.Line1Addr as StreetAddress
						,NPI
						,City
						,State
						,Zipcode
						,ActionsTaken                        
					FROM Pharmacies P
					LEFT JOIN @TempPharmRiskLevel TR ON P.Id = TR.PharmacyId
					LEFT JOIN @TempPharmActionsTaken TA ON P.Id = TA.PharmacyId
					WHERE Lat is not null and Lng is not null 
			
                    ";

        private const string BaseSelectQuery = @"
                    SELECT id
                        ,name
                        ,entityTypeName
                        ,latitude
                        ,longitude
                        ,color 
                        ";



        public MapDataSearchEngine()
        {
            _queryGenerationEngine = new DapperQueryGenerationEngine<GoogleMapPoint>();
            _generalQueryWhereFilters = new List<GeneralQueryWhereFilter>();
        }


        private string GetBaseMapQuery()
        {
            var sb = new StringBuilder();
            sb.AppendLine(PreDataQuery);
            sb.AppendLine(BaseSelectQuery);
            sb.AppendLine(@"FROM (");
            sb.AppendLine(BaseDataQuery);
            sb.AppendLine(") as baseQuery");
            return sb.ToString();
        }

        public IEnumerable<GoogleMapPoint> GetGoogleMapResults(GoogleCombinedSearch googleSearch, List<long> modelIds)
        {
            var baseMapQuery = GetBaseMapQuery();
            GenerateSearchStringFilter(googleSearch.SearchFields, googleSearch.SearchString);
            GenerateLocationFilter(googleSearch.Location, (googleSearch.SearchType == "Any"));
            GenerateAddressFilter(googleSearch.StreetAddress);
            GenerateStateFilter(googleSearch.SelectedStates);
            GenerateModelFilter(googleSearch.RiskModelHeaders, googleSearch.TimePeriods);
            GenerateIntRangeFilter("ActionsTaken", googleSearch.ActionsTaken);
            GenerateHighRiskFilter(googleSearch.HighRiskOnly);


            if (_generalQueryWhereFilters.Any())
            {
                var whereClauses = _generalQueryWhereFilters.Select(i => i.WhereFilterText);
                var whereParamDicts = _generalQueryWhereFilters.Select(i => i.WhereFilterParams);

                var whereClauseString = " WHERE " +
                                        string.Join((googleSearch.SearchType == "Any") ? " OR " : " AND ", whereClauses);

                var combinedParamDict = new Dictionary<string, object>();
                foreach (var whereParamDict in whereParamDicts)
                {
                    foreach (var entry in whereParamDict)
                    {
                        if (!combinedParamDict.ContainsKey(entry.Key))
                        {
                            combinedParamDict.Add(entry.Key, entry.Value);
                        }
                    }
                }

                //var combinedParamDict = whereParamDicts.SelectMany(d => d)
                //                        .GroupBy(kvp => kvp.Key, (key, kvps) => new {Key = key, Value = kvps.First().Value})
                //                        .ToDictionary(x => x.Key, x => x.Value);

                var results = _queryGenerationEngine.PerformQuery(baseMapQuery + whereClauseString, combinedParamDict);
                return results;
            }
            else
            {
                var results = _queryGenerationEngine.PerformQuery(baseMapQuery);
                return results;
            }


        }


        private void GenerateHighRiskFilter(bool highRiskOnly)
        {
            if (highRiskOnly)
            {
                var whereClause = "( RiskLevel = @RiskLevel )";
                var whereParams = new Dictionary<string, object> { { "RiskLevel", 3 } };
                _generalQueryWhereFilters.Add(new GeneralQueryWhereFilter { WhereFilterText = whereClause, WhereFilterParams = whereParams });
            }
        }

        private void GenerateModelFilter(List<int> riskModelHeaders, List<int> timePeriods)
        {
            if (riskModelHeaders.Any())
            {
                var whereInSection = timePeriods.Any() ? "(Models.ModelHeaderId in @ModelHeaderIds AND Models.TimePeriodId in @TimePeriodIds)"
                                                        : "(Models.ModelHeaderId in @ModelHeaderIds )";


                var whereClause = @"EXISTS(SELECT 1 FROM Results 
							INNER JOIN Models on Results.ModelId = Models.Id
							WHERE Discriminator in ('PharmacyResult', 'ProviderResult') AND ((baseQuery.PharmacyId = Results.PharmacyId) OR (baseQuery.ProviderId = Results.ProviderId)) 
							AND " + whereInSection + " )";

                var whereParams = new Dictionary<string, object>();

                whereParams.Add("ModelHeaderIds", riskModelHeaders);
                if (timePeriods.Any())
                {
                    whereParams.Add("TimePeriodIds", timePeriods);
                }
                _generalQueryWhereFilters.Add(new GeneralQueryWhereFilter { WhereFilterParams = whereParams, WhereFilterText = whereClause });
            }
        }

        private void GenerateStateFilter(ICollection<string> selectedStates)
        {
            if (selectedStates.Any())
            {
                var whereClause = "(State in @States)";
                var whereParams = new Dictionary<string, object> { { "States", selectedStates.ToArray() } };
                _generalQueryWhereFilters.Add(new GeneralQueryWhereFilter { WhereFilterParams = whereParams, WhereFilterText = whereClause });
            }
        }

        private void GenerateAddressFilter(string streetAddress)
        {
            if (!string.IsNullOrEmpty(streetAddress))
            {
                var whereClause = "(StreetAddress = @StreetAddress)";
                var whereParams = new Dictionary<string, object> { { "StreetAddress", streetAddress } };
                _generalQueryWhereFilters.Add(new GeneralQueryWhereFilter { WhereFilterParams = whereParams, WhereFilterText = whereClause });
            }
        }

        private void GenerateLocationFilter(string location, bool orsearch)
        {
            if (!string.IsNullOrEmpty(location))
            {
                const RegexOptions options = RegexOptions.None;
                var regex = new Regex(@"((""((?<token>.*?)(?<!\\)"")|(?<token>[\w]+))(\s)*)", options);
                var locationList = (from Match m in regex.Matches(location)
                                    where m.Groups["token"].Success
                                    select (object)m.Groups["token"].Value).ToList();

                var whereClauses = new List<string>();
                var whereParams = new Dictionary<string, object>();
                for (var j = 0; j < locationList.Count(); j++)
                {
                    whereClauses.Add("(City  = @City" + j + " OR State = @State" + j + " OR ZipCode = @ZipCode" + j + ")");
                    whereParams.Add("City" + j, locationList[j]);
                    whereParams.Add("State" + j, locationList[j]);
                    whereParams.Add("ZipCode" + j, locationList[j]);
                }
                var combinedWhereClause = string.Join(orsearch ? " OR " : " AND ", whereClauses);

                _generalQueryWhereFilters.Add(new GeneralQueryWhereFilter { WhereFilterParams = whereParams, WhereFilterText = combinedWhereClause });
            }

        }

        private void GenerateSearchStringFilter(List<string> searchFields, string searchString)
        {
            if (string.IsNullOrEmpty(searchString) || searchString.Trim() == "NPI or Name Search")
                return;
            var whereClauses = new List<string>();
            var whereParams = new Dictionary<string, object>();
            foreach (var searchField in searchFields)
            {
                whereClauses.Add("( " + searchField + " = @" + searchField + " )");
                whereParams.Add(searchField, searchString.Trim());
            }
            var combinedWhereClause = "( " + string.Join(" OR ", whereClauses) + " )";
            _generalQueryWhereFilters.Add(new GeneralQueryWhereFilter { WhereFilterParams = whereParams, WhereFilterText = combinedWhereClause });
        }

        private void GenerateIntRangeFilter(string field, string filterExpression)
        {
            if (!string.IsNullOrEmpty(filterExpression))
            {
                var expression = filterExpression.Split(' ');
                if (expression[0] == "gt")
                {
                    var expressVal = Convert.ToInt32(expression[1]);
                    var paramName = string.Format("{0}Value", field);
                    var whereClause = string.Format("({0} > @{1})", field, paramName);
                    var whereParams = new Dictionary<string, object> { { paramName, expressVal } };
                    _generalQueryWhereFilters.Add(new GeneralQueryWhereFilter { WhereFilterParams = whereParams, WhereFilterText = whereClause });
                }
                if (expression[0] == "lt")
                {
                    var expressVal = Convert.ToInt32(expression[1]);
                    var paramName = string.Format("{0}Value", field);
                    var whereClause = string.Format("({0} < @{1})", field, paramName);
                    var whereParams = new Dictionary<string, object> { { paramName, expressVal } };
                    _generalQueryWhereFilters.Add(new GeneralQueryWhereFilter { WhereFilterParams = whereParams, WhereFilterText = whereClause });
                }
                if (expression[0] == "bt")
                {
                    var expressVal1 = Convert.ToInt32(expression[1].Split('-')[0]);
                    var expressVal2 = Convert.ToInt32(expression[1].Split('-')[1]);
                    var paramName1 = string.Format("{0}Value1", field);
                    var paramName2 = string.Format("{0}Value2", field);
                    var whereClause = string.Format("({0} > @{1} AND {0} < @{2})", field, paramName1, paramName2);
                    var whereParams = new Dictionary<string, object> { { paramName1, expressVal1 }, { paramName2, expressVal2 } };
                    _generalQueryWhereFilters.Add(new GeneralQueryWhereFilter { WhereFilterParams = whereParams, WhereFilterText = whereClause });
                }
            }

        }
    }

    public class GeneralQueryWhereFilter
    {
        public string WhereFilterText { get; set; }
        public Dictionary<string, object> WhereFilterParams { get; set; }
    }
}
