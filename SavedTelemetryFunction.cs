using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Kusto.Data;
using Kusto.Data.Net.Client;
using Kusto.Data.Common;
using Kusto.Ingest;
using Newtonsoft.Json;

namespace SavedTelemetryFunction
{
    public class SavedTelemetryFunction
    {
        private static string clusterUri = Environment.GetEnvironmentVariable("ADX_clusterUri");
        private static string ingestUri = Environment.GetEnvironmentVariable("ADX_ingestUri");
        private static string database = Environment.GetEnvironmentVariable("ADX_database");
        private static string tableName = Environment.GetEnvironmentVariable("ADX_table");

        [FunctionName("SavedTelemetryFunction")]
        public void Run([TimerTrigger("0 5 0 * * *")] TimerInfo myTimer, ILogger log)
        {
            log.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");

            var kcsb = new KustoConnectionStringBuilder(clusterUri).WithAadUserPromptAuthentication();

            using (var kustoClient = KustoClientFactory.CreateCslQueryProvider(kcsb))
            {
                //Set mydate to yesterday because the trigger runs at midnight
                DateTime myDate = DateTime.Now.AddDays(-1).Date;


                //Define the query with parameters
                var query = @"declare query_parameters(_startdate:datetime, _enddate:datetime);
                              Fill_forward(_startdate,_enddate)";                               

                var crp = new ClientRequestProperties();
                crp.SetParameter("_startdate", myDate);
                crp.SetParameter("_enddate", myDate.AddDays(1));

                //Execute the query
                using (var response = kustoClient.ExecuteQuery(database, query, crp))
                {
                    //Retreive the column indexes based on the column name
                    int columndeviceId = response.GetOrdinal("deviceId");
                    int columntimeStamp = response.GetOrdinal("timeStamp");
                    int columnlokatie = response.GetOrdinal("lokatie");
                    int columnmodel = response.GetOrdinal("model");
                    int columnstatus = response.GetOrdinal("status");
                    int columngps = response.GetOrdinal("gps");
                    
                    while (response.Read())
                    {
                        //Create a object of each record
                        SavedTelemetry savedTelemetry = new SavedTelemetry()
                        {
                            deviceId = response.GetString(columndeviceId),
                            timeStamp = response.GetDateTime(columntimeStamp).ToLocalTime(),
                            lokatie = Extensions.GetNullableInt(response, columnlokatie),
                            model = Extensions.GetNullableString(response, columnmodel),
                            status = Extensions.GetNullableString(response, columnstatus),
                            gps = Extensions.GetNullableString(response, columngps)
                        };

                        //only add the last record of day
                        DateTime dateToSave = myDate.Add(new TimeSpan(0, 23, 59, 59));
                        if (savedTelemetry.timeStamp.Equals(dateToSave))
                        {
                            Console.WriteLine("{0} - {1}", savedTelemetry.deviceId, savedTelemetry.timeStamp);
                            AddToTable(savedTelemetry);
                        }
                    }
                }
            }
        }

        /// <summary>
        /// This function will store the SavedTelemetry object to ADX
        /// </summary>
        /// <param name="savedTelemetry">Object to store</param>
        public static void AddToTable(SavedTelemetry savedTelemetry)
        {
            var kcsb = new KustoConnectionStringBuilder(ingestUri).WithAadUserPromptAuthentication();

            using (var ingestClient = KustoIngestFactory.CreateQueuedIngestClient(kcsb))
            {
                var ingestionProperties = new KustoIngestionProperties
                {
                    DatabaseName = database,
                    TableName = tableName,
                    Format = DataSourceFormat.json
                };

                var jsonString = JsonConvert.SerializeObject(savedTelemetry);
                ingestClient.IngestFromStream(Extensions.GenerateStreamFromString(jsonString), ingestionProperties);
            };
        }
    }
}
