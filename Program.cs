using System;
using System.Data;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Snowflake.Data.Client;

namespace Azure_Snowflake
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine($"started at {DateTime.Now.ToString()}");

            try
            {
                
                // snowsql -a gr73978 -u ThangaganeshSoundarapandian  -p Ganesh@0987
                // add connection  string here.
                 string snowflakeConnectionString =
                 "";

                using (IDbConnection conn = new SnowflakeDbConnection())
                {
                    Console.WriteLine($"{conn.State}");

                    conn.ConnectionString = snowflakeConnectionString;

                    conn.Open();

                    Console.WriteLine($"{conn.State}");

                    //string output = string.Empty;

                   string output = JsonConvert.SerializeObject(getRecords(conn), Formatting.Indented, new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore });

                    //insertRecords(conn);

                    Console.WriteLine(output);

                    conn.Close();
                }

            }
            catch (Exception e)
            {

                // Create a JSON error object
                var output = new JObject();
                output.Add("ClassName", e.GetType().Name);
                output.Add("Message", e.Message);
                output.Add("StackTrace", e.StackTrace);


                Console.WriteLine(
                    JsonConvert.SerializeObject(output, Formatting.Indented, new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore })
                );
            }

        }

        // example fetching records
        public static JObject getRecords( IDbConnection conn)
        {

            IDbCommand cmd = conn.CreateCommand();
            cmd.CommandText = "select * from SERVICEPROVIDERSCONTENT;";
            IDataReader reader = cmd.ExecuteReader();
            JObject output = new JObject();

            int count = 0;
            while (reader.Read())
            {
                
                JObject data = new JObject();
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    var columnName = reader.GetName(i);
                    var value = reader[i].ToString();
                    data.Add(columnName, value);
                }
                output.Add(new JProperty(count.ToString(), data));
                count++;
            }

            return output;

        }

        // exmaple inserting records
        public static void  insertRecords( IDbConnection conn)
        {
          try
          {
               IDbCommand cmd = conn.CreateCommand();

               string date = DateTime.Now.ToUniversalTime()
                         .ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff'Z'");

                Console.WriteLine($"date {date}");

                    cmd.CommandText = $@"INSERT INTO SERVICEPROVIDERSCONTENT
                                    VALUES('1','test1','test2','test3','test4','test5',
                                    '{date}','{date}')";


                var count = cmd.ExecuteNonQuery();
                //Assert.AreEqual(3, count);

                Console.WriteLine($"Count:- {count}");
                Console.WriteLine("insertion completed");
               
                    // cmd.CommandText = @"INSERT INTO SERVICEPROVIDERSCONTENT VALUES 
                    //                     (ID),
                    //                     (RESTAURANTID),
                    //                     (SERVICEPROVIDER),
                    //                     (CATEGORY),
                    //                     (STANDARDCONTENT),
                    //                     (SERVICEPROVIDERCONTENT),
                    //                     (CREATEDDATE),
                    //                     (UPDATEDATE);";

                    // cmd.CommandText = @"INSERT INTO SERVICEPROVIDERSCONTENT (ID,RESTAURANTID,SERVICEPROVIDER,CATEGORY,STANDARDCONTENT,SERVICEPROVIDERCONTENT,CREATEDDATE,
                    //             UPDATEDATE)";

                    

             //cmd.CommandText = @"INSERT INTO SERVICEPROVIDERSCONTENT values (?),(?),(?),(?),(?),(?),(?),(?)";

                    // var p1 = cmd.CreateParameter();
                    // p1.ParameterName = "ID";
                    // p1.Value = "2";
                    // p1.DbType = DbType.String;
                    // cmd.Parameters.Add(p1);

                    // var p2 = cmd.CreateParameter();
                    // p2.ParameterName = "RESTAURANTID";
                    // p2.Value = "test2";
                    // p2.DbType = DbType.String;
                    // cmd.Parameters.Add(p2);

                    // var p3 = cmd.CreateParameter();
                    // p3.ParameterName = "SERVICEPROVIDER";
                    // p3.Value = "test2";
                    // p3.DbType = DbType.String;
                    // cmd.Parameters.Add(p3);

                    // var p4 = cmd.CreateParameter();
                    // p4.ParameterName = "CATEGORY";
                    // p4.Value = "test2";
                    // p4.DbType = DbType.String;
                    // cmd.Parameters.Add(p4);

                    //  var p5 = cmd.CreateParameter();
                    // p5.ParameterName = "STANDARDCONTENT";
                    // p5.Value = "test2";
                    // p5.DbType = DbType.String;
                    // cmd.Parameters.Add(p5);

                    // var p6 = cmd.CreateParameter();
                    // p6.ParameterName = "SERVICEPROVIDERCONTENT";
                    // p6.Value = "test2";
                    // p6.DbType = DbType.String;
                    // cmd.Parameters.Add(p6);

                    //  var p7 = cmd.CreateParameter();
                    // p7.ParameterName = "CREATEDDATE";
                    // p7.Value = DateTime.Now;
                    // p7.DbType = DbType.DateTime;
                    // cmd.Parameters.Add(p7);

                    //  var p8 = cmd.CreateParameter();
                    // p8.ParameterName = "UPDATEDATE";
                    // p8.Value = DateTime.Now;
                    // p8.DbType = DbType.DateTime;
                    // cmd.Parameters.Add(p8);

                    
          }
          catch (Exception e)
          {
              throw e;
          }
        }
    }
}
