using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SQLite;
using System.IO;

namespace SQLite
{
    class SQLiteConnecter
    {
        static SQLiteConnection m_dbConnection;

        static StreamWriter str = new StreamWriter("meta_dbQuerys.txt");
        
        
        static int db_size;
        static ISet<string> brand_values;
        static ISet<string> model_values;
        static ISet<string> type_values;

        static string[] num_columns = { "mpg", "cylinders", "displacement", "weight", "acceleration", "model_year", "origin" };
        static string[] cat_columns = { "brand", "model", "type" };

        static void Main(string[] args)
        {
            
            // inladen van autompg
            

            SQLiteConnection.CreateFile("autompg.sqlite");
            m_dbConnection = new SQLiteConnection("Data Source=autompg.sqlite;Version=3;");
            m_dbConnection.Open();

            string s;
            StreamReader str = new StreamReader("autompg.sql");
            s = str.ReadToEnd();
            SQLiteCommand command = new SQLiteCommand(s, m_dbConnection);
            command.ExecuteNonQuery();


            // meta-db voor idf en tf
            SQLiteConnection meta_db;
            SQLiteConnection.CreateFile("meta_db.sqlite");
            meta_db = new SQLiteConnection("Data Source=meta_db.sqlite;Version=3;");
            meta_db.Open();

            AddQuery("create table idf_cat (category varchar(20), value varchar(20), score real)");

            AddQuery("create table idf_num (category varchar(20), value real, score real)");
            

            try
            {
                

                command = new SQLiteCommand("select * from autompg order by brand desc", m_dbConnection);

                db_size = 0;
                SQLiteDataReader reader = command.ExecuteReader();
                while (reader.Read())
                {
                    db_size++;
                    brand_values.Add((string)reader["brand"]);
                    model_values.Add((string)reader["model"]);
                    type_values.Add((string)reader["type"]);

                }

                //FillIdf_cat(meta_db);

                //FillIdf_num(meta_db);

                /*Console.WriteLine("IDF(8 cylinders): " + IDF("cylinders", 8, m_dbConnection));
                Console.WriteLine("IDF(7 cylinders): " + IDF("cylinders", 7, m_dbConnection));
                Console.WriteLine("IDF(6 cylinders): " + IDF("cylinders",6,m_dbConnection));
                Console.WriteLine("IDF(5 cylinders): " + IDF("cylinders", 5, m_dbConnection));
                Console.WriteLine("IDF(4 cylinders): " + IDF("cylinders", 4, m_dbConnection));
                Console.WriteLine("IDF(3 cylinders): " + IDF("cylinders", 3, m_dbConnection));
                Console.WriteLine("IDF(2 cylinders): " + IDF("cylinders", 2, m_dbConnection));
                Console.WriteLine("IDF(1 cylinders): " + IDF("cylinders", 1, m_dbConnection));
                */
                 

                

            }

            finally
            {
                meta_db.Close();
                m_dbConnection.Close();
            }



            /*
            SQLiteConnection m_dbConnection;
            SQLiteConnection.CreateFile("MyDatabase.sqlite");
            m_dbConnection = new SQLiteConnection("Data Source=MyDatabase.sqlite;Version=3;");
            m_dbConnection.Open();

            string sql = "create table highscores (name varchar(20), score int)";
            SQLiteCommand command = new SQLiteCommand(sql, m_dbConnection);

            command.ExecuteNonQuery();

            sql = "insert into highscores (name, score) values ('Me', 3000)";
            SQLiteCommand command2 = new SQLiteCommand(sql, m_dbConnection);
            command2.ExecuteNonQuery();
            sql = "insert into highscores (name, score) values ('Myself', 6000)";
            command2 = new SQLiteCommand(sql, m_dbConnection);
            command2.ExecuteNonQuery();
            sql = "insert into highscores (name, score) values ('And I', 9001)";
            command2 = new SQLiteCommand(sql, m_dbConnection);
            command2.ExecuteNonQuery();

            string sql2 = "select * from highscores order by score desc";
            SQLiteCommand command3 = new SQLiteCommand(sql2, m_dbConnection);

            SQLiteDataReader reader = command3.ExecuteReader();

            string sql3 = "select * from highscores order by score desc";
            SQLiteCommand command4 = new SQLiteCommand(sql3, m_dbConnection);
            SQLiteDataReader reader2 = command4.ExecuteReader();
            while (reader2.Read())
                Console.WriteLine("Name: " + reader2["name"] + "\tScore: " + reader2["score"]);
            */
            Console.Read();
        }

        private static void FillIdf_num(SQLiteConnection meta_db)
        {
            SQLiteCommand query_command;
            //SQLiteCommand update_command;
            SQLiteDataReader reader;
            foreach (string s in num_columns)
            {
                query_command = new SQLiteCommand("select distinct " + s + " from autompg", m_dbConnection);
                reader = query_command.ExecuteReader();
                while (reader.Read())
                {
                    AddQuery("insert into idf_num values (\'" + s + "\', \'" + reader[s] + "\', \'" + IDF(s, (double)reader[s], m_dbConnection));
                    
                }

            }
            // omdat er bij numerieke velden een oneindig aantal verschillende waarden zijn, 
            // is het niet mogelijk de hele idf-table al te vullen.
            // een goed alternatief lijkt me om de table te vullen met enkel de waarden die
            // al in de oorspronkelijke table voorkomen.

            // voor iedere kolom
            // voor iedere verschillende waarde
            // bereken de IDF-waarde
            // store de waarde in de db
        }

        private static void AddQuery(string command)
        {
            str.Write(command + ";\n");
            str.Flush();

        }

        private static void FillIdf_cat(SQLiteConnection meta_db)
        {
            SQLiteCommand query_command;
            //SQLiteCommand update_command;
            SQLiteDataReader reader;
            foreach (string s in cat_columns)
            {
                query_command = new SQLiteCommand("select distinct " + s + " from autompg", m_dbConnection);
                reader = query_command.ExecuteReader();
                while (reader.Read())
                {
                    AddQuery("insert into idf_cat values (\'" + s + "\', \'" + reader[s] + "\', \'" + IDF(s, (string)reader[s], m_dbConnection));
                    
                }

            }

           

            // voor iedere kolom
            // voor iedere verschillende waarde 
            // (hardcoded of via sql alle verschillende waarden opvragen)
            // bereken de IDF-waarde
            // store de waarde in de db
        }

        private static double S(string category, string query_value, string db_value, SQLiteConnection db)
        {
            if (query_value.Equals(db_value))
                return IDF(category, query_value, db);
            else
                return 0;
        }

        private static double S(string category, double query_value, double db_value, SQLiteConnection db)
        {
            double h = ComputeH(category, db);
            return
                Math.Pow(Math.E, -0.5 * Math.Pow((db_value - query_value) / h, 2)) *
                IDF(category, query_value, db);
        }

        private static double IDF(string category, string value, SQLiteConnection db)
        {
            string q = "select " + category + " from autompg where " + category + "=\'" + value + "\'";
            SQLiteCommand command = new SQLiteCommand(q, db);
            SQLiteDataReader reader = command.ExecuteReader();
            int freq = 0;

            // tel hoeveel matches
            while (reader.Read()) { freq++;};
            
            return Math.Log10(db_size / freq);
        }

        private static double IDF(string category, double value, SQLiteConnection db)
        {
            double h = ComputeH(category, db);

            double freq = 0;
            string q = "select " + category + " from autompg";
            SQLiteCommand command = new SQLiteCommand(q, db);
            SQLiteDataReader reader = command.ExecuteReader();

            while (reader.Read())
            {
                freq += Math.Pow(Math.E, -0.5 * Math.Pow((Convert.ToDouble(reader[category]) - value) / h, 2));

            }

            return Math.Log10(db_size / freq);
        }


        private static double ComputeH(string category, SQLiteConnection db)
        {
            double sum = 0;
            double mean = 0;
            
            SQLiteCommand command = new SQLiteCommand("select " + category + " from autompg", db);
            SQLiteDataReader reader = command.ExecuteReader();
            while (reader.Read()) {
                mean += (double)reader[category];
            }
            mean = mean / db_size;

            while (reader.Read()) {
                double x = (double)reader[category];
                double y = x - mean;
                sum += y * y;
            }

            double sigma = Math.Sqrt((1/db_size) * sum);
            double h = 1.06 * sigma * Math.Pow(db_size, -0.2);
            return h;
        }

        private static int ComputeRQFMax() {
            return 0;
        }

        private static double QF() {
            //deel de query freq door RQFMax
            return 0;
        }

        private static double Jaccard(HashSet<string> t, HashSet<string> q){
            int intersection = 0;
            int union = 0;
            //make sure t is the smaller one
            if(t.Count > q.Count){
                HashSet<string> temp = t;
                t = q;
                q = temp;
            }

            foreach(string s in t){
                if(q.Contains(s)){
                    intersection++;
                }
            }

            union = t.Count + q.Count - intersection;

            return intersection/union;
        }


        
        private Dictionary<Entry,int> ParseDieShit()
        {
            Dictionary<Entry,int> workload = new Dictionary<Entry,int>();

            StreamReader str = new StreamReader("workload.txt");
            str.ReadLine(); str.ReadLine(); // eerste twee regels hebben geen info

            int times;
            string[] input;
            while (!str.EndOfStream)
            {
                input = str.ReadLine().Split();
                times = Convert.ToInt32(input[0]);
                //ISet<Entry> gevraagd = new HashSet<Entry>();
                for (int i = 0; i < input.Length; i++)
                {
                    if (num_columns.Contains(input[i]) || cat_columns.Contains(input[i]))
                    {
                        if (input[i + 1].Equals("="))
                        {
                            TryAdd(workload, new Entry(input[i], input[1 + 2]), times);

                            i += 2; // loop verder langs wat je al hebt gezien
                        }
                        else if (input[i+1].Equals("IN"))
                        {
                            foreach(string s in input[i+2].Split(new char[]{'(',',',')'}))
                                TryAdd(workload, new Entry(input[i], s), times);

                            i += 2; // loop verder
                             
                        }
                    }
                    

                }
            }

            return workload;
        }

        private void TryAdd(Dictionary<Entry, int> workload, Entry entry, int times) {
            if (workload.ContainsKey(entry))
                workload[entry] += times;
            else
                workload.Add(entry, times);
            
        }

    }

    

    public struct Entry
    {
        public string category;
        public string value;
        public Entry(string category, string value)
        {
            this.category = category;
            this.value = value;
        }

    }
}
