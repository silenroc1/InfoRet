using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SQLite;
using System.IO;

namespace InformationRetrieval
{
    class Preprocessor
    {
        static SQLiteConnection m_dbConnection;
        static SQLiteConnection meta_db;

        static StreamWriter str = new StreamWriter("meta_dbQuerys.txt");
        
        
        static int db_size;

        /* zijn deze drie echt nodig?
        // static ISet<string> brand_values = null;
        // static ISet<string> model_values = null;
        // static ISet<string> type_values = null;
        */

        static string[] num_columns = { "mpg", "cylinders", "displacement", "weight", "acceleration", "model_year", "origin" };
        static string[] cat_columns = { "brand", "model", "type" };

        static void Main(string[] args)
        {
            
            // open de van autompg-db
            LoadAutompg();
            
            // Open de meta_database
            OpenMeta_db();

            // parse de workload naar een dictionary<Entry(category,value),hoeveelheid>
            ParseWorkload();
            
            // bereken de h-waardes en sla op
            ComputeH();
            
            // vull idf-tables
            FillIdf_cat(meta_db);
            FillIdf_num(meta_db);

            // parse de workload en plaats in db
            ParseWorkload();

            if(meta_db != null) meta_db.Close();
            if(m_dbConnection != null) m_dbConnection.Close();
            Console.Read();
        }

        /*private static void StoreHValues()
        {
            AddQuery("create table h-value (category varchar(20), score real)");
            foreach (string s in cat_columns)
                AddQuery("insert into h-value values (" + s + "," + ComputeH(s, m_dbConnection) + ")");
            foreach (string s in num_columns)
                AddQuery("insert into h-value values (" + s + "," + ComputeH(s, m_dbConnection) + ")");
        }*/

        private static void OpenMeta_db()
        {
            
            SQLiteConnection.CreateFile("meta_db.sqlite");
            meta_db = new SQLiteConnection("Data Source=meta_db.sqlite;Version=3;");
            meta_db.Open();
            
            SQLiteCommand command = new SQLiteCommand("select * from autompg", m_dbConnection);

            db_size = 0;
            SQLiteDataReader reader = command.ExecuteReader();
            while (reader.Read())
                db_size++;            
        }

        private static void LoadAutompg()
        {
            // open de database connectie
            SQLiteConnection.CreateFile("autompg.sqlite");
            m_dbConnection = new SQLiteConnection("Data Source=autompg.sqlite;Version=3;");
            m_dbConnection.Open();

            // plaats de hele gegeven database in autompg.sqlite
            StreamReader str = new StreamReader("autompg.sql");
            SQLiteCommand command = new SQLiteCommand(str.ReadToEnd(), m_dbConnection);
            command.ExecuteNonQuery();
        }

        private static void FillIdf_num(SQLiteConnection meta_db)
        {
            AddQuery("create table idf_num (category varchar(20), value real, score real)");

            SQLiteCommand query_command;
            //SQLiteCommand update_command;
            SQLiteDataReader reader;
            foreach (string s in num_columns)
            {
                query_command = new SQLiteCommand("select distinct " + s + " from autompg", m_dbConnection);
                reader = query_command.ExecuteReader();
                while (reader.Read())
                {
                    AddQuery("insert into idf_num values (\'" + s + "\', \'" + reader[s] + "\', \'" + IDF(s, Convert.ToDouble(reader[s]), m_dbConnection));

                }

            }
        }

        private static void FillIdf_cat(SQLiteConnection meta_db)
        {
            AddQuery("create table idf_cat (category varchar(20), value varchar(20), score real)");

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
            double h = ObtainH(category);
            return
                Math.Pow(Math.E, -0.5 * Math.Pow((db_value - query_value) / h, 2)) *
                IDF(category, query_value, db);
        }

        private static double ObtainH(string category)
        {
            SQLiteCommand c = new SQLiteCommand("select score from meta_db where category = \'" + category + "\'");
            SQLiteDataReader r = c.ExecuteReader();
            while (r.Read())
                return Convert.ToDouble(r["score"]);

            return 0;
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
            double h = ObtainH(category);

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


        private static void ComputeH()
        {
            // initialiseer dictionarys
            Dictionary<string, double> dictmean = new Dictionary<string, double>();
            Dictionary<string, double> dictdev = new Dictionary<string, double>();
            foreach (string s in num_columns) { dictdev.Add(s, 0); dictmean.Add(s, 0); }


            //double sum = 0;
            //double mean = 0;
            
            SQLiteCommand command = new SQLiteCommand("select * from autompg", m_dbConnection);
            SQLiteDataReader reader = command.ExecuteReader();
            while (reader.Read()) {
                foreach(string s in dictmean.Keys) 
                    dictmean[s] += Convert.ToDouble(reader[s]);
            }

            foreach(string s in dictmean.Keys)
                dictmean[s] = dictmean[s] / db_size;

            command = new SQLiteCommand("select * from autompg", m_dbConnection);
            reader = command.ExecuteReader();

            while (reader.Read()) {
                foreach (string s in dictdev.Keys)
                {
                    //double x = Convert.ToDouble(reader[category]);
                    //double y = x - mean;
                    //sum += y * y;
                    dictdev[s] += Math.Pow(Convert.ToDouble(reader[s]) - dictmean[s], 2);
                }
            }

            foreach (string s in dictdev.Keys)
            {
                double sigma = (double)Math.Sqrt((1 / (double)db_size) * dictdev[s]);
                Console.WriteLine("sigma is: " + sigma);
                double h = 1.06 * sigma * Math.Pow(db_size, -0.2);

                AddQuery("insert into h-values values ("+ s+","+h+")");
                Console.WriteLine(h);
            }
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
            return (double)intersection/union;
        }


        
        private static void ParseWorkload()
        {
            Dictionary<Entry,int> workload = new Dictionary<Entry,int>();

            StreamReader streamreader = new StreamReader("workload.txt");
            streamreader.ReadLine(); streamreader.ReadLine(); // eerste twee regels hebben geen info

            int times;
            string[] input;
            while (!streamreader.EndOfStream)
            {
                input = streamreader.ReadLine().Split();

                if (input[0] != "") { // aan het eind staan lege regels, vandaar deze check
                    times = Convert.ToInt32(input[0]);

                    for (int i = 0; i < input.Length; i++)
                    {
                        if (num_columns.Contains(input[i]) || cat_columns.Contains(input[i]))
                        {
                            if (input[i + 1].Equals("="))
                            {
                                // randgeval voor de station wagon
                                if (input[i + 2].EndsWith("station")) input[i + 2] += " " + input[i + 3];
                                TryAdd(workload, new Entry(input[i], input[i + 2]), times);

                                //i += 2; // loop verder langs wat je al hebt gezien
                            }
                            else if (input[i + 1].Equals("IN"))
                            {
                                // randgeval voor de station wagon

                                if (input[i + 2].EndsWith("station")) input[i + 2] += " " + input[i + 3];
                                Console.Write(input[i + 2] + "leidt tot:");
                                foreach (string s in input[i + 2].Split(new char[] { '(', ',', ')' }))
                                {
                                    Console.Write(s + " ");
                                    TryAdd(workload, new Entry(input[i], s), times);
                                }
                                Console.WriteLine();                         //i += 2; // loop verder

                            }
                        }
                    }


                }
            }
            
            
            // plaats in meta_db
            AddQuery("create table query-frequency (category varchar(20), value varchar(20), score real, glob_import real)");
            foreach (KeyValuePair<Entry, int> p in workload)
                AddQuery("insert into query-frequency (" + p.Key.category + "," + p.Key.value + "," + p.Value + ")");

            // print workload voor debugging
            foreach (KeyValuePair<Entry, int> p in workload)
            {
                str.WriteLine(p.Key.category + " " + p.Key.value + ": " + p.Value);
                str.Flush();
            }
        }

        private static void TryAdd(Dictionary<Entry, int> workload, Entry entry, int times) {
            if (workload.ContainsKey(entry))
                workload[entry] += times;
            else
                workload.Add(entry, times);
            
        }

        private static void AddQuery(string command)
        {
            str.Write(command + ";\n");
            str.Flush();

        }    

    }

   

    public struct Entry
    {
        // alles in kleine letters
        public string category; // zonder quotes
        public string value; // met quotes: 'volkswagen'
        public Entry(string category, string value)
        {
            this.category = category;
            this.value = value;
        }

    }
}
