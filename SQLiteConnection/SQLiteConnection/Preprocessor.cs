﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SQLite;
using System.IO;
using System.Globalization;

namespace InformationRetrieval
{
    class Preprocessor
    {
        public static SQLiteConnection m_dbConnection;
        public static SQLiteConnection meta_db;

        static string metadb_commands = "";
        static StreamWriter str = new StreamWriter("metadb_commands.txt");
        
        public static int db_size;

        public static string[] num_columns = { "mpg", "cylinders", "displacement", "weight", "acceleration", "model_year", "origin" };
        public static string[] cat_columns = { "brand", "model", "type" };

        public static void Init()
        {
            
            // open de van autompg-db
            //Console.WriteLine("Loadautompg");
            LoadAutompg();

            //Console.WriteLine("Open meta-db");
            // Open de meta_database
            OpenMeta_db();

            //Console.WriteLine("Parse workload");
            // parse de workload naar een dictionary<Entry(category,value),hoeveelheid>
            ParseWorkload();

            //Console.WriteLine("Compute-H");
            // bereken de h-waardes en sla op
            ComputeH();

            //Console.Write("Fill-idf-cat");
            // vull idf-tables
            FillIdf_cat(meta_db);
            //Console.WriteLine("& num");
            FillIdf_num(meta_db);

            
            // niet meer nodig, querys worden in AddQuery direct uitgevoerd
            //SQLiteCommand c = new SQLiteCommand(metadb_commands, meta_db);
            //c.ExecuteNonQuery();
            
        }

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
            SQLiteDataReader reader;
            foreach (string s in num_columns)
            {
                query_command = new SQLiteCommand("select distinct " + s + " from autompg", m_dbConnection);
                reader = query_command.ExecuteReader();
                while (reader.Read())
                {
                    AddQuery("insert into idf_num values (\'" + s + "\', \'" + Convert.ToDouble(reader[s]).ToString(new CultureInfo("en-US")) + "\', \'" + IDF(s, Convert.ToDouble(reader[s]), m_dbConnection).ToString(new CultureInfo("en-US")) + "\')");

                }

            }
        }

        private static void FillIdf_cat(SQLiteConnection meta_db)
        {
            AddQuery("create table idf_cat (category varchar(20), value varchar(20), score real)");

            SQLiteCommand query_command;
            SQLiteDataReader reader;
            foreach (string s in cat_columns)
            {
                query_command = new SQLiteCommand("select distinct " + s + " from autompg", m_dbConnection);
                reader = query_command.ExecuteReader();
                while (reader.Read())
                {
                    AddQuery("insert into idf_cat values (\'" + s + "\', \'" + reader[s] + "\', \'" + IDF(s, (string)reader[s], m_dbConnection).ToString(new CultureInfo("en-US")) + "\')");
                    
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

        public static double ObtainH(string category)
        {
            SQLiteCommand c = new SQLiteCommand("select score from hvalues where category = \'" + category + "\'", meta_db);
            SQLiteDataReader r = c.ExecuteReader();
            while (r.Read())
                return Convert.ToDouble(r["score"]);

            return 0;
        }

        // overloaded voor strings en doubles
        public static double IDF(string category, string value, SQLiteConnection db)
        {
            string q = "select " + category + " from autompg where " + category + "=\'" + value + "\'";
            SQLiteCommand command = new SQLiteCommand(q, db);
            SQLiteDataReader reader = command.ExecuteReader();
            int freq = 0;

            // tel hoeveel matches
            while (reader.Read()) { freq++;}
            
            return Math.Log10(db_size / freq);
        }

        private static double IDF(string category, double value, SQLiteConnection db)
        {
            double h = ObtainH(category);
            //Console.WriteLine("category is " +category+" with h: " + h);

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

            AddQuery("create table hvalues (category varchar(20), score real)");
            
            SQLiteCommand command = new SQLiteCommand("select * from autompg", m_dbConnection);
            SQLiteDataReader reader = command.ExecuteReader();
            while (reader.Read()) {
                foreach(string s in dictmean.Keys.ToList()) 
                    dictmean[s] += Convert.ToDouble(reader[s]);
            }

            foreach(string s in dictmean.Keys.ToList())
                dictmean[s] = dictmean[s] / db_size;

            command = new SQLiteCommand("select * from autompg", m_dbConnection);
            reader = command.ExecuteReader();

            while (reader.Read()) {
                foreach (string s in dictdev.Keys.ToList())
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
                double h = 1.06 * sigma * Math.Pow(db_size, -0.2);
                AddQuery("insert into hvalues values (\'" + s + "\',\'" + h.ToString(new CultureInfo("en-US")) + "\')");
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
                                TryAdd(workload, new Entry(input[i], input[i + 2].Trim(new char[] { '\''})), times);

                                //i += 2; // loop verder langs wat je al hebt gezien
                            }
                            else if (input[i + 1].Equals("IN"))
                            {
                                // randgeval voor de station wagon

                                if (input[i + 2].EndsWith("station")) input[i + 2] += " " + input[i + 3];
                                //Console.Write(input[i + 2] + "leidt tot:");
                                foreach (string s in input[i + 2].Split(new char[] { '(', ',', ')' }))
                                {
                                    //Console.Write(s + " ");
                                    TryAdd(workload, new Entry(input[i], s.Trim(new char[] {'\''}) ), times);
                                }
                                //Console.WriteLine();                         //i += 2; // loop verder

                            }
                        }
                    }


                }
            }

            // compute global importance
            double total_globimport = 1;
            foreach (int i in workload.Values)
                total_globimport += Math.Log10(i);
            int maxQF = workload.Values.Max();

            // plaats in meta_db
            AddQuery("create table queryfrequency (category varchar(20), value varchar(20), score real, glob_import real)");

            // voor iedere in de originele database voorkomende waarde moet een qf-waarde worden ingevoerd.
            foreach (string s in cat_columns.Union(num_columns))
            {
                SQLiteCommand c = new SQLiteCommand("select distinct " + s + " from autompg", m_dbConnection);
                SQLiteDataReader r = c.ExecuteReader();

                while (r.Read())
                {
                    Entry e = new Entry();
                    if(cat_columns.Contains(s))
                        e = new Entry(s, (string)r[s]);
                    else
                        e = new Entry(s, ((Convert.ToDouble(r[s])).ToString(new CultureInfo("en-US"))));
                    if(workload.ContainsKey(e))
                        AddQuery("insert into queryfrequency values (\'" + e.category + "\',\'" + e.value + "\',\'" + ((1 + workload[e]) / (double)maxQF).ToString(new CultureInfo("en-US")) + "\',\'" + Math.Log10((1 + workload[e]) / (double)maxQF).ToString(new CultureInfo("en-US")) + "\')");
                    else
                        AddQuery("insert into queryfrequency values (\'" + e.category + "\',\'" + e.value + "\',\'" + ((1) / (double)maxQF).ToString(new CultureInfo("en-US")) + "\',\'" + Math.Log10((1) / (double)maxQF).ToString(new CultureInfo("en-US")) + "\')");

                }
            }
            //foreach (KeyValuePair<Entry, int> p in workload) {
            //    AddQuery("insert into queryfrequency values (\'" + p.Key.category + "\',\'" + p.Key.value + "\',\'" + ((1 + p.Value) / (double)maxQF) + "\',\'" + Math.Log10((1 + p.Value) / (double)maxQF) + "\')");
                
            //}




           
        }

        private static void TryAdd(Dictionary<Entry, int> workload, Entry entry, int times) {
            if (workload.ContainsKey(entry))
                workload[entry] += times;
            else
                workload.Add(entry, times);
            
        }

        private static void AddQuery(string command)
        {
            try
            {
                // voer het ook uit
                SQLiteCommand c = new SQLiteCommand(command, meta_db);
                c.ExecuteNonQuery();

                metadb_commands += command + ";";

                // schrijf het naar een file, want die moet worden ingeleverd
                // ook handig voor debuggen
                str.Write(command + ";\n");
                str.Flush();
            }
            catch (Exception)
            {
                str.Write("EXCEPTION: " + command + ";\n");
                str.Flush();
            }

            

           

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
