﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SQLite;
using System.Collections.Specialized;

namespace InformationRetrieval
{
    class QueryHandler
    {
        //private static SQLiteConnection m_dbConnection = Preprocessor.m_dbConnection; 
        //private static SQLiteConnection meta_db = Preprocessor.meta_db;
        // Gebruik Preprocessor.meta_db en Preprocessor.m_dbConnection!!

        private static ISet<int> seen_ids = new HashSet<int>();

        static void Main()
        {
            
            Console.Write("           Data-analyse en Retrieval! \nPracticum 1:  \nDoor Cornelis Bouter & Alex Klein\n\n");
            Console.Write("Preprocessing...\n");
            Preprocessor.Init();
            Console.Write("Preprocessing done!\n\n");

            Console.Write("Voer uw query in! \nVoorbeeldinputs: \nk = 6, brand = 'volkswagen';\ncylinders = 4, brand = 'ford';\n");
            char[] splitchars = { ',' };
            while (true)
            {
                string[] input = "mpg = 18, brand = \'ford\'".Split(splitchars);

                int K = ExtractK(input);
                Dictionary<string, string> querys = ExtractQueries(input);
                //Console.WriteLine(querys);
                //Console.Write(K);
                List<TopKEntry> topK = ITA(querys, K);
                foreach (TopKEntry kv in topK)
                {
                    Console.WriteLine("Entry met id " + kv.entry["id"] + " met " + kv.score + " punten.");
                }

                Console.WriteLine("\nNog een query?");
                Console.ReadLine();
            }

            //if (meta_db != null) meta_db.Close();
            //if (m_dbConnection != null) m_dbConnection.Close();
            //Console.Read();
            
        }

        private static int ExtractK(string[] input)
        {
            foreach (string s in input)
                if (s[0] == 'k')
                    return Convert.ToInt32(s[0]);

            return 10;
        }

        private static Dictionary<string, string> ExtractQueries(string[] input)
        {
            Dictionary<string, string> querys = new Dictionary<string, string>();
            string[] split;
            char[] splitchar = { '=' };
            foreach (string s in input)
            {
                split = s.Split(splitchar);
                if (!split[0].Equals('K'))
                {
                    querys.Add(split[0].Trim(), split[1].Trim(new char[] {'\'', ' '} ));

                }
            }

            return querys;
        }

        private static List<TopKEntry> ITA(Dictionary<string,string> query, int K) {
            List<TopKEntry> buffer = new List<TopKEntry>();

            // voor de stopping-condition
            Dictionary<string, double> currentValue = new Dictionary<string, double>();

            while(seen_ids.Count < Preprocessor.db_size){
                foreach(string cat  in Preprocessor.num_columns){
                    Console.WriteLine(cat);
                    Console.WriteLine(seen_ids.Count);
                    //Lk is the the location in the ordering that is given for the query
                    int TIDk = IndexLookupGetNextTID(cat, query);
                    seen_ids.Add(TIDk);     
               
                    SQLiteDataReader Tk = TupleLookup(TIDk);
                    if (Tk.Read())
                    {
                        //sla current value op voor stopping condition
                        if (currentValue.ContainsKey(cat))
                            currentValue[cat] = Convert.ToDouble(Tk[cat]);
                        else
                            currentValue.Add(cat, Convert.ToDouble(Tk[cat]));
                        
                        
                        double score = ComputeScore(Tk, query);

                        Console.WriteLine("Store in buffer");
                        StoreInBuffer(buffer, Tk, score, K);

                        Console.WriteLine("Stopping cond");
                        if (StoppingCondition(currentValue, buffer[buffer.Count - 1].score, query))
                        {
                            return buffer;
                        }

                    }
                }
                //
                //currentValue.Clear();
            }
            return buffer;
        }

        private static void StoreInBuffer(List<TopKEntry> buffer, SQLiteDataReader Tk, double score, int K)
        {
            if (buffer.Count < K)
            {
                buffer.Add(new TopKEntry(Tk,score));
                buffer.Sort();
            }
            else if (score > buffer[buffer.Count - 1].score)
            {
                buffer[buffer.Count - 1] = new TopKEntry(Tk, score);

                buffer.Sort();
            }

        }


        private static SQLiteDataReader TupleLookup(int TID){
            SQLiteCommand command = new SQLiteCommand("select * from autompg WHERE id = \'" + TID + "\'", Preprocessor.m_dbConnection);
            SQLiteDataReader reader = command.ExecuteReader();
            //reader.Read();
            //Console.Write(Convert.ToDouble(reader["cylinders"]));
            
            return reader;
        }

        private static bool StoppingCondition(Dictionary<string,double> currentvalue, double lowestScore, Dictionary<string,string> query) {
            //Compute the highest possible score from what needs to be check
            //and compare that to the lowest score in the top-K
            string command = "select * from autompg where ";
            foreach (string s in Preprocessor.num_columns) {
                if (currentvalue.ContainsKey(s)) command += (s + "= \'" + currentvalue[s] + "\' AND ");
                else // nog niet iedere categorie is langs geweest, dus nog geen hypothetical max te maken.
                    return false;
            }

            foreach (string s in Preprocessor.cat_columns)
            {
                if (query.Keys.Contains(s))
                    command += (s + "= \'" + query[s] + "\' AND ");
                // evt een else met zoeken naar waarde met max global importance
            }
            char[] trim = {'A','N', 'D', ' '};

            Console.WriteLine(command.TrimEnd(trim));
            SQLiteCommand c = new SQLiteCommand(command.TrimEnd(trim), Preprocessor.m_dbConnection);
            SQLiteDataReader r = c.ExecuteReader();
            if (r.Read())
                return ComputeScore(r, query) < lowestScore;
            else
                return false;
        }

        // TODO: optimaliseren. Sla de DataReader bv op in een Dict, zodat je ze niet steeds opnieuw hoeft aan te maken
        //returnt de gevraagde TID als deze nog niet eerder is opgevraagd, anders -1
        private static int IndexLookupGetNextTID(string category, Dictionary<string,string> query){
            // zoek naar eerste id met waarde die nog niet is gezien
            if (query.ContainsKey(category))
            {
                SQLiteCommand c = new SQLiteCommand("select * from autompg where " + category + "= \'" + query[category] + "\'", Preprocessor.m_dbConnection);
                SQLiteDataReader reader = c.ExecuteReader();

                while (reader.Read())
                {
                    if (!seen_ids.Contains(Convert.ToInt32(reader["id"])))
                        return Convert.ToInt32(reader["id"]);


                }
            }

            // zoek daarna naar eerste id zonder de gevraagde waarde
            SQLiteCommand c2 = query.ContainsKey(category) ?
                new SQLiteCommand("select * from autompg where " + category + "<> \'" + query[category] + "\'", Preprocessor.m_dbConnection) :
                new SQLiteCommand("select * from autompg", Preprocessor.m_dbConnection);
            SQLiteDataReader reader2 = c2.ExecuteReader();
            while (reader2.Read())
            {
                if (!seen_ids.Contains(Convert.ToInt32(reader2["id"])))
                    return Convert.ToInt32(reader2["id"]);
            }

            // should be unreachable
            return -1;

        }




       


        private static int HypothethicalMax()
        {
            throw new NotImplementedException();
        }

        private static double ComputeScore(SQLiteDataReader tuple, Dictionary<string,string> querys )//, string type)
        {
            string type = "qfidf";
            double score = 0;

            // Overlap (sectie 6.2.1)
            if(type.Equals("overlap"))
            {
               
            }

            // IDF (sectie 3)
            else if (type.Equals("idf"))
            {
                foreach (string q in querys.Keys)
                {
                    Console.Write(q);

                    SQLiteCommand c = Preprocessor.cat_columns.Contains(q) ?
                        new SQLiteCommand("select * from idf_cat where category = \'" + q + "\' AND value = \'" + querys[q] + "\'", Preprocessor.meta_db) :
                        new SQLiteCommand("select * from idf_num where category = \'" + q + "\' AND value = \'" + querys[q] + "\'", Preprocessor.meta_db);
                    SQLiteDataReader r = c.ExecuteReader();

                    if (r.Read())
                    {
                        score += ComputeIDFSimilarity(q, querys[q], tuple[q], Convert.ToDouble(r["score"]));
                    }
                    else
                    {
                        if (Preprocessor.cat_columns.Contains(q))
                            score += 0;
                        else
                            score += Preprocessor.IDF(q, querys[q], Preprocessor.m_dbConnection); // bereken idf online
                    }

                }

            }
                // QFIDF (sectie 4)
            else if (type.Equals("qfidf"))
            {
                // waarschijnlijk moet nog ergens de Jaccard, maar ik weet nog niet precies waar
                foreach (string q in querys.Keys)
                {
                    Console.Write(q);
                    SQLiteCommand c_idf = Preprocessor.cat_columns.Contains(q) ?
                        new SQLiteCommand("select * from idf_cat where category = \'" + q + "\' AND value = \'" + querys[q] + "\'", Preprocessor.meta_db) :
                        new SQLiteCommand("select * from idf_num where category = \'" + q + "\' AND value = \'" + querys[q] + "\'", Preprocessor.meta_db);
                    SQLiteCommand c_qf =
                        new SQLiteCommand("select * from queryfrequency where category = \'" + q + "\' AND value= \'" + querys[q] + "\'", Preprocessor.meta_db);
                    SQLiteDataReader r_idf = c_idf.ExecuteReader();
                    SQLiteDataReader r_qf = c_qf.ExecuteReader();

                    if (r_idf.Read() && r_qf.Read())
                    {

                        score += ComputeIDFSimilarity(q,querys[q],tuple[q],Convert.ToDouble(r_idf["score"])) *
                            ComputeQFSimilarity(q,querys[q],tuple[q], Convert.ToDouble(r_qf["score"])); 
                            
                    }
                    else
                    {
                        if (Preprocessor.cat_columns.Contains(q))
                            score += 0;
                        else
                            score += Preprocessor.IDF(q, querys[q], Preprocessor.m_dbConnection);; // bereken idf online
                    }

                }


            }

           return score;
        }

        private static double ComputeQFSimilarity(string category, string query_value, object db_value, double score)
        {
            if (Preprocessor.cat_columns.Contains(category))
                return query_value.Equals((string)db_value) ? score : 0;
            else
                return Convert.ToDouble(query_value) == Convert.ToDouble(db_value) ? score : 0;
        }

        private static double ComputeIDFSimilarity(string category, string query_value, object db_value, double score)
        {
            if (Preprocessor.cat_columns.Contains(category))
                return query_value.Equals((string)db_value) ? score : 0;
            else
            {
                double h = Preprocessor.ObtainH(category);
                return
                    Math.Pow(Math.E, -0.5 * Math.Pow((Convert.ToDouble(db_value) - Convert.ToDouble(query_value)) / h, 2)) * score;
            }
                
        }
        
    }

    struct TopKEntry : IComparable
    {
        public SQLiteDataReader entry;
        public double score;
        public TopKEntry(SQLiteDataReader entry, double score)
        {
            this.entry = entry;
            this.score = score;
        }

        public double GlobalImportance()
        {
            double score = 0;
            SQLiteCommand c;
            SQLiteDataReader r;
            foreach(string s in Preprocessor.cat_columns) {
                c = new SQLiteCommand("select glob_import from queryfrequency where category = \'"+s+"\' AND value = \'"+(string)entry[s]+"\'", Preprocessor.meta_db);
                Console.WriteLine("select glob_import from queryfrequency where category = \'" + s + "\' AND value = \'" + (string)entry[s] + "\'");
                r = c.ExecuteReader();
                r.Read();
                score += Convert.ToDouble(r["glob_import"]);
            }
            foreach(string s in Preprocessor.num_columns) {
                c = new SQLiteCommand("select glob_import from queryfrequency where category = \'"+s+"\' AND value = \'"+entry[s]+"\'", Preprocessor.meta_db);
                Console.WriteLine("select glob_import from queryfrequency where category = \'" + s + "\' AND value = \'" + entry[s] + "\'");

                r = c.ExecuteReader();
                r.Read();
                score += Convert.ToDouble(r["glob_import"]);

            }

            return score;

        }

        public int CompareTo(object obj)
        {
            if (obj == null) return 1;

            TopKEntry other = (TopKEntry)obj;
            // draai om, zodat hoogste score vooraan staat
            if (other.score.CompareTo(this.score) != 0)
                return other.score.CompareTo(this.score);
            else
                return this.GlobalImportance().CompareTo(other.GlobalImportance());
           


        }


    }



}

