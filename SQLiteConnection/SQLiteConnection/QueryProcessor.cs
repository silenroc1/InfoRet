﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SQLite;

namespace InformationRetrieval
{
    class QueryHandler
    {
        private static SQLiteConnection m_dbConnection = Preprocessor.m_dbConnection;
        private static ISet<int> seen_ids = new HashSet<int>();
        private static SortedList<double,int> topkBuffer;

        private static SQLiteDataReader TupleLookup(int TID){
            SQLiteCommand command = new SQLiteCommand("select * from autompg WHERE id = \'" + TID + "\'", m_dbConnection);
            SQLiteDataReader reader = command.ExecuteReader();
            reader.Read();
            return reader;
        }
     

        private static int IndexLookupGetNextTID(Object Lk){
            return 0;
        }

        public static void IndexbasedThresholdAlgorithm(string[] terms) 
        {
            topkBuffer = new SortedList<double,int>();

            
            while(seen_ids.Count == Preprocessor.db_size){

                foreach(string s in Preprocessor.num_columns) {
                    int nextId = IndexLookupGetNextTID(s);
                    seen_ids.Add(nextId);


                    SQLiteDataReader tuple = TupleLookup(nextId);


                    double score = ComputeScore(tuple);

                    if (score > topkBuffer.Keys.Min())
                    {
                        topkBuffer.Remove(topkBuffer.Keys.Min());
                        topkBuffer.Add(score, nextId);

                    }

                    if (HypothethicalMax() > topkBuffer.Keys.Min())
                    {
                        //Kap ermee


                    }
                }
            }         
        }

        private static int HypothethicalMax()
        {
            throw new NotImplementedException();
        }

        private static double ComputeScore(SQLiteDataReader tuple)
        {
            double score = 0.0;
            return score;
        }

        
    }



}

