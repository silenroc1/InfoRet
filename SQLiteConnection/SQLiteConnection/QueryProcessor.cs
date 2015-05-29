﻿﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SQLite;
using System.Collections.Specialized.OrderedDictionary;

namespace InformationRetrieval
{
    class QueryHandler
    {
        private static SQLiteConnection m_dbConnection = Preprocessor.m_dbConnection;
        private static ISet<int> seen_ids = new HashSet<int>();
        private static SortedList<double,int> topkBuffer;


        private static SortedDictionary<int, SQLiteDataReader> ITA(string[] query, int K) {
            HashSet<int> seen = new HashSet<int>();
            SortedDictionary<Int32, SQLiteDataReader> buffer = new SortedDictionary<Int32, SQLiteDataReader>();
            int p = 0; //= amount of matches in the database with the query

            while(seen.Count < Preprocessor.db_size){
                foreach(string q  in query){

                    SQLiteCommand command = new SQLiteCommand("select category from table where category = " + q, m_dbConnection);
                    SQLiteDataReader reader = command.ExecuteReader();
                    for (int k = 0; reader.HasRows; k++) {
                        //Lk is the the location in the ordering that is given for the query
                        int TIDk = IndexLookupGetNextTID(reader, k);
                        if (TIDk > 0) {
                            SQLiteDataReader Tk = TupleLookup(TIDk);

                            int score = 0;//score function
                            if (buffer.Count < K) {
                                buffer.Add(score, Tk);
                            }
                            else if (score > buffer.Keys.Last()) {
                                buffer.Add(score, Tk);
                                if (buffer.Count > K) {
                                    buffer.Remove(buffer.Keys.Last());
                                }
                            }


                            if (stoppingCondition(Tk, buffer.Keys.Last())) {
                                return buffer;
                            }
                        }
                    }
                }
            }
            return buffer;
            /*
             ITA: Index-based Threshold Algorithm
                Initialize Top-K buffer to empty
                REPEAT
                    FOR EACH k = 1 TO p DO
                        1. TIDk = IndexLookupGetNextTID(Lk)
                        2. Tk = TupleLookup(TIDi)
                        3. Compute value of ranking function for Tk
                        4. If rank of Tk is higher than the lowest ranking tuple in the Top-K buffer
                            then update Top-K buffer
                        5. If stopping condition has been reached then EXIT
                    END FOR
                UNTIL indexLookupGetNextTID(L1) …
                      indexLookupGetNextTID(Lp)
                are all completed 
             */

        }


        private static SQLiteDataReader TupleLookup(int TID){
            SQLiteCommand command = new SQLiteCommand("select * from autompg WHERE id = \'" + TID + "\'", m_dbConnection);
            SQLiteDataReader reader = command.ExecuteReader();
            reader.Read();
            return reader;
        }

        private static bool stoppingCondition(SQLiteDataReader Tk, int lowestScore) {
            //Compute the highest possible score from what needs to be check
            //and compare that to the lowest score in the top-K
            
            return true;
        }

        //returnt de gevraagde TID als deze nog niet eerder is opgevraagd, anders -1
        private static int IndexLookupGetNextTID(SQLiteDataReader reader, int k){
            int TID = reader.GetInt32(k);
            if(seen_ids.Contains(TID)){
                return -1;
            }
            seen_ids.Add(TID);
            return TID;
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

