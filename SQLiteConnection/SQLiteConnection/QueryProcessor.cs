﻿using System;
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

            SQLiteCommand command = new SQLiteCommand("select * from autompg ORDER BY ", m_dbConnection);

            while(true){//there is a next Lk
                for (int k = 1; k < p;k++ ) {
                    //Lk is the the location in the ordering that is given for the query
                    int TIDk = IndexLookupGetNextTID(9);
                    SQLiteDataReader Tk = TupleLookup(TIDk);

                    int score = 0;//score function
                    if (buffer.Count < K) {
                        buffer.Add(score, Tk);
                    }
                    else if(score > buffer.Keys.Last()) {
                        buffer.Add(score, Tk);
                        if (buffer.Count > K) {
                            buffer.Remove(buffer.Keys.Last());
                        }
                    }


                    if(stoppingCondition(Tk, buffer.Keys.Last())){
                        return buffer;
                    }
                }
            }
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


        private static int IndexLookupGetNextTID(Object Lk){
            //gegeven een orderning op de database (op een kolom), pak de ID van de volgende entry

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

        private static double ComputeScore(SQLiteDataReader tuple, string function)
        {
            // Overlap (sectie 6.2.1)
            if(function.Equals("overlap"))
            {

            }

            // IDF (sectie 3)
            else if (function.Equals("idf"))
            {


            }
                // QFIDF (sectie 4)
            else if (function.Equals("qfidf"))
            {



            }


            




            double score = 0.0;
            return score;
        }

        
    }



}

