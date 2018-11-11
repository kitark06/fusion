# Project Fusion

The main objective of Project Fusion is to efficiently and effectively join streams of data in real-time. The main challenge in any streaming project is to handle problems like out-of-order and delayed message delivery. At the same time, with disparate systems handling data influx, it is vital to know the precise moment when all data [spread across multiple schemas] have been inserted onto the storage system.

By keeping a track of documents inserted by various components/streams , it guarantees us data completeness & eliminates the need to fire any unnecessary queries to check if data is present.
<p>
If we have n streams, a brute force implementation would mean we fire n*n queries on the database & only when the n-th/last bit of data is inserted will the data be marked as complete and viable for join.
So out of the total <b>n*(n-1)</b> queries are fired, only (n-1) would actually bring the complete info once all the data is inserted. The rest (n-1)^2 are wasted. 
<p>
This project addresses and provides the solution to this problem by performing stateful stream processing & delivering atleast-once semantics. reducing the number of queries fired from <b>O(N^2- N) to O(N)</b>
<p>

FusionCore is the brains of the project and has the code and the DAG for the pipeline which is responsible for the entire functionality.

StreamingCore is a supporting class with it being responsible for generating a steady stream of input from the input files.

A more Detailed Explanation of this project can be found at 
http://www.kartikiyer.com/2018/11/12/data-enrichment-designing-optimizing-a-real-time-stream-joining-pipeline/
